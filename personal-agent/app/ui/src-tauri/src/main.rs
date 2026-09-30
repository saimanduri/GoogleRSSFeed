// pa-ui: the single Personal Agent window (Tauri 2 + WebView2).
//
// Security notes (spec 2.2, 2.4, 39.1-39.3):
// - holds no long-term secrets; the only credential is the per-launch UI token read from the gateway's
//   rendezvous file in the ACL-protected data folder
// - talks ONLY to pa-gateway over a named pipe (no TCP, no localhost web server)
// - the pipe is opened with SECURITY_IDENTIFICATION QoS and the server PID is checked (anti-squatting)
// - the web view cannot navigate away from the app; external links open in the default browser only
//   via the opener plugin after the UI's click-through confirmation
// - devtools are not compiled into release builds
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tauri::menu::{Menu, MenuItem, PredefinedMenuItem};
use tauri::tray::{MouseButton, TrayIconBuilder, TrayIconEvent};
use tauri::{AppHandle, Emitter, Manager, State, WebviewUrl, WebviewWindowBuilder, WindowEvent};
use tauri_plugin_global_shortcut::{GlobalShortcutExt, ShortcutState};
use tauri_plugin_notification::NotificationExt;
use tokio::sync::{oneshot, Mutex};

type Writer = Box<dyn tokio::io::AsyncWrite + Send + Unpin>;

#[derive(Default)]
struct Gateway {
    writer: Mutex<Option<Writer>>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Value>>>,
    next_id: AtomicU64,
    connected: AtomicBool,
}

fn frame(msg: &Value) -> Vec<u8> {
    let body = serde_json::to_vec(msg).unwrap_or_default();
    let mut out = (body.len() as u32).to_le_bytes().to_vec();
    out.extend_from_slice(&body);
    out
}

async fn send(gw: &Gateway, msg: &Value) -> Result<(), String> {
    use tokio::io::AsyncWriteExt;
    let mut guard = gw.writer.lock().await;
    let w = guard.as_mut().ok_or_else(|| "disconnected".to_string())?;
    w.write_all(&frame(msg)).await.map_err(|e| e.to_string())?;
    w.flush().await.map_err(|e| e.to_string())
}

#[tauri::command]
async fn rpc(method: String, params: Option<Value>, gw: State<'_, Arc<Gateway>>) -> Result<Value, Value> {
    let id = gw.next_id.fetch_add(1, Ordering::SeqCst) + 1;
    let (tx, rx) = oneshot::channel();
    gw.pending.lock().await.insert(id, tx);
    let msg = json!({"type": "req", "id": id, "method": method, "params": params.unwrap_or(json!({}))});
    if let Err(e) = send(&gw, &msg).await {
        gw.pending.lock().await.remove(&id);
        return Err(json!({"code": "disconnected", "message": format!("gateway not connected: {e}"), "details": {}}));
    }
    match tokio::time::timeout(Duration::from_secs(900), rx).await {
        Ok(Ok(res)) => {
            if res.get("ok").and_then(Value::as_bool) == Some(true) {
                Ok(res.get("result").cloned().unwrap_or(Value::Null))
            } else {
                Err(res.get("error").cloned().unwrap_or(json!({"code": "error", "message": "unknown error"})))
            }
        }
        _ => {
            gw.pending.lock().await.remove(&id);
            Err(json!({"code": "timeout", "message": "the gateway did not answer in time", "details": {}}))
        }
    }
}

#[tauri::command]
fn gateway_connected(gw: State<'_, Arc<Gateway>>) -> bool {
    gw.connected.load(Ordering::SeqCst)
}

/// Screens that show secrets are excluded from screenshots / screen sharing (spec 4.7).
#[tauri::command]
fn set_capture_protection(window: tauri::WebviewWindow, enabled: bool) -> Result<(), String> {
    #[cfg(windows)]
    {
        #[link(name = "user32")]
        extern "system" {
            fn SetWindowDisplayAffinity(hwnd: isize, affinity: u32) -> i32;
        }
        const WDA_NONE: u32 = 0x0;
        const WDA_EXCLUDEFROMCAPTURE: u32 = 0x11;
        let hwnd = window.hwnd().map_err(|e| e.to_string())?;
        let ok = unsafe { SetWindowDisplayAffinity(hwnd.0 as isize, if enabled { WDA_EXCLUDEFROMCAPTURE } else { WDA_NONE }) };
        if ok == 0 {
            return Err("SetWindowDisplayAffinity failed".into());
        }
    }
    let _ = (window, enabled);
    Ok(())
}

#[tauri::command]
fn set_stop_hotkey(app: AppHandle, accelerator: String) -> Result<(), String> {
    let gs = app.global_shortcut();
    let _ = gs.unregister_all();
    gs.register(accelerator.as_str()).map_err(|e| e.to_string())
}

#[tauri::command]
fn quit_app(app: AppHandle) {
    app.exit(0);
}

async fn call(app: &AppHandle, method: &str, params: Value) -> Result<Value, Value> {
    let gw = app.state::<Arc<Gateway>>();
    rpc(method.to_string(), Some(params), gw).await
}

fn show_main(app: &AppHandle) {
    if let Some(w) = app.get_webview_window("main") {
        let _ = w.show();
        let _ = w.unminimize();
        let _ = w.set_focus();
    }
}

// ------------------------------------------------------------------------------------ gateway link
#[cfg(windows)]
mod link {
    use super::*;
    use std::os::windows::io::AsRawHandle;
    use tokio::io::AsyncReadExt;
    use tokio::net::windows::named_pipe::ClientOptions;

    #[link(name = "kernel32")]
    extern "system" {
        fn GetNamedPipeServerProcessId(pipe: isize, pid: *mut u32) -> i32;
    }
    const SECURITY_IDENTIFICATION: u32 = 0x0001_0000;

    fn rendezvous() -> Option<Value> {
        let base = std::env::var("PA_DATA_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::path::PathBuf::from(std::env::var("LOCALAPPDATA").unwrap_or_default()).join("PersonalAgent"));
        let text = std::fs::read_to_string(base.join("run").join("gateway.json")).ok()?;
        serde_json::from_str(&text).ok()
    }

    fn start_gateway() {
        // Release: pa-gateway.exe next to pa-ui.exe (normally already started by the logon task).
        // Developer mode: PA_GATEWAY_CMD, e.g. "python -m pa_gateway" run from app/.
        let exe_dir = std::env::current_exe().ok().and_then(|p| p.parent().map(|p| p.to_path_buf()));
        if let Ok(cmd) = std::env::var("PA_GATEWAY_CMD") {
            let mut parts = cmd.split_whitespace();
            if let Some(prog) = parts.next() {
                let _ = std::process::Command::new(prog).args(parts).spawn();
            }
        } else if let Some(dir) = exe_dir {
            let gw = dir.join("pa-gateway.exe");
            if gw.exists() {
                let _ = std::process::Command::new(gw).spawn();
            }
        }
    }

    pub async fn run(app: AppHandle) {
        let mut last_start = std::time::Instant::now() - Duration::from_secs(60);
        loop {
            match connect_once(&app).await {
                Ok(()) => {}
                Err(e) => {
                    let _ = app.emit("gw-status", json!({"connected": false, "error": e}));
                    if e.contains("rendezvous") && last_start.elapsed() > Duration::from_secs(30) {
                        start_gateway();
                        last_start = std::time::Instant::now();
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(800)).await;
        }
    }

    async fn connect_once(app: &AppHandle) -> Result<(), String> {
        let rv = rendezvous().ok_or_else(|| "rendezvous missing (gateway not running)".to_string())?;
        let pipe = rv["pipe"].as_str().ok_or("bad rendezvous")?.to_string();
        let token = rv["ui_token"].as_str().ok_or("bad rendezvous")?.to_string();
        let expected_pid = rv["pid"].as_u64().unwrap_or(0) as u32;
        let client = ClientOptions::new()
            .security_qos_flags(SECURITY_IDENTIFICATION)
            .open(&pipe)
            .map_err(|e| format!("pipe open failed: {e}"))?;
        let mut server_pid: u32 = 0;
        let ok = unsafe { GetNamedPipeServerProcessId(client.as_raw_handle() as isize, &mut server_pid) };
        if ok == 0 || server_pid != expected_pid {
            return Err("pipe server is not the gateway (possible squatting) - refusing".into());
        }
        let (mut reader, writer) = tokio::io::split(client);
        let gw = app.state::<Arc<Gateway>>().inner().clone();
        *gw.writer.lock().await = Some(Box::new(writer));
        send(&gw, &json!({"type": "hello", "role": "ui", "token": token, "protocol": 1})).await?;
        let hello = read_frame(&mut reader).await?;
        if hello.get("ok").and_then(Value::as_bool) != Some(true) {
            *gw.writer.lock().await = None;
            return Err("gateway rejected this window".into());
        }
        gw.connected.store(true, Ordering::SeqCst);
        let _ = app.emit("gw-status", json!({"connected": true}));
        let result = loop {
            let msg = match read_frame(&mut reader).await {
                Ok(m) => m,
                Err(e) => break Err(e),
            };
            match msg.get("type").and_then(Value::as_str) {
                Some("res") => {
                    if let Some(id) = msg.get("id").and_then(Value::as_u64) {
                        if let Some(tx) = gw.pending.lock().await.remove(&id) {
                            let _ = tx.send(msg);
                        }
                    }
                }
                Some("evt") => {
                    if msg.get("topic").and_then(Value::as_str) == Some("notify") {
                        toast(app, &msg["data"]);
                    }
                    let _ = app.emit("gw-event", msg);
                }
                _ => {}
            }
        };
        gw.connected.store(false, Ordering::SeqCst);
        *gw.writer.lock().await = None;
        let mut pending = gw.pending.lock().await;
        for (_, tx) in pending.drain() {
            let _ = tx.send(json!({"ok": false, "error": {"code": "disconnected", "message": "gateway disconnected"}}));
        }
        result
    }

    async fn read_frame<R: tokio::io::AsyncRead + Unpin>(r: &mut R) -> Result<Value, String> {
        let mut len = [0u8; 4];
        r.read_exact(&mut len).await.map_err(|e| e.to_string())?;
        let n = u32::from_le_bytes(len) as usize;
        if n > 16 * 1024 * 1024 {
            return Err("frame too large".into());
        }
        let mut buf = vec![0u8; n];
        r.read_exact(&mut buf).await.map_err(|e| e.to_string())?;
        serde_json::from_slice(&buf).map_err(|e| e.to_string())
    }

    fn toast(app: &AppHandle, data: &Value) {
        // Content was already reduced by the gateway (content level, never CONFIDENTIAL). No actions on toasts:
        // clicking only opens the app, which requires unlock (spec 26).
        if data.get("toast").and_then(Value::as_bool) != Some(true) {
            return;
        }
        let title = data.get("title").and_then(Value::as_str).unwrap_or("Personal Agent");
        let mut b = app.notification().builder().title(title);
        if let Some(body) = data.get("body").and_then(Value::as_str) {
            b = b.body(body);
        }
        let _ = b.show();
    }
}

#[cfg(not(windows))]
mod link {
    use super::*;
    pub async fn run(app: AppHandle) {
        let _ = app.emit("gw-status", json!({"connected": false, "error": "Personal Agent runs on Windows"}));
    }
}

// ------------------------------------------------------------------------------------ main
fn main() {
    let start_hidden = std::env::args().any(|a| a == "--tray");
    tauri::Builder::default()
        .plugin(tauri_plugin_single_instance::init(|app, _args, _cwd| show_main(app)))
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_notification::init())
        .plugin(tauri_plugin_opener::init())
        .plugin(
            tauri_plugin_global_shortcut::Builder::new()
                .with_handler(|app, _shortcut, event| {
                    if event.state() == ShortcutState::Pressed {
                        let app = app.clone();
                        tauri::async_runtime::spawn(async move {
                            let _ = call(&app, "killswitch.activate", json!({"level": "stop_all", "source": "hotkey"})).await;
                            let _ = app.emit("gw-local", json!({"kind": "stop_all_hotkey"}));
                        });
                    }
                })
                .build(),
        )
        .manage(Arc::new(Gateway::default()))
        .invoke_handler(tauri::generate_handler![rpc, gateway_connected, set_capture_protection, set_stop_hotkey, quit_app])
        .setup(move |app| {
            let handle = app.handle().clone();
            // The single window. It may only show the bundled app; any other navigation is refused.
            let win = WebviewWindowBuilder::new(app, "main", WebviewUrl::App("index.html".into()))
                .title("Personal Agent")
                .inner_size(1280.0, 820.0)
                .min_inner_size(900.0, 600.0)
                .center()
                .visible(!start_hidden)
                .on_navigation(|url| {
                    let host = url.host_str().unwrap_or("");
                    url.scheme() == "tauri" || host == "tauri.localhost" || (cfg!(debug_assertions) && host == "localhost")
                })
                .build()?;
            let w2 = win.clone();
            win.on_window_event(move |e| {
                if let WindowEvent::CloseRequested { api, .. } = e {
                    // closing the window keeps the agent running in the tray (spec 16.2)
                    api.prevent_close();
                    let _ = w2.hide();
                }
            });
            // tray: Open / Pause agent / Stop everything / Lock / Quit window
            let open = MenuItem::with_id(app, "open", "Open", true, None::<&str>)?;
            let pause = MenuItem::with_id(app, "pause", "Pause agent", true, None::<&str>)?;
            let stop = MenuItem::with_id(app, "stop", "Stop everything", true, None::<&str>)?;
            let lock = MenuItem::with_id(app, "lock", "Lock", true, None::<&str>)?;
            let quit = MenuItem::with_id(app, "quit", "Close window (agent keeps running)", true, None::<&str>)?;
            let sep = PredefinedMenuItem::separator(app)?;
            let menu = Menu::with_items(app, &[&open, &pause, &stop, &lock, &sep, &quit])?;
            let mut tray = TrayIconBuilder::with_id("main").tooltip("Personal Agent").menu(&menu).show_menu_on_left_click(false);
            if let Some(icon) = app.default_window_icon() {
                tray = tray.icon(icon.clone());
            }
            tray.on_menu_event(|app, event| {
                let app = app.clone();
                let id = event.id().as_ref().to_string();
                tauri::async_runtime::spawn(async move {
                    match id.as_str() {
                        "open" => show_main(&app),
                        "pause" => { let _ = call(&app, "killswitch.activate", json!({"level": "pause_agent", "source": "tray"})).await; }
                        "stop" => { let _ = call(&app, "killswitch.activate", json!({"level": "stop_all", "source": "tray"})).await; }
                        "lock" => { let _ = call(&app, "auth.lock", json!({"reason": "tray"})).await; }
                        "quit" => app.exit(0),
                        _ => {}
                    }
                });
            })
            .on_tray_icon_event(|tray, event| {
                if let TrayIconEvent::DoubleClick { button: MouseButton::Left, .. } = event {
                    show_main(tray.app_handle());
                }
            })
            .build(app)?;
            let _ = app.global_shortcut().register("ctrl+alt+shift+s");
            tauri::async_runtime::spawn(link::run(handle));
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running Personal Agent");
}
