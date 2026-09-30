import { useEffect, useMemo, useState } from "react";
import { native } from "./api/gateway";
import { Screen, useApp } from "./app";
import { Icon } from "./components/Icon";
import { Button, Modal } from "./components/ui";
import { Activity } from "./screens/Activity";
import { Approvals } from "./screens/Approvals";
import { Chat } from "./screens/Chat";
import { Files } from "./screens/Files";
import { Home } from "./screens/Home";
import { Memory } from "./screens/Memory";
import { Missions } from "./screens/Missions";
import { Reminders } from "./screens/Reminders";
import { Secrets } from "./screens/Secrets";
import { Tasks } from "./screens/Tasks";
import { Settings } from "./screens/settings/Settings";

const NAV: [Screen, string, string][] = [
  ["home", "Home", "home"], ["chat", "Chat", "chat"], ["missions", "Missions & Routines", "missions"], ["reminders", "Reminders", "reminders"],
  ["tasks", "Tasks", "tasks"], ["approvals", "Approvals", "approvals"], ["files", "My Files", "files"], ["memory", "Memory", "memory"],
  ["secrets", "Secrets", "secrets"], ["activity", "Activity", "activity"],
];

export function Shell() {
  const { status, route, go, call, toast, connected } = useApp();
  const [collapsed, setCollapsed] = useState(false);
  const [palette, setPalette] = useState(false);
  const [stopMenu, setStopMenu] = useState(false);
  const ks = status?.killswitch;

  useEffect(() => {
    const k = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "k") { e.preventDefault(); setPalette(true); }
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "l" && e.shiftKey) { e.preventDefault(); void call("auth.lock"); }
    };
    window.addEventListener("keydown", k);
    return () => window.removeEventListener("keydown", k);
  }, [call]);

  useEffect(() => {
    const hk = status?.ui?.["emergency.hotkey"];
    if (hk) void native("set_stop_hotkey", { accelerator: String(hk).toLowerCase().replace(/\s/g, "") }).catch(() => undefined);
  }, [status?.ui]);

  const stopAll = async (level = "stop_all") => {
    await call("killswitch.activate", { level, source: "ui" });
    toast(level === "stop_all" ? "STOP ALL activated - everything halted" : `Emergency stop: ${level.replace(/_/g, " ")}`, "danger");
    setStopMenu(false);
  };

  const running = status?.tasks_running ?? 0;
  const pill = ks?.any ? { dot: "danger", text: "Stopped (emergency stop)" } : !connected ? { dot: "warn", text: "Service disconnected" }
    : running ? { dot: "pulse", text: `Running · ${running} task${running === 1 ? "" : "s"}` } : { dot: "", text: "Idle · ready" };

  const page = useMemo(() => {
    switch (route.screen) {
      case "home": return <Home />;
      case "chat": return <Chat />;
      case "missions": return <Missions />;
      case "reminders": return <Reminders />;
      case "tasks": return <Tasks />;
      case "approvals": return <Approvals />;
      case "files": return <Files />;
      case "memory": return <Memory />;
      case "secrets": return <Secrets />;
      case "activity": return <Activity />;
      case "settings": return <Settings />;
    }
  }, [route.screen]);

  return (
    <div className={`shell ${collapsed ? "collapsed" : ""}`}>
      <header className="topbar">
        <Button kind="ghost" icon="menu" title="Collapse navigation" onClick={() => setCollapsed(!collapsed)} />
        <div className="brand"><div className="brand-logo"><Icon name="shield" size={15} /></div>Personal Agent</div>
        {status?.dev_mode && <span className="badge warn" title="Developer mode - never use with real data">DEV MODE</span>}
        <div className="spacer" />
        <button className="status-pill" onClick={() => go(ks?.any ? "settings" : "tasks", ks?.any ? { section: "emergency" } : undefined)} style={{ cursor: "pointer" }}>
          <span className={`dot ${pill.dot}`} />{pill.text}
        </button>
        <Button kind="ghost" icon="search" title="Search / commands (Ctrl+K)" onClick={() => setPalette(true)} />
        <Button icon="lock" onClick={() => call("auth.lock")} title="Lock (Ctrl+Shift+L)">Lock</Button>
        <div style={{ position: "relative" }}>
          <button className={`stop-all ${ks?.any ? "active" : ""}`} onClick={() => setStopMenu(true)} title="Emergency stop (Ctrl+Alt+Shift+S)">STOP ALL</button>
        </div>
      </header>
      <nav className="nav" aria-label="Main">
        {NAV.map(([id, label, icon]) => (
          <button key={id} className={`nav-item ${route.screen === id ? "active" : ""}`} onClick={() => go(id)} title={label}>
            <span className="icon"><Icon name={icon} /></span><span className="nav-label">{label}</span>
            {id === "approvals" && status?.approvals_pending > 0 && <span className="badge-count">{status.approvals_pending}</span>}
          </button>
        ))}
        <div className="spacer" />
        <div className="nav-sep" />
        <button className={`nav-item ${route.screen === "settings" ? "active" : ""}`} onClick={() => go("settings")} title="Settings">
          <span className="icon"><Icon name="settings" /></span><span className="nav-label">Settings</span>
        </button>
        <div className="nav-user"><div className="avatar">{(status?.username ?? "?").slice(0, 1).toUpperCase()}</div>
          <span className="ellipsis">user: {status?.username} <span className="dot" style={{ display: "inline-block", marginLeft: 4 }} /></span></div>
      </nav>
      <main className="main">{page}</main>
      {palette && <CommandPalette onClose={() => setPalette(false)} onStop={() => stopAll()} />}
      {stopMenu && (
        <Modal title="Emergency stop" onClose={() => setStopMenu(false)}>
          <p className="muted">Stops take effect within 2 seconds. Queued work is held, not deleted. Releasing needs your password (Settings &gt; Emergency Stop).</p>
          <div className="col">
            <button className="stop-all" style={{ padding: 14, fontSize: "1.05em" }} onClick={() => stopAll("stop_all")}>STOP ALL - halt everything now</button>
            <div className="grid-2">
              {[["pause_agent", "Pause agent"], ["stop_tasks", "Stop all tasks"], ["disable_connectors", "Disable all connectors"],
                ["disable_web", "Disable web access"], ["disable_sandbox", "Disable Python sandbox"]].map(([lvl, label]) => (
                <Button key={lvl} onClick={() => stopAll(lvl)}>{label}{ks?.levels?.[lvl] ? " ✓" : ""}</Button>
              ))}
            </div>
          </div>
        </Modal>
      )}
    </div>
  );
}

function CommandPalette({ onClose, onStop }: { onClose: () => void; onStop: () => void }) {
  const { go, call } = useApp();
  const [q, setQ] = useState("");
  const [sel, setSel] = useState(0);
  const [hits, setHits] = useState<any[]>([]);
  const items = useMemo(() => {
    const base = [
      ...NAV.map(([id, label, icon]) => ({ label: `Go to ${label}`, icon, run: () => go(id) })),
      { label: "New chat", icon: "plus", run: () => go("chat", { new: Date.now() }) },
      { label: "New routine", icon: "missions", run: () => go("missions", { create: Date.now() }) },
      { label: "Settings", icon: "settings", run: () => go("settings") },
      { label: "Security posture", icon: "shield", run: () => go("settings", { section: "account", tab: "posture" }) },
      { label: "Lock now", icon: "lock", run: () => call("auth.lock") },
      { label: "STOP ALL", icon: "alert", run: onStop },
    ];
    const f = base.filter((i) => i.label.toLowerCase().includes(q.toLowerCase()));
    return [...f, ...hits.map((h) => ({ label: `${h.kind}: ${h.title} - ${String(h.snippet).replace(/\s+/g, " ").slice(0, 80)}`, icon: "search",
      run: () => go(h.kind === "file" ? "files" : h.kind === "chat" ? "chat" : "activity", { ref: h.ref_id }) }))];
  }, [q, hits, go, call, onStop]);
  useEffect(() => {
    if (q.length < 3) { setHits([]); return; }
    const t = setTimeout(() => call<any[]>("history.search", { query: q, limit: 8 }).then(setHits).catch(() => setHits([])), 200);
    return () => clearTimeout(t);
  }, [q, call]);
  return (
    <div className="overlay" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
      <div className="palette" role="dialog" aria-label="Command palette">
        <input autoFocus placeholder="Type a command or search your history..." value={q} onChange={(e) => { setQ(e.target.value); setSel(0); }}
          onKeyDown={(e) => {
            if (e.key === "Escape") onClose();
            if (e.key === "ArrowDown") setSel((s) => Math.min(items.length - 1, s + 1));
            if (e.key === "ArrowUp") setSel((s) => Math.max(0, s - 1));
            if (e.key === "Enter" && items[sel]) { items[sel].run(); onClose(); }
          }} />
        <div style={{ maxHeight: 380, overflowY: "auto" }}>
          {items.map((it, i) => (
            <div key={i} className={`palette-item ${i === sel ? "active" : ""}`} onMouseEnter={() => setSel(i)} onClick={() => { it.run(); onClose(); }}>
              <Icon name={it.icon} /><span className="ellipsis">{it.label}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
