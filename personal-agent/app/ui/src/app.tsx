// App-wide state: session status, navigation, toasts and the `call()` helper that handles
// step-up (PIN/password re-authentication) and password confirmations transparently.
import { createContext, ReactNode, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { ApiError, onEvent, rpc } from "./api/gateway";
import { fontStack, sizeFactor } from "./fonts";
import { Button, Field, Modal } from "./components/ui";
import { useExitGhost } from "./components/motion";

export type Screen = "home" | "chat" | "missions" | "reminders" | "tasks" | "approvals" | "files" | "memory" | "secrets" | "activity" | "history" | "guide" | "outlook" | "settings";
export type Route = { screen: Screen; params?: Record<string, any> };
type ToastOpts = { action?: { label: string; run: () => void }; ms?: number };
type Toast = { id: number; text: string; kind?: "ok" | "danger" | "warn" | "info"; onClick?: () => void; action?: ToastOpts["action"] };
type DeferredDelete = { key: string; label: string; commit: () => Promise<unknown>; after?: () => void };

const SCREENS: Screen[] = ["home", "chat", "missions", "reminders", "tasks", "approvals", "files", "memory", "secrets", "activity", "history", "guide", "outlook", "settings"];
const LAST_SCREEN = "pa.lastScreen";
const UNDO_MS = 8000;
function savedScreen(): Screen {
  try { const s = localStorage.getItem(LAST_SCREEN) as Screen | null; if (s && SCREENS.includes(s)) return s; } catch { /* storage unavailable */ }
  return "home";
}

type Ctx = {
  status: any;
  refresh: () => Promise<void>;
  route: Route;
  go: (screen: Screen, params?: Record<string, any>) => void;
  toast: (text: string, kind?: Toast["kind"], onClick?: () => void, opts?: ToastOpts) => void;
  /** Hide an item now, show an Undo toast, and really delete after a few seconds (or immediately on lock/close). */
  deferDelete: (d: DeferredDelete) => void;
  isHidden: (key: string) => boolean;
  call: <T = any>(method: string, params?: Record<string, unknown>) => Promise<T>;
  confirmPassword: (reason: string) => Promise<string | null>;
  connected: boolean;
};

const AppCtx = createContext<Ctx>(null as any);
export const useApp = () => useContext(AppCtx);

const CATEGORY_TEXT: Record<string, string> = {
  secrets: "reveal or change a stored secret", export: "export data", security_settings: "change a security setting",
  approvals_high: "approve a high-risk action", connectors: "connect or disconnect a connector", backup_restore: "restore a backup",
  transcripts: "view full transcripts", skills: "activate a skill", updates: "install an update",
};

export function AppProvider({ children }: { children: ReactNode }) {
  const [status, setStatus] = useState<any>(null);
  const [route, setRoute] = useState<Route>(() => ({ screen: savedScreen() }));
  const [toasts, setToasts] = useState<Toast[]>([]);
  const [stepUp, setStepUp] = useState<null | { category: string; resolve: (ok: boolean) => void }>(null);
  const [pwAsk, setPwAsk] = useState<null | { reason: string; resolve: (pw: string | null) => void }>(null);
  const [connected, setConnected] = useState(true);
  const [hidden, setHidden] = useState<Set<string>>(new Set());
  const pending = useRef(new Map<string, { d: DeferredDelete; timer: number }>());
  const lastTouch = useRef(0);

  const refresh = useCallback(async () => {
    try {
      setStatus(await rpc("session.status"));
      setConnected(true);
    } catch (e: any) {
      if (e.code === "disconnected") setConnected(false);
    }
  }, []);

  const toast = useCallback((text: string, kind?: Toast["kind"], onClick?: () => void, opts?: ToastOpts) => {
    const id = Date.now() + Math.random();
    setToasts((t) => [...t.slice(-4), { id, text, kind, onClick, action: opts?.action }]);
    setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), opts?.ms ?? 6000);
  }, []);

  const unhide = useCallback((key: string) => setHidden((h) => { const n = new Set(h); n.delete(key); return n; }), []);
  const commitDeferred = useCallback(async (key: string) => {
    const p = pending.current.get(key);
    if (!p) return;
    pending.current.delete(key);
    window.clearTimeout(p.timer);
    try { await p.d.commit(); } catch (e: any) { toast(`Could not delete: ${e?.message ?? "error"} - it was kept`, "danger"); }
    unhide(key);
    p.d.after?.();
  }, [toast, unhide]);
  const deferDelete = useCallback((d: DeferredDelete) => {
    void commitDeferred(d.key); // a second delete of the same thing commits the first
    setHidden((h) => new Set(h).add(d.key));
    const timer = window.setTimeout(() => void commitDeferred(d.key), UNDO_MS);
    pending.current.set(d.key, { d, timer });
    toast(d.label, "info", undefined, {
      ms: UNDO_MS,
      action: { label: "Undo", run: () => { const p = pending.current.get(d.key); if (!p) return; window.clearTimeout(p.timer); pending.current.delete(d.key); unhide(d.key); d.after?.(); } },
    });
  }, [commitDeferred, toast, unhide]);
  const flushDeferred = useCallback(async () => { await Promise.all([...pending.current.keys()].map((k) => commitDeferred(k))); }, [commitDeferred]);
  const isHidden = useCallback((key: string) => hidden.has(key), [hidden]);
  useEffect(() => {
    // deleting is only deferred while the app is open: closing or hiding the window completes pending deletes
    const f = () => void flushDeferred();
    window.addEventListener("pagehide", f);
    window.addEventListener("beforeunload", f);
    return () => { window.removeEventListener("pagehide", f); window.removeEventListener("beforeunload", f); };
  }, [flushDeferred]);

  const confirmPassword = useCallback((reason: string) => new Promise<string | null>((resolve) => setPwAsk({ reason, resolve })), []);

  const call = useCallback(async <T,>(method: string, params: Record<string, unknown> = {}): Promise<T> => {
    if (method === "auth.lock") await flushDeferred(); // pending deletes finish before the vault locks
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        return await rpc<T>(method, params);
      } catch (e: any) {
        if (!(e instanceof ApiError)) throw e;
        if (e.code === "step_up_required") {
          const ok = await new Promise<boolean>((resolve) => setStepUp({ category: String(e.details.category ?? "security_settings"), resolve }));
          if (!ok) throw e;
          continue;
        }
        if (e.code === "password_required" && !("password" in params)) {
          const pw = await confirmPassword(e.message);
          if (!pw) throw e;
          params = { ...params, password: pw };
          continue;
        }
        if (e.code === "locked") void refresh();
        throw e;
      }
    }
    throw new ApiError({ code: "error", message: "could not complete the request" });
  }, [confirmPassword, refresh, flushDeferred]);

  useEffect(() => {
    void refresh();
    const off = onEvent((topic, data) => {
      if (topic === "session.changed") setStatus(data);
      if (topic === "gw.status") { setConnected(!!data.connected); if (data.connected) void refresh(); }
      if (["approvals.changed", "tasks.changed", "killswitch.changed", "run.finished"].includes(topic)) void refresh();
      if (topic === "reminder.fired") toast(`⏰ Reminder: ${data.text}`, "info", () => setRoute({ screen: "reminders" }));
      if (topic === "notify" && data.kind !== "reminder" && data.kind !== "chat") toast(data.body ? `${data.title}: ${data.body}` : data.title, "info",
        () => data.screen && setRoute({ screen: data.screen }));
      if (topic === "security.event" && data.severity !== "info") toast(`Security: ${String(data.event_type).replace(/[._]/g, " ")}`, "warn");
      if (topic === "gw.local" && data.kind === "stop_all_hotkey") toast("STOP ALL activated from the hotkey", "danger");
    });
    const poll = setInterval(refresh, 15000);
    return () => { off(); clearInterval(poll); };
  }, [refresh, toast]);

  // activity -> gateway idle timer (auto-lock is enforced by the gateway, not by the UI)
  useEffect(() => {
    const touch = () => {
      if (Date.now() - lastTouch.current > 30000 && status?.state === "UNLOCKED") {
        lastTouch.current = Date.now();
        void rpc("session.touch").catch(() => undefined);
      }
    };
    window.addEventListener("pointerdown", touch);
    window.addEventListener("keydown", touch);
    return () => { window.removeEventListener("pointerdown", touch); window.removeEventListener("keydown", touch); };
  }, [status?.state]);

  // appearance: theme (incl. "time_of_day" from the PC clock), animated background, text size, reduce motion
  useEffect(() => {
    const ui = status?.ui;
    if (!ui) return;
    const root = document.documentElement;
    const toMin = (t: string, d: number) => { const m = /^(\d{1,2}):(\d{2})/.exec(t ?? ""); return m ? +m[1] * 60 + +m[2] : d; };
    const apply = () => {
      let theme: string = ui["ui.theme"] ?? "system";
      if (theme === "time_of_day") {
        const now = new Date(); const mins = now.getHours() * 60 + now.getMinutes();
        const day = toMin(ui["ui.day_starts"], 420), night = toMin(ui["ui.night_starts"], 1140);
        theme = mins >= day && mins < night ? "light" : "dark";
      }
      if (theme === "system") root.removeAttribute("data-theme"); else root.setAttribute("data-theme", theme);
      const dark = theme === "system" ? window.matchMedia("(prefers-color-scheme: dark)").matches : ["dark", "aurora", "forest"].includes(theme);
      root.setAttribute("data-scheme", dark ? "dark" : "light"); // lets the accent colours pick their light/dark variant
      const accent = ui["ui.accent"] ?? "theme";
      if (accent === "theme") root.removeAttribute("data-accent"); else root.setAttribute("data-accent", accent);
    };
    apply();
    const timer = window.setInterval(apply, 60_000); // keeps day/night in step with the laptop clock
    root.style.setProperty("--scale", String(sizeFactor(ui["ui.font_size"]) * (ui["ui.text_scale"] ?? 100) / 100));
    root.style.setProperty("--font", fontStack(ui["ui.font"]));
    if (ui["ui.reduce_motion"]) root.setAttribute("data-motion", "reduce"); else root.removeAttribute("data-motion");
    root.setAttribute("data-bg", ui["ui.reduce_motion"] ? "off" : (ui["ui.background"] ?? "off"));
    return () => window.clearInterval(timer);
  }, [status?.ui]);

  const go = useCallback((screen: Screen, params?: Record<string, any>) => {
    setRoute({ screen, params });
    try { localStorage.setItem(LAST_SCREEN, screen); } catch { /* storage unavailable */ }
  }, []);
  const value = useMemo(() => ({ status, refresh, route, go, toast, deferDelete, isHidden, call, confirmPassword, connected }),
    [status, refresh, route, go, toast, deferDelete, isHidden, call, confirmPassword, connected]);

  return (
    <AppCtx.Provider value={value}>
      {children}
      <div className="toasts" aria-live="polite">
        {toasts.map((t) => (
          <ToastItem key={t.id} t={t} onDone={() => setToasts((x) => x.filter((y) => y.id !== t.id))} />
        ))}
      </div>
      {stepUp && <StepUpDialog category={stepUp.category} pinAllowed={true} onDone={(ok) => { stepUp.resolve(ok); setStepUp(null); }} />}
      {pwAsk && <PasswordDialog reason={pwAsk.reason} onDone={(pw) => { pwAsk.resolve(pw); setPwAsk(null); }} />}
    </AppCtx.Provider>
  );
}

function ToastItem({ t, onDone }: { t: Toast; onDone: () => void }) {
  const ref = useExitGhost<HTMLDivElement>("rect");
  return (
    <div ref={ref} className={`toast ${t.kind ?? ""}`} onClick={() => { t.onClick?.(); onDone(); }}>
      <div className="toast-row">
        <span className="toast-text">{t.text}</span>
        {t.action && <button className="toast-action" onClick={(e) => { e.stopPropagation(); t.action!.run(); onDone(); }}>{t.action.label}</button>}
      </div>
    </div>
  );
}

function StepUpDialog({ category, onDone, pinAllowed }: { category: string; onDone: (ok: boolean) => void; pinAllowed: boolean }) {
  const [method, setMethod] = useState<"pin" | "password">(pinAllowed ? "pin" : "password");
  const [secret, setSecret] = useState("");
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);
  const submit = async () => {
    setBusy(true);
    setErr("");
    try {
      await rpc("auth.step_up", { category, method, secret });
      onDone(true);
    } catch (e: any) {
      setErr(e.message);
      setSecret("");
    } finally {
      setBusy(false);
    }
  };
  return (
    <Modal title="Confirm it's you" onClose={() => onDone(false)}
      actions={<><Button onClick={() => onDone(false)}>Cancel</Button><Button kind="primary" busy={busy} onClick={submit} disabled={!secret}>Confirm</Button></>}>
      <p className="muted">To {CATEGORY_TEXT[category] ?? "continue"}, enter your {method === "pin" ? "PIN" : "password"}. This stays valid for a few minutes.</p>
      <Field error={err}>
        <input className="input" type="password" autoFocus value={secret} inputMode={method === "pin" ? "numeric" : undefined}
          placeholder={method === "pin" ? "PIN" : "Password"} onChange={(e) => setSecret(e.target.value)} onKeyDown={(e) => e.key === "Enter" && secret && submit()} />
      </Field>
      <div style={{ marginTop: 10 }}>
        <Button kind="ghost" small onClick={() => { setMethod(method === "pin" ? "password" : "pin"); setSecret(""); }}>
          Use {method === "pin" ? "password" : "PIN"} instead
        </Button>
      </div>
    </Modal>
  );
}

function PasswordDialog({ reason, onDone }: { reason: string; onDone: (pw: string | null) => void }) {
  const [pw, setPw] = useState("");
  return (
    <Modal title="Password required" onClose={() => onDone(null)}
      actions={<><Button onClick={() => onDone(null)}>Cancel</Button><Button kind="primary" disabled={!pw} onClick={() => onDone(pw)}>Confirm</Button></>}>
      <p className="muted">{reason.charAt(0).toUpperCase() + reason.slice(1)}.</p>
      <input className="input" type="password" autoFocus value={pw} placeholder="Your password" onChange={(e) => setPw(e.target.value)}
        onKeyDown={(e) => e.key === "Enter" && pw && onDone(pw)} />
    </Modal>
  );
}

export function errText(e: any): string {
  return e?.details?.errors ? (e.details.errors as string[]).join(" · ") : e?.message ?? String(e);
}
