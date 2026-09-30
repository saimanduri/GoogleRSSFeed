// App-wide state: session status, navigation, toasts and the `call()` helper that handles
// step-up (PIN/password re-authentication) and password confirmations transparently.
import { createContext, ReactNode, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { ApiError, onEvent, rpc } from "./api/gateway";
import { Button, Field, Modal } from "./components/ui";

export type Screen = "home" | "chat" | "missions" | "reminders" | "tasks" | "approvals" | "files" | "memory" | "secrets" | "activity" | "settings";
export type Route = { screen: Screen; params?: Record<string, any> };
type Toast = { id: number; text: string; kind?: "ok" | "danger" | "warn" | "info"; onClick?: () => void };

type Ctx = {
  status: any;
  refresh: () => Promise<void>;
  route: Route;
  go: (screen: Screen, params?: Record<string, any>) => void;
  toast: (text: string, kind?: Toast["kind"], onClick?: () => void) => void;
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
  const [route, setRoute] = useState<Route>({ screen: "home" });
  const [toasts, setToasts] = useState<Toast[]>([]);
  const [stepUp, setStepUp] = useState<null | { category: string; resolve: (ok: boolean) => void }>(null);
  const [pwAsk, setPwAsk] = useState<null | { reason: string; resolve: (pw: string | null) => void }>(null);
  const [connected, setConnected] = useState(true);
  const lastTouch = useRef(0);

  const refresh = useCallback(async () => {
    try {
      setStatus(await rpc("session.status"));
      setConnected(true);
    } catch (e: any) {
      if (e.code === "disconnected") setConnected(false);
    }
  }, []);

  const toast = useCallback((text: string, kind?: Toast["kind"], onClick?: () => void) => {
    const id = Date.now() + Math.random();
    setToasts((t) => [...t.slice(-4), { id, text, kind, onClick }]);
    setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), 6000);
  }, []);

  const confirmPassword = useCallback((reason: string) => new Promise<string | null>((resolve) => setPwAsk({ reason, resolve })), []);

  const call = useCallback(async <T,>(method: string, params: Record<string, unknown> = {}): Promise<T> => {
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
  }, [confirmPassword, refresh]);

  useEffect(() => {
    void refresh();
    const off = onEvent((topic, data) => {
      if (topic === "session.changed") setStatus(data);
      if (topic === "gw.status") { setConnected(!!data.connected); if (data.connected) void refresh(); }
      if (["approvals.changed", "tasks.changed", "killswitch.changed", "run.finished"].includes(topic)) void refresh();
      if (topic === "reminder.fired") toast(`⏰ Reminder: ${data.text}`, "info", () => setRoute({ screen: "reminders" }));
      if (topic === "notify" && data.kind !== "reminder") toast(data.body ? `${data.title}: ${data.body}` : data.title, "info",
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

  // appearance
  useEffect(() => {
    const ui = status?.ui;
    if (!ui) return;
    const root = document.documentElement;
    if (ui["ui.theme"] === "system") root.removeAttribute("data-theme"); else root.setAttribute("data-theme", ui["ui.theme"]);
    root.style.setProperty("--scale", String((ui["ui.text_scale"] ?? 100) / 100));
    if (ui["ui.reduce_motion"]) root.setAttribute("data-motion", "reduce"); else root.removeAttribute("data-motion");
  }, [status?.ui]);

  const go = useCallback((screen: Screen, params?: Record<string, any>) => setRoute({ screen, params }), []);
  const value = useMemo(() => ({ status, refresh, route, go, toast, call, confirmPassword, connected }),
    [status, refresh, route, go, toast, call, confirmPassword, connected]);

  return (
    <AppCtx.Provider value={value}>
      {children}
      <div className="toasts" aria-live="polite">
        {toasts.map((t) => (
          <div key={t.id} className={`toast ${t.kind ?? ""}`} onClick={() => { t.onClick?.(); setToasts((x) => x.filter((y) => y.id !== t.id)); }}>{t.text}</div>
        ))}
      </div>
      {stepUp && <StepUpDialog category={stepUp.category} pinAllowed={true} onDone={(ok) => { stepUp.resolve(ok); setStepUp(null); }} />}
      {pwAsk && <PasswordDialog reason={pwAsk.reason} onDone={(pw) => { pwAsk.resolve(pw); setPwAsk(null); }} />}
    </AppCtx.Provider>
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
