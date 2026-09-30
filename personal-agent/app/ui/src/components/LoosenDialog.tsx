// Loosening a setting (spec 5.5): plain-language risk, 10-second read delay, password, logged with before/after.
import { useEffect, useState } from "react";
import { errText, useApp } from "../app";
import { Icon } from "./Icon";
import { Button, Field, Modal } from "./ui";

export function LoosenDialog({ changes, loosening, onClose, onApplied }: { changes: Record<string, any>; loosening: any[]; onClose: () => void; onApplied: () => void }) {
  const { call } = useApp();
  const [token, setToken] = useState<string | null>(null);
  const [left, setLeft] = useState(10);
  const [pw, setPw] = useState("");
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);
  useEffect(() => { call<any>("settings.begin_loosen", { changes }).then((r) => { setToken(r.token); setLeft(r.delay_seconds); }).catch((e) => setErr(errText(e))); }, [call, changes]);
  useEffect(() => { if (left > 0) { const t = setTimeout(() => setLeft(left - 1), 1000); return () => clearTimeout(t); } }, [left]);
  const apply = async () => {
    setBusy(true);
    try { await call("settings.apply", { changes, password: pw, loosen_token: token }); onApplied(); onClose(); }
    catch (e: any) { setErr(errText(e)); } finally { setBusy(false); }
  };
  return (
    <Modal title="This makes the agent less restrictive" onClose={onClose}
      actions={<><Button onClick={onClose}>Cancel</Button><Button kind="danger" busy={busy} disabled={left > 0 || !pw || !token} onClick={apply}>
        {left > 0 ? <span className="countdown">Read first ({left}s)</span> : "Confirm change"}</Button></>}>
      <div className="col">
        {loosening.map((l) => (
          <div key={l.key} className="banner warn" style={{ alignItems: "flex-start" }}><Icon name="alert" />
            <div><b>{l.label}</b>: {JSON.stringify(l.before)} → {JSON.stringify(l.after)}<div className="small">{l.risk}</div></div></div>
        ))}
        <p className="small muted">The change is recorded in the security log with the before and after values. You can revert it any time.</p>
        <Field label="Your password" error={err}><input className="input" type="password" value={pw} autoFocus onChange={(e) => setPw(e.target.value)} /></Field>
      </div>
    </Modal>
  );
}
