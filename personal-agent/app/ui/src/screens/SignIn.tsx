// Cold start (password), quick unlock (PIN), forgot password (PIN + recovery key) - spec 4.5/4.6.
import { useEffect, useState } from "react";
import { pickFile, rpc } from "../api/gateway";
import { errText, useApp } from "../app";
import { Icon } from "../components/Icon";
import { Button, Field } from "../components/ui";
import { RecoveryKeyStep, StrengthMeter } from "./Onboarding";

export function SignIn() {
  const { status, refresh } = useApp();
  const locked = status.state === "UI_LOCKED";
  const [mode, setMode] = useState<"password" | "pin" | "forgot" | "newkey" | "restore">(locked && status.pin_available ? "pin" : "password");
  const [username, setUsername] = useState(status.username ?? "");
  const [secret, setSecret] = useState("");
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);
  const [wait, setWait] = useState(0);
  const [rk, setRk] = useState<any>(null);

  useEffect(() => {
    if (status.password_wait > 0) setWait(Math.ceil(status.password_wait));
  }, [status.password_wait]);
  useEffect(() => {
    if (wait <= 0) return;
    const t = setTimeout(() => setWait(wait - 1), 1000);
    return () => clearTimeout(t);
  }, [wait]);

  const submit = async () => {
    setBusy(true);
    setErr("");
    try {
      if (mode === "pin") await rpc("auth.quick_unlock", { pin: secret });
      else await rpc("auth.sign_in", { username, password: secret });
      await refresh();
    } catch (e: any) {
      setErr(errText(e));
      setSecret("");
      if (e.details?.wait) setWait(Math.ceil(e.details.wait));
      if (e.code === "pin_unavailable") setMode("password");
    } finally {
      setBusy(false);
    }
  };

  if (mode === "newkey" && rk) {
    return <div className="auth-wrap"><div className="auth-card wizard-card"><RecoveryKeyStep rk={rk} onDone={refresh} /></div></div>;
  }
  if (mode === "forgot") return <Forgot onBack={() => setMode("password")} onDone={(r) => { setRk(r); setMode("newkey"); }} />;
  if (mode === "restore") return <RestoreNewPc onBack={() => setMode("password")} />;

  return (
    <div className="auth-wrap">
      <div className="auth-card">
        <div className="row" style={{ marginBottom: 18 }}>
          <div className="brand-logo" style={{ width: 42, height: 42, borderRadius: 12 }}><Icon name={locked ? "lock" : "shield"} size={22} /></div>
          <div><h1 style={{ margin: 0 }}>{locked ? "Locked" : "Welcome back"}</h1>
            <div className="muted small">{locked ? "The agent keeps working in the background." : "Sign in to unlock your encrypted vault."}</div></div>
        </div>
        <div className="col">
          {mode === "password" && !locked && (
            <Field label="Username"><input className="input" value={username} onChange={(e) => setUsername(e.target.value)} autoComplete="username" /></Field>
          )}
          <Field label={mode === "pin" ? "PIN" : "Password"} error={err}>
            <input className="input" type="password" autoFocus value={secret} inputMode={mode === "pin" ? "numeric" : undefined}
              autoComplete={mode === "pin" ? "off" : "current-password"} onChange={(e) => setSecret(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && secret && !wait && submit()} />
          </Field>
          {wait > 0 && <div className="banner warn countdown"><Icon name="clock" />Too many attempts. Try again in {wait}s.</div>}
          <Button kind="primary" busy={busy} disabled={!secret || wait > 0} onClick={submit}>{mode === "pin" ? "Unlock" : locked ? "Unlock with password" : "Sign in"}</Button>
          <div className="row wrap" style={{ justifyContent: "space-between" }}>
            {locked && status.pin_available && (
              <Button kind="ghost" small onClick={() => { setMode(mode === "pin" ? "password" : "pin"); setSecret(""); setErr(""); }}>
                Use {mode === "pin" ? "password" : "PIN"}
              </Button>
            )}
            {!locked && <Button kind="ghost" small onClick={() => setMode("forgot")}>Forgot password?</Button>}
            {!locked && <Button kind="ghost" small onClick={() => setMode("restore")}>Restore a backup</Button>}
            {locked && <Button kind="ghost" small onClick={async () => { await rpc("killswitch.activate", { level: "stop_all", source: "lock_screen" }); }}>STOP ALL</Button>}
          </div>
          {status.dev_mode && <div className="banner warn small"><Icon name="alert" />Developer mode</div>}
        </div>
      </div>
    </div>
  );
}

function Forgot({ onBack, onDone }: { onBack: () => void; onDone: (rk: any) => void }) {
  const { status } = useApp();
  const [pin, setPin] = useState("");
  const [rk, setRk] = useState("");
  const [pw, setPw] = useState("");
  const [pw2, setPw2] = useState("");
  const [strength, setStrength] = useState<any>(null);
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    if (!pw) return;
    const t = setTimeout(() => rpc("setup.check_password", { password: pw, username: status.username }).then(setStrength).catch(() => undefined), 150);
    return () => clearTimeout(t);
  }, [pw, status.username]);
  const go = async () => {
    setBusy(true);
    setErr("");
    try { onDone(await rpc("auth.forgot_password", { pin, recovery_key: rk, new_password: pw })); } catch (e: any) { setErr(errText(e)); } finally { setBusy(false); }
  };
  return (
    <div className="auth-wrap">
      <div className="auth-card">
        <h1>Reset your password</h1>
        <p className="muted">You need BOTH your PIN and your recovery key, on this computer. A new recovery key will be issued afterwards.</p>
        {status.recovery_wait > 0 && <div className="banner warn">Recovery is temporarily disabled after failed attempts.</div>}
        <div className="col">
          <Field label="PIN"><input className="input" type="password" value={pin} onChange={(e) => setPin(e.target.value)} /></Field>
          <Field label="Recovery key"><input className="input mono" value={rk} placeholder="XXXXX-XXXXX-..." onChange={(e) => setRk(e.target.value)} /></Field>
          <Field label="New password"><input className="input" type="password" value={pw} onChange={(e) => setPw(e.target.value)} /></Field>
          {strength && pw && <StrengthMeter s={strength} />}
          <Field label="Confirm new password" error={pw2 && pw2 !== pw ? "Passwords do not match" : err}>
            <input className="input" type="password" value={pw2} onChange={(e) => setPw2(e.target.value)} />
          </Field>
          <div className="row"><Button onClick={onBack}>Back</Button><div className="spacer" />
            <Button kind="primary" busy={busy} disabled={!pin || !rk || !pw || pw !== pw2} onClick={go}>Reset password</Button></div>
        </div>
      </div>
    </div>
  );
}

function RestoreNewPc({ onBack }: { onBack: () => void }) {
  const [path, setPath] = useState("");
  const [pw, setPw] = useState("");
  const [msg, setMsg] = useState("");
  const [busy, setBusy] = useState(false);
  return (
    <div className="auth-wrap">
      <div className="auth-card">
        <h1>Restore a backup</h1>
        <p className="muted">Only possible on a PC without an existing vault (e.g. a new PC). You need the password that was used when the backup was made. You will then create a new PIN and recovery key.</p>
        <div className="col">
          <Field label="Backup file (.pabk)"><div className="row"><input className="input" value={path} onChange={(e) => setPath(e.target.value)} />
            <Button onClick={async () => { const f = await pickFile({ filters: [{ name: "Personal Agent backup", extensions: ["pabk"] }] }); if (f) setPath(String(f)); }}>Browse</Button></div></Field>
          <Field label="Backup password"><input className="input" type="password" value={pw} onChange={(e) => setPw(e.target.value)} /></Field>
          {msg && <div className="banner info">{msg}</div>}
          <div className="row"><Button onClick={onBack}>Back</Button><div className="spacer" />
            <Button kind="primary" busy={busy} disabled={!path || !pw} onClick={async () => {
              setBusy(true);
              try { await rpc("backup.restore_signed_out", { path, password: pw }); setMsg("Restore prepared. Restart Personal Agent (tray > Close window, then start it again) and sign in with the backup's password."); }
              catch (e: any) { setMsg(errText(e)); } finally { setBusy(false); }
            }}>Restore</Button></div>
        </div>
      </div>
    </div>
  );
}
