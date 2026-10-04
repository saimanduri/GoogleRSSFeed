// First-run wizard (spec 4.2): checks -> account -> PIN -> recovery key -> model -> connectors -> autonomy -> backup -> done.
import { useEffect, useState } from "react";
import { native, pickFile, pickSavePath, rpc } from "../api/gateway";
import { errText, useApp } from "../app";
import { Icon } from "../components/Icon";
import { Badge, Button, Field, Toggle } from "../components/ui";
import { LoosenDialog } from "../components/LoosenDialog";
import { RestoreNewPc } from "./SignIn";
import { ModelAdder } from "./settings/ModelSettings";

const STEPS = ["Welcome", "Account", "PIN", "Recovery key", "AI model", "Connectors", "Autonomy", "Backup", "Done"];

export function Onboarding() {
  const { refresh } = useApp();
  const [step, setStep] = useState(0);
  const [pre, setPre] = useState<any>(null);
  const [username, setUsername] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [assistantName, setAssistantName] = useState("");
  const [password, setPassword] = useState("");
  const [password2, setPassword2] = useState("");
  const [pin, setPin] = useState("");
  const [pin2, setPin2] = useState("");
  const [letters, setLetters] = useState(false);
  const [strength, setStrength] = useState<any>(null);
  const [pinErrors, setPinErrors] = useState<string[]>([]);
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);
  const [rk, setRk] = useState<{ recovery_key: string; confirm_groups: number[] } | null>(null);
  const [restore, setRestore] = useState(false);

  useEffect(() => { void rpc("setup.preflight").then(setPre).catch((e) => setErr(errText(e))); }, []);
  useEffect(() => {
    if (!password) { setStrength(null); return; }
    const t = setTimeout(() => rpc("setup.check_password", { password, username }).then(setStrength).catch(() => undefined), 150);
    return () => clearTimeout(t);
  }, [password, username]);
  useEffect(() => {
    if (!pin) { setPinErrors([]); return; }
    const t = setTimeout(() => rpc<any>("setup.check_pin", { pin, allow_letters: letters, password }).then((r) => setPinErrors(r.errors)).catch(() => undefined), 150);
    return () => clearTimeout(t);
  }, [pin, letters, password]);

  const create = async () => {
    setBusy(true);
    setErr("");
    try {
      const r = await rpc<any>("setup.create", { username, password, pin, allow_letters: letters, display_name: displayName.trim(), assistant_name: assistantName.trim() });
      setRk(r);
      setPassword(""); setPassword2(""); setPin(""); setPin2("");
      setStep(3);
    } catch (e: any) {
      setErr(errText(e));
    } finally {
      setBusy(false);
    }
  };

  const next = () => setStep((s) => s + 1);
  const pwOk = strength && strength.errors.length === 0 && password === password2;
  const pinOk = pin.length >= 6 && pin === pin2 && pinErrors.length === 0;

  if (restore) return <RestoreNewPc onBack={() => setRestore(false)} />;
  return (
    <div className="auth-wrap">
      <div className="auth-card wizard-card">
        <div className="stepper">{STEPS.map((s, i) => <div key={s} className={i <= step ? "done" : ""} title={s} />)}</div>
        <div className="faint small" style={{ marginBottom: 4 }}>Step {step + 1} of {STEPS.length} · {STEPS[step]}</div>

        {step === 0 && (
          <div className="col">
            <h1>Welcome to ChiRAG Agent</h1>
            <p className="muted">A private assistant that runs on this PC. Your data, keys and secrets stay here, encrypted. Let's check this PC first.</p>
            {!pre ? <div className="skeleton" style={{ height: 180 }} /> : (
              <div className="card flat">
                <Check ok={pre.windows} label="Windows" detail={pre.version} />
                <Check ok={pre.tpm_ok} label="TPM 2.0" detail={pre.tpm.usable ? "ready" : pre.dev_mode ? "not usable - developer mode uses a software stand-in" : pre.tpm.detail} block={!pre.tpm_ok} />
                <Check ok={pre.bitlocker === "on"} warn={pre.bitlocker !== "on"} label="BitLocker" detail={pre.bitlocker === "on" ? "on" : "off or unknown - recommended: turn on device encryption"} />
                <Check ok={pre.sandbox !== "UNAVAILABLE"} warn={pre.sandbox !== "STRONG"} label="Python sandbox"
                  detail={{ STRONG: "Windows Sandbox (strong isolation)", STANDARD: "AppContainer (standard isolation)", UNAVAILABLE: "not available - Python analysis disabled" }[pre.sandbox as string]} />
                <Check ok={!pre.cloud_synced} label="Data folder" detail={pre.data_folder} block={pre.cloud_synced} />
                <Check ok={pre.disk_free_gb > 10} warn={pre.disk_free_gb <= 10} label="Disk space" detail={`${pre.disk_free_gb} GB free`} />
                <Check ok info label="GPU" detail={(pre.gpu ?? []).join(", ") || "none detected"} />
              </div>
            )}
            {pre?.dev_mode && <div className="banner warn"><Icon name="alert" />Developer mode is on. Do not use real data.</div>}
            <div className="row"><Button kind="ghost" onClick={() => setRestore(true)}>Restore from a backup instead</Button><div className="spacer" />
              <Button kind="primary" disabled={!pre || !pre.tpm_ok || pre.cloud_synced} onClick={next}>Continue</Button></div>
          </div>
        )}

        {step === 1 && (
          <div className="col">
            <h1>Create your account</h1>
            <div className="banner info"><Icon name="info" />Your password encrypts everything. We cannot recover it for you.</div>
            <Field label="Your name" help="How the assistant greets you. You can change it any time in Settings.">
              <input className="input" autoFocus value={displayName} maxLength={40} placeholder="e.g. Sai" onChange={(e) => setDisplayName(e.target.value)} />
            </Field>
            <Field label="Name your assistant" help="Address the assistant by this name. You can change it any time in Settings.">
              <input className="input" value={assistantName} maxLength={40} placeholder="ChiRAG Agent" onChange={(e) => setAssistantName(e.target.value)} />
            </Field>
            <Field label="Username" help="3-64 characters, used to sign in. Not a secret.">
              <input className="input" value={username} onChange={(e) => setUsername(e.target.value)} />
            </Field>
            <Field label="Password" help="At least 12 characters. Paste from a password manager is fine.">
              <input className="input" type="password" value={password} onChange={(e) => setPassword(e.target.value)} />
            </Field>
            {strength && <StrengthMeter s={strength} />}
            <Field label="Confirm password" error={password2 && password !== password2 ? "Passwords do not match" : ""}>
              <input className="input" type="password" value={password2} onChange={(e) => setPassword2(e.target.value)} />
            </Field>
            {(() => {
              const todo = [
                !displayName.trim() && "enter your name",
                username.length < 3 && "username needs 3+ characters",
                password.length < 12 && `password needs 12+ characters (now ${password.length})`,
                password.length >= 12 && !pwOk && "password is not accepted yet (see above)",
                password && password !== password2 && "the two passwords must match",
              ].filter(Boolean) as string[];
              return todo.length ? <div className="small" style={{ color: "var(--warn)" }} role="status">To continue: {todo.join(" · ")}</div> : null;
            })()}
            <div className="row"><Button onClick={() => setStep(0)}>Back</Button><div className="spacer" />
              <Button kind="primary" disabled={!pwOk || username.length < 3 || !displayName.trim()} onClick={next}>Continue</Button></div>
          </div>
        )}

        {step === 2 && (
          <div className="col">
            <h1>Create a PIN</h1>
            <p className="muted">The PIN quickly unlocks the window while the agent keeps running, and together with your recovery key it can reset a forgotten password. It is protected by this PC's TPM chip.</p>
            <div className="row"><Toggle on={letters} onChange={setLetters} label="Allow letters" /><span>Allow letters (alphanumeric PIN)</span></div>
            <Field label="PIN" help="6-12 characters. No simple sequences or repeated digits." error={pinErrors.join(" · ")}>
              <input className="input" type="password" autoFocus inputMode={letters ? "text" : "numeric"} value={pin} maxLength={12} onChange={(e) => setPin(e.target.value)} />
            </Field>
            <Field label="Confirm PIN" error={pin2 && pin !== pin2 ? "PINs do not match" : ""}>
              <input className="input" type="password" inputMode={letters ? "text" : "numeric"} value={pin2} maxLength={12} onChange={(e) => setPin2(e.target.value)} />
            </Field>
            {err && <div className="banner danger">{err}</div>}
            <div className="row"><Button onClick={() => setStep(1)}>Back</Button><div className="spacer" />
              <Button kind="primary" busy={busy} disabled={!pinOk} onClick={create}>Create account</Button></div>
          </div>
        )}

        {step === 3 && rk && <RecoveryKeyStep rk={rk} onDone={next} />}
        {step === 4 && (
          <div className="col">
            <h1>Local AI model</h1>
            <p className="muted">Connect the model that will run on this PC: the built-in runtime (a .gguf file), Ollama, vLLM, LM Studio, Run:ai or any OpenAI-compatible server. You can add a speech-to-text model for voice too. Add or change models any time in Settings &gt; AI Model.</p>
            <ModelAdder compact onAdded={() => undefined} />
            <div className="row"><div className="spacer" /><Button onClick={next}>I'll add one later</Button><Button kind="primary" onClick={next}>Continue</Button></div>
          </div>
        )}
        {step === 5 && <ConnectorsStep onNext={next} />}
        {step === 6 && <AutonomyStep onNext={next} />}
        {step === 7 && <BackupStep onNext={next} />}
        {step === 8 && (
          <div className="col center" style={{ textAlign: "center", padding: "10px 0" }}>
            <div className="brand-logo logo-img" style={{ width: 72, height: 72, borderRadius: 18 }}><img src="/chirag-logo.png" alt="" /></div>
            <h1>You're all set</h1>
            <p className="muted" style={{ maxWidth: 480 }}>Everything is encrypted with your password and this PC's TPM. The agent can only act through the gateway, which checks every action against your rules. The red STOP ALL button is always one click away.</p>
            <Button kind="primary" onClick={refresh}>Open {assistantName.trim() || "ChiRAG Agent"}</Button>
          </div>
        )}
      </div>
    </div>
  );
}

function Check({ ok, warn, info, label, detail, block }: { ok: boolean; warn?: boolean; info?: boolean; label: string; detail?: string; block?: boolean }) {
  const tone = ok && !warn ? "ok" : block ? "danger" : warn || info ? "warn" : "danger";
  return (
    <div className="check-row">
      <Badge tone={info ? "info" : tone}>{info ? "info" : ok && !warn ? "OK" : block ? "BLOCKED" : "WARNING"}</Badge>
      <strong style={{ width: 140 }}>{label}</strong><span className="muted grow">{detail}</span>
    </div>
  );
}

export function StrengthMeter({ s }: { s: any }) {
  const score = s.strength.score as number;
  const colors = ["var(--danger)", "var(--danger)", "var(--warn)", "var(--info)", "var(--ok)"];
  return (
    <div className="col" style={{ gap: 4 }}>
      <div className="meter"><div style={{ width: `${(score + 1) * 20}%`, background: colors[score] }} /></div>
      <div className="small"><span className="faint">Strength: </span><strong>{s.strength.label}</strong>
        {s.errors.length ? <span style={{ color: "var(--danger)" }}> · Not accepted yet: {s.errors.join(" · ")}</span> : <span style={{ color: "var(--ok)" }}> · accepted</span>}
        {s.strength.feedback?.length ? <span className="faint"> · {s.strength.feedback.join(" · ")}</span> : null}</div>
    </div>
  );
}

export function RecoveryKeyStep({ rk, onDone }: { rk: { recovery_key: string; confirm_groups: number[] }; onDone: () => void }) {
  const { toast } = useApp();
  const [answers, setAnswers] = useState<Record<number, string>>({});
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);
  useEffect(() => { void native("set_capture_protection", { enabled: true }); return () => { void native("set_capture_protection", { enabled: false }); }; }, []);
  const copy = async () => {
    await navigator.clipboard.writeText(rk.recovery_key);
    toast("Copied. The clipboard will be cleared in 30 seconds.", "ok");
    setTimeout(() => navigator.clipboard.writeText("").catch(() => undefined), 30000);
  };
  const savePdf = async () => {
    const path = await pickSavePath("PersonalAgent-recovery-key.pdf", [{ name: "PDF", extensions: ["pdf"] }]);
    if (path) { await rpc("setup.recovery_pdf", { path }); toast("Saved. Print it, store it offline, then delete the file.", "ok"); }
  };
  const confirm = async () => {
    setBusy(true);
    try {
      await rpc("setup.confirm_recovery", { answers: Object.fromEntries(rk.confirm_groups.map((g) => [String(g), answers[g] ?? ""])) });
      onDone();
    } catch (e: any) { setErr(errText(e)); } finally { setBusy(false); }
  };
  return (
    <div className="col">
      <h1>Your recovery key</h1>
      <p className="muted">Write it down or print it. It is shown only now.</p>
      <div className="rk">{rk.recovery_key}</div>
      <div className="row"><Button icon="copy" onClick={copy}>Copy</Button><Button icon="download" onClick={savePdf}>Save as PDF</Button></div>
      <div className="banner warn" style={{ alignItems: "flex-start" }}><Icon name="alert" /><div>
        To reset a forgotten password you need <b>BOTH</b> your PIN and this recovery key, on <b>THIS</b> computer.
        If you forget your password AND lose either your PIN or this key, your data cannot be recovered.
        Keep an encrypted backup (Settings &gt; Backup) - backups are restored with your password.</div></div>
      <h3>Type back two groups to confirm you recorded it</h3>
      <div className="row wrap">
        {rk.confirm_groups.map((g) => (
          <Field key={g} label={`Group ${g + 1}`}>
            <input className="input mono" style={{ width: 130, textTransform: "uppercase" }} maxLength={5} value={answers[g] ?? ""}
              onChange={(e) => setAnswers({ ...answers, [g]: e.target.value })} />
          </Field>
        ))}
      </div>
      {err && <div className="banner danger">{err}</div>}
      <div className="row"><div className="spacer" /><Button kind="primary" busy={busy} onClick={confirm}
        disabled={rk.confirm_groups.some((g) => (answers[g] ?? "").length !== 5)}>I've saved it</Button></div>
    </div>
  );
}

function ConnectorsStep({ onNext }: { onNext: () => void }) {
  const [list, setList] = useState<any[]>([]);
  const load = () => rpc<any>("connectors.list").then((r) => setList(r.connectors));
  useEffect(() => { void load(); }, []);
  return (
    <div className="col">
      <h1>Connectors</h1>
      <p className="muted">All connectors are OFF by default. You can connect them now or later in Settings &gt; Connectors. Microsoft 365 sign-in is done later from Settings.</p>
      {list.map((c) => (
        <div key={c.id} className="card flat row">
          <div className="grow"><strong>{c.label}</strong><div className="small muted">{c.description}</div>
            {!c.connection_ok && c.connection_reason && <div className="small faint">{c.connection_reason}</div>}</div>
          <Toggle on={c.enabled} label={c.label} onChange={async (v) => { await rpc("connectors.set", { connector: c.id, enabled: v }).catch(() => undefined); void load(); }} />
        </div>
      ))}
      <div className="row"><div className="spacer" /><Button kind="primary" onClick={onNext}>Continue</Button></div>
    </div>
  );
}

function AutonomyStep({ onNext }: { onNext: () => void }) {
  const [profile, setProfile] = useState("cautious");
  const [loosen, setLoosen] = useState<null | { changes: any; loosening: any[] }>(null);
  const next = async () => {
    if (profile === "balanced") {
      const preview = await rpc<any>("settings.profile_preview", { profile });
      setLoosen({ changes: preview.changes, loosening: preview.loosening });
      return;
    }
    onNext();
  };
  return (
    <div className="col">
      <h1>How independent should the agent be?</h1>
      <div className="grid-2">
        {[["cautious", "Cautious (recommended)", "Small budgets, approvals for anything that leaves this PC, no auto-confirmed memories."],
          ["balanced", "Balanced", "Larger budgets for longer missions. The security floor still applies - there is no profile that removes it."]].map(([id, t, d]) => (
          <div key={id} className="card clickable" style={profile === id ? { borderColor: "var(--accent)", boxShadow: "0 0 0 3px var(--accent-soft)" } : {}}
            onClick={() => setProfile(id)} role="radio" aria-checked={profile === id} tabIndex={0} onKeyDown={(e) => e.key === "Enter" && setProfile(id)}>
            <div className="card-title">{t}</div><p className="muted small">{d}</p>
          </div>
        ))}
      </div>
      <div className="row"><div className="spacer" /><Button kind="primary" onClick={next}>Continue</Button></div>
      {loosen && <LoosenDialog changes={loosen.changes} loosening={loosen.loosening} onClose={() => setLoosen(null)} onApplied={onNext} />}
    </div>
  );
}

function BackupStep({ onNext }: { onNext: () => void }) {
  const [folder, setFolder] = useState("");
  const [schedule, setSchedule] = useState("weekly");
  const [pw, setPw] = useState("");
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);
  const save = async () => {
    setBusy(true);
    try {
      await rpc("auth.step_up", { category: "security_settings", method: "password", secret: pw });
      await rpc("settings.apply", { changes: { "backup.folder": folder, "backup.schedule": schedule } });
      await rpc("backup.enable_schedule", { password: pw });
      onNext();
    } catch (e: any) { setErr(errText(e)); } finally { setBusy(false); }
  };
  return (
    <div className="col">
      <h1>Backups</h1>
      <p className="muted">Encrypted backups protect you if this PC fails. An external drive is recommended. Backups are restored with your password.</p>
      <Field label="Backup folder">
        <div className="row"><input className="input" value={folder} onChange={(e) => setFolder(e.target.value)} placeholder="E:\\Backups\\PersonalAgent" />
          <Button onClick={async () => { const f = await pickFile({ directory: true }); if (f) setFolder(String(f)); }}>Browse</Button></div>
      </Field>
      <Field label="Schedule">
        <select className="input" value={schedule} onChange={(e) => setSchedule(e.target.value)}>
          <option value="daily">Daily</option><option value="weekly">Weekly</option>
        </select>
      </Field>
      <Field label="Your password" help="Needed once so scheduled backups can be restored with it.">
        <input className="input" type="password" value={pw} onChange={(e) => setPw(e.target.value)} />
      </Field>
      {err && <div className="banner danger">{err}</div>}
      <div className="banner warn"><Icon name="alert" />Skipping means no backup - if this PC fails, your data is lost.</div>
      <div className="row"><div className="spacer" /><Button onClick={onNext}>Skip for now</Button>
        <Button kind="primary" busy={busy} disabled={!folder || !pw} onClick={save}>Save</Button></div>
    </div>
  );
}
