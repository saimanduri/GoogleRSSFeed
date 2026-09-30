import { ReactNode, useEffect, useState } from "react";
import { errText, useApp } from "../../app";
import { Badge, Button, Card, Field, Modal, Tabs, Time } from "../../components/ui";
import { RecoveryKeyStep } from "../Onboarding";

export function AccountSettings({ generic, tab: initialTab }: { generic: ReactNode; tab?: string }) {
  const [tab, setTab] = useState<"security" | "credentials" | "posture" | "history">((initialTab as any) ?? "security");
  return (
    <>
      <Tabs tabs={[["security", "Locking & sessions"], ["credentials", "Password, PIN & recovery"], ["posture", "Security posture"], ["history", "Sign-in history"]]} value={tab} onChange={setTab} />
      {tab === "security" && generic}
      {tab === "credentials" && <Credentials />}
      {tab === "posture" && <Posture />}
      {tab === "history" && <History />}
    </>
  );
}

function Credentials() {
  const { call, toast, status } = useApp();
  const [dlg, setDlg] = useState<string | null>(null);
  const [f, setF] = useState<any>({});
  const [rk, setRk] = useState<any>(null);
  const run = async (fn: () => Promise<any>, ok: string) => {
    try { const r = await fn(); toast(ok, "ok"); setDlg(null); setF({}); if (r?.recovery_key) setRk(r); } catch (e: any) { toast(errText(e), "danger"); }
  };
  const input = (k: string, label: string, type = "password") => (
    <Field label={label}><input className="input" type={type} value={f[k] ?? ""} onChange={(e) => setF({ ...f, [k]: e.target.value })} /></Field>);
  return (
    <div className="grid-2">
      {status?.needs_pin_setup && <div className="banner warn" style={{ gridColumn: "1/-1" }}>Restored on a new PC: set a new PIN (a new recovery key will be generated).</div>}
      <Card title="Username" actions={<Button small onClick={() => setDlg("user")}>Change</Button>}><div>{status?.username}</div></Card>
      <Card title="Password" actions={<Button small onClick={() => setDlg("pw")}>Change</Button>}><div className="muted small">Encrypts everything. Required at every cold start.</div></Card>
      <Card title="PIN" actions={<Button small onClick={() => setDlg("pin")}>Set new PIN</Button>}><div className="muted small">Quick unlock + password reset (with the recovery key). Bound to this PC's TPM.</div></Card>
      <Card title="Recovery key" actions={<Button small onClick={() => setDlg("rk")}>Generate new</Button>}><div className="muted small">Lost it? Generate a new one (needs password + PIN). The old one stops working.</div></Card>
      {dlg === "user" && <Modal title="Change username" onClose={() => setDlg(null)} actions={<Button kind="primary" onClick={() => run(() => call("account.change_username", { username: f.username, password: f.password }), "Username changed")}>Save</Button>}>{input("username", "New username", "text")}{input("password", "Password")}</Modal>}
      {dlg === "pw" && <Modal title="Change password" onClose={() => setDlg(null)} actions={<Button kind="primary" disabled={f.new !== f.new2} onClick={() => run(() => call("account.change_password", { current: f.current, new: f.new }), "Password changed")}>Save</Button>}>{input("current", "Current password")}{input("new", "New password")}{input("new2", "Confirm new password")}</Modal>}
      {dlg === "pin" && <Modal title="Set a new PIN" onClose={() => setDlg(null)} actions={<Button kind="primary" onClick={() => run(() => call("account.set_pin", { password: f.password, pin: f.pin, recovery_key: f.rk || undefined }), "PIN changed")}>Save</Button>}>
        {input("password", "Password")}{input("pin", "New PIN")}<Field label="Current recovery key (leave empty to generate a new one)"><input className="input mono" value={f.rk ?? ""} onChange={(e) => setF({ ...f, rk: e.target.value })} /></Field></Modal>}
      {dlg === "rk" && <Modal title="Generate a new recovery key" onClose={() => setDlg(null)} actions={<Button kind="primary" onClick={() => run(() => call("account.new_recovery_key", { password: f.password, pin: f.pin }), "New recovery key generated")}>Generate</Button>}>{input("password", "Password")}{input("pin", "PIN")}</Modal>}
      {rk && <Modal wide title="New recovery key" onClose={() => setRk(null)}><RecoveryKeyStep rk={rk} onDone={() => setRk(null)} /></Modal>}
    </div>
  );
}

function Posture() {
  const { call, toast } = useApp();
  const [checks, setChecks] = useState<any[] | null>(null);
  const load = () => call<any[]>("posture.run").then(setChecks).catch((e) => toast(errText(e), "danger"));
  useEffect(() => { void load(); /* eslint-disable-next-line */ }, []);
  const tone: Record<string, string> = { ok: "ok", warn: "warn", high: "danger", info: "info", unknown: "" };
  return (
    <Card title="Security posture" actions={<Button small icon="refresh" onClick={load}>Re-check</Button>}>
      {!checks ? <div className="skeleton" style={{ height: 200 }} /> : (
        <div className="list">{checks.map((c) => (
          <div key={c.id} className="list-item">
            <Badge tone={tone[c.status]}>{c.status.toUpperCase()}</Badge>
            <div className="grow"><div style={{ fontWeight: 600 }}>{c.title}</div><div className="small muted">{c.detail}</div></div>
            {c.fix && <Button small onClick={async () => { try { await call("posture.fix", { action: c.fix }); toast("Done", "ok"); void load(); } catch (e: any) { toast(errText(e), "danger"); } }}>Fix</Button>}
          </div>))}</div>
      )}
    </Card>
  );
}

function History() {
  const { call } = useApp();
  const [rows, setRows] = useState<any[]>([]);
  useEffect(() => { call<any[]>("account.signin_history").then(setRows).catch(() => undefined); }, [call]);
  return (
    <Card title="Last 50 sign-in attempts">
      <table className="table"><tbody>{rows.map((r) => <tr key={r.id}><td><Badge tone={r.success ? "ok" : "danger"}>{r.success ? "success" : "failed"}</Badge></td><td>{r.kind}</td><td className="small"><Time iso={r.ts} relative={false} /></td></tr>)}</tbody></table>
    </Card>
  );
}
