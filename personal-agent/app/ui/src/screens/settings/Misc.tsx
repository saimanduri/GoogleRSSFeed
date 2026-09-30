import { ReactNode, useEffect, useState } from "react";
import { pickFile, pickSavePath } from "../../api/gateway";
import { errText, useApp } from "../../app";
import { Badge, Button, bytes, Card, Field, Modal, Time } from "../../components/ui";

export function BackupSettings({ generic }: { generic: ReactNode }) {
  const { call, toast, confirmPassword } = useApp();
  const [d, setD] = useState<any>(null);
  const [busy, setBusy] = useState(false);
  const load = () => call("backup.status").then(setD).catch(() => undefined);
  useEffect(() => { void load(); /* eslint-disable-next-line */ }, []);
  const withPw = async (fn: (pw: string) => Promise<any>, ok: string) => {
    const pw = await confirmPassword("your password encrypts the backup");
    if (!pw) return;
    setBusy(true);
    try { await fn(pw); toast(ok, "ok"); void load(); } catch (e: any) { toast(errText(e), "danger"); } finally { setBusy(false); }
  };
  return (
    <div className="col">
      {d?.warn && <div className="banner warn">No backup in the last 14 days.</div>}
      <Card title="Backups" actions={<div className="row">
        <Button busy={busy} onClick={() => withPw((pw) => call("backup.run_now", { password: pw }), "Backup created")}>Back up now</Button>
        <Button onClick={() => withPw((pw) => call("backup.enable_schedule", { password: pw }), "Scheduled backups enabled")}>{d?.scheduled_enabled ? "Re-key schedule" : "Enable schedule"}</Button></div>}>
        <div className="small muted">Last backup: <Time iso={d?.last} /> · Folder: {d?.folder || "not set"}</div>
        <table className="table" style={{ marginTop: 8 }}><tbody>{(d?.files ?? []).map((f: any) => (
          <tr key={f.name}><td className="mono small">{f.name}</td><td className="small">{bytes(f.size)}</td>
            <td style={{ textAlign: "right" }}>
              <Button small onClick={() => withPw(async (pw) => { const r = await call<any>("backup.verify", { path: `${d.folder}\\${f.name}`, password: pw }); if (!r.ok) throw new Error("verification failed"); }, "Backup verified (full test decrypt)")}>Verify</Button>
              <Button small kind="danger" onClick={() => confirm("Restore this backup? The app will sign out; restart it to finish.") && withPw((pw) => call("backup.restore", { path: `${d.folder}\\${f.name}`, password: pw }), "Restore staged - restart Personal Agent")}>Restore</Button>
            </td></tr>))}</tbody></table>
      </Card>
      {generic}
      <Card title="Restore from another file"><Button onClick={async () => { const f = await pickFile({ filters: [{ name: "Backup", extensions: ["pabk"] }] }); if (f) await withPw((pw) => call("backup.restore", { path: String(f), password: pw }), "Restore staged - restart Personal Agent"); }}>Choose .pabk file</Button></Card>
    </div>
  );
}

export function EmergencySettings({ generic }: { generic: ReactNode }) {
  const { call, toast, status, refresh } = useApp();
  const ks = status?.killswitch;
  return (
    <div className="col">
      <Card title="Current state">
        {Object.entries(ks?.levels ?? {}).filter(([k]) => k !== "activated_at").map(([k, v]: any) => (
          <div key={k} className="setting-row"><div>{k.replace(/_/g, " ")}</div><div style={{ justifySelf: "end" }}>
            {v ? <Badge tone="danger">ACTIVE</Badge> : <Button small onClick={async () => { await call("killswitch.activate", { level: k }); void refresh(); }}>Activate</Button>}</div></div>))}
        {ks?.any && <div style={{ marginTop: 12 }}><Button kind="primary" onClick={async () => { try { await call("killswitch.release", { level: "all" }); toast("Released", "ok"); void refresh(); } catch (e: any) { toast(errText(e), "danger"); } }}>Release (password required)</Button></div>}
      </Card>
      {generic}
    </div>
  );
}

export function PrivacySettings() {
  const { call, toast } = useApp();
  const [map, setMap] = useState<any[]>([]);
  const [del, setDel] = useState(false);
  const [pw, setPw] = useState("");
  const [conf, setConf] = useState("");
  useEffect(() => { call<any[]>("privacy.data_map").then(setMap).catch(() => undefined); }, [call]);
  return (
    <div className="col">
      <Card title="What is stored and where">
        <table className="table"><thead><tr><th>What</th><th>Where</th><th>Protection</th><th>Size</th></tr></thead><tbody>
          {map.map((r) => <tr key={r.what}><td>{r.what}</td><td className="mono small">{r.where}</td><td className="small">{r.protection}</td><td className="small">{bytes(r.bytes)}</td></tr>)}</tbody></table>
        <p className="small faint">No telemetry ever leaves this PC.</p>
      </Card>
      <Card title="Export my data" actions={<Button onClick={async () => { const f = await pickFile({ directory: true }); if (!f) return; try { const r = await call<any>("privacy.export", { folder: String(f) }); toast(`Exported to ${r.file}`, "ok"); } catch (e: any) { toast(errText(e), "danger"); } }}>Export (encrypted)</Button>}>
        <div className="small muted">An encrypted archive, restorable with your password.</div></Card>
      <Card title="Delete everything" actions={<Button kind="danger" onClick={() => setDel(true)}>Delete everything...</Button>}>
        <div className="small muted">Wipes the keys first (crypto-erase), then deletes all data. This cannot be undone.</div></Card>
      {del && <Modal title="Delete everything" onClose={() => setDel(false)} actions={<Button kind="danger" disabled={conf !== "DELETE EVERYTHING" || !pw}
        onClick={async () => { try { await call("privacy.delete_everything", { password: pw, confirmation: conf }); } catch (e: any) { toast(errText(e), "danger"); } }}>Delete permanently</Button>}>
        <Field label="Password"><input className="input" type="password" value={pw} onChange={(e) => setPw(e.target.value)} /></Field>
        <Field label='Type "DELETE EVERYTHING"'><input className="input" value={conf} onChange={(e) => setConf(e.target.value)} /></Field></Modal>}
    </div>
  );
}

export function DiagnosticsSettings() {
  const { call, toast } = useApp();
  const [h, setH] = useState<any>(null);
  const [about, setAbout] = useState<any>(null);
  const [bundle, setBundle] = useState<any>(null);
  useEffect(() => { call("diagnostics.health").then(setH).catch(() => undefined); call("about").then(setAbout).catch(() => undefined); }, [call]);
  return (
    <div className="col">
      <Card title="Component health">{h && (<div className="kv small">
        <span>Gateway</span><span><Badge tone="ok">running</Badge> uptime {Math.round(h.gateway.uptime_s / 60)} min</span>
        <span>Agent runtime</span><span><Badge tone={h.core.ok ? "ok" : "danger"}>{h.core.ok ? "running" : "stopped"}</Badge> {h.core.mode}</span>
        <span>Model runtime</span><span>{h.model.builtin_running ? "built-in running" : "idle"} · {h.model.models} models</span>
        <span>Outlook worker</span><span>{h.outlook_worker?.running ? "running" : "idle"}</span>
        <span>Sandbox</span><span>{h.sandbox.label ?? h.sandbox.strength}</span>
        <span>Tasks</span><span className="mono">{JSON.stringify(h.metrics.tasks ?? {})}</span>
        <span>Approvals</span><span className="mono">{JSON.stringify(h.metrics.approvals ?? {})}</span>
      </div>)}</Card>
      <Card title="Diagnostics bundle" actions={<Button onClick={async () => setBundle(await call("diagnostics.bundle"))}>Preview</Button>}>
        <div className="small muted">Metadata only - no content, no secrets. You see exactly what would be saved.</div></Card>
      {about && <Card title="About"><div className="kv small"><span>Version</span><span>{about.version}</span><span>Build</span><span className="mono">{about.build}</span>
        <span>Licences</span><span>{about.licences.join(" · ")}</span></div></Card>}
      {bundle && <Modal wide title="Diagnostics bundle preview" onClose={() => setBundle(null)} actions={<Button kind="primary" onClick={async () => { const p = await pickSavePath("personal-agent-diagnostics.json"); if (p) { await call("diagnostics.bundle", { path: p }); toast("Saved", "ok"); setBundle(null); } }}>Save</Button>}>
        <pre className="code" style={{ maxHeight: 500 }}>{JSON.stringify(bundle, null, 2)}</pre></Modal>}
    </div>
  );
}

export function UpdatesSettings({ generic }: { generic: ReactNode }) {
  const { call } = useApp();
  const [u, setU] = useState<any>(null);
  useEffect(() => { call("updates.status").then(setU).catch(() => undefined); }, [call]);
  return <div className="col"><Card title="Version">{u && <div className="small">Current: <b>{u.current}</b>. {u.note}</div>}</Card>{generic}</div>;
}

export function LogsExtras() {
  const { call, toast } = useApp();
  const [s, setS] = useState<any>(null);
  useEffect(() => { call("logs.status").then(setS).catch(() => undefined); }, [call]);
  return (
    <Card title="Log file" actions={<div className="row">
      <Button small onClick={async () => { const r = await call<any>("logs.verify"); toast(r.ok ? `Verified ${r.events} events` : `FAILED: ${r.reason}`, r.ok ? "ok" : "danger"); }}>Verify integrity now</Button>
      <Button small onClick={async () => { const p = await pickSavePath("agent-security-export.jsonl"); if (p) { const r = await call<any>("logs.export", { path: p }); toast(`Exported ${r.count} events`, "ok"); } }}>Export</Button>
      <Button small onClick={async () => { const r = await call<any>("logs.siem_test"); toast(r.ok ? "SIEM connection OK" : `SIEM: ${r.error}`, r.ok ? "ok" : "danger"); }}>Test SIEM</Button></div>}>
      {s && <div className="kv small"><span>Active file</span><span className="mono">{s.path}</span><span>Sealed segments</span><span>{s.segments.length}</span>
        <span>Integrity</span><span>{s.integrity?.ok ? <Badge tone="ok">verified</Badge> : <Badge tone="danger">{s.integrity?.reason ?? "unknown"}</Badge>}</span>
        <span>SIEM</span><span>{s.siem.enabled ? `forwarded ${s.siem.forwarded}, queued ${s.siem.queued}, last ok ${s.siem.last_ok ?? "-"}` : "off"}</span></div>}
    </Card>
  );
}

export function ToolsExtras() {
  const { call, toast } = useApp();
  const [cat, setCat] = useState<any>(null);
  const [skills, setSkills] = useState<any[]>([]);
  const [review, setReview] = useState<any>(null);
  const load = () => { call("tools.catalog").then(setCat).catch(() => undefined); call<any[]>("skills.list").then(setSkills).catch(() => undefined); };
  useEffect(() => { load(); /* eslint-disable-next-line */ }, []);
  return (
    <div className="col" style={{ marginTop: 14 }}>
      {cat && <Card title={`Tools · sandbox: ${cat.sandbox.label}`}>
        <table className="table"><thead><tr><th>Tool</th><th>Effect</th><th>Risk</th><th>Status</th></tr></thead><tbody>
          {cat.tools.map((t: any) => <tr key={t.name}><td><span className="mono">{t.name}</span><div className="small faint">{t.description}</div></td>
            <td><Badge tone={t.side_effect === "EXTERNAL_WRITE" ? "danger" : t.side_effect === "EGRESS" ? "warn" : ""}>{t.side_effect}</Badge></td>
            <td className="small">{t.risk}{t.always_approval ? " · approval" : ""}</td><td>{t.enabled ? <Badge tone="ok">enabled</Badge> : <Badge>off</Badge>}</td></tr>)}</tbody></table>
        <div className="small faint">Disable tools with "Disabled tools" above. Optional tools (drafts, sending) are enabled in Connectors.</div>
      </Card>}
      <Card title="Skills" actions={<Button small onClick={async () => { const f = await pickFile({ filters: [{ name: "Skill (JSON)", extensions: ["json"] }] }); if (f) { try { await call("skills.import", { path: String(f) }); toast("Imported for review", "ok"); load(); } catch (e: any) { toast(errText(e), "danger"); } } }}>Import skill file</Button>}>
        {!skills.length && <div className="small muted">No skills. The agent can propose reusable skills; you review them here.</div>}
        {skills.map((s) => (
          <div key={s.id} className="list-item">
            <div className="grow"><b>{s.name}</b> v{s.version} {s.tainted ? <Badge tone="danger">tainted origin</Badge> : null} {!s.signature_ok && <Badge tone="danger">signature invalid</Badge>}
              <div className="small faint">{s.definition.description}</div></div>
            <Badge tone={s.status === "ACTIVE" ? "ok" : s.status === "PROPOSED" ? "accent" : ""}>{s.status}</Badge>
            <Badge tone={s.tests.passed ? "ok" : "danger"}>{s.tests.passed ? "checks pass" : "checks fail"}</Badge>
            <Button small onClick={() => setReview(s)}>Review</Button>
          </div>))}
      </Card>
      {review && <Modal wide title={`Review skill: ${review.name} v${review.version}`} onClose={() => setReview(null)}
        actions={<>{review.status === "ACTIVE" && <Button onClick={async () => { await call("skills.set_status", { skill_id: review.id, status: "RETIRED" }); setReview(null); load(); }}>Retire</Button>}
          {review.status === "PROPOSED" && <Button onClick={async () => { await call("skills.set_status", { skill_id: review.id, status: "REJECTED" }); setReview(null); load(); }}>Reject</Button>}
          {review.status !== "ACTIVE" && <Button kind="primary" onClick={async () => { try { await call("skills.activate", { skill_id: review.id }); toast("Activated", "ok"); setReview(null); load(); } catch (e: any) { toast(errText(e), "danger"); } }}>Activate</Button>}</>}>
        {review.tainted ? <div className="banner danger">Tainted origin: the run that produced this skill had denials, rejected approvals or untrusted inputs.</div> : null}
        {(review.review.new_destinations?.length || review.review.new_recipients?.length || review.review.skip_check_instructions?.length || review.review.encoded_text) ?
          <div className="banner warn">Highlights: {[...(review.review.new_destinations ?? []), ...(review.review.new_recipients ?? []), ...(review.review.skip_check_instructions ?? [])].join(", ")} {review.review.encoded_text ? "encoded text" : ""}</div> : null}
        <div className="grid-2"><div><h3>Automated checks</h3>{review.tests.checks?.map((c: any) => <div key={c.name} className="small"><Badge tone={c.ok ? "ok" : "danger"}>{c.ok ? "ok" : "fail"}</Badge> {c.name} <span className="faint">{c.note}</span></div>)}
          <h3 style={{ marginTop: 10 }}>Lineage</h3><pre className="code">{JSON.stringify(review.lineage, null, 2)}</pre></div>
          <div><h3>Diff against active version</h3><pre className="code">{(review.review.diff ?? []).join("\n") || JSON.stringify(review.definition, null, 2)}</pre></div></div>
      </Modal>}
    </div>
  );
}

export function RulesExtras({ reload }: { reload: () => void }) {
  const { call, toast } = useApp();
  const [rules, setRules] = useState<any>(null);
  const [hist, setHist] = useState<any[]>([]);
  useEffect(() => { call("settings.rules_plain").then(setRules).catch(() => undefined); call<any[]>("settings.history").then(setHist).catch(() => undefined); }, [call]);
  return (
    <div className="col" style={{ marginTop: 14 }}>
      {rules && <Card title={`Active rules in plain language · policy ${rules.policy_version}`}>
        {rules.rules.map((r: any, i: number) => <div key={i} className="list-item"><Badge tone={r.floor ? "info" : ""}>{r.floor ? "security floor" : "your setting"}</Badge><div className="grow small">{r.rule}</div></div>)}</Card>}
      <Card title="Change history">
        <table className="table"><tbody>{hist.map((h) => <tr key={h.id}><td className="small"><Time iso={h.ts} /></td><td className="mono small">{h.key}</td>
          <td className="small">{h.before_json} → {h.after_json}</td><td><Badge tone={h.direction === "loosen" ? "warn" : ""}>{h.direction}</Badge></td>
          <td><Button small kind="ghost" onClick={async () => { try { await call("settings.apply", { changes: { [h.key]: JSON.parse(h.before_json) } }); toast("Reverted", "ok"); reload(); } catch (e: any) { toast(errText(e), "danger"); } }}>Revert</Button></td></tr>)}</tbody></table>
      </Card>
    </div>
  );
}
