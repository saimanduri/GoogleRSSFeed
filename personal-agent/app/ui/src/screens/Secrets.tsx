// Local password/secret vault (spec 6). Values need step-up to reveal/copy, auto-hide after 20 s, the window is
// excluded from screen capture while a value is visible, and the clipboard is excluded from history and cleared.
import { SearchBox } from "../components/SearchBox";
import { useEffect, useState } from "react";
import { native, pickFile, pickSavePath } from "../api/gateway";
import { errText, useApp } from "../app";
import { Badge, Button, Card, ChipInput, Empty, Field, Modal, Time } from "../components/ui";

const TYPES = [["password", "Password"], ["api_key", "API key"], ["token", "Token"], ["note", "Secure note"], ["card", "Card-like data"]];

export function Secrets() {
  const { call, toast } = useApp();
  const [list, setList] = useState<any[]>([]);
  const [q, setQ] = useState("");
  const [edit, setEdit] = useState<any>(null);
  const [shown, setShown] = useState<any>(null);
  const [health, setHealth] = useState<any>(null);
  const load = () => call<any[]>("secrets.list").then(setList).catch(() => undefined);
  useEffect(() => { void load(); call("secrets.health").then(setHealth).catch(() => undefined); /* eslint-disable-next-line */ }, []);
  const reveal = async (s: any) => {
    try {
      const item = await call<any>("secrets.reveal", { id: s.id });
      await native("set_capture_protection", { enabled: true });
      setShown(item);
      setTimeout(() => { setShown(null); void native("set_capture_protection", { enabled: false }); }, (item.hide_after_seconds ?? 20) * 1000);
    } catch (e: any) { toast(errText(e), "danger"); }
  };
  const copy = async (s: any) => { try { const r = await call<any>("secrets.copy", { id: s.id }); toast(`Copied - clipboard clears in ${r.cleared_in}s and is kept out of clipboard history`, "ok"); } catch (e: any) { toast(errText(e), "danger"); } };
  const filtered = list.filter((s) => `${s.title} ${s.username} ${s.url} ${s.tags.join(" ")}`.toLowerCase().includes(q.toLowerCase()));
  return (
    <div className="page">
      <div className="page-header">
        <div className="grow"><h1>Secrets</h1><div className="muted">Your passwords and keys, encrypted on this PC. The agent never sees these values; a bound secret is injected by the gateway only into its tool's request.</div></div>
        <SearchBox style={{ width: 220 }} placeholder="Search" value={q} onChange={setQ} />
        <Button onClick={async () => { const p = await pickFile({ filters: [{ name: "CSV", extensions: ["csv"] }] }); if (p) { try { const r = await call<any>("secrets.import_csv", { path: p, secure_delete: confirm("Securely delete the CSV after import? (recommended)") }); toast(`Imported ${r.imported}`, "ok"); void load(); } catch (e: any) { toast(errText(e), "danger"); } } }}>Import CSV</Button>
        <Button onClick={async () => { const p = await pickSavePath("secrets-export.json"); if (p) { try { const r = await call<any>("secrets.export", { path: p }); toast(`Exported ${r.count} (encrypted with your password)`, "ok"); } catch (e: any) { toast(errText(e), "danger"); } } }}>Export</Button>
        <Button kind="primary" icon="plus" onClick={() => setEdit({ type: "password", title: "", value: "", tags: [], bindings: [] })}>Add</Button>
      </div>
      {health && (health.weak.length + health.reused.length + health.old.length > 0) && (
        <div className="banner warn">Health: {health.weak.length} weak, {health.reused.length} reused, {health.old.length} older than a year.</div>
      )}
      <Card>
        {!filtered.length ? <Empty icon="secrets" title="No secrets yet" /> : (
          <div className="list">{filtered.map((s) => (
            <div key={s.id} className="list-item">
              <div className="grow"><div style={{ fontWeight: 600 }}>{s.title}</div>
                <div className="small faint">{TYPES.find((t) => t[0] === s.type)?.[1]} {s.username && `· ${s.username}`} {s.url && `· ${s.url}`} · updated <Time iso={s.updated_at} /></div></div>
              {s.bindings.map((b: string) => <Badge key={b} tone="accent">used by {b}</Badge>)}
              {s.tags.map((t: string) => <Badge key={t}>{t}</Badge>)}
              <Button small icon="eye" onClick={() => reveal(s)}>Reveal</Button>
              <Button small icon="copy" onClick={() => copy(s)}>Copy</Button>
              <Button small kind="ghost" icon="edit" onClick={async () => { try { const item = await call<any>("secrets.reveal", { id: s.id }); setEdit({ ...item, id: s.id, bindings: s.bindings }); } catch (e: any) { toast(errText(e), "danger"); } }} />
            </div>))}</div>
        )}
      </Card>
      {shown && (
        <Modal title={shown.title} onClose={() => { setShown(null); void native("set_capture_protection", { enabled: false }); }}>
          <div className="rk" style={{ fontSize: "1.1em", wordBreak: "break-all" }}>{shown.value}</div>
          <p className="small faint" style={{ marginTop: 8 }}>Hidden automatically in 20 seconds. This window is excluded from screenshots and screen sharing while visible.</p>
        </Modal>
      )}
      {edit && <SecretEditor s={edit} onClose={() => setEdit(null)} onSaved={() => { setEdit(null); void load(); }} />}
    </div>
  );
}

function SecretEditor({ s, onClose, onSaved }: { s: any; onClose: () => void; onSaved: () => void }) {
  const { call, toast } = useApp();
  const [v, setV] = useState<any>(s);
  const [targets, setTargets] = useState<string[]>([]);
  const [len, setLen] = useState(20);
  useEffect(() => { call<string[]>("secrets.binding_targets").then(setTargets).catch(() => undefined); }, [call]);
  const set = (k: string, val: any) => setV({ ...v, [k]: val });
  const save = async () => {
    try {
      if (v.id) { await call("secrets.update", { id: v.id, item: v }); await call("secrets.set_bindings", { id: v.id, bindings: v.bindings ?? [] }); }
      else await call("secrets.create", { item: v, bindings: v.bindings ?? [] });
      toast("Saved", "ok"); onSaved();
    } catch (e: any) { toast(errText(e), "danger"); }
  };
  return (
    <Modal title={v.id ? "Edit secret" : "Add secret"} onClose={onClose} wide
      actions={<>{v.id && <Button kind="danger" onClick={async () => { if (confirm("Delete this secret?")) { await call("secrets.delete", { id: v.id }); onSaved(); } }}>Delete</Button>}<div className="spacer" />
        <Button onClick={onClose}>Cancel</Button><Button kind="primary" disabled={!v.title || !v.value} onClick={save}>Save</Button></>}>
      <div className="grid-2">
        <div className="col">
          <Field label="Title"><input className="input" value={v.title} onChange={(e) => set("title", e.target.value)} /></Field>
          <Field label="Type"><select className="input" value={v.type} onChange={(e) => set("type", e.target.value)}>{TYPES.map(([k, l]) => <option key={k} value={k}>{l}</option>)}</select></Field>
          <Field label="Username"><input className="input" value={v.username ?? ""} onChange={(e) => set("username", e.target.value)} /></Field>
          <Field label="URL"><input className="input" value={v.url ?? ""} onChange={(e) => set("url", e.target.value)} /></Field>
        </div>
        <div className="col">
          <Field label="Value"><div className="row"><input className="input mono" type="password" value={v.value} onChange={(e) => set("value", e.target.value)} />
            <input className="input" type="number" style={{ width: 70 }} min={8} max={128} value={len} onChange={(e) => setLen(+e.target.value)} />
            <Button onClick={async () => set("value", (await call<any>("secrets.generate", { length: len })).value)}>Generate</Button></div></Field>
          <Field label="Notes"><textarea className="input" rows={3} value={v.notes ?? ""} onChange={(e) => set("notes", e.target.value)} /></Field>
          <Field label="Tags"><ChipInput value={v.tags ?? []} onChange={(t) => set("tags", t)} /></Field>
          <Field label="Used by (bindings)" help="Only these tools/connectors may use the value - injected by the gateway, never shown to the model. Unbound secrets are never used by the agent.">
            <div className="col" style={{ gap: 4 }}>{targets.map((t) => (
              <label key={t} className="row small"><input type="checkbox" checked={(v.bindings ?? []).includes(t)}
                onChange={(e) => set("bindings", e.target.checked ? [...(v.bindings ?? []), t] : (v.bindings ?? []).filter((x: string) => x !== t))} /><span className="mono">{t}</span></label>))}</div>
          </Field>
        </div>
      </div>
    </Modal>
  );
}
