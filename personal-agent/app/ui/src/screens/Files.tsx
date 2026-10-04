import { useEffect, useState } from "react";
import { SearchBox } from "../components/SearchBox";
import { FileMeta } from "../components/FileMeta";
import { onEvent, pickFile, pickSavePath } from "../api/gateway";
import { errText, useApp } from "../app";
import { Badge, Button, bytes, Card, ChipInput, Empty, Modal, Sensitivity, Tabs, Time } from "../components/ui";

const LEVELS = ["PUBLIC", "INTERNAL", "CONFIDENTIAL", "RESTRICTED"];

export function Files() {
  const { call, toast, isHidden } = useApp();
  const [data, setData] = useState<any>({ files: [], storage: {} });
  const [q, setQ] = useState("");
  const [tab, setTab] = useState<"all" | "quarantine">("all");
  const [sel, setSel] = useState<any>(null);
  const [drag, setDrag] = useState(false);
  const load = () => call("files.list", { query: q }).then(setData).catch(() => undefined);
  useEffect(() => { void load(); return onEvent((t) => t === "files.changed" && void load()); /* eslint-disable-next-line */ }, [q]);
  const upload = async (paths: string[] | string | null) => {
    if (!paths) return;
    for (const p of Array.isArray(paths) ? paths : [paths]) {
      try { await call("files.upload", { path: p }); } catch (e: any) { toast(errText(e), "danger"); }
    }
    toast("Uploaded - scanning in quarantine first", "info");
    void load();
  };
  const onDrop = async (e: React.DragEvent) => {
    e.preventDefault();
    setDrag(false);
    for (const f of Array.from(e.dataTransfer.files)) {
      const buf = await f.arrayBuffer();
      const b64 = btoa(Array.from(new Uint8Array(buf), (c) => String.fromCharCode(c)).join(""));
      try { await call("files.upload", { name: f.name, data_b64: b64 }); } catch (er: any) { toast(errText(er), "danger"); }
    }
    void load();
  };
  const held = data.files.filter((f: any) => f.status === "REJECTED" && f.scan_json?.antivirus?.unavailable);
  const release = async (list: any[]) => {
    let ok = 0;
    for (const f of list) {
      try { await call("files.release_unscanned", { file_id: f.id }); ok++; } catch (e: any) { toast(errText(e), "danger"); break; }
    }
    if (ok) { toast(`${ok} file${ok === 1 ? "" : "s"} accepted without antivirus scan - marked as not scanned`, "warn"); void load(); }
  };
  const files = data.files.filter((f: any) => !isHidden(`file:${f.id}`)).filter((f: any) => (tab === "quarantine" ? f.status === "REJECTED" : f.status !== "REJECTED"));
  return (
    <div className="page" onDragOver={(e) => { e.preventDefault(); setDrag(true); }} onDragLeave={() => setDrag(false)} onDrop={onDrop}>
      <div className="page-header">
        <div className="grow"><h1>My Files</h1><div className="muted">Every file is quarantined, scanned and parsed in an isolated process before the agent can read it.</div></div>
        <SearchBox style={{ width: 240 }} placeholder="Search name or tag" value={q} onChange={setQ} />
        <Button kind="primary" icon="upload" onClick={async () => upload(await pickFile({ multiple: true }))}>Upload</Button>
      </div>
      <div className="row small muted" style={{ marginBottom: 10 }}>{bytes(data.storage.used_bytes ?? 0)} of {bytes(data.storage.quota_bytes ?? 0)} used · {data.storage.count ?? 0} files</div>
      <Tabs tabs={[["all", "Files"], ["quarantine", `Quarantine (${data.storage.quarantined ?? 0})`]]} value={tab} onChange={setTab} />
      {tab === "quarantine" && held.length > 0 && (
        <div className="banner warn" role="alert" style={{ alignItems: "center", marginBottom: 10 }}>
          <div className="grow"><b>{held.length} file{held.length === 1 ? " is" : "s are"} held because no antivirus could scan {held.length === 1 ? "it" : "them"}.</b>
            <div className="small">Microsoft Defender is turned off (another antivirus is active) and that antivirus cannot be asked from here. The files passed no malware check, so they wait here.
              You can accept them one by one, all at once, or switch on "Accept files that no antivirus could scan" in Settings &gt; Files &amp; Storage. Accepted files are marked <i>not antivirus-scanned</i>;
              they are still checked for type, structure and active content and only opened by the isolated reader. Keep your antivirus's real-time protection on.</div></div>
          <Button small onClick={() => { if (confirm(`Accept all ${held.length} held files without an antivirus scan?`)) void release(held); }}>Accept all {held.length}</Button>
        </div>
      )}
      <Card className={drag ? "drag" : ""}>
        {!files.length ? <Empty icon="files" title={tab === "all" ? "Drop files here or click Upload" : "Quarantine is empty"} /> : (
          <table className="table"><thead><tr><th>Name</th><th>Folder</th><th>Label</th><th>Status</th><th>Size</th><th>Added</th></tr></thead><tbody>
            {files.map((f: any) => (
              <tr key={f.id} style={{ cursor: "pointer" }} onClick={async () => setSel(await call("files.preview", { file_id: f.id }))}>
                <td><b>{f.name}</b>{f.in_knowledge ? <Badge tone="accent">knowledge</Badge> : null}
                  {f.meta?.doc_type && <div className="small"><Badge>{f.meta.doc_type}</Badge> <span className="faint">{(f.meta.summary || "").slice(0, 90)}</span></div>}
                  {f.meta?.status === "ANALYSING" && <div className="small faint">summarising…</div>}</td><td className="small">{f.folder}</td>
                <td><Sensitivity level={f.sensitivity} /></td>
                <td><Badge tone={f.status === "READY" ? "ok" : f.status === "REJECTED" ? "danger" : "info"}>{f.status}</Badge>{f.status_reason && <div className="small faint">{f.status_reason}</div>}
                  {f.status === "REJECTED" && f.scan_json?.antivirus?.unavailable && <span onClick={(e) => e.stopPropagation()}><Button small onClick={() => void release([f])}>Allow without antivirus scan</Button></span>}</td>
                <td className="small">{bytes(f.size_bytes)}</td><td className="small"><Time iso={f.created_at} /></td>
              </tr>))}
          </tbody></table>
        )}
      </Card>
      {sel && <FileDialog f={sel} onClose={() => setSel(null)} onChanged={() => { void load(); }} />}
    </div>
  );
}

function FileDialog({ f, onClose, onChanged }: { f: any; onClose: () => void; onChanged: () => void }) {
  const { call, toast, deferDelete } = useApp();
  const [tags, setTags] = useState<string[]>(f.tags_json ?? []);
  const [folder, setFolder] = useState(f.folder);
  const act = async (fn: () => Promise<any>, ok: string) => { try { await fn(); toast(ok, "ok"); onChanged(); } catch (e: any) { toast(errText(e), "danger"); } };
  return (
    <Modal wide title={f.name} onClose={onClose}>
      <div className="grid-2">
        <div className="col">
          <div className="kv small">
            <span>Status</span><span>{f.status} {f.status_reason && `- ${f.status_reason}`}</span>
            <span>Source</span><span>{f.source}</span><span>SHA-256</span><span className="mono ellipsis">{f.sha256}</span>
            <span>Scan</span><span>{f.scan_json?.antivirus?.engine ?? "-"} {f.scan_json?.antivirus?.clean ? "clean" : ""}</span>
            <span>Type</span><span>{f.sniffed_type ?? f.declared_type}</span>
          </div>
          <FileMeta fileId={f.id} initial={f.meta} />
          <label className="small"><b>Label</b></label>
          <select className="input" value={LEVELS[f.sensitivity]} onChange={(e) => act(() => call("files.set_label", { file_id: f.id, level: e.target.value }), "Label changed")}>
            {LEVELS.map((l) => <option key={l}>{l}</option>)}</select>
          <label className="small"><b>Folder</b></label>
          <input className="input" value={folder} onChange={(e) => setFolder(e.target.value)} onBlur={() => act(() => call("files.update", { file_id: f.id, folder }), "Moved")} />
          <label className="small"><b>Tags</b></label>
          <ChipInput value={tags} onChange={(t) => { setTags(t); void act(() => call("files.update", { file_id: f.id, tags: t }), "Tags saved"); }} />
          <div className="row wrap">
            <Button small onClick={() => act(() => call("files.update", { file_id: f.id, in_knowledge: !f.in_knowledge }), f.in_knowledge ? "Removed from knowledge" : "Added to My Knowledge")}>
              {f.in_knowledge ? "Remove from My Knowledge" : "Add to My Knowledge"}</Button>
            {f.status === "READY" && (f.scan_json?.vision || /^(png|jpeg|gif|riff)$/.test(f.sniffed_type ?? "")) && <Button small icon="eye" title="Read this picture or scan again with the vision model (after adding or switching one on)"
              onClick={() => act(() => call("files.reread", { file_id: f.id }), "Read again with the vision model")}>Read with vision model</Button>}
            {f.status === "READY" && <Button small icon="download" onClick={async () => { const p = await pickSavePath(f.name); if (p) await act(() => call("files.save_copy", { file_id: f.id, path: p }), "Saved a copy"); }}>Save a copy</Button>}
            <Button small kind="danger" icon="trash" onClick={() => { deferDelete({ key: `file:${f.id}`, label: `Deleted ${f.name} and everything derived from it`, commit: () => call("files.delete", { file_id: f.id }), after: onChanged }); onClose(); }}>Delete</Button>
          </div>
          {f.hidden_json?.length > 0 && (<><h3>Hidden content (labelled, untrusted)</h3>{f.hidden_json.slice(0, 20).map((h: any, i: number) => <div key={i} className="small"><Badge tone="warn">{h.kind}</Badge> {String(h.text).slice(0, 200)}</div>)}</>)}
        </div>
        <div><h3>Preview (text only, no scripts)</h3><pre className="code" style={{ maxHeight: 460 }}>{f.text ?? "Not available until the file passes the checks."}</pre></div>
      </div>
    </Modal>
  );
}
