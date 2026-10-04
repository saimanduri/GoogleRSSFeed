// Automatic summary ("metadata") of a file in My Files: kind, short summary, keywords and the KINDS of personal data it contains.
// Everything is editable; the assistant also keeps a memory about the file (where it is and what it is - never the secret values).
import { useEffect, useState } from "react";
import { onEvent } from "../api/gateway";
import { errText, useApp } from "../app";
import { Badge, Button, ChipInput } from "./ui";

export function FileMeta({ fileId, initial }: { fileId: string; initial: any }) {
  const { call, toast } = useApp();
  const [meta, setMeta] = useState<any>(initial);
  const [edit, setEdit] = useState(false);
  const [f, setF] = useState({ title: "", doc_type: "", summary: "", keywords: [] as string[] });
  const reload = () => call<any>("files.preview", { file_id: fileId }).then((r) => setMeta(r.meta)).catch(() => undefined);
  useEffect(() => onEvent((t, d) => { if (t === "files.changed" && d?.file_id === fileId) void reload(); }), [fileId]);       // eslint-disable-line
  const start = () => { setF({ title: meta?.title ?? "", doc_type: meta?.doc_type ?? "", summary: meta?.summary ?? "", keywords: meta?.keywords ?? [] }); setEdit(true); };
  const save = async () => {
    try { setMeta(await call("files.meta_update", { file_id: fileId, ...f })); setEdit(false); toast("Summary saved", "ok"); } catch (e: any) { toast(errText(e), "danger"); }
  };
  const again = async () => { try { await call("files.analyse", { file_id: fileId }); toast("Summarising again in the background", "info"); void reload(); } catch (e: any) { toast(errText(e), "danger"); } };
  return (
    <div className="col" style={{ gap: 6, padding: "10px 12px", border: "1px solid var(--border)", borderRadius: 12 }}>
      <div className="row wrap" style={{ gap: 6 }}><b style={{ flex: "1 1 100%" }}>Summary (automatic)</b>
        {meta?.status === "ANALYSING" && <Badge tone="info">analysing…</Badge>}
        {meta?.edited && <Badge tone="accent">edited by you</Badge>}
        {meta?.model && <span className="small faint">by {meta.model}</span>}
        {!edit && <Button small kind="ghost" icon="edit" onClick={start} disabled={!meta}>Edit</Button>}
        <Button small kind="ghost" icon="refresh" onClick={again} title="Write the summary again (replaces your edits)">Summarise again</Button></div>
      {!meta && <div className="small muted">No summary yet. It is made automatically after a file is ready (Settings &gt; Files &amp; Storage &gt; Summarise new files automatically) - or press Summarise again.</div>}
      {meta && !edit && (
        <>
          <div><b>{meta.title}</b> <Badge>{meta.doc_type || "document"}</Badge></div>
          {meta.summary && <div className="small">{meta.summary}</div>}
          {meta.keywords?.length > 0 && <div className="row wrap" style={{ gap: 4 }}>{meta.keywords.map((k: string) => <Badge key={k}>{k}</Badge>)}</div>}
          {meta.personal_data?.length > 0 && <div className="small"><Badge tone="warn">personal data</Badge> {meta.personal_data.join(", ")} <span className="faint">- the values stay in the file; the assistant's memory only knows the kinds.</span></div>}
          {meta.note && <div className="small faint">{meta.note}</div>}
        </>
      )}
      {edit && (
        <div className="col" style={{ gap: 6 }}>
          <input className="input" aria-label="Title" placeholder="Title" value={f.title} onChange={(e) => setF({ ...f, title: e.target.value })} />
          <input className="input" aria-label="Kind of document" placeholder="Kind of document (e.g. PAN card, invoice)" value={f.doc_type} onChange={(e) => setF({ ...f, doc_type: e.target.value })} />
          <textarea className="input" rows={3} aria-label="Summary" placeholder="Summary - do not type ID or account numbers here, they stay in the file" value={f.summary} onChange={(e) => setF({ ...f, summary: e.target.value })} />
          <ChipInput value={f.keywords} onChange={(k) => setF({ ...f, keywords: k })} />
          <div className="row"><div className="spacer" /><Button small onClick={() => setEdit(false)}>Cancel</Button><Button small kind="primary" onClick={save} disabled={!f.title.trim()}>Save</Button></div>
        </div>
      )}
    </div>
  );
}
