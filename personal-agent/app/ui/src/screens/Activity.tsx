// Activity: the step timeline of the last requests (runs), the unified security log with integrity status,
// and "What did the agent do between X and Y?".
import { useEffect, useState } from "react";
import { onEvent } from "../api/gateway";
import { errText, useApp } from "../app";
import { RunTimeline } from "../components/RunTimeline";
import { Badge, Button, Card, Empty, Field, Modal, Sensitivity, Tabs, Time } from "../components/ui";

export function Activity() {
  const { call, route } = useApp();
  const [tab, setTab] = useState<"runs" | "log" | "what" | "search">("runs");
  return (
    <div className="page">
      <div className="page-header"><div><h1>Activity</h1><div className="muted">Every request and every step, plus the tamper-evident security log.</div></div></div>
      <Tabs tabs={[["runs", "Requests & steps"], ["log", "Security log"], ["what", "What did the agent do?"], ["search", "Search history"]]} value={tab} onChange={setTab} />
      {tab === "runs" && <Runs initial={route.params?.ref} />}
      {tab === "log" && <SecurityLog />}
      {tab === "what" && <WhatDid />}
      {tab === "search" && <SearchHistory call={call} />}
    </div>
  );
}

function Runs({ initial }: { initial?: string }) {
  const { call, toast } = useApp();
  const [runs, setRuns] = useState<any[]>([]);
  const [archived, setArchived] = useState(false);
  const [sel, setSel] = useState<string | null>(initial ?? null);
  const [raw, setRaw] = useState<any[] | null>(null);
  const load = () => call<any[]>("runs.list", { limit: archived ? 2000 : 100, archived }).then((r) => { setRuns(r); if (!sel && r[0]) setSel(r[0].id); }).catch(() => undefined);
  useEffect(() => { void load(); return onEvent((t) => (t === "run.started" || t === "run.finished") && void load()); /* eslint-disable-next-line */ }, [archived]);
  return (
    <div className="grid-2" style={{ gridTemplateColumns: "minmax(320px, 1fr) 1.3fr" }}>
      <Card title={`Last ${archived ? "all" : "100"} requests`} actions={<Button small kind="ghost" onClick={() => setArchived(!archived)}>{archived ? "Recent only" : "Include archived"}</Button>}>
        {!runs.length ? <Empty title="No requests yet" icon="activity" /> : (
          <div className="list" style={{ maxHeight: "68vh", overflowY: "auto" }}>{runs.map((r) => (
            <div key={r.id} className={`list-item clickable ${sel === r.id ? "selected" : ""}`} onClick={() => setSel(r.id)}>
              <Badge tone={r.status === "COMPLETED" ? "ok" : r.status === "FAILED" ? "danger" : r.status === "RUNNING" ? "accent" : "warn"}>{r.kind}</Badge>
              <div className="grow"><div className="ellipsis">{r.title}</div><div className="small faint"><Time iso={r.started_at} /> · {r.tool_calls} tools · {r.tokens_in + r.tokens_out} tok</div></div>
              <Sensitivity level={r.hwm} />
            </div>))}</div>
        )}
      </Card>
      <Card title="Steps" actions={sel && <Button small onClick={async () => { try { setRaw(await call("runs.transcript", { run_id: sel })); } catch (e: any) { toast(errText(e), "danger"); } }}>Full transcript</Button>}>
        <RunTimeline runId={sel} />
      </Card>
      {raw && (
        <Modal wide title="Full transcript (encrypted Transcript Store)" onClose={() => setRaw(null)}>
          <p className="small muted">Everything that entered or left the model for this request, in order. Nothing reaches the model unless it is recorded here first.</p>
          <div className="col">{raw.map((e) => (
            <div key={e.seq} className="card flat"><div className="row small"><Badge>{e.kind}</Badge><Badge tone={e.trust === "UNTRUSTED" ? "danger" : e.trust === "TRUSTED" ? "ok" : "warn"}>{e.trust}</Badge>
              <Sensitivity level={e.sensitivity} /><span className="faint">{e.source} · #{e.seq}</span><div className="spacer" />
              {e.kind === "llm.request" && <ReplayButton id={e.id} />}</div>
              <pre className="code" style={{ marginTop: 6 }}>{e.content.slice(0, 20000)}</pre></div>))}</div>
        </Modal>
      )}
    </div>
  );
}

function ReplayButton({ id }: { id: string }) {
  const { call, toast } = useApp();
  const [res, setRes] = useState<any>(null);
  return (<>
    <Button small onClick={async () => { try { setRes(await call("runs.replay", { event_id: id })); } catch (e: any) { toast(errText(e), "danger"); } }}>Replay step</Button>
    {res && <Modal wide title={`Replay with ${res.model}`} onClose={() => setRes(null)}><div className="grid-2"><div><h3>Original</h3><pre className="code">{res.original}</pre></div><div><h3>Replayed</h3><pre className="code">{res.replayed}</pre></div></div></Modal>}
  </>);
}

function SecurityLog() {
  const { call, toast } = useApp();
  const [d, setD] = useState<any>({ events: [] });
  const [f, setF] = useState<any>({});
  const [sel, setSel] = useState<any>(null);
  const load = () => call("activity.events", { limit: 500, filters: f }).then(setD).catch(() => undefined);
  useEffect(() => { void load(); /* eslint-disable-next-line */ }, [f]);
  return (
    <Card>
      <div className="row wrap" style={{ marginBottom: 10 }}>
        {d.integrity && <Badge tone={d.integrity.ok ? "ok" : "danger"}>{d.integrity.ok ? `Log chain verified (${d.integrity.events} events)` : `LOG CHAIN BROKEN: ${d.integrity.reason}`}</Badge>}
        <select className="input" style={{ width: 180 }} value={f.category ?? ""} onChange={(e) => setF({ ...f, category: e.target.value || undefined })}>
          <option value="">All categories</option>{["authentication", "authorization", "execution", "egress", "dlp", "approval", "configuration", "connector", "killswitch", "security", "file", "secrets", "model", "task", "mission", "backup"].map((c) => <option key={c}>{c}</option>)}
        </select>
        <select className="input" style={{ width: 150 }} value={f.severity ?? ""} onChange={(e) => setF({ ...f, severity: e.target.value || undefined })}>
          <option value="">Any severity</option>{["info", "medium", "high", "critical"].map((c) => <option key={c}>{c}</option>)}</select>
        <input className="input" style={{ width: 170 }} placeholder="tool (e.g. web.fetch)" value={f.tool ?? ""} onChange={(e) => setF({ ...f, tool: e.target.value || undefined })} />
        <div className="spacer" />
        <Button small icon="shield" onClick={async () => { const r = await call<any>("logs.verify"); toast(r.ok ? "Integrity verified" : `Integrity FAILED: ${r.reason}`, r.ok ? "ok" : "danger"); void load(); }}>Verify integrity</Button>
      </div>
      <table className="table"><thead><tr><th>#</th><th>Time</th><th>Event</th><th>Category</th><th>Details</th></tr></thead><tbody>
        {d.events.map((e: any) => (
          <tr key={e.event_id} style={{ cursor: "pointer" }} onClick={() => setSel(e)}>
            <td className="small faint">{e.sequence}</td><td className="small"><Time iso={e.timestamp} /></td>
            <td><Badge tone={e.severity === "high" || e.severity === "critical" ? "danger" : e.severity === "medium" ? "warn" : ""}>{e.event_type}</Badge></td>
            <td className="small">{e.category}</td>
            <td className="small faint ellipsis" style={{ maxWidth: 380 }}>{[e.tool, e.policy_decision, e.decision_reason, e.connector, e.reason].filter(Boolean).join(" · ")}</td>
          </tr>))}
      </tbody></table>
      {sel && <Modal wide title={sel.event_type} onClose={() => setSel(null)}><pre className="code">{JSON.stringify(sel, null, 2)}</pre></Modal>}
    </Card>
  );
}

function WhatDid() {
  const { call, toast } = useApp();
  const iso = (d: Date) => new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
  const [from, setFrom] = useState(iso(new Date(Date.now() - 12 * 3600e3)));
  const [to, setTo] = useState(iso(new Date()));
  const [r, setR] = useState<any>(null);
  return (
    <Card>
      <div className="row wrap" style={{ alignItems: "flex-end" }}>
        <Field label="From"><input className="input" type="datetime-local" value={from} onChange={(e) => setFrom(e.target.value)} /></Field>
        <Field label="To"><input className="input" type="datetime-local" value={to} onChange={(e) => setTo(e.target.value)} /></Field>
        <Button kind="primary" onClick={async () => { try { setR(await call("activity.what_did_agent_do", { since: new Date(from).toISOString(), until: new Date(to).toISOString() })); } catch (e: any) { toast(errText(e), "danger"); } }}>Show</Button>
      </div>
      {r && (
        <div className="grid-2" style={{ marginTop: 14 }}>
          <div><h3>Requests ({r.runs.length})</h3>{r.runs.map((x: any) => <div key={x.id} className="small">• {x.title} <Badge>{x.status}</Badge></div>)}
            <h3 style={{ marginTop: 12 }}>Tool calls ({r.tool_calls.length})</h3>{r.tool_calls.slice(0, 100).map((t: any, i: number) => <div key={i} className="small mono">{new Date(t.at).toLocaleTimeString()} {t.tool} {t.destination ?? ""}</div>)}</div>
          <div><h3>Blocked / denied ({r.denied.length})</h3>{r.denied.map((t: any, i: number) => <div key={i} className="small">{new Date(t.at).toLocaleTimeString()} <span className="mono">{t.tool}</span> - {t.reason ?? t.code}</div>)}
            <h3 style={{ marginTop: 12 }}>Event counts</h3>{Object.entries(r.counts).map(([k, v]: any) => <div key={k} className="small row"><span className="grow mono">{k}</span><b>{v}</b></div>)}</div>
        </div>
      )}
    </Card>
  );
}

function SearchHistory({ call }: { call: any }) {
  const [q, setQ] = useState("");
  const [hits, setHits] = useState<any[]>([]);
  return (
    <Card>
      <div className="row"><input className="input grow" placeholder="Search chats, results and files (encrypted full-text index)" value={q} onChange={(e) => setQ(e.target.value)}
        onKeyDown={async (e) => e.key === "Enter" && setHits(await call("history.search", { query: q, limit: 50 }))} />
        <Button kind="primary" onClick={async () => setHits(await call("history.search", { query: q, limit: 50 }))}>Search</Button></div>
      <div className="list" style={{ marginTop: 10 }}>{hits.map((h, i) => <div key={i} className="list-item"><Badge>{h.kind}</Badge><div className="grow"><b>{h.title}</b><div className="small muted">{h.snippet}</div></div><span className="small faint"><Time iso={h.created_at} /></span></div>)}</div>
      {!hits.length && q && <div className="faint small" style={{ marginTop: 10 }}>No results (deleted items are never shown).</div>}
    </Card>
  );
}
