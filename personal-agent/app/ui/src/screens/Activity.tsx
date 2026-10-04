// Activity: the step timeline of the last requests (runs), the unified security log with integrity status,
// and "What did the agent do between X and Y?".
import { SearchBox } from "../components/SearchBox";
import { useEffect, useState } from "react";
import { onEvent } from "../api/gateway";
import { errText, useApp } from "../app";
import { RunTimeline } from "../components/RunTimeline";
import { NetworkLogs } from "./NetworkLogs";
import { Badge, Button, Card, Empty, Field, Modal, Sensitivity, Tabs, Time } from "../components/ui";

function Usage() {
  const { call, go } = useApp();
  const [d, setD] = useState<any>(null);
  useEffect(() => { call<any>("home.summary").then((x) => setD(x.budget)).catch(() => setD(false)); /* eslint-disable-next-line */ }, []);
  if (d === null) return <div className="skeleton" style={{ height: 160 }} />;
  if (!d) return <Empty icon="activity" title="Usage is not available right now" />;
  const rows: [string, string, (n: number) => string][] = [
    ["tokens", "Model tokens (AI answers)", (n) => n.toLocaleString()], ["web_requests", "Web requests", (n) => n.toLocaleString()],
    ["egress_bytes", "Data sent out", (n) => (n >= 1e6 ? `${(n / 1e6).toFixed(1)} MB` : `${Math.round(n / 1e3)} KB`)], ["tool_calls", "Tool calls", (n) => n.toLocaleString()],
  ];
  return (
    <Card title="Today's usage and budgets" icon="activity">
      <div className="small muted" style={{ marginBottom: 10 }}>Counted per day. Local models cost nothing, but the budget keeps runaway loops in check. Change the limits in Settings &gt; Autonomy &amp; Budgets.</div>
      {rows.filter(([k]) => d.limits?.[k]).map(([k, label, fmt]) => {
        const used = d.used?.[k] ?? 0, limit = d.limits[k], pct = Math.min(100, Math.round((used / limit) * 100));
        return (
          <div key={k} style={{ marginBottom: 12 }}>
            <div className="row small"><span className="grow">{label}</span><span className="faint">{fmt(used)} of {fmt(limit)} · {pct}%</span></div>
            <div className="meter"><div style={{ width: `${pct}%`, background: pct > 80 ? "var(--warn)" : "var(--accent)" }} /></div>
          </div>
        );
      })}
      <Button small onClick={() => go("settings", { section: "autonomy" })}>Change limits</Button>
    </Card>
  );
}

export function Activity() {
  const { call, route } = useApp();
  const [tab, setTab] = useState<"runs" | "log" | "what" | "search" | "network" | "usage">((route.params?.tab as any) ?? "runs");
  useEffect(() => { if (route.params?.tab) setTab(route.params.tab); }, [route.params?.tab]);
  return (
    <div className="page">
      <div className="page-header"><div><h1>Activity</h1><div className="muted">Every request and every step (with what is running now, Stop and Resume), plus the tamper-evident security log.</div></div></div>
      <Tabs tabs={[["runs", "Requests & steps"], ["log", "Security log"], ["what", "What did the agent do?"], ["search", "Search history"], ["network", "Network Logs"], ["usage", "Usage & budgets"]]} value={tab} onChange={setTab} />
      {tab === "runs" && <Runs initial={route.params?.ref} />}
      {tab === "log" && <SecurityLog />}
      {tab === "what" && <WhatDid />}
      {tab === "search" && <SearchHistory call={call} />}
      {tab === "network" && <NetworkLogs />}
      {tab === "usage" && <Usage />}
    </div>
  );
}

const TASK_TONE: Record<string, string> = { RUNNING: "accent", QUEUED: "info", COMPLETED: "ok", FAILED: "danger", CANCELLED: "", TIMED_OUT: "danger",
  WAITING_FOR_APPROVAL: "warn", WAITING_FOR_RESOURCE: "warn", OUTCOME_UNKNOWN: "danger", PAUSED: "warn", SUSPENDED: "warn" };
const ACTIVE = ["RUNNING", "QUEUED", "WAITING_FOR_APPROVAL", "WAITING_FOR_RESOURCE", "PAUSED", "SUSPENDED", "OUTCOME_UNKNOWN"];
const STOPPABLE = ["RUNNING", "QUEUED", "WAITING_FOR_APPROVAL"];
const RESUMABLE = ["WAITING_FOR_RESOURCE", "SUSPENDED", "PAUSED"];
const words = (s: string) => s.toLowerCase().replace(/_/g, " ");

function TaskButtons({ t, onDone }: { t: any; onDone: () => void }) {
  const { call, toast } = useApp();
  const act = async (m: string, ok: string) => { try { await call(m, { task_id: t.id }); toast(ok, "ok"); onDone(); } catch (e: any) { toast(errText(e), "danger"); } };
  return <>
    {STOPPABLE.includes(t.state) && <Button small kind="danger" onClick={(e?: any) => { e?.stopPropagation?.(); void act("tasks.stop", "Stopped"); }}>Stop</Button>}
    {RESUMABLE.includes(t.state) && <Button small onClick={(e?: any) => { e?.stopPropagation?.(); void act("tasks.resume", "Resumed"); }}>Resume</Button>}
  </>;
}

/** What used to be the Tasks screen, for the selected request: state, trigger, budget, state history, Stop / Resume. */
function TaskPanel({ taskId, onChanged }: { taskId: string; onChanged: () => void }) {
  const { call } = useApp();
  const [t, setT] = useState<any>(null);
  const [hist, setHist] = useState(false);
  const load = () => call<any>("tasks.get", { task_id: taskId }).then(setT).catch(() => setT(null));
  useEffect(() => { void load(); return onEvent((topic) => topic === "tasks.changed" && void load()); /* eslint-disable-next-line */ }, [taskId]);
  if (!t) return null;
  return (
    <div className="task-panel">
      <div className="row wrap" style={{ gap: 8 }}>
        <Badge tone={TASK_TONE[t.state]}>{words(t.state)}</Badge>
        <span className="small muted">started by {words(t.trigger_type ?? "user")}</span>
        <div className="spacer" /><TaskButtons t={t} onDone={() => { void load(); onChanged(); }} />
      </div>
      {t.error && <div className="banner danger small" style={{ marginTop: 8 }}>{t.error}</div>}
      <div className="task-budget">{Object.entries(t.limits ?? {}).map(([k, v]: any) => {
        const used = Math.round(t.usage?.[k] ?? 0), pct = v ? Math.min(100, Math.round((used / v) * 100)) : 0;
        return <div key={k} className="small"><div className="row"><span className="grow">{words(k)}</span><span className="faint">{used.toLocaleString()} / {Math.round(v).toLocaleString()}</span></div>
          <div className="meter"><div style={{ width: `${pct}%`, background: pct > 80 ? "var(--warn)" : "var(--accent)" }} /></div></div>;
      })}</div>
      {t.transitions?.length > 0 && <button className="link-btn small" onClick={() => setHist(!hist)}>{hist ? "Hide" : "Show"} state history ({t.transitions.length})</button>}
      {hist && <div className="col" style={{ gap: 2, marginTop: 4 }}>{t.transitions.map((tr: any) => <div key={tr.id} className="small"><Badge tone={TASK_TONE[tr.to_state]}>{words(tr.to_state)}</Badge> <span className="faint">{tr.reason}</span> · <Time iso={tr.ts} /></div>)}</div>}
    </div>
  );
}

function Runs({ initial }: { initial?: string }) {
  const { call, toast } = useApp();
  const [runs, setRuns] = useState<any[]>([]);
  const [active, setActive] = useState<any[]>([]);
  const [archived, setArchived] = useState(false);
  const [sel, setSel] = useState<string | null>(initial ?? null);
  const [raw, setRaw] = useState<any[] | null>(null);
  const load = () => call<any[]>("runs.list", { limit: archived ? 2000 : 100, archived }).then((r) => { setRuns(r); if (!sel && r[0]) setSel(r[0].id); }).catch(() => undefined);
  const loadActive = () => call<any[]>("tasks.list", { states: ACTIVE, limit: 50 }).then(setActive).catch(() => undefined);
  useEffect(() => { void load(); void loadActive(); return onEvent((t) => { if (t === "run.started" || t === "run.finished") void load(); if (t === "tasks.changed" || t === "run.started" || t === "run.finished") void loadActive(); }); /* eslint-disable-next-line */ }, [archived]);
  const selRun = runs.find((r) => r.id === sel);
  return (
    <div className="col">
    {active.length > 0 && <Card title={`Active now (${active.length})`}>
      <div className="list">{active.map((t) => (
        <div key={t.id} className={`list-item ${t.run_id ? "clickable" : ""}`} onClick={() => t.run_id && setSel(t.run_id)}>
          <Badge tone={TASK_TONE[t.state]}>{words(t.state)}</Badge>
          <div className="grow"><div className="ellipsis">{t.objective}</div><div className="small faint">{words(t.trigger_type ?? "user")} · <Time iso={t.updated_at} /> · {t.usage?.tool_calls ?? 0}/{t.limits?.tool_calls} tools</div></div>
          <TaskButtons t={t} onDone={() => { void loadActive(); void load(); }} />
        </div>))}</div>
    </Card>}
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
        {selRun?.task_id && <TaskPanel taskId={selRun.task_id} onChanged={() => { void load(); void loadActive(); }} />}
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
  const run = async () => { if (q.trim().length >= 2) setHits(await call("history.search", { query: q.trim(), limit: 50 })); else setHits([]); };
  useEffect(() => { const t = setTimeout(() => void run(), 250); return () => clearTimeout(t); /* eslint-disable-next-line */ }, [q]);
  return (
    <Card>
      <div className="row"><SearchBox className="grow" placeholder="Search chats, results and files (word beginnings work; results appear as you type)" value={q} onChange={setQ} onEnter={run} />
        <Button kind="primary" onClick={run}>Search</Button></div>
      <div className="list" style={{ marginTop: 10 }}>{hits.map((h, i) => <div key={i} className="list-item"><Badge>{h.kind}</Badge><div className="grow"><b>{h.title}</b><div className="small muted">{h.snippet}</div></div><span className="small faint"><Time iso={h.created_at} /></span></div>)}</div>
      {!hits.length && q && <div className="faint small" style={{ marginTop: 10 }}>No results (deleted items are never shown).</div>}
    </Card>
  );
}
