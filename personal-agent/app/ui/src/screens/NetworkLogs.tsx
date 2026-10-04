// Activity > Network Logs: every network request this application made in the last days - when, to which host, why,
// and what happened (allowed / redirected / blocked / failed) - for observability. No bodies, tokens or query strings.
import { SearchBox } from "../components/SearchBox";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useApp } from "../app";
import { Badge, Button, bytes, Card, Empty, Notice } from "../components/ui";

type Row = { id: string; ts: string; component: string; method: string; scheme: string; host: string; port: number | null; path: string; status: number | null;
  outcome: string; reason: string; bytes_out: number; bytes_in: number; duration_ms: number; ip: string; loopback: number; purpose: string; tool: string; task_id: string | null };
type Data = { rows: Row[]; total: number; retention_days: number; summary: { requests: number; bytes_out: number; bytes_in: number; blocked: number; errors: number; local: number;
  hosts: { host: string; requests: number; bytes_out: number; bytes_in: number; blocked: number; last_ts: string; component: string; loopback: number }[];
  by_day: { day: string; requests: number; blocked: number }[]; by_component: { component: string; requests: number }[] } };

const COMPONENT: Record<string, string> = { web: "Web (search & fetch)", m365: "Microsoft 365", llm: "AI model" };
const stamp = (iso: string) => new Date(iso).toLocaleString([], { day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit", second: "2-digit" });

function Status({ r }: { r: Row }) {
  if (r.outcome === "blocked") return <Badge tone="danger">blocked</Badge>;
  if (r.outcome === "error") return <Badge tone="danger">{r.status ?? "failed"}</Badge>;
  if (r.status && r.status >= 300) return <Badge tone="warn">{r.status}{r.reason === "redirect" ? " redirect" : ""}</Badge>;
  return <Badge tone="ok">{r.status ?? "ok"}</Badge>;
}

function csv(rows: Row[]): string {
  const cols: (keyof Row)[] = ["ts", "component", "method", "scheme", "host", "port", "path", "status", "outcome", "reason", "purpose", "tool", "bytes_out", "bytes_in", "duration_ms", "ip", "loopback"];
  // a cell starting with = + - @ (or tab/CR) would run as a formula when the CSV is opened in Excel: neutralise it with a leading apostrophe
  const esc = (v: unknown) => { let t = String(v ?? ""); if (/^[=+\-@\t\r]/.test(t)) t = "'" + t; return `"${t.replace(/"/g, '""')}"`; };
  return [cols.join(","), ...rows.map((r) => cols.map((c) => esc(r[c])).join(","))].join("\n");
}

export function NetworkLogs() {
  const { call, toast } = useApp();
  const [days, setDays] = useState(7);
  const [host, setHost] = useState("");
  const [component, setComponent] = useState("");
  const [outcome, setOutcome] = useState("");
  const [local, setLocal] = useState(true);
  const [limit, setLimit] = useState(200);
  const [d, setD] = useState<Data | null>(null);
  const load = useCallback(() => call<Data>("network.logs", { days, host, component, outcome, include_local: local, limit }).then(setD).catch(() => undefined), [call, days, host, component, outcome, local, limit]);
  useEffect(() => { void load(); const t = window.setInterval(load, 15000); return () => window.clearInterval(t); }, [load]);
  const maxDay = useMemo(() => Math.max(1, ...(d?.summary.by_day.map((x) => x.requests) ?? [1])), [d]);
  if (!d) return <div className="skeleton" style={{ height: 260 }} />;
  const s = d.summary;
  return (
    <div className="col" style={{ gap: 14 }}>
      <Notice text={`Every request this application made in the last ${days} day${days === 1 ? "" : "s"} (kept ${d.retention_days} days). Request contents, passwords, tokens and web-address queries are never stored.`} />
      <div className="grid-3">
        <Card title="Requests"><div className="stat">{s.requests}</div><div className="small muted">{s.local} to this PC (local AI model)</div></Card>
        <Card title="Blocked or failed"><div className="stat" style={{ color: s.blocked + s.errors ? "var(--danger)" : undefined }}>{s.blocked + s.errors}</div><div className="small muted">{s.blocked} blocked by your rules · {s.errors} errors</div></Card>
        <Card title="Data"><div className="stat">{bytes(s.bytes_out)} <span className="small muted">sent</span></div><div className="small muted">{bytes(s.bytes_in)} received</div></Card>
      </div>
      <Card title="Requests per day" icon="activity">
        {!s.by_day.length ? <div className="small faint">Nothing in this period.</div> : (
          <div className="daybars" role="img" aria-label="Requests per day">
            {s.by_day.map((x) => (
              <div key={x.day} className="daybar" title={`${x.day}: ${x.requests} requests, ${x.blocked} blocked`}>
                <div className="bar-col"><div className="bar" style={{ height: `${(x.requests / maxDay) * 100}%` }}>{x.blocked > 0 && <div className="bar-blocked" style={{ height: `${(x.blocked / x.requests) * 100}%` }} />}</div></div>
                <div className="small faint">{x.day.slice(5)}</div>
              </div>))}
          </div>)}
      </Card>
      <Card title="Where the application connects" icon="globe">
        {!s.hosts.length ? <Empty icon="shield" title="No network activity" /> : (
          <table className="table"><thead><tr><th>Destination</th><th>Used for</th><th>Requests</th><th>Sent</th><th>Received</th><th>Blocked</th><th>Last</th></tr></thead><tbody>
            {s.hosts.map((h) => (
              <tr key={h.host} style={{ cursor: "pointer" }} onClick={() => setHost(h.host)} title="Click to filter">
                <td className="mono small">{h.host}{h.loopback ? <Badge>this PC</Badge> : null}</td><td className="small">{COMPONENT[h.component] ?? h.component}</td>
                <td>{h.requests}</td><td className="small">{bytes(h.bytes_out)}</td><td className="small">{bytes(h.bytes_in)}</td>
                <td>{h.blocked ? <Badge tone="danger">{h.blocked}</Badge> : "-"}</td><td className="small">{stamp(h.last_ts)}</td></tr>))}
          </tbody></table>)}
      </Card>
      <Card title={`Every request (${d.total})`} icon="steps">
        <div className="row wrap" style={{ marginBottom: 10 }}>
          <select className="input" style={{ width: 130 }} value={days} onChange={(e) => setDays(+e.target.value)} aria-label="Period">
            {[[1, "Last 24 h"], [3, "Last 3 days"], [7, "Last 7 days"], [14, "Last 14 days"]].map(([v, l]) => <option key={v} value={v}>{l}</option>)}</select>
          <SearchBox style={{ width: 220 }} placeholder="Filter by website (e.g. wikipedia)" value={host} onChange={setHost} label="Filter by website" />
          <select className="input" style={{ width: 170 }} value={component} onChange={(e) => setComponent(e.target.value)} aria-label="Used for">
            <option value="">All uses</option>{Object.entries(COMPONENT).map(([k, v]) => <option key={k} value={k}>{v}</option>)}</select>
          <select className="input" style={{ width: 140 }} value={outcome} onChange={(e) => setOutcome(e.target.value)} aria-label="Result">
            <option value="">All results</option><option value="ok">Allowed</option><option value="blocked">Blocked</option><option value="error">Failed</option></select>
          <label className="row small"><input type="checkbox" checked={local} onChange={(e) => setLocal(e.target.checked)} />Include this PC (local AI model)</label>
          <div className="spacer" />
          <Button small icon="refresh" onClick={() => void load()}>Refresh</Button>
          <Button small icon="copy" onClick={async () => { try { await navigator.clipboard.writeText(csv(d.rows)); toast(`Copied ${d.rows.length} rows as CSV`, "ok"); } catch { toast("Could not copy", "warn"); } }}>Copy as CSV</Button>
        </div>
        {!d.rows.length ? <Empty icon="shield" title="No requests match" /> : (
          <div style={{ overflowX: "auto" }}>
            <table className="table nowrap"><thead><tr><th>Time</th><th>Result</th><th>Used for</th><th>Method</th><th>Destination</th><th>Why</th><th>Sent</th><th>Received</th><th>Took</th><th>IP</th></tr></thead><tbody>
              {d.rows.map((r) => (
                <tr key={r.id}>
                  <td className="small">{stamp(r.ts)}</td><td><Status r={r} /></td><td className="small">{COMPONENT[r.component] ?? r.component}</td><td className="mono small">{r.method}</td>
                  <td className="mono small">{r.scheme}://{r.host}{r.port && r.port !== 443 && r.port !== 80 ? `:${r.port}` : ""}{r.path}{r.loopback ? " " : ""}{r.loopback ? <Badge>this PC</Badge> : null}</td>
                  <td className="small">{r.purpose || r.tool || "-"}{r.outcome === "blocked" && <div style={{ color: "var(--danger)" }}>{r.reason}</div>}</td>
                  <td className="small">{r.bytes_out ? bytes(r.bytes_out) : "-"}</td><td className="small">{r.bytes_in ? bytes(r.bytes_in) : "-"}</td>
                  <td className="small">{r.duration_ms} ms</td><td className="mono small faint">{r.ip || "-"}</td></tr>))}
            </tbody></table>
          </div>)}
        {d.total > d.rows.length && <div className="row" style={{ marginTop: 10 }}><Button small onClick={() => setLimit(limit + 300)}>Show more ({d.total - d.rows.length} more)</Button></div>}
      </Card>
    </div>
  );
}
