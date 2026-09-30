import { useEffect, useState } from "react";
import { onEvent } from "../api/gateway";
import { useApp } from "../app";
import { Icon } from "../components/Icon";
import { Badge, Button, Card, Empty, Time } from "../components/ui";

export function Home() {
  const { call, go, status } = useApp();
  const [d, setD] = useState<any>(null);
  const load = () => call("home.summary").then(setD).catch(() => undefined);
  useEffect(() => { void load(); return onEvent((t) => { if (["home.changed", "tasks.changed", "approvals.changed", "reminders.changed"].includes(t)) void load(); }); /* eslint-disable-next-line */ }, []);
  if (!d) return <div className="page"><div className="skeleton" style={{ height: 240 }} /></div>;
  const hour = new Date().getHours();
  const greet = hour < 12 ? "Good morning" : hour < 18 ? "Good afternoon" : "Good evening";
  const pct = (k: string) => Math.min(100, Math.round(((d.budget.used[k] ?? 0) / (d.budget.limits[k] || 1)) * 100));
  return (
    <div className="page">
      <div className="page-header"><div><h1>{greet}, {status?.username}</h1><div className="muted">While you were away</div></div></div>
      {!d.recovery_key_confirmed && <div className="banner warn"><Icon name="alert" />Your recovery key was not confirmed. Generate a new one in Settings &gt; Account &amp; Security.</div>}
      {status?.needs_pin_setup && <div className="banner warn"><Icon name="alert" />This vault was restored on a new PC. Create a new PIN and recovery key in Settings &gt; Account &amp; Security.
        <Button small onClick={() => go("settings", { section: "account" })}>Open</Button></div>}
      <div className="grid-3" style={{ marginBottom: 14 }}>
        <Card onClick={() => go("approvals")} icon="approvals" title="Approvals waiting"><div className="stat">{d.approvals_pending}</div></Card>
        <Card onClick={() => go("tasks")} icon="check" title="Completed (24 h)"><div className="stat">{d.completed.length}</div></Card>
        <Card onClick={() => go("tasks")} icon="alert" title="Failed / waiting"><div className="stat">{d.failed.length + d.waiting.length}</div></Card>
        <Card onClick={() => go("reminders")} icon="reminders" title="Upcoming reminders"><div className="stat">{d.reminders_today.filter((r: any) => r.status === "SCHEDULED").length}</div></Card>
      </div>
      <div className="grid-2">
        <Card title="Updates" icon="info">
          {!d.events.length ? <Empty title="Nothing new" icon="check" /> : (
            <div className="list">{d.events.slice(0, 12).map((e: any) => (
              <div key={e.id} className="list-item">
                <Badge tone={e.severity === "high" ? "danger" : e.severity === "medium" ? "warn" : "info"}>{e.kind.replace(/_/g, " ")}</Badge>
                <div className="grow"><div style={{ fontWeight: 550 }}>{e.title}</div>{e.detail && <div className="small muted">{e.detail}</div>}</div>
                <span className="faint small"><Time iso={e.created_at} /></span>
                <Button small kind="ghost" icon="x" title="Dismiss" onClick={async () => { await call("home.dismiss", { id: e.id }); void load(); }} />
              </div>))}</div>
          )}
        </Card>
        <div className="col">
          {d.outcome_unknown.length > 0 && (
            <Card title="Outcome unknown - check manually" icon="alert">
              {d.outcome_unknown.map((t: any) => <div key={t.id} className="small">{t.objective}</div>)}
              <div className="small faint">These are never retried automatically.</div>
            </Card>
          )}
          {d.waiting.length > 0 && (
            <Card title="Waiting" icon="clock">{d.waiting.map((t: any) => <div key={t.id} className="list-item"><Badge tone="warn">{t.state.replace(/_/g, " ")}</Badge>
              <div className="grow small">{t.objective.slice(0, 80)}<div className="faint">{t.wait_reason}</div></div></div>)}</Card>
          )}
          {d.memories_proposed.length > 0 && (
            <Card title="Proposed memories" icon="memory" actions={<Button small onClick={() => go("memory", { tab: "proposed" })}>Review</Button>}>
              {d.memories_proposed.slice(0, 5).map((m: any) => <div key={m.id} className="small">• {m.content}</div>)}
            </Card>
          )}
          {d.files_created.length > 0 && (
            <Card title="Files created" icon="files">{d.files_created.map((f: any) => <div key={f.id} className="small">{f.folder}/{f.name}</div>)}</Card>
          )}
          {d.security.length > 0 && (
            <Card title="Security events" icon="shield">{d.security.map((s: any, i: number) => <div key={i} className="small">Failed {s.kind} sign-in · <Time iso={s.ts} /></div>)}</Card>
          )}
          <Card title="Today's budget" icon="activity">
            {[["tokens", "Model tokens"], ["web_requests", "Web requests"], ["egress_bytes", "Data sent out"]].map(([k, l]) => (
              <div key={k} style={{ marginBottom: 8 }}><div className="row small"><span className="grow">{l}</span><span className="faint">{pct(k)}%</span></div>
                <div className="meter"><div style={{ width: `${pct(k)}%`, background: pct(k) > 80 ? "var(--warn)" : "var(--accent)" }} /></div></div>
            ))}
          </Card>
        </div>
      </div>
    </div>
  );
}
