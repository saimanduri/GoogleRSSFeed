import { useEffect, useState } from "react";
import { onEvent } from "../api/gateway";
import { errText, useApp } from "../app";
import { ApprovalCard } from "../components/ApprovalCard";
import { Badge, Button, Card, Empty, Tabs, Time, Notice } from "../components/ui";

export function Approvals() {
  const { call, go, toast, refresh } = useApp();
  const [props, setProps] = useState<any[]>([]);
  const loadProps = () => call<any[]>("missions.list").then((l) => setProps(l.filter((m) => m.status === "DRAFT" && m.proposed_by === "agent"))).catch(() => undefined);
  const [tab, setTab] = useState<"PENDING" | "history">("PENDING");
  const [list, setList] = useState<any[]>([]);
  const load = () => call<any[]>("approvals.list", { status: tab === "PENDING" ? "PENDING" : "" }).then(setList).catch(() => undefined);
  useEffect(() => { void load(); void loadProps(); return onEvent((t) => { if (t === "approvals.changed") void load(); if (t === "missions.changed") void loadProps(); }); /* eslint-disable-next-line */ }, [tab]);
  return (
    <div className="page">
      <div className="page-header"><div><h1>Approvals</h1><div className="muted">Exact action, exact payload. Approvals are single-use and bound to what you see here.</div></div></div>
      {props.length > 0 && (
        <Card title={`Proposed routines (${props.length})`} icon="missions">
          <div className="small muted" style={{ marginBottom: 8 }}>The assistant suggested these. Nothing runs until you press Activate.</div>
          <div className="col">{props.map((m) => (
            <div key={m.id} className="card flat">
              <div className="row"><strong className="grow">{m.name}</strong><Badge tone="warn">proposed</Badge></div>
              <div className="small muted" style={{ margin: "4px 0" }}>{m.objective.slice(0, 300)}</div>
              <div className="kv small"><span>When</span><span>{m.schedule_text}</span><span>Tools</span><span>{m.allowed_tools.join(", ") || "none"}</span></div>
              {m.warnings?.filter((w: string) => !/^Runs only while/.test(w)).map((w: string) => <Notice key={w} text={w} />)}
              <div className="row" style={{ marginTop: 10 }}>
                <Button small kind="primary" icon="play" onClick={async () => { try { await call("missions.activate", { mission_id: m.id }); toast("Routine activated", "ok"); void loadProps(); void refresh(); } catch (e: any) { toast(errText(e), e?.code === "needs_setup" ? "warn" : "danger"); } }}>Activate</Button>
                <Button small icon="edit" onClick={() => go("missions")}>Review / edit</Button>
                <Button small kind="ghost" icon="x" onClick={async () => { await call("missions.set_status", { mission_id: m.id, status: "CANCELLED" }); toast("Proposal dismissed", "info"); void loadProps(); void refresh(); }}>Dismiss</Button>
              </div>
            </div>))}</div>
        </Card>
      )}
      <div style={{ height: 12 }} />
      <Tabs tabs={[["PENDING", "Waiting"], ["history", "History"]]} value={tab} onChange={setTab} />
      {tab === "PENDING" ? (
        !list.length ? <Card><Empty icon="approvals" title="Nothing waiting for you" /></Card> : <div className="col">{list.map((a) => <ApprovalCard key={a.id} a={a} onDone={load} />)}</div>
      ) : (
        <Card><table className="table"><thead><tr><th>Status</th><th>Action</th><th>Risk</th><th>Decided</th><th>Time to decide</th></tr></thead><tbody>
          {list.map((a) => <tr key={a.id}><td><Badge tone={a.status === "APPROVED" || a.status === "USED" ? "ok" : a.status === "DENIED" ? "danger" : ""}>{a.status}</Badge></td>
            <td className="mono small">{a.tool}</td><td>{a.risk}</td><td className="small"><Time iso={a.decided_at} /></td>
            <td className="small faint">{a.decision_ms != null ? `${(a.decision_ms / 1000).toFixed(1)} s` : "-"}</td></tr>)}
        </tbody></table></Card>
      )}
    </div>
  );
}
