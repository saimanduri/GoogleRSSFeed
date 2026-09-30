import { useEffect, useState } from "react";
import { onEvent } from "../api/gateway";
import { useApp } from "../app";
import { ApprovalCard } from "../components/ApprovalCard";
import { Badge, Card, Empty, Tabs, Time } from "../components/ui";

export function Approvals() {
  const { call } = useApp();
  const [tab, setTab] = useState<"PENDING" | "history">("PENDING");
  const [list, setList] = useState<any[]>([]);
  const load = () => call<any[]>("approvals.list", { status: tab === "PENDING" ? "PENDING" : "" }).then(setList).catch(() => undefined);
  useEffect(() => { void load(); return onEvent((t) => t === "approvals.changed" && void load()); /* eslint-disable-next-line */ }, [tab]);
  return (
    <div className="page">
      <div className="page-header"><div><h1>Approvals</h1><div className="muted">Exact action, exact payload. Approvals are single-use and bound to what you see here.</div></div></div>
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
