import { useEffect, useState } from "react";
import { onEvent } from "../api/gateway";
import { errText, useApp } from "../app";
import { RunTimeline } from "../components/RunTimeline";
import { Badge, Button, Card, Empty, Modal, Time } from "../components/ui";

const TONE: Record<string, string> = { RUNNING: "accent", QUEUED: "info", COMPLETED: "ok", FAILED: "danger", CANCELLED: "", TIMED_OUT: "danger",
  WAITING_FOR_APPROVAL: "warn", WAITING_FOR_RESOURCE: "warn", OUTCOME_UNKNOWN: "danger", PAUSED: "warn", SUSPENDED: "warn" };

export function Tasks() {
  const { call, toast } = useApp();
  const [list, setList] = useState<any[]>([]);
  const [sel, setSel] = useState<any>(null);
  const load = () => call<any[]>("tasks.list", { limit: 300 }).then(setList).catch(() => undefined);
  useEffect(() => { void load(); return onEvent((t) => t === "tasks.changed" && void load()); /* eslint-disable-next-line */ }, []);
  return (
    <div className="page">
      <div className="page-header"><div><h1>Tasks</h1><div className="muted">Everything the agent is doing or did, with budgets and state history.</div></div></div>
      <Card>
        {!list.length ? <Empty icon="tasks" title="No tasks yet" /> : (
          <table className="table"><thead><tr><th>State</th><th>Task</th><th>Trigger</th><th>Usage</th><th>Updated</th><th /></tr></thead><tbody>
            {list.map((t) => (
              <tr key={t.id} style={{ cursor: "pointer" }} onClick={async () => setSel(await call("tasks.get", { task_id: t.id }))}>
                <td><Badge tone={TONE[t.state]}>{t.state.replace(/_/g, " ")}</Badge></td>
                <td className="ellipsis" style={{ maxWidth: 420 }}>{t.objective}</td>
                <td className="small">{t.trigger_type}</td>
                <td className="small faint">{t.usage?.tool_calls ?? 0}/{t.limits?.tool_calls} tools · {Math.round((t.usage?.tokens ?? 0) / 1000)}k tok</td>
                <td className="small"><Time iso={t.updated_at} /></td>
                <td>{["RUNNING", "QUEUED", "WAITING_FOR_APPROVAL"].includes(t.state) && <Button small kind="danger" onClick={async (e?: any) => { e?.stopPropagation?.(); try { await call("tasks.stop", { task_id: t.id }); toast("Stopped", "ok"); void load(); } catch (er: any) { toast(errText(er), "danger"); } }}>Stop</Button>}
                  {["WAITING_FOR_RESOURCE", "SUSPENDED", "PAUSED"].includes(t.state) && <Button small onClick={async () => { await call("tasks.resume", { task_id: t.id }); void load(); }}>Resume</Button>}</td>
              </tr>))}
          </tbody></table>
        )}
      </Card>
      {sel && (
        <Modal wide title="Task" onClose={() => setSel(null)}>
          <p>{sel.objective}</p>
          <div className="grid-2">
            <div><h3>Timeline</h3><RunTimeline runId={sel.run_id} /></div>
            <div className="col">
              <h3>State history</h3>
              {sel.transitions.map((tr: any) => <div key={tr.id} className="small"><Badge tone={TONE[tr.to_state]}>{tr.to_state}</Badge> <span className="faint">{tr.reason}</span> · <Time iso={tr.ts} /></div>)}
              <h3>Budget</h3>
              {Object.entries(sel.limits).map(([k, v]: any) => <div key={k} className="small row"><span className="grow">{k.replace(/_/g, " ")}</span><span className="faint">{Math.round(sel.usage[k] ?? 0)} / {Math.round(v)}</span></div>)}
              {sel.error && <div className="banner danger">{sel.error}</div>}
            </div>
          </div>
        </Modal>
      )}
    </div>
  );
}
