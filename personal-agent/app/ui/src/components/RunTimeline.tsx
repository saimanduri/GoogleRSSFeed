// Live step timeline for one run: every plan, model call, policy decision, tool call, approval and result.
import { useEffect, useState } from "react";
import { onEvent } from "../api/gateway";
import { useApp } from "../app";
import { Icon } from "./Icon";
import { Badge, Sensitivity, Spinner } from "./ui";

const ICON: Record<string, string> = { input: "chat", llm: "sparkle", thought: "info", plan: "steps", tool: "plug", policy: "shield", approval: "approvals",
  summary: "archive", retry: "refresh", error: "alert", info: "info" };

export function useRun(runId?: string | null) {
  const { call } = useApp();
  const [run, setRun] = useState<any>(null);
  useEffect(() => {
    if (!runId) { setRun(null); return; }
    let alive = true;
    call("runs.get", { run_id: runId }).then((r) => alive && setRun(r)).catch(() => undefined);
    const off = onEvent((topic, d) => {
      if (d?.run_id !== runId) return;
      if (topic === "run.step") {
        setRun((r: any) => {
          if (!r) return r;
          const steps = [...(r.steps ?? [])];
          const i = steps.findIndex((s: any) => s.id === d.step_id);
          const s = { id: d.step_id, seq: d.seq, type: d.type, title: d.title, status: d.status, detail: d.detail, duration_ms: d.duration_ms };
          if (i >= 0) steps[i] = { ...steps[i], ...s }; else steps.push(s);
          return { ...r, steps: steps.sort((a: any, b: any) => a.seq - b.seq) };
        });
      }
      if (topic === "run.finished") call("runs.get", { run_id: runId }).then((r) => alive && setRun(r)).catch(() => undefined);
    });
    return () => { alive = false; off(); };
  }, [runId, call]);
  return run;
}

export function RunTimeline({ runId, compact }: { runId?: string | null; compact?: boolean }) {
  const run = useRun(runId);
  const [open, setOpen] = useState<string | null>(null);
  if (!runId) return <div className="faint small">Select a request to see its steps.</div>;
  if (!run) return <div className="skeleton" style={{ height: 120 }} />;
  return (
    <div className="col" style={{ gap: 8 }}>
      {!compact && (
        <div className="row wrap">
          <Badge tone={run.status === "COMPLETED" ? "ok" : run.status === "FAILED" ? "danger" : "accent"}>{run.status}</Badge>
          <Sensitivity level={run.hwm} />
          <span className="faint small">{run.tokens_in + run.tokens_out} tokens · {run.tool_calls} tool calls</span>
        </div>
      )}
      <div className="timeline">
        {(run.steps ?? []).map((s: any) => (
          <div key={s.id} className={`step ${s.status}`} onClick={() => setOpen(open === s.id ? null : s.id)}>
            <div className="step-dot">{s.status === "running" || s.status === "waiting" ? "" : s.status === "denied" || s.status === "error" ? "!" : "✓"}</div>
            <div className="step-title">
              <Icon name={ICON[s.type] ?? "info"} size={14} />
              <span className="grow ellipsis">{s.title}</span>
              {(s.status === "running" || s.status === "waiting") && <Spinner />}
              {s.duration_ms != null && s.status !== "running" && <span className="faint small">{s.duration_ms} ms</span>}
            </div>
            {s.detail?.text && s.type === "thought" && <div className="small muted" style={{ marginTop: 3 }}>{s.detail.text}</div>}
            {s.detail?.reason && <div className="small" style={{ color: "var(--danger)", marginTop: 3 }}>{s.detail.reason}</div>}
            {open === s.id && <pre className="code step-detail">{JSON.stringify(s.detail, null, 2)}</pre>}
          </div>
        ))}
        {run.status === "RUNNING" && (run.steps ?? []).every((s: any) => s.status !== "running" && s.status !== "waiting") && (
          <div className="step running"><div className="step-dot" /><div className="step-title faint">working...</div></div>
        )}
      </div>
    </div>
  );
}
