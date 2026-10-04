// CPU, memory, GPU and storage of this app and of the whole PC (Settings > Diagnostics & About).
// Red at 90 % or more, orange from 75 %; refreshed every 3 s while the screen is open.
import { useEffect, useState } from "react";
import { useApp } from "../app";
import { Badge, Card } from "./ui";


export function ResourceMeters() {
  const { call } = useApp();
  const [snap, setSnap] = useState<any>(null);
  const [showProcs, setShowProcs] = useState(false);
  useEffect(() => {
    let alive = true;
    const load = () => call<any>("diagnostics.resources").then((s) => alive && setSnap(s)).catch(() => undefined);
    void load();
    const t = window.setInterval(() => { if (document.visibilityState === "visible") void load(); }, 3000);
    return () => { alive = false; window.clearInterval(t); };
  }, [call]);
  const critical = (snap?.meters ?? []).filter((m: any) => m.level === "critical");
  return (
    <Card title="Resource use" actions={snap && <Badge tone={critical.length ? "danger" : "ok"}>{critical.length ? `${critical.length} at ${snap.red_at}% or more` : "all below " + snap.red_at + "%"}</Badge>}>
      {!snap ? <div className="skeleton" style={{ height: 140 }} /> : (
        <div className="col" style={{ gap: 12 }}>
          {snap.meters.map((m: any) => (
            <div key={m.label} className={`res-meter lvl-${m.level}`} role="group" aria-label={`${m.label} ${m.pct ?? "unknown"} percent`}>
              <div className="row" style={{ justifyContent: "space-between" }}>
                <span style={{ fontWeight: 600 }}>{m.label}</span>
                <span className={`res-pct lvl-${m.level}`}>{m.pct === null ? "-" : `${Math.round(m.pct)}%`}{m.level === "critical" && " · high"}</span>
              </div>
              <div className="res-track"><div className={`res-fill lvl-${m.level}`} style={{ width: `${m.pct ?? 0}%` }} />
                {m.app_pct !== null && <div className="res-app" style={{ width: `${Math.max(m.app_pct, 0.5)}%` }} title={`this app: ${m.app_pct}%`} />}</div>
              <div className="small muted">{m.detail}{m.app_detail && <> · <b>{m.app_detail}</b>{m.app_pct !== null && ` (${m.app_pct}%)`}</>}</div>
            </div>
          ))}
          <div className="small faint">Darker part of a bar = this app. Red means 90 % or more is in use, orange 75 % or more.
            {snap.processes?.length > 0 && <> <button className="link-btn" onClick={() => setShowProcs(!showProcs)}>{showProcs ? "Hide" : "Show"} this app's processes</button></>}</div>
          {showProcs && <table className="table small"><thead><tr><th>Process</th><th>ID</th><th>Memory</th></tr></thead>
            <tbody>{snap.processes.map((p: any) => <tr key={p.pid}><td>{p.name}</td><td className="mono">{p.pid}</td><td>{p.memory_mb} MB</td></tr>)}</tbody></table>}
        </div>
      )}
    </Card>
  );
}
