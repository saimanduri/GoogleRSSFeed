// Small glass box above Settings: live GPU activity so you can see the AI model working while you wait for an answer.
import { useEffect, useRef, useState } from "react";
import { useApp } from "../app";

type Usage = { available: boolean; util: number; vram_mb: number; history: number[] };
const W = 100, H = 30;

function path(h: number[], closed: boolean): string {
  if (h.length < 2) return "";
  const step = W / (Math.max(h.length, 40) - 1);
  const x0 = W - (h.length - 1) * step;
  const pts = h.map((v, i) => `${(x0 + i * step).toFixed(1)},${(H - 2 - (Math.min(100, v) / 100) * (H - 4)).toFixed(1)}`);
  return (closed ? `M${x0.toFixed(1)},${H} L` : "M") + pts.join(" L") + (closed ? ` L${W},${H} Z` : "");
}

export function GpuMeter() {
  const { call, status } = useApp();
  const [u, setU] = useState<Usage | null>(null);
  const on = status?.ui?.["ui.show_gpu_meter"] !== false;
  const timer = useRef<number | undefined>(undefined);
  useEffect(() => {
    if (!on) return;
    let stop = false;
    const tick = async () => {
      if (!stop && document.visibilityState === "visible") { try { setU(await call<Usage>("system.usage")); } catch { /* gateway busy */ } }
      if (!stop) timer.current = window.setTimeout(tick, 2000);
    };
    void tick();
    return () => { stop = true; window.clearTimeout(timer.current); };
  }, [on, call]);
  if (!on || !u || !u.available) return null;
  const busy = u.util >= 8;
  const working = (status?.tasks_running ?? 0) > 0;
  const gb = u.vram_mb >= 1024 ? `${(u.vram_mb / 1024).toFixed(1)} GB` : `${u.vram_mb} MB`;
  return (
    <div className={`gpu-box ${busy ? "busy" : ""}`} role="img" aria-label={`GPU ${Math.round(u.util)} percent busy, ${gb} video memory in use`}
      title={`GPU ${Math.round(u.util)}% · video memory ${gb}${working ? " · the assistant is working" : ""}`}>
      <div className="gpu-text">
        <span className="gpu-label"><span className={`gpu-dot ${busy ? "on" : ""}`} aria-hidden="true" />GPU</span>
        <span className="gpu-num">{Math.round(u.util)}%</span>
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" aria-hidden="true">
        <defs><linearGradient id="gpug" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="var(--accent)" stopOpacity="0.55" /><stop offset="100%" stopColor="var(--accent)" stopOpacity="0.02" /></linearGradient></defs>
        <path d={path(u.history, true)} fill="url(#gpug)" />
        <path d={path(u.history, false)} fill="none" stroke="var(--accent)" strokeWidth="1.4" vectorEffect="non-scaling-stroke" />
      </svg>
      <div className="gpu-text small faint"><span>{working ? "AI working…" : busy ? "busy" : "idle"}</span><span>{gb}</span></div>
    </div>
  );
}
