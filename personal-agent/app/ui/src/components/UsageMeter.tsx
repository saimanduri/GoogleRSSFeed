import { useEffect, useState } from "react";
import { useApp } from "../app";

// Today's token use against the daily budget as a small ring in the chat header. Hover for the numbers, click for the full Usage page.
export function UsageMeter({ refreshKey }: { refreshKey?: unknown }) {
  const { call, go } = useApp();
  const [b, setB] = useState<{ used: number; limit: number } | null>(null);
  useEffect(() => {
    call<any>("home.summary").then((d) => setB({ used: d.budget?.used?.tokens ?? 0, limit: d.budget?.limits?.tokens ?? 0 })).catch(() => undefined);
  }, [call, refreshKey]);
  if (!b || !b.limit) return null;
  const pct = Math.min(100, (b.used / b.limit) * 100);
  const fmt = (n: number) => (n >= 1e6 ? `${(n / 1e6).toFixed(1)}M` : n >= 1e3 ? `${Math.round(n / 1e3)}k` : String(Math.round(n)));
  const r = 7, c = 2 * Math.PI * r;
  const tone = pct > 90 ? "var(--danger)" : pct > 70 ? "var(--warn)" : "var(--accent)";
  return (
    <button type="button" className="usage-ring" onClick={() => go("activity", { tab: "usage" })}
      title={`Today: ${b.used.toLocaleString()} of ${b.limit.toLocaleString()} model tokens used (${Math.round(pct)} %). Click for details.`}
      aria-label={`Model tokens today: ${fmt(b.used)} of ${fmt(b.limit)}`}>
      <svg width="20" height="20" viewBox="0 0 20 20" aria-hidden="true">
        <circle cx="10" cy="10" r={r} fill="none" stroke="var(--border)" strokeWidth="3" />
        <circle cx="10" cy="10" r={r} fill="none" stroke={tone} strokeWidth="3" strokeLinecap="round" strokeDasharray={`${(pct / 100) * c} ${c}`} transform="rotate(-90 10 10)" />
      </svg>
      <span>{fmt(b.used)}</span>
    </button>
  );
}
