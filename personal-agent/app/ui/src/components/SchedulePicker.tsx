// Friendly schedule chooser for routines/missions. It produces the same text the backend already understands
// ("every 2 hours" or a 5-field cron), so nothing changes server-side. "Advanced" keeps the free-text box.
import { useState } from "react";
import { Field } from "./ui";

type Mode = "daily" | "multi" | "advanced";
type DaysKind = "all" | "weekdays" | "weekends" | "custom";
type Pick = { mode: Mode; time: string; days: DaysKind; custom: number[]; every: number; windowOn: boolean; from: number; to: number };

const DAY_NAMES = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
export const INTERVALS: [number, string][] = [[5, "5 minutes"], [10, "10 minutes"], [15, "15 minutes"], [30, "30 minutes"], [60, "1 hour"], [120, "2 hours"],
  [180, "3 hours"], [240, "4 hours"], [360, "6 hours"], [480, "8 hours"], [720, "12 hours"]];

const dowField = (p: Pick) => p.days === "all" ? "*" : p.days === "weekdays" ? "1-5" : p.days === "weekends" ? "0,6" : (p.custom.length ? [...p.custom].sort().join(",") : "*");

/** The schedule text for the current choices. */
export function toText(p: Pick, advanced: string): string {
  if (p.mode === "advanced") return advanced;
  const dow = dowField(p);
  if (p.mode === "daily") {
    const [h, m] = p.time.split(":").map(Number);
    return `${m || 0} ${h || 0} * * ${dow}`;
  }
  const n = p.every;
  if (!p.windowOn && dow === "*") return n % 60 === 0 ? `every ${n / 60} hours` : `every ${n} minutes`;
  const hours = p.windowOn ? `${p.from}-${Math.max(p.from, p.to)}` : "*";
  if (n < 60) return `*/${n} ${hours} * * ${dow}`;
  return `0 ${p.windowOn ? `${hours}/${n / 60}` : `*/${n / 60}`} * * ${dow}`;
}

/** Work out the picker state from an existing schedule (text or the stored object). */
export function fromSchedule(s: unknown): { pick: Pick; advanced: string } {
  const base: Pick = { mode: "daily", time: "09:00", days: "all", custom: [1, 2, 3, 4, 5], every: 120, windowOn: false, from: 9, to: 17 };
  const text = typeof s === "string" ? s : (s as any)?.type === "cron" ? (s as any).cron : (s as any)?.type === "interval" ? `every ${(s as any).minutes} minutes`
    : (s as any)?.type === "event" ? `when ${String((s as any).event).replace("_", " ")} arrives` : "";
  const adv = { pick: { ...base, mode: "advanced" as Mode }, advanced: typeof s === "string" ? s : text || (s ? JSON.stringify(s) : "") };
  if (!text) return { pick: base, advanced: "" };
  const interval = /^every (\d+) (minutes|hours)$/.exec(text);
  if (interval) {
    const n = +interval[1] * (interval[2] === "hours" ? 60 : 1);
    return INTERVALS.some(([v]) => v === n) ? { pick: { ...base, mode: "multi", every: n }, advanced: text } : adv;
  }
  const parts = text.trim().split(/\s+/);
  if (parts.length !== 5 || parts[2] !== "*" || parts[3] !== "*") return adv;
  const [mi, ho, , , dw] = parts;
  const days: Partial<Pick> = dw === "*" ? { days: "all" } : dw === "1-5" ? { days: "weekdays" } : dw === "0,6" ? { days: "weekends" } : /^[0-6](,[0-6])*$/.test(dw) ? { days: "custom", custom: dw.split(",").map(Number) } : { days: "all" };
  if (!/^(\*|1-5|0,6|[0-6](,[0-6])*)$/.test(dw)) return adv;
  if (/^\d+$/.test(mi) && /^\d+$/.test(ho)) return { pick: { ...base, ...days, mode: "daily", time: `${ho.padStart(2, "0")}:${mi.padStart(2, "0")}` } as Pick, advanced: text };
  let m: RegExpExecArray | null;
  if ((m = /^\*\/(\d+)$/.exec(mi)) && (ho === "*" || /^\d+-\d+$/.test(ho))) {
    const [a, b] = ho === "*" ? [9, 17] : ho.split("-").map(Number);
    return { pick: { ...base, ...days, mode: "multi", every: +m[1], windowOn: ho !== "*", from: a, to: b } as Pick, advanced: text };
  }
  if (mi === "0" && (m = /^(\*|\d+-\d+)\/(\d+)$/.exec(ho))) {
    const [a, b] = m[1] === "*" ? [9, 17] : m[1].split("-").map(Number);
    return { pick: { ...base, ...days, mode: "multi", every: +m[2] * 60, windowOn: m[1] !== "*", from: a, to: b } as Pick, advanced: text };
  }
  return adv;
}

export function SchedulePicker({ value, onChange }: { value: unknown; onChange: (text: string) => void }) {
  const init = fromSchedule(value);
  const [p, setP] = useState<Pick>(init.pick);
  const [adv, setAdv] = useState(init.advanced);
  const upd = (patch: Partial<Pick>, advText = adv) => { const n = { ...p, ...patch }; setP(n); onChange(toText(n, advText)); };
  const hourOpts = Array.from({ length: 24 }, (_, h) => <option key={h} value={h}>{String(h).padStart(2, "0")}:00</option>);
  return (
    <div className="col" style={{ gap: 10 }}>
      <Field label="How often?">
        <select className="input" value={p.mode} onChange={(e) => upd({ mode: e.target.value as Mode })}>
          <option value="daily">Once a day (at a set time)</option>
          <option value="multi">More than once a day (repeat every...)</option>
          <option value="advanced">Advanced (type it in words or cron)</option>
        </select>
      </Field>
      {p.mode === "daily" && (
        <Field label="At what time?"><input className="input" type="time" value={p.time} onChange={(e) => upd({ time: e.target.value || "09:00" })} /></Field>
      )}
      {p.mode === "multi" && (
        <>
          <Field label="Repeat every"><select className="input" value={p.every} onChange={(e) => upd({ every: +e.target.value })}>
            {INTERVALS.map(([v, l]) => <option key={v} value={v}>{l}</option>)}</select></Field>
          <label className="row small"><input type="checkbox" checked={p.windowOn} onChange={(e) => upd({ windowOn: e.target.checked })} />Only during these hours</label>
          {p.windowOn && (
            <div className="row">
              <Field label="From"><select className="input" value={p.from} onChange={(e) => upd({ from: +e.target.value })}>{hourOpts}</select></Field>
              <Field label="Until"><select className="input" value={p.to} onChange={(e) => upd({ to: +e.target.value })}>{hourOpts}</select></Field>
            </div>
          )}
        </>
      )}
      {p.mode !== "advanced" && (
        <Field label="On which days?">
          <select className="input" value={p.days} onChange={(e) => upd({ days: e.target.value as DaysKind })}>
            <option value="all">Every day</option><option value="weekdays">Weekdays (Mon-Fri)</option>
            <option value="weekends">Weekends</option><option value="custom">Choose days...</option>
          </select>
        </Field>
      )}
      {p.mode !== "advanced" && p.days === "custom" && (
        <div className="row wrap" role="group" aria-label="Days of the week">
          {DAY_NAMES.map((d, i) => (
            <button key={d} type="button" role="checkbox" aria-checked={p.custom.includes(i)} className={`chip-choice ${p.custom.includes(i) ? "selected" : ""}`}
              onClick={() => upd({ custom: p.custom.includes(i) ? p.custom.filter((x) => x !== i) : [...p.custom, i] })}>{d}</button>
          ))}
        </div>
      )}
      {p.mode === "advanced" && (
        <Field label="When" help="e.g. 'every weekday at 07:30', 'every 2 hours', 'when new mail arrives', or a cron expression">
          <input className="input" value={adv} onChange={(e) => { setAdv(e.target.value); onChange(e.target.value); }} />
        </Field>
      )}
    </div>
  );
}
