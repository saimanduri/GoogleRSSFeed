// Renders every setting of a group from the gateway's schema. Tightening applies immediately; loosening opens
// the LoosenDialog (risk text, 10-second delay, password). Endpoint settings trigger step-up automatically.
import { useState } from "react";
import { errText, useApp } from "../../app";
import { LoosenDialog } from "../../components/LoosenDialog";
import { Badge, Card, ChipInput, Toggle } from "../../components/ui";

export function GenericGroup({ schema, group, onChanged }: { schema: any; group: string; onChanged: () => void }) {
  const { call, toast, refresh } = useApp();
  const [loosen, setLoosen] = useState<null | { changes: any; loosening: any[] }>(null);
  const items = schema.settings.filter((s: any) => s.group === group);
  if (!items.length) return null;
  const change = async (key: string, value: any) => {
    const changes = { [key]: value };
    try {
      const c = await call<any>("settings.classify", { changes });
      if (c.loosening.length) { setLoosen({ changes, loosening: c.loosening }); return; }
      await call("settings.apply", { changes });
      toast("Saved", "ok");
      onChanged();
      void refresh();
    } catch (e: any) { toast(errText(e), "danger"); }
  };
  return (
    <Card>
      {items.map((s: any) => (
        <div key={s.key} className="setting-row">
          <div>
            <div style={{ fontWeight: 600 }}>{s.label} {s.floor && <Badge tone="info">floor-bound</Badge>} {s.stepup && <Badge>re-auth</Badge>}</div>
            {s.help && <div className="small muted">{s.help}</div>}
            {s.value !== s.default && JSON.stringify(s.value) !== JSON.stringify(s.default) && <div className="small faint">Default: {JSON.stringify(s.default)}</div>}
          </div>
          <Control s={s} onChange={(v) => change(s.key, v)} />
        </div>
      ))}
      {loosen && <LoosenDialog changes={loosen.changes} loosening={loosen.loosening} onClose={() => setLoosen(null)} onApplied={() => { toast("Changed (logged)", "warn"); onChanged(); void refresh(); }} />}
    </Card>
  );
}

function Control({ s, onChange }: { s: any; onChange: (v: any) => void }) {
  const [draft, setDraft] = useState<any>(s.value);
  if (s.type === "bool") return <div style={{ justifySelf: "end" }}><Toggle on={!!s.value} onChange={onChange} label={s.label} /></div>;
  if (s.type === "enum") return (
    <select className="input" value={s.value} onChange={(e) => onChange(e.target.value)}>
      {s.options.map((o: string) => <option key={o} value={o}>{o.replace(/_/g, " ")}</option>)}
    </select>
  );
  if (s.type === "list") return <ChipInput value={s.value ?? []} onChange={onChange} />;
  if (s.type === "int" || s.type === "float") return (
    <input className="input" type="number" min={s.min ?? undefined} max={s.max ?? undefined} step={s.type === "float" ? 0.1 : 1} value={draft}
      onChange={(e) => setDraft(e.target.value)} onBlur={() => { const n = s.type === "int" ? parseInt(draft, 10) : parseFloat(draft); if (!Number.isNaN(n) && n !== s.value) onChange(n); else setDraft(s.value); }}
      onKeyDown={(e) => e.key === "Enter" && (e.target as HTMLInputElement).blur()} />
  );
  return (
    <input className="input" type={s.type === "time" ? "time" : "text"} value={draft ?? ""} onChange={(e) => setDraft(e.target.value)}
      onBlur={() => draft !== s.value && onChange(draft)} onKeyDown={(e) => e.key === "Enter" && (e.target as HTMLInputElement).blur()} />
  );
}
