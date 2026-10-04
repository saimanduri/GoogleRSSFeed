import { useEffect, useState } from "react";
import { onEvent } from "../api/gateway";
import { errText, useApp } from "../app";
import { Icon } from "../components/Icon";
import { SchedulePicker } from "../components/SchedulePicker";
import { Badge, Button, Card, ChipInput, Empty, Field, Modal, Notice, Time, toneOf } from "../components/ui";

const TONE: Record<string, string> = { ACTIVE: "ok", DRAFT: "accent", PAUSED: "warn", SUSPENDED: "warn", FAILED: "danger", COMPLETED: "info" };

export function Missions() {
  const { call, toast, route, go, deferDelete, isHidden } = useApp();
  const [list, setList] = useState<any[]>([]);
  const [edit, setEdit] = useState<any>(null);
  const [describe, setDescribe] = useState(false);
  const load = () => call<any[]>("missions.list").then(setList).catch(() => undefined);
  useEffect(() => { void load(); return onEvent((t) => t === "missions.changed" && void load()); /* eslint-disable-next-line */ }, []);
  useEffect(() => { if (route.params?.create) setDescribe(true); }, [route.params?.create]);
  const act = async (fn: () => Promise<any>, ok: string) => { try { await fn(); toast(ok, "ok"); void load(); } catch (e: any) { toast(errText(e), e?.code === "needs_setup" ? "warn" : "danger"); } };
  const shown = list.filter((m) => !isHidden(`mission:${m.id}`));
  return (
    <div className="page">
      <div className="page-header">
        <div className="grow"><h1>Routines</h1><div className="muted">Work that runs on a schedule or when something happens - even while the window is closed.</div></div>
        <Button icon="sparkle" onClick={() => setDescribe(true)}>Describe in plain words</Button>
        <Button kind="primary" icon="plus" onClick={() => setEdit({ kind: "routine", schedule: "every weekday at 07:30", allowed_tools: ["notify.user"], missed_run_policy: "RUN_ONCE" })}>New</Button>
      </div>
      {!shown.length ? <Card><Empty icon="missions" title="No routines yet">Try "Every weekday at 7:30 summarise important mail".</Empty></Card> : (
        <div className="grid-2">
          {shown.map((m) => (
            <Card key={m.id} title={<span>{m.name} {m.proposed_by === "agent" && <Badge tone="info">proposed by agent</Badge>}</span>}
              actions={<Badge tone={TONE[m.status]}>{m.status}</Badge>} icon="refresh">
              <p className="small muted">{m.objective.slice(0, 220)}</p>
              <div className="kv small">
                <span>Schedule</span><span>{m.schedule_text} <span className="faint">({m.timezone})</span></span>
                <span>Tools</span><span>{m.allowed_tools.join(", ") || "none"}</span>
                <span>Last run</span><span><Time iso={m.last_run_at} /></span>
                <span>Next run</span><span><Time iso={m.next_run_at} /></span>
                <span>If missed</span><span>{m.missed_run_policy.replace("_", " ").toLowerCase()}</span>
              </div>
              {m.warnings?.length > 0 && <div className="col small" style={{ marginTop: 8, gap: 6 }}>{m.warnings.map((w: string) => (
                <Notice key={w} text={w}>{/web|search provider/i.test(w) && <Button small tone={toneOf(w) === "error" ? "error" : "warn"} onClick={() => go("settings", { section: /provider|API key/i.test(w) ? "web" : "connectors" })}>Fix this</Button>}</Notice>))}</div>}
              <div className="row wrap" style={{ marginTop: 12 }}>
                {m.status !== "ACTIVE" && <Button small kind="primary" icon="play" onClick={() => act(() => call("missions.activate", { mission_id: m.id }), "Activated")}>Activate</Button>}
                {m.status === "ACTIVE" && <Button small icon="pause" onClick={() => act(() => call("missions.set_status", { mission_id: m.id, status: "PAUSED" }), "Paused")}>Pause</Button>}
                <Button small icon="play" onClick={() => act(() => call("missions.run_now", { mission_id: m.id }), "Started - see Activity log")}>Run now</Button>
                <Button small kind="ghost" icon="edit" onClick={() => setEdit({ ...m, schedule: m.schedule })}>Edit</Button>
                <Button small kind="ghost" icon="trash" title="Cancel" onClick={() => deferDelete({ key: `mission:${m.id}`, label: `Cancelled "${m.name}"`, commit: () => call("missions.set_status", { mission_id: m.id, status: "CANCELLED" }), after: () => void load() })} />
              </div>
            </Card>
          ))}
        </div>
      )}
      {describe && <DescribeDialog onClose={() => setDescribe(false)} onForm={(f) => { setDescribe(false); setEdit(f); }} />}
      {edit && <MissionForm initial={edit} onClose={() => setEdit(null)} onSaved={() => { setEdit(null); void load(); }} />}
    </div>
  );
}

function DescribeDialog({ onClose, onForm }: { onClose: () => void; onForm: (f: any) => void }) {
  const { call, toast } = useApp();
  const [text, setText] = useState("");
  const [busy, setBusy] = useState(false);
  return (
    <Modal title="Describe a routine" onClose={onClose} actions={<><Button onClick={onClose}>Cancel</Button>
      <Button kind="primary" busy={busy} disabled={!text.trim()} onClick={async () => { setBusy(true); try { onForm(await call("missions.describe", { text })); } catch (e: any) { toast(errText(e), "danger"); } finally { setBusy(false); } }}>Fill in the form</Button></>}>
      <p className="muted">Say what you want in your own words. The form is filled in for you to review - nothing is saved or activated until you confirm, and it cannot exceed your current settings.</p>
      <textarea className="input" autoFocus rows={4} value={text} onChange={(e) => setText(e.target.value)} placeholder="Every weekday at 7:30, summarise important emails and list what needs action." />
    </Modal>
  );
}

const OUTPUT_FORMATS: [string, string][] = [["markdown", "Report (Markdown, headings and bullets)"], ["bullets", "Short bullet list"], ["digest", "Digest (headline + 3 bullets + sources)"],
  ["detailed", "Detailed report with sources"], ["table", "Table"], ["email", "Email-style message"], ["plain", "Plain text"], ["json", "JSON (for other tools)"]];

function MissionForm({ initial, onClose, onSaved }: { initial: any; onClose: () => void; onSaved: () => void }) {
  const { call, toast, go } = useApp();
  const [m, setM] = useState<any>({ ...initial, schedule: typeof initial.schedule === "string" ? initial.schedule : initial.schedule?.cron ?? JSON.stringify(initial.schedule) });
  const [catalog, setCatalog] = useState<any[]>([]);
  const [preview, setPreview] = useState<any>(null);
  const [busy, setBusy] = useState(false);
  const [web, setWeb] = useState<any>(null);
  useEffect(() => { call<any>("connectors.list").then((r) => setWeb(r.connectors?.find((c: any) => c.id === "web") ?? null)).catch(() => undefined); }, [call]);
  const tools: string[] = m.allowed_tools ?? [];
  const webNeeded = web && tools.some((t) => t.startsWith("web.")) ? (!web.enabled || !web.use_missions
    ? "Web tools are selected but the Web connector is turned off for routines (Settings > Connectors)."
    : tools.includes("web.search") && !web.search_ready ? "Web search needs a search provider (Exa, Brave, Tavily or a SearXNG server) and its API key. Until that is set up this routine cannot search and will report an error each time it runs." : "") : "";
  useEffect(() => { call<any>("tools.catalog").then((c) => setCatalog(c.tools.filter((t: any) => t.enabled))).catch(() => undefined); }, [call]);
  useEffect(() => {
    if (!m.schedule) return;
    const t = setTimeout(() => call("missions.parse_schedule", { text: String(m.schedule) }).then(setPreview).catch(() => setPreview(null)), 250);
    return () => clearTimeout(t);
  }, [m.schedule, call]);
  const set = (k: string, v: any) => setM({ ...m, [k]: v });
  const save = async () => {
    setBusy(true);
    const body = { name: m.name, objective: m.objective, schedule: typeof initial.schedule === "object" && m.schedule === JSON.stringify(initial.schedule) ? initial.schedule : m.schedule,
      timezone: m.timezone, allowed_tools: m.allowed_tools, missed_run_policy: m.missed_run_policy, notification_level: m.notification_level ?? "notify",
      output_format: m.output_format ?? "markdown", kind: m.kind ?? "routine", overnight: m.overnight ?? true, model_id: m.model_id ?? null };
    try {
      if (m.id) await call("missions.update", { mission_id: m.id, mission: body }); else await call("missions.create", { mission: body });
      toast("Saved as draft - activate it when ready", "ok");
      onSaved();
    } catch (e: any) { toast(errText(e), "danger"); } finally { setBusy(false); }
  };
  return (
    <Modal wide title={m.id ? "Edit routine" : "New routine"} onClose={onClose}
      actions={<><Button onClick={onClose}>Cancel</Button><Button kind="primary" busy={busy} disabled={!m.name || !m.objective} onClick={save}>Save</Button></>}>
      <div className="grid-2">
        <div className="col">
          <Field label="Name"><input className="input" value={m.name ?? ""} onChange={(e) => set("name", e.target.value)} /></Field>
{/* one kind only: 'Routines' (the old Mission type behaved identically) */}
          <Field label="What should it do?"><textarea className="input" rows={5} value={m.objective ?? ""} onChange={(e) => set("objective", e.target.value)} /></Field>
          <SchedulePicker value={initial.schedule} onChange={(t) => set("schedule", t)} />
          <div className="small muted" role="status">
            {preview?.schedule ? <>Runs: <b>{preview.text ?? "as set"}</b>{preview.next_run ? <> · next: {new Date(preview.next_run).toLocaleString([], { weekday: "short", day: "numeric", month: "short", hour: "numeric", minute: "2-digit" })}</> : null}</>
              : m.schedule ? <span style={{ color: "var(--warn)" }}>Could not understand this schedule yet.</span> : null}
          </div>
          {m.schedule_note && <div className="banner warn small">{m.schedule_note}</div>}
        </div>
        <div className="col">
          <Field label="Allowed tools" help="The routine can use only these. Widening an active routine pauses it until you re-activate.">
            <div className="card flat" style={{ maxHeight: 220, overflowY: "auto", padding: 8 }}>
              {catalog.map((t) => (
                <label key={t.name} className="row small" style={{ padding: 3 }}>
                  <input type="checkbox" checked={(m.allowed_tools ?? []).includes(t.name)}
                    onChange={(e) => set("allowed_tools", e.target.checked ? [...(m.allowed_tools ?? []), t.name] : m.allowed_tools.filter((x: string) => x !== t.name))} />
                  <span className="mono grow">{t.name}</span>{t.side_effect !== "NONE" && <Badge tone={t.side_effect === "EXTERNAL_WRITE" ? "danger" : "warn"}>{t.side_effect.toLowerCase()}</Badge>}
                </label>))}
              {!catalog.length && <ChipInput value={m.allowed_tools ?? []} onChange={(v) => set("allowed_tools", v)} />}
            </div>
          </Field>
          <Field label="If runs were missed (PC off / signed out)"><select className="input" value={m.missed_run_policy ?? "RUN_ONCE"} onChange={(e) => set("missed_run_policy", e.target.value)}>
            <option value="RUN_ONCE">Run once when I'm back</option><option value="SKIP">Skip</option><option value="RUN_ALL">Run all missed (max 5)</option></select></Field>
          <Field label="Notify me"><select className="input" value={m.notification_level ?? "notify"} onChange={(e) => set("notification_level", e.target.value)}>
            <option value="notify">When finished</option><option value="silent">Silently (Home only)</option></select></Field>
          <Field label="Output format" help="How the result should be written.">
            <select className="input" value={m.output_format ?? "markdown"} onChange={(e) => set("output_format", e.target.value)}>
              {OUTPUT_FORMATS.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
            </select></Field>
          {webNeeded && (
            <div className="banner warn small" role="alert"><Icon name="alert" /><span className="grow">{webNeeded}</span>
              <Button small tone="warn" onClick={() => go("settings", { section: "web" })}>Open Web Access</Button></div>
          )}
          <div className="banner info small"><Icon name="info" />Runs while this PC is on and you're signed in to Windows (locked is fine).</div>
        </div>
      </div>
    </Modal>
  );
}
