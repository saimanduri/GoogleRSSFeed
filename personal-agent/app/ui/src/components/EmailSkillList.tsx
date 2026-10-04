// Shared by Settings > Email monitoring and the Outlook screen: the Outlook skills with their on/off switch.
import { useCallback, useEffect, useState } from "react";
import { onEvent } from "../api/gateway";
import { errText, useApp } from "../app";
import { Markdown } from "./Markdown";
import { Badge, Button, Card, Notice, Time, Toggle } from "./ui";

export type Skill = {
  id: string; title: string; description: string; needs_vips: boolean; enabled: boolean; status: string; schedule_text: string; mission_id: string | null;
  next_run_at: string | null; last_run_at: string | null; last_state: string | null; last_at: string | null; last_error: string | null;
  last_result: string; last_nothing: boolean;
};
export type SkillData = { skills: Skill[]; outlook_installed: boolean; usable: boolean; reason: string; connector_enabled: boolean; vips: string[]; show_nav: boolean };

export function useSkills(markSeen = false) {
  const { call } = useApp();
  const [data, setData] = useState<SkillData | null>(null);
  const load = useCallback(() => call<SkillData>("emailskills.list", markSeen ? { mark_seen: true } : {}).then(setData).catch(() => undefined), [call, markSeen]);
  useEffect(() => { void load(); return onEvent((t) => { if (["emailskills.changed", "tasks.changed", "missions.changed"].includes(t)) void load(); }); }, [load]);
  return { data, load };
}

export function ConnectNotice({ data }: { data: SkillData }) {
  const { go } = useApp();
  if (!data.outlook_installed) return <Notice text="Classic Outlook is not installed on this PC (the new Outlook cannot be read). Email monitoring needs classic Outlook." />;
  if (!data.connector_enabled || !data.usable) {
    return (
      <Notice text={data.connector_enabled ? `Local Outlook is not usable for routines yet: ${data.reason || "check Settings > Connectors"}.` : "Local Outlook is not connected yet. Turn it on in Settings > Connectors (and switch on 'Use in missions')."}>
        <Button small tone="warn" onClick={() => go("settings", { section: "connectors" })}>Open Connectors</Button>
      </Notice>
    );
  }
  return null;
}

export function SkillCard({ s, onChanged, showResult = true }: { s: Skill; onChanged: () => void; showResult?: boolean }) {
  const { call, toast, go } = useApp();
  const [busy, setBusy] = useState(false);
  const set = async (enabled: boolean) => {
    setBusy(true);
    try { await call("emailskills.set", { skill: s.id, enabled }); onChanged(); toast(enabled ? `${s.title} is on` : `${s.title} is off`, enabled ? "ok" : "info"); }
    catch (e: any) { toast(errText(e), e?.code === "needs_setup" ? "warn" : "danger"); } finally { setBusy(false); }
  };
  const run = async () => {
    setBusy(true);
    try { await call("emailskills.run", { skill: s.id }); toast("Started - the result appears here in a minute or two", "info"); onChanged(); }
    catch (e: any) { toast(errText(e), e?.code === "needs_setup" ? "warn" : "danger"); } finally { setBusy(false); }
  };
  return (
    <Card>
      <div className="row" style={{ alignItems: "flex-start" }}>
        <div className="grow">
          <div className="row" style={{ gap: 8 }}><strong>{s.title}</strong>{s.enabled ? <Badge tone="ok">on</Badge> : s.status === "PAUSED" ? <Badge>paused</Badge> : <Badge>off</Badge>}</div>
          <div className="small muted" style={{ marginTop: 4 }}>{s.description}</div>
          <div className="small faint" style={{ marginTop: 4 }}>Runs: {s.schedule_text}{s.enabled && s.next_run_at && <> · next <Time iso={s.next_run_at} smart /></>}</div>
        </div>
        <Toggle on={s.enabled} disabled={busy} label={`${s.title} on or off`} onChange={set} />
      </div>
      <div className="row" style={{ marginTop: 10 }}>
        <Button small icon="play" busy={busy} onClick={run}>Run now</Button>
        {s.mission_id && <Button small kind="ghost" icon="edit" onClick={() => go("missions")}>Edit schedule</Button>}
      </div>
      {showResult && s.last_error && s.last_state !== "COMPLETED" && <div style={{ marginTop: 10 }}><Notice text={`Last run: ${s.last_error}`} /></div>}
      {showResult && s.last_state === "COMPLETED" && (
        <div style={{ marginTop: 12 }}>
          <div className="small faint" style={{ marginBottom: 6 }}>Last result · <Time iso={s.last_at} smart /></div>
          {s.last_nothing ? <div className="notice info"><span>Nothing new.</span></div> : <div className="card flat"><Markdown text={s.last_result} /></div>}
        </div>
      )}
      {showResult && s.enabled && !s.last_state && <div className="small faint" style={{ marginTop: 10 }}>No run yet. It will run on its schedule, or press Run now.</div>}
    </Card>
  );
}
