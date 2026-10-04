import { useState } from "react";
import { errText, useApp } from "../../app";
import { ConnectNotice, SkillCard, useSkills } from "../../components/EmailSkillList";
import { Button, Card, Toggle } from "../../components/ui";

// Settings > Email monitoring: switch each Outlook skill on or off, set VIP names, show or hide the Outlook button.
export function EmailMonitoring({ generic, onChanged }: { generic: React.ReactNode; onChanged: () => void }) {
  const { call, toast, refresh, go, status } = useApp();
  const { data, load } = useSkills();
  const [busy, setBusy] = useState(false);
  if (!data) return <div className="skeleton" style={{ height: 240 }} />;
  const showNav = status?.ui?.["ui.show_outlook_nav"] !== false;
  const setNav = async (v: boolean) => {
    try { await call("settings.apply", { changes: { "ui.show_outlook_nav": v } }); await refresh(); onChanged(); } catch (e: any) { toast(errText(e), "danger"); }
  };
  const bulk = async (enabled: boolean) => {
    setBusy(true);
    let failed = "";
    for (const s of data.skills) {
      if (s.enabled === enabled || (enabled && s.needs_vips && !data.vips.length)) continue;
      try { await call("emailskills.set", { skill: s.id, enabled }); } catch (e: any) { failed = errText(e); break; }
    }
    setBusy(false);
    await load();
    if (failed) toast(failed, "warn"); else toast(enabled ? "Skills switched on" : "All skills switched off", enabled ? "ok" : "info");
  };
  return (
    <div className="col">
      <ConnectNotice data={data} />
      <Card>
        <div className="setting-row" style={{ borderBottom: 0 }}>
          <div><b>Show the Outlook button in the left menu</b><div className="small muted">The Outlook screen shows what your skills found. Hiding the button does not stop the skills.</div></div>
          <Toggle on={showNav} onChange={setNav} label="Show the Outlook button" />
        </div>
        <div className="row wrap" style={{ marginTop: 6 }}>
          <Button small kind="primary" busy={busy} onClick={() => bulk(true)}>Switch all on</Button>
          <Button small busy={busy} onClick={() => bulk(false)}>Switch all off</Button>
          <Button small kind="ghost" icon="mail" onClick={() => go("outlook")}>Open Outlook screen</Button>
        </div>
        <div className="small faint" style={{ marginTop: 8 }}>
          Each skill is a read-only routine: it reads your mail in classic Outlook, and nothing is sent, moved or deleted. It runs while this PC is on
          and you are signed in to Windows. Change a schedule in Missions &amp; Routines.
        </div>
      </Card>
      <div className="grid-2">{data.skills.map((s) => <SkillCard key={s.id} s={s} onChanged={load} />)}</div>
      <h2 style={{ marginTop: 10 }}>VIP names</h2>
      {generic}
    </div>
  );
}
