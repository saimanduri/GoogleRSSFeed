import { useApp } from "../app";
import { ConnectNotice, SkillCard, useSkills } from "../components/EmailSkillList";
import { Icon } from "../components/Icon";
import { Button, Card, Empty } from "../components/ui";

const QUICK: [string, string][] = [
  ["Today's mail in numbers", "How many emails did I receive today, how many are unread, and in how many of them am I marked in To?"],
  ["What needs my approval?", "Check my inbox for emails waiting for my approval. For each, read it and tell me exactly what approval is sought."],
  ["Deadlines this week", "Which emails in my inbox mention a deadline in the next 3 days or an overdue one? List them by date."],
  ["Mail I have not answered", "Which emails addressed to me (in To) with a question have I not replied to in the last 2 days?"],
];

// Results of the email-monitoring skills (the same routines as Settings > Email monitoring) plus quick questions.
export function Outlook() {
  const { go } = useApp();
  const { data, load } = useSkills(true);
  if (!data) return <div className="page"><div className="skeleton" style={{ height: 240 }} /></div>;
  const on = data.skills.filter((s) => s.enabled);
  const off = data.skills.filter((s) => !s.enabled);
  return (
    <div className="page">
      <div className="page-header">
        <div className="grow"><h1>Outlook</h1><div className="muted">What your email monitoring found. Read-only: nothing is sent, moved or deleted.</div></div>
        <Button icon="settings" onClick={() => go("settings", { section: "emailmon" })}>Email monitoring settings</Button>
      </div>
      <ConnectNotice data={data} />
      <Card title="Ask about your mail now" icon="chat">
        <div className="row wrap">
          {QUICK.map(([label, prompt]) => <Button key={label} small onClick={() => go("chat", { new: Date.now(), prefill: prompt })}>{label}</Button>)}
        </div>
      </Card>
      <div style={{ height: 14 }} />
      {on.length === 0 ? (
        <Card><Empty icon="mail" title="No email skill is switched on yet">Switch skills on below, or in Settings &gt; Email monitoring. Try "Hourly inbox check" and "Emails waiting for my approval".</Empty></Card>
      ) : (
        <div className="grid-2">{on.map((s) => <SkillCard key={s.id} s={s} onChanged={load} />)}</div>
      )}
      {off.length > 0 && (
        <>
          <h3 className="group-title" style={{ paddingLeft: 0, marginTop: 18 }}><Icon name="plus" size={12} /> More skills you can switch on</h3>
          <div className="grid-2">{off.map((s) => <SkillCard key={s.id} s={s} onChanged={load} showResult={false} />)}</div>
        </>
      )}
    </div>
  );
}
