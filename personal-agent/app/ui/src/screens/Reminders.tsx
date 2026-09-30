import { useEffect, useState } from "react";
import { onEvent } from "../api/gateway";
import { errText, useApp } from "../app";
import { Badge, Button, Card, Empty, Field, Time } from "../components/ui";

export function Reminders() {
  const { call, toast, go } = useApp();
  const [list, setList] = useState<any[]>([]);
  const [text, setText] = useState("");
  const [due, setDue] = useState(() => { const d = new Date(Date.now() + 3600e3); d.setMinutes(0, 0, 0); return toLocalInput(d); });
  const [all, setAll] = useState(false);
  const load = () => call<any[]>("reminders.list", { include_done: all }).then(setList).catch(() => undefined);
  useEffect(() => { void load(); return onEvent((t) => (t === "reminders.changed" || t === "reminder.fired") && void load()); /* eslint-disable-next-line */ }, [all]);
  const act = async (id: string, action: string, extra: any = {}) => { try { await call("reminders.action", { id, action, ...extra }); void load(); } catch (e: any) { toast(errText(e), "danger"); } };
  return (
    <div className="page">
      <div className="page-header"><div className="grow"><h1>Reminders</h1>
        <div className="muted">Ask in chat or by voice - "remind me to call the dentist on Friday at 9" - and confirm the card. Or add one here.</div></div>
        <Button icon="mic" onClick={() => go("chat", { new: Date.now() })}>Ask by voice</Button></div>
      <Card title="New reminder" icon="plus">
        <div className="row wrap" style={{ alignItems: "flex-end" }}>
          <div className="grow"><Field label="What"><input className="input" value={text} onChange={(e) => setText(e.target.value)} placeholder="Call the dentist" /></Field></div>
          <Field label="When"><input className="input" type="datetime-local" value={due} onChange={(e) => setDue(e.target.value)} /></Field>
          <Button kind="primary" disabled={!text} onClick={async () => { try { await call("reminders.create", { text, due_at: due }); setText(""); toast("Reminder set", "ok"); void load(); } catch (e: any) { toast(errText(e), "danger"); } }}>Add</Button>
        </div>
      </Card>
      <div style={{ height: 14 }} />
      <Card title="Upcoming" actions={<Button small kind="ghost" onClick={() => setAll(!all)}>{all ? "Hide done" : "Show all"}</Button>}>
        {!list.length ? <Empty icon="reminders" title="No reminders" /> : (
          <div className="list">{list.map((r) => (
            <div key={r.id} className="list-item">
              <div style={{ fontSize: 22 }}>{r.status === "FIRED" ? "🔔" : "⏰"}</div>
              <div className="grow"><div style={{ fontWeight: 600 }}>{r.text}</div><div className="small muted">{new Date(r.due_at).toLocaleString()} · <Time iso={r.due_at} /></div></div>
              <Badge tone={r.status === "FIRED" ? "warn" : r.status === "SCHEDULED" ? "accent" : ""}>{r.status}</Badge>
              {r.status === "FIRED" && <><Button small onClick={() => act(r.id, "snooze", { minutes: 10 })}>Snooze 10 min</Button><Button small onClick={() => act(r.id, "dismiss")}>Done</Button></>}
              {r.status === "SCHEDULED" && <Button small kind="ghost" icon="x" title="Cancel" onClick={() => act(r.id, "cancel")} />}
            </div>))}</div>
        )}
      </Card>
    </div>
  );
}

function toLocalInput(d: Date) {
  const p = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}T${p(d.getHours())}:${p(d.getMinutes())}`;
}
