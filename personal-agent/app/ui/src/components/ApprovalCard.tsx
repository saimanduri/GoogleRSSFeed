// Approval shown with the EXACT payload (spec 23, 39.14). The payload hash the user saw is sent back and
// re-checked by the gateway; high-risk approvals require step-up; edits create a new approval.
import { useEffect, useRef, useState } from "react";
import { errText, useApp } from "../app";
import { Icon } from "./Icon";
import { Badge, Button, Sensitivity, Time } from "./ui";

const TITLES: Record<string, string> = {
  "reminders.propose": "Set a reminder?", "m365.send_mail": "Send this email?", "m365.create_draft": "Create this draft?",
  "outlook_local.create_draft": "Create this Outlook draft?", "web.search": "Search the web with this query?", "web.fetch": "Open this web page?",
};

export function ApprovalCard({ a, onDone }: { a: any; onDone?: () => void }) {
  const { call, toast } = useApp();
  const opened = useRef(Date.now());
  const [busy, setBusy] = useState(false);
  const [edit, setEdit] = useState(false);
  const [draft, setDraft] = useState(JSON.stringify(a.payload, null, 2));
  useEffect(() => { opened.current = Date.now(); }, [a.id]);
  const decide = async (approve: boolean, edited?: any) => {
    setBusy(true);
    try {
      await call("approvals.decide", { approval_id: a.id, approve, payload_hash: a.payload_hash, opened_at_ms: opened.current,
        ...(edited ? { edited_payload: edited } : {}) });
      toast(edited ? "Edited and re-proposed - review the new version" : approve ? (a.tool === "reminders.propose" ? "Reminder confirmed" : "Allowed once") : "Denied", approve ? "ok" : "info");
      onDone?.();
    } catch (e: any) { toast(errText(e), "danger"); } finally { setBusy(false); }
  };
  const isReminder = a.tool === "reminders.propose";
  return (
    <div className={`approval-card ${a.risk === "high" ? "high" : ""}`}>
      <div className="row" style={{ marginBottom: 8 }}>
        <Icon name={isReminder ? "reminders" : "approvals"} />
        <strong className="grow">{TITLES[a.tool] ?? `Allow ${a.tool}?`}</strong>
        <Badge tone={a.risk === "high" ? "danger" : "warn"}>{a.risk} risk</Badge>
        <Sensitivity level={a.sensitivity} />
      </div>
      {isReminder ? (
        <div className="col" style={{ gap: 4 }}>
          <div style={{ fontSize: "1.05em" }}>⏰ <b>{a.payload.text}</b></div>
          <div className="muted">{new Date(a.payload.due_at).toLocaleString([], { weekday: "short", day: "numeric", month: "short", year: "numeric", hour: "numeric", minute: "2-digit" })}</div>
        </div>
      ) : edit ? (
        <textarea className="input mono" rows={8} value={draft} onChange={(e) => setDraft(e.target.value)} />
      ) : (
        <pre className="code">{JSON.stringify(a.payload, null, 2)}</pre>
      )}
      {a.destination && <div className="small" style={{ marginTop: 6 }}>Destination: <span className="mono">{a.destination}</span></div>}
      <div className="small muted" style={{ marginTop: 4 }}>{a.reason} · expires <Time iso={a.expires_at} /></div>
      <div className="row" style={{ marginTop: 10 }}>
        {edit ? (
          <>
            <Button small onClick={() => setEdit(false)}>Cancel edit</Button>
            <Button small kind="primary" busy={busy} onClick={() => { try { void decide(false, JSON.parse(draft)); } catch { toast("Invalid JSON", "danger"); } }}>Re-propose edited</Button>
          </>
        ) : (
          <>
            <Button small kind="primary" icon="check" busy={busy} onClick={() => decide(true)}>{isReminder ? "Confirm" : "Allow once"}</Button>
            <Button small icon="x" busy={busy} onClick={() => decide(false)}>Deny</Button>
            {!isReminder && <Button small kind="ghost" icon="edit" onClick={() => setEdit(true)}>Edit</Button>}
          </>
        )}
      </div>
    </div>
  );
}
