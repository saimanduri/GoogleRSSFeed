import { useEffect, useState } from "react";
import { errText, useApp } from "../app";
import { Badge, Button, Card, Empty, Tabs, Time, Toggle } from "../components/ui";
import { SearchBox } from "../components/SearchBox";

const srcLabel = (s: string) => s.startsWith("learned:chat") ? "learned from your chats" : s.startsWith("learned:routine") ? "learned from a routine you use" : s.startsWith("learned:file") ? "learned from a file in My Files"
  : s === "user" ? "you told me" : s.startsWith("agent:") ? "proposed by the assistant" : s;
const TRUST: Record<string, string> = { TRUSTED: "ok", VERIFIED: "info", INFERRED: "warn", UNTRUSTED: "danger" };

export function Memory() {
  const { call, toast, route, deferDelete, isHidden, status, refresh } = useApp();
  const [find, setFind] = useState("");
  const learning = status?.ui?.["memory.auto_learn"] !== false;
  const [tab, setTab] = useState<"about" | "all" | "proposed" | "review">((route.params?.tab as any) ?? "about");
  const [list, setList] = useState<any[]>([]);
  const [about, setAbout] = useState<any>(null);
  const [review, setReview] = useState<any>(null);
  const [text, setText] = useState("");
  const load = async () => {
    if (tab === "about") setAbout(await call("memory.about_me"));
    else if (tab === "review") setReview(await call("memory.review"));
    else setList(await call<any[]>("memory.list", { status: tab === "proposed" ? "PROPOSED" : "" }));
  };
  useEffect(() => { void load().catch(() => undefined); /* eslint-disable-next-line */ }, [tab]);
  const act = async (id: string, action: string, content?: string) => { try { await call("memory.action", { id, action, content }); void load(); } catch (e: any) { toast(errText(e), "danger"); } };
  const Row = ({ m }: { m: any }) => isHidden(`memory:${m.id}`) || isHidden("memory:*") || (find && !String(m.content).toLowerCase().includes(find.toLowerCase())) ? null : (
    <div className="list-item">
      <div className="grow"><div>{m.content}</div><div className="small faint">{m.type} · {srcLabel(m.source)} · <Time iso={m.created_at} /></div></div>
      <Badge tone={TRUST[m.trust]}>{m.trust}</Badge>{m.status !== "ACTIVE" && <Badge>{m.status}</Badge>}
      {m.status === "PROPOSED" && <Button small kind="primary" onClick={() => act(m.id, "confirm")}>Confirm</Button>}
      {m.status === "PROPOSED" && <Button small onClick={() => act(m.id, "reject")}>Reject</Button>}
      <Button small kind="ghost" icon="edit" title="Edit" onClick={() => { const c = prompt("Edit memory", m.content); if (c) void act(m.id, "edit", c); }} />
      {m.status === "ACTIVE" && <Button small kind="ghost" title="Disable" onClick={() => act(m.id, "disable")}>Disable</Button>}
      <Button small kind="ghost" icon="trash" title="Forget this - it will not be learned again" onClick={() => deferDelete({ key: `memory:${m.id}`, label: "Memory deleted", commit: () => call("memory.action", { id: m.id, action: "delete" }), after: () => void load() })} />
    </div>
  );
  return (
    <div className="page">
      <div className="page-header"><div className="grow"><h1>Memory</h1><div className="muted">What the assistant knows about you. It <b>learns automatically</b> from your chats, your routines and the files you add - and you decide what stays: edit, disable or <b>Forget</b> anything. Forgotten facts are never learned again. Memory can never change settings, and secrets or ID numbers are never stored.</div></div>
        <label className="row small" title="Settings > Memory > Learn about me automatically"><span>Learn automatically</span>
          <Toggle on={learning} label="Learn about me automatically" onChange={async (v) => { try { await call("settings.apply", { changes: { "memory.auto_learn": v } }); await refresh(); toast(v ? "Learning is on" : "Learning is off - existing memories stay until you forget them", "ok"); } catch (e: any) { toast(errText(e), "danger"); } }} /></label></div>
      <Card>
        <div className="row"><input className="input grow" value={text} placeholder="Tell the agent something to remember, e.g. 'I prefer short bullet-point reports'" onChange={(e) => setText(e.target.value)}
          onKeyDown={async (e) => { if (e.key === "Enter" && text) { await call("memory.add", { content: text }); setText(""); void load(); } }} />
          <Button kind="primary" disabled={!text} onClick={async () => { await call("memory.add", { content: text }); setText(""); void load(); }}>Remember</Button></div>
      </Card>
      <div style={{ height: 14 }} />
      <SearchBox style={{ marginBottom: 10 }} placeholder="Search what I remember about you" value={find} onChange={setFind} />
      <Tabs tabs={[["about", "About me"], ["all", "All"], ["proposed", "Proposed"], ["review", "Weekly review"]]} value={tab} onChange={setTab} />
      {tab === "about" && about && (
        <div className="grid-2">
          <Card title="What you told me">{about.stated.length ? <div className="list">{about.stated.map((m: any) => <Row key={m.id} m={m} />)}</div> : <Empty title="Nothing yet" icon="memory" />}</Card>
          <Card title="What I learned about you (automatic)">{about.inferred.length ? <div className="list">{about.inferred.map((m: any) => <Row key={m.id} m={m} />)}</div> : <Empty title={learning ? "Nothing learned yet - it grows as you chat" : "Learning is off"} icon="memory" />}</Card>
        </div>
      )}
      {(tab === "all" || tab === "proposed") && <Card>{list.length ? <div className="list">{list.map((m) => <Row key={m.id} m={m} />)}</div> : <Empty title="No memories" icon="memory" />}</Card>}
      {tab === "review" && review && (
        <div className="grid-2">
          <Card title="Proposed">{review.proposed.map((m: any) => <Row key={m.id} m={{ ...m, status: "PROPOSED", trust: "INFERRED" }} />)}{!review.proposed.length && <div className="faint small">None</div>}</Card>
          <Card title="Stale (not confirmed in 90 days)">{review.stale.map((m: any) => <Row key={m.id} m={{ ...m, status: "ACTIVE" }} />)}{!review.stale.length && <div className="faint small">None</div>}</Card>
          <Card title="Possibly conflicting">{review.conflicts.map((c: any, i: number) => <div key={i} className="small">"{c.a.content}" ⟷ "{c.b.content}"</div>)}{!review.conflicts.length && <div className="faint small">None</div>}</Card>
        </div>
      )}
      <div style={{ marginTop: 14 }}><Button kind="ghost" small onClick={() => deferDelete({ key: "memory:*", label: "All memory deleted", commit: () => call("memory.delete_all"), after: () => void load() })}>Delete all memory</Button></div>
    </div>
  );
}
