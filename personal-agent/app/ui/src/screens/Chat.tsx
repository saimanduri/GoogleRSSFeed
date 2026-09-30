import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { onEvent } from "../api/gateway";
import { errText, useApp } from "../app";
import { ApprovalCard } from "../components/ApprovalCard";
import { Icon } from "../components/Icon";
import { Markdown } from "../components/Markdown";
import { RunTimeline } from "../components/RunTimeline";
import { Badge, Button, Empty, Sensitivity, Time, Toggle } from "../components/ui";
import { VoiceButton } from "../components/VoiceButton";

// Extract the (partial) "answer" text from a streaming JSON action, e.g. {"thought":"..","action":"final","answer":"Hel
function partialAnswer(raw: string): string | null {
  const t = raw.trimStart();
  if (!t.startsWith("{") && !t.startsWith("```")) return raw;
  const m = raw.match(/"answer"\s*:\s*"((?:[^"\\]|\\.)*)/s);
  if (!m) return null;
  try { return JSON.parse(`"${m[1].replace(/\\$/, "")}"`); } catch { return m[1].replace(/\\n/g, "\n").replace(/\\"/g, '"'); }
}

export function Chat() {
  const { call, toast, route, status } = useApp();
  const [chats, setChats] = useState<any[]>([]);
  const [archived, setArchived] = useState(false);
  const [active, setActive] = useState<string | null>(null);
  const [data, setData] = useState<any>(null);
  const [text, setText] = useState("");
  const [sending, setSending] = useState(false);
  const [liveRun, setLiveRun] = useState<string | null>(null);
  const [stream, setStream] = useState("");
  const [stepsRun, setStepsRun] = useState<string | null>(null);
  const [showSteps, setShowSteps] = useState<boolean>(status?.ui?.["chat.show_steps"] ?? true);
  const [filter, setFilter] = useState("");
  const endRef = useRef<HTMLDivElement>(null);
  const taRef = useRef<HTMLTextAreaElement>(null);

  const loadChats = useCallback(async () => setChats(await call<any[]>("chat.list", { archived })), [call, archived]);
  const load = useCallback(async (id = active) => { if (id) setData(await call("chat.get", { chat_id: id })); }, [call, active]);

  useEffect(() => { void loadChats(); }, [loadChats]);
  useEffect(() => { void load(); }, [active, load]);
  useEffect(() => { if (route.params?.new) void newChat(); /* eslint-disable-next-line */ }, [route.params?.new]);
  useEffect(() => { if (!active && chats.length && !route.params?.new) setActive(chats[0].id); }, [chats, active, route.params?.new]);
  useEffect(() => { endRef.current?.scrollIntoView({ behavior: "smooth" }); }, [data?.messages?.length, stream, data?.pending_approvals?.length]);

  useEffect(() => onEvent((topic, d) => {
    if (topic === "llm.delta" && d.run_id && d.run_id === liveRunRef.current) {
      if (d.done) return;
      setStream((s) => s + d.delta);
    }
    if (topic === "chat.message" && d.chat_id === activeRef.current) {
      void load(d.chat_id);
      if (d.run_id === liveRunRef.current) { setLiveRun(null); setStream(""); }
      void loadChats();
    }
    if (topic === "approvals.changed" && (!d.chat_id || d.chat_id === activeRef.current)) void load(activeRef.current);
  }), [load, loadChats]);
  const liveRunRef = useRef<string | null>(null);
  const activeRef = useRef<string | null>(null);
  liveRunRef.current = liveRun;
  activeRef.current = active;

  const newChat = async () => {
    const r = await call<any>("chat.create", {});
    await loadChats();
    setActive(r.id);
    setStepsRun(null);
    setTimeout(() => taRef.current?.focus(), 50);
  };

  const send = async (msg = text, voice = false) => {
    const t = msg.trim();
    if (!t || sending) return;
    let chatId = active;
    if (!chatId) { chatId = (await call<any>("chat.create", {})).id; setActive(chatId); }
    setSending(true);
    setText("");
    try {
      const r = await call<any>("chat.send", { chat_id: chatId, text: t, voice });
      setLiveRun(r.run_id);
      setStepsRun(r.run_id);
      setStream("");
      await load(chatId);
      void loadChats();
    } catch (e: any) { toast(errText(e), "danger"); setText(t); } finally { setSending(false); }
  };

  const chat = data?.chat;
  const sensitive = chat && ["CONFIDENTIAL", "RESTRICTED"].includes(chat.sensitivity);
  const visibleChats = useMemo(() => chats.filter((c) => c.title.toLowerCase().includes(filter.toLowerCase())), [chats, filter]);
  const partial = partialAnswer(stream);

  return (
    <div className={`chat-layout ${showSteps ? "with-steps" : ""}`}>
      <aside className="chat-list">
        <Button kind="primary" icon="plus" onClick={newChat}>New chat</Button>
        <input className="input" style={{ margin: "8px 0" }} placeholder="Filter chats" value={filter} onChange={(e) => setFilter(e.target.value)} />
        {visibleChats.map((c) => (
          <div key={c.id} className={`list-item clickable ${c.id === active ? "selected" : ""}`} onClick={() => { setActive(c.id); setStepsRun(null); }}>
            <Icon name="chat" size={15} />
            <div className="grow"><div className="ellipsis" style={{ fontWeight: 550 }}>{c.title}</div><div className="faint small"><Time iso={c.updated_at} /></div></div>
            {c.hwm >= 2 && <span className="dot warn" title="contains confidential data" />}
          </div>
        ))}
        {!chats.length && <div className="faint small" style={{ padding: 10 }}>No chats yet.</div>}
        <div className="spacer" />
        <Button kind="ghost" small icon="archive" onClick={() => setArchived(!archived)}>{archived ? "Show active" : "Show archived"}</Button>
      </aside>

      <section className="chat-main">
        <div className="chat-head">
          <div className="grow ellipsis" style={{ fontWeight: 650 }}>{chat?.title ?? "New chat"}</div>
          {chat && <Sensitivity level={chat.sensitivity} />}
          {chat?.sources?.map((s: string) => <Badge key={s}>{s}</Badge>)}
          {chat && (
            <span className="row small muted" title="Allow the agent to use tools in this chat">
              Tools <Toggle on={!!chat.allow_tools} label="Allow tools in this chat" onChange={async (v) => { await call("chat.update", { chat_id: chat.id, allow_tools: v }); void load(); }} />
            </span>
          )}
          {chat && <Button small kind="ghost" icon="archive" title="Archive chat" onClick={async () => { await call("chat.update", { chat_id: chat.id, archived: !chat.archived }); setActive(null); void loadChats(); }} />}
          {chat && <Button small kind="ghost" icon="trash" title="Delete chat" onClick={async () => { if (confirm("Delete this chat and its history?")) { await call("chat.delete", { chat_id: chat.id }); setActive(null); setData(null); void loadChats(); } }} />}
          <Button small kind={showSteps ? "primary" : undefined} icon="steps" onClick={() => setShowSteps(!showSteps)}>Steps</Button>
        </div>
        {sensitive && <div className="banner warn" style={{ margin: "10px 18px 0" }}><Icon name="alert" />This chat contains {chat.sensitivity} data. Web access and sending need your approval (or are blocked).</div>}
        <div className="messages">
          {!data?.messages?.length && !liveRun && (
            <Empty icon="sparkle" title="How can I help?">
              <div className="row wrap" style={{ justifyContent: "center", marginTop: 8 }}>
                {["Summarise my unread important emails", "Remind me to call the dentist on Friday at 9", "Search the web for local LLM news",
                  "Every weekday at 7:30 summarise important mail"].map((s) => <Button key={s} small onClick={() => send(s)}>{s}</Button>)}
              </div>
            </Empty>
          )}
          {data?.messages?.map((m: any) => (
            <div key={m.id} className={`msg ${m.role}`}>
              {m.role === "assistant" ? <Markdown text={m.content} /> : m.content}
              {m.role === "assistant" && (
                <div className="msg-meta">
                  {(m.sources?.length ? m.sources : ["model only"]).map((s: string) => <Badge key={s}>{s}</Badge>)}
                  {m.sensitivity > 0 && <Sensitivity level={m.sensitivity} />}
                  {m.run_id && <Button small kind="ghost" icon="steps" onClick={() => { setStepsRun(m.run_id); setShowSteps(true); }}>Steps</Button>}
                  <span className="faint small"><Time iso={m.created_at} /></span>
                </div>
              )}
            </div>
          ))}
          {data?.pending_approvals?.map((a: any) => <ApprovalCard key={a.id} a={a} onDone={() => load()} />)}
          {liveRun && (
            <div className="msg assistant">
              {partial ? <Markdown text={partial} /> : <span className="typing"><span /><span /><span /></span>}
            </div>
          )}
          <div ref={endRef} />
        </div>
        <div className="composer">
          <div className="composer-box">
            <textarea ref={taRef} rows={1} value={text} placeholder="Message Personal Agent…" title="Enter to send · Shift+Enter for a new line"
              onChange={(e) => { setText(e.target.value); e.target.style.height = "auto"; e.target.style.height = `${Math.min(220, e.target.scrollHeight)}px`; }}
              onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); void send(); } }} />
            {status?.ui?.["voice.enabled"] !== false && <VoiceButton onText={(t) => send(t, true)} />}
            <Button kind="primary" icon="send" busy={sending} disabled={!text.trim()} title="Send" onClick={() => send()} />
          </div>
          <div className="faint small" style={{ marginTop: 6 }}>The agent proposes; the gateway decides. Every step is logged and shown in Steps.</div>
        </div>
      </section>

      {showSteps && (
        <aside className="steps-panel">
          <div className="row" style={{ marginBottom: 10 }}><h3 className="grow" style={{ margin: 0 }}>Steps</h3></div>
          <RunTimeline runId={stepsRun ?? liveRun ?? data?.messages?.filter((m: any) => m.run_id).slice(-1)[0]?.run_id} />
        </aside>
      )}
    </div>
  );
}
