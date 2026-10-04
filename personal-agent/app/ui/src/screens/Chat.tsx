import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { SearchBox } from "../components/SearchBox";
import { onEvent, onFileDrop, pickFile } from "../api/gateway";
import { errText, useApp } from "../app";
import { ApprovalCard } from "../components/ApprovalCard";
import { ChatRail, turnLabel } from "../components/ChatRail";
import { Icon } from "../components/Icon";
import { InlineRename } from "../components/InlineRename";
import { FolderDialog, ShareChips, SubfolderCards } from "../components/LocalShare";
import { useExitGhost } from "../components/motion";
import { Markdown } from "../components/Markdown";
import { RunTimeline } from "../components/RunTimeline";
import { Badge, Button, Empty, Sensitivity, Time, Toggle } from "../components/ui";
import { UsageMeter } from "../components/UsageMeter";
import { VoiceButton, clearVoiceDraft, readVoiceDraft } from "../components/VoiceButton";

// Extract the (partial) "answer" text from a streaming JSON action, e.g. {"thought":"..","action":"final","answer":"Hel
function partialAnswer(raw: string): string | null {
  const t = raw.trimStart();
  if (!t.startsWith("{") && !t.startsWith("```")) return raw;
  const m = raw.match(/"answer"\s*:\s*"((?:[^"\\]|\\.)*)/s);
  if (!m) return null;
  try { return JSON.parse(`"${m[1].replace(/\\$/, "")}"`); } catch { return m[1].replace(/\\n/g, "\n").replace(/\\"/g, '"'); }
}

// Where the data in a chat came from. "gateway" (the app's own built-in tools) is plumbing, not information, so it is not shown.
const SOURCE_NAMES: Record<string, string | null> = { gateway: null, model: null, web: "web", outlook_local: "Outlook", m365: "Microsoft 365", files: "files", sandbox: "python", notify: null };
const sourceLabel = (s: string): string | null => (s in SOURCE_NAMES ? SOURCE_NAMES[s] : s);

const SLASH: { cmd: string; help: string; arg?: string }[] = [
  { cmd: "/new", help: "Start a new chat" },
  { cmd: "/remind", help: "Create a reminder (asks you to confirm)", arg: "what and when" },
  { cmd: "/mission", help: "Describe a routine in plain words", arg: "e.g. every weekday at 7:30 summarise mail" },
  { cmd: "/search", help: "Search the web", arg: "query" },
  { cmd: "/attach", help: "Attach a file from this PC (read in place, not uploaded)" },
  { cmd: "/folder", help: "Share a folder from this PC with this chat (read-only)" },
  { cmd: "/history", help: "Open History" },
  { cmd: "/steps", help: "Show or hide the step timeline" },
  { cmd: "/pin", help: "Pin or unpin this chat" },
  { cmd: "/rename", help: "Rename this chat (or press F2)", arg: "new name" },
  { cmd: "/theme", help: "Change theme", arg: "light | dark | aurora | ocean | forest | sunset | system" },
  { cmd: "/lock", help: "Lock the app" },
  { cmd: "/help", help: "List commands" },
];
const THEME_NAMES = ["system", "time_of_day", "light", "dark", "aurora", "ocean", "forest", "sunset"];

function dateGroup(iso: string): string {
  const day = (x: Date) => new Date(x.getFullYear(), x.getMonth(), x.getDate()).getTime();
  const n = Math.round((day(new Date()) - day(new Date(iso))) / 86400000);
  return n <= 0 ? "Today" : n === 1 ? "Yesterday" : n < 7 ? "Previous 7 days" : "Older";
}

export function Chat() {
  const { call, toast, route, status, go, refresh, deferDelete, isHidden } = useApp();
  const [chats, setChats] = useState<any[]>([]);
  const [archived, setArchived] = useState(false);
  const [active, setActive] = useState<string | null>(null);
  const [data, setData] = useState<any>(null);
  const [text, setText] = useState("");
  const [sending, setSending] = useState(false);
  const [liveRun, setLiveRun] = useState<string | null>(null);
  const [stream, setStream] = useState("");
  const [stepsRun, setStepsRun] = useState<string | null>(null);
  const [showSteps, setShowSteps] = useState<boolean>(status?.ui?.["chat.show_steps"] ?? false);
  const [listOpen, setListOpen] = useState(false); // the chat list is a drawer: closed until asked for
  const [filter, setFilter] = useState("");
  const endRef = useRef<HTMLDivElement>(null);
  const msgsRef = useRef<HTMLDivElement>(null);
  const [atTop, setAtTop] = useState(true);
  const [atBottom, setAtBottom] = useState(true);
  const [scrollable, setScrollable] = useState(false);
  const [unseen, setUnseen] = useState(0);
  const [slashSel, setSlashSel] = useState(0);
  const [usageKey, setUsageKey] = useState(0);
  const justOpened = useRef(true);
  const taRef = useRef<HTMLTextAreaElement>(null);

  const loadChats = useCallback(async () => setChats(await call<any[]>("chat.list", { archived })), [call, archived]);
  const load = useCallback(async (id = active) => { if (id) setData(await call("chat.get", { chat_id: id })); }, [call, active]);

  useEffect(() => { void loadChats(); }, [loadChats]);
  const renameChat = async (id: string, title: string) => {
    try { await call("chat.update", { chat_id: id, title }); await loadChats(); if (id === active) await load(); toast("Chat renamed", "ok"); }
    catch (e: any) { toast(errText(e), "danger"); }
  };
  // F2 renames the open chat (same key as renaming a file in Explorer)
  useEffect(() => {
    const h = (e: KeyboardEvent) => { if (e.key === "F2" && active) { e.preventDefault(); setRenamingHead(true); } };
    window.addEventListener("keydown", h);
    return () => window.removeEventListener("keydown", h);
  }, [active]);
  useEffect(() => { void load(); }, [active, load]);
  useEffect(() => { if (route.params?.new) void newChat(); /* eslint-disable-next-line */ }, [route.params?.new]);
  useEffect(() => { if (!active && chats.length && !route.params?.new) setActive(chats[0].id); }, [chats, active, route.params?.new]);
  useEffect(() => { if (route.params?.prefill) setText(String(route.params.prefill)); }, [route.params?.prefill]);
  useEffect(() => { if (route.params?.open) { setActive(route.params.open); setStepsRun(null); } }, [route.params?.open]);
  const onScroll = useCallback(() => {
    const el = msgsRef.current;
    if (!el) return;
    setScrollable(el.scrollHeight > el.clientHeight + 40);
    setAtTop(el.scrollTop < 40);
    const bottom = el.scrollHeight - el.scrollTop - el.clientHeight < 80;
    setAtBottom(bottom);
    if (bottom) setUnseen(0);
  }, []);
  useEffect(() => {
    const el = msgsRef.current;
    if (justOpened.current && data?.messages?.length) { justOpened.current = false; setUnseen(0); setTimeout(() => { endRef.current?.scrollIntoView(); onScroll(); }, 30); return; }
    if (el && el.scrollHeight - el.scrollTop - el.clientHeight < 200) endRef.current?.scrollIntoView({ behavior: "smooth" });
    else if (data?.messages?.length) setUnseen((n) => n + 1);
    setTimeout(onScroll, 350);
    setUsageKey((k) => k + 1);
    /* eslint-disable-next-line */
  }, [data?.messages?.length, stream, data?.pending_approvals?.length]);
  useEffect(() => { justOpened.current = true; setUnseen(0); }, [active]);
  const toBottom = () => { endRef.current?.scrollIntoView({ behavior: "smooth" }); setUnseen(0); };
  const toTop = () => msgsRef.current?.scrollTo({ top: 0, behavior: "smooth" });

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

  const newChat = async (): Promise<string> => {
    const r = await call<any>("chat.create", {});
    await loadChats();
    setActive(r.id);
    setStepsRun(null);
    setTimeout(() => taRef.current?.focus(), 50);
    return r.id;
  };

  // local files: attached from this PC, read in place (never uploaded or copied)
  const [attached, setAttached] = useState<any[]>([]);
  const [dropping, setDropping] = useState(false);
  const [renaming, setRenaming] = useState<string | null>(null);
  const [folderPath, setFolderPath] = useState<string | null>(null);
  const voiceBase = useRef("");
  // text that was dictated but not sent (window closed or crashed during a long recording) comes back
  useEffect(() => { const d = readVoiceDraft(); if (d) { setText((t) => t || d); toast("Restored your unsent voice text", "info"); } /* eslint-disable-next-line */ }, []);
  const [renamingHead, setRenamingHead] = useState(false);
  const loadAttached = useCallback(async () => {
    if (!active) { setAttached([]); return; }
    try { setAttached(await call<any[]>("localfiles.list", { chat_id: active })); } catch { setAttached([]); }
  }, [call, active]);
  useEffect(() => { void loadAttached(); return onEvent((t) => t === "localfiles.changed" && void loadAttached()); }, [loadAttached]);
  const attachPaths = useCallback(async (paths: string[]) => {
    const cid = active ?? await newChat();
    let ok = 0;
    for (const p of paths) {
      try { await call("localfiles.grant", { chat_id: cid, path: p }); ok++; }
      catch (e: any) { if (String(e?.message ?? e).includes("not a folder")) setFolderPath(p); else toast(errText(e), "warn"); }
    }
    if (ok) { toast(ok === 1 ? "File attached - read in place, not uploaded" : `${ok} files attached - read in place, not uploaded`, "ok"); void loadAttached(); }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [active, call, toast, loadAttached]);
  const attachFolder = async () => {
    const picked = await pickFile({ directory: true });
    const one = Array.isArray(picked) ? picked[0] : picked;
    if (one) { if (!active) await newChat(); setFolderPath(String(one)); }
  };
  // a picture pasted into the message box (screenshot, copied image) is saved in My Files, read by the vision model there, and referenced in the message
  const pasteImage = async (e: React.ClipboardEvent) => {
    const item = Array.from(e.clipboardData?.items ?? []).find((i) => i.kind === "file" && i.type.startsWith("image/"));
    if (!item) return;
    const file = item.getAsFile();
    if (!file) return;
    e.preventDefault();
    const b64 = await new Promise<string>((res, rej) => { const r = new FileReader(); r.onload = () => res(String(r.result).split(",")[1] ?? ""); r.onerror = rej; r.readAsDataURL(file); });
    const stamp = new Date().toISOString().slice(0, 16).replace(/[:T]/g, "-");
    try {
      const f = await call<any>("files.upload", { name: `Pasted picture ${stamp}.${file.type.split("/")[1] === "jpeg" ? "jpg" : file.type.split("/")[1] || "png"}`, data_b64: b64, folder: "/Pasted pictures", tags: [] });
      setText((t) => `${t}${t && !t.endsWith("\n") ? "\n" : ""}[picture saved in My Files: "${f.name}", file id ${f.id} - read it with files.read]`);
      toast("Picture added to your message (saved in My Files, read by the vision model)", "ok");
    } catch (er: any) { toast(errText(er), "danger"); }
  };
  const attach = async () => {
    const picked = await pickFile({ multiple: true, filters: [{ name: "Spreadsheets, documents and pictures", extensions: ["xlsx", "xlsm", "csv", "tsv", "txt", "md", "json", "docx", "pdf", "log", "png", "jpg", "jpeg", "gif", "webp"] }] });
    const list = !picked ? [] : Array.isArray(picked) ? picked : [picked];
    if (list.length) await attachPaths(list.map(String));
  };
  useEffect(() => {
    let off = () => undefined as void;
    let alive = true;
    void onFileDrop((paths, phase) => { setDropping(phase === "over"); if (phase === "drop" && paths.length) void attachPaths(paths); }).then((f) => { if (alive) off = f; else f(); });
    return () => { alive = false; off(); };
  }, [attachPaths]);

  const runSlash = async (raw: string): Promise<boolean> => {
    const m = /^\/(\w+)\s*(.*)$/s.exec(raw.trim());
    if (!m) return false;
    const cmd = `/${m[1].toLowerCase()}`, arg = m[2].trim();
    if (!SLASH.some((x) => x.cmd === cmd)) return false;
    setText("");
    switch (cmd) {
      case "/new": await newChat(); break;
      case "/attach": await attach(); break;
      case "/folder": await attachFolder(); break;
      case "/history": go("history"); break;
      case "/steps": setShowSteps((v) => !v); break;
      case "/lock": await call("auth.lock"); break;
      case "/rename": if (!chat) break; if (!arg) { setRenamingHead(true); break; } await renameChat(chat.id, arg); break;
      case "/pin": if (chat) { await call("chat.update", { chat_id: chat.id, pinned: !chat.pinned }); await load(); await loadChats(); } break;
      case "/help": toast(SLASH.map((x) => `${x.cmd} - ${x.help}`).join("\n")); break;
      case "/theme": {
        if (!THEME_NAMES.includes(arg)) { toast(`Themes: ${THEME_NAMES.join(", ")}`); break; }
        await call("settings.apply", { changes: { "ui.theme": arg } }); await refresh(); break;
      }
      case "/remind": if (!arg) { toast("Usage: /remind what and when"); setText("/remind "); break; } await send(`Remind me ${arg}`); break;
      case "/mission": if (!arg) { toast("Usage: /mission every weekday at 7:30 summarise important mail"); setText("/mission "); break; } await send(`Create a routine: ${arg}`); break;
      case "/search": if (!arg) { toast("Usage: /search query"); setText("/search "); break; } await send(`Search the web for ${arg}`); break;
    }
    return true;
  };

  const send = async (msg = text, voice = false) => {
    const t = msg.trim();
    if (!t || sending) return;
    if (t.startsWith("/") && !voice && await runSlash(t)) return;
    let chatId = active;
    if (!chatId) { chatId = (await call<any>("chat.create", {})).id; setActive(chatId); }
    setSending(true);
    setText("");
    clearVoiceDraft();
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
  const visibleChats = useMemo(() => chats.filter((c) => !isHidden(`chat:${c.id}`) && c.title.toLowerCase().includes(filter.toLowerCase())), [chats, filter, isHidden]);
  const partial = partialAnswer(stream);
  const groupedChats = useMemo(() => {
    const m = new Map<string, any[]>();
    const add = (k: string, c: any) => m.set(k, [...(m.get(k) ?? []), c]);
    visibleChats.forEach((c) => add(c.pinned ? "Pinned" : c.folder ? `Folder: ${c.folder}` : dateGroup(c.updated_at), c));
    const order = (k: string) => (k === "Pinned" ? 0 : k.startsWith("Folder: ") ? 1 : ["Today", "Yesterday", "Previous 7 days", "Older"].indexOf(k) + 2);
    return [...m.entries()].sort((a, b) => order(a[0]) - order(b[0]));
  }, [visibleChats]);
  const slashOpen = text.startsWith("/") && !text.includes(" ") && text.length < 14;
  const slashItems = slashOpen ? SLASH.filter((x) => x.cmd.startsWith(text.toLowerCase())) : [];

  return (
    <div className={`chat-layout ${listOpen ? "list-open" : ""} ${showSteps ? "with-steps" : ""}`}>
      {folderPath && active && <FolderDialog path={folderPath} chatId={active} onClose={() => setFolderPath(null)} onDone={() => { setFolderPath(null); void loadAttached(); }} />}
      {listOpen && <aside className="chat-list">
        <Button kind="primary" icon="plus" onClick={() => { newChat(); setListOpen(false); }}>New chat</Button>
        <SearchBox style={{ margin: "8px 0" }} placeholder="Filter chats" value={filter} onChange={setFilter} />
        {groupedChats.map(([title, list]) => (
          <div key={title}>
            <div className="group-title">{title === "Pinned" && <Icon name="pin" size={12} />}{title}</div>
            {list.map((c) => (
              <div key={c.id} className={`list-item clickable ${c.id === active ? "selected" : ""}`} onClick={() => { setActive(c.id); setStepsRun(null); setListOpen(false); }}>
                <Icon name="chat" size={15} />
                <div className="grow">{renaming === c.id ? <InlineRename value={c.title} label="Chat name" onSave={(t) => { setRenaming(null); void renameChat(c.id, t); }} onCancel={() => setRenaming(null)} /> : <div className="ellipsis" style={{ fontWeight: 550 }} onDoubleClick={(e) => { e.stopPropagation(); setRenaming(c.id); }}>{c.title}</div>}<div className="faint small"><Time iso={c.updated_at} smart /></div></div>
                {c.hwm >= 2 && <span className="dot warn" title="contains confidential data" />}
                <span className="row-actions">
                  <button className={`btn ghost icon ${c.pinned ? "pin-on" : ""}`} style={{ width: 26, height: 26, padding: 3 }} title={c.pinned ? "Unpin" : "Pin"}
                    onClick={async (e) => { e.stopPropagation(); await call("chat.update", { chat_id: c.id, pinned: !c.pinned }); void loadChats(); }}><Icon name="pin" size={14} /></button>
                  <button className="btn ghost icon" style={{ width: 26, height: 26, padding: 3 }} title="Rename" aria-label="Rename chat"
                    onClick={(e) => { e.stopPropagation(); setRenaming(c.id); }}><Icon name="edit" size={14} /></button>
                  <button className="btn ghost icon" style={{ width: 26, height: 26, padding: 3 }} title="Move to folder"
                    onClick={async (e) => { e.stopPropagation(); const f = window.prompt("Folder name (blank = none)", c.folder ?? ""); if (f !== null) { await call("chat.update", { chat_id: c.id, folder: f.trim() }); void loadChats(); } }}><Icon name="folder" size={14} /></button>
                </span>
              </div>
            ))}
          </div>
        ))}
        {!chats.length && <div className="faint small" style={{ padding: 10 }}>No chats yet.</div>}
        <div className="spacer" />
        <Button kind="ghost" small icon="archive" onClick={() => setArchived(!archived)}>{archived ? "Show active" : "Show archived"}</Button>
      </aside>}

      <section className="chat-main">
        <div className="chat-head">
          <Button small kind={listOpen ? "primary" : "ghost"} icon="menu" title={listOpen ? "Hide chats" : `Show chats (${chats.length})`} onClick={() => setListOpen(!listOpen)} />
          <Button small kind="ghost" icon="plus" title="New chat" onClick={newChat} />
          {chat && renamingHead
            ? <div className="grow"><InlineRename value={chat.title} label="Chat name" onSave={(t) => { setRenamingHead(false); void renameChat(chat.id, t); }} onCancel={() => setRenamingHead(false)} /></div>
            : <div className="grow ellipsis" style={{ fontWeight: 650, cursor: chat ? "text" : undefined }} title={chat ? "Double-click or press F2 to rename" : undefined} onDoubleClick={() => chat && setRenamingHead(true)}>{chat?.title ?? "New chat"}</div>}
          {chat && !renamingHead && <Button small kind="ghost" icon="edit" title="Rename chat (F2)" onClick={() => setRenamingHead(true)} />}
          {chat && <Button small kind={chat.pinned ? "primary" : "ghost"} icon="pin" title={chat.pinned ? "Unpin chat" : "Pin chat"} onClick={async () => { await call("chat.update", { chat_id: chat.id, pinned: !chat.pinned }); await load(); void loadChats(); }} />}
          {chat && <Sensitivity level={chat.sensitivity} />}
          {chat?.sources?.filter((s: string) => sourceLabel(s)).map((s: string) => <Badge key={s} >{sourceLabel(s)}</Badge>)}
          <UsageMeter refreshKey={usageKey} />
          {chat && (
            <span className="row small muted" title="Allow the agent to use tools in this chat">
              Tools <Toggle on={!!chat.allow_tools} label="Allow tools in this chat" onChange={async (v) => { await call("chat.update", { chat_id: chat.id, allow_tools: v }); void load(); }} />
            </span>
          )}
          {chat && <Button small kind="ghost" icon="archive" title="Archive chat" onClick={async () => { await call("chat.update", { chat_id: chat.id, archived: !chat.archived }); setActive(null); void loadChats(); }} />}
          {chat && <Button small kind="ghost" icon="trash" title="Delete chat" onClick={() => { const id = chat.id; deferDelete({ key: `chat:${id}`, label: `Deleted "${chat.title}"`, commit: () => call("chat.delete", { chat_id: id }), after: () => void loadChats() }); setActive(null); setData(null); }} />}
          <Button small kind={showSteps ? "primary" : undefined} icon="steps" onClick={() => setShowSteps(!showSteps)}>Steps</Button>
        </div>
        {sensitive && <div className="banner warn" style={{ margin: "10px 18px 0" }}><Icon name="alert" />This chat contains {chat.sensitivity} data. Web access and sending need your approval (or are blocked).</div>}
        <div className="scroll-wrap">
        <div className="messages" ref={msgsRef} onScroll={onScroll}>
          {!data?.messages?.length && !liveRun && (
            <Empty icon="sparkle" title="How can I help?">
              <div className="row wrap" style={{ justifyContent: "center", marginTop: 8 }}>
                {["Summarise my unread important emails", "Remind me to call the dentist on Friday at 9", "Search the web for local LLM news",
                  "Every weekday at 7:30 summarise important mail"].map((s) => <Button key={s} small onClick={() => send(s)}>{s}</Button>)}
              </div>
            </Empty>
          )}
          {data?.messages?.map((m: any) => (
            <div key={m.id} className={`msg ${m.role}`} data-turn={m.role === "user" ? m.id : undefined}>
              {m.role === "assistant" ? <Markdown text={m.content} /> : m.content}
              {m.role === "assistant" && (
                <div className="msg-meta">
                  {(m.sources?.filter((s: string) => sourceLabel(s)).length ? m.sources.filter((s: string) => sourceLabel(s)) : ["model only"]).map((s: string) => <Badge key={s}>{sourceLabel(s) ?? s}</Badge>)}
                  {m.sensitivity > 0 && <Sensitivity level={m.sensitivity} />}
                  {m.run_id && <Button small kind="ghost" icon="steps" onClick={() => { setStepsRun(m.run_id); setShowSteps(true); }}>Steps</Button>}
                  <span className="faint small"><Time iso={m.created_at} smart /></span>
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
        <ChatRail container={msgsRef} turns={(data?.messages ?? []).filter((m: any) => m.role === "user").map((m: any) => ({ id: m.id, label: turnLabel(m.content) }))} />
        {scrollable && (
          <div className="float-btns">
            {!atTop && <button className="float-btn" title="Scroll to top" aria-label="Scroll to top" onClick={toTop}><Icon name="up" /></button>}
            {!atBottom && <button className="float-btn" title="Jump to latest" aria-label="Jump to latest" onClick={toBottom}><Icon name="down" />{unseen > 0 && <span className="count">{unseen}</span>}</button>}
          </div>
        )}
        </div>
        {active && <SubfolderCards chatId={active} reload={loadAttached} />}
        <div className={`composer ${dropping ? "dropping" : ""}`}>
          {dropping && <div className="drop-hint" role="status">Drop to attach - the file stays where it is</div>}
          {active && <ShareChips items={attached} reload={loadAttached} chatId={active} />}
          <div className="composer-box">
            <Button kind="ghost" icon="attach" title="Attach a file from this PC (spreadsheet, CSV, document, picture or scan). It is read where it is - not uploaded or copied." onClick={() => void attach()} />
            <Button kind="ghost" icon="folder" title="Share a folder from this PC with this chat (read-only; subfolders only if you tick it; ends with the session)" onClick={() => void attachFolder()} />
            {slashItems.length > 0 && (
              <SlashMenu items={slashItems} sel={slashSel} onHover={setSlashSel} onPick={(x) => { setText(x.arg ? `${x.cmd} ` : x.cmd); taRef.current?.focus(); }} />
            )}
            <textarea ref={taRef} rows={1} value={text} onPaste={(e) => void pasteImage(e)} placeholder={`Message ${status?.assistant_name ?? "ChiRAG Agent"}…`} title="Enter to send · Shift+Enter for a new line"
              onChange={(e) => { setText(e.target.value); e.target.style.height = "auto"; e.target.style.height = `${Math.min(220, e.target.scrollHeight)}px`; }}
              onKeyDown={(e) => {
                if (slashItems.length) {
                  if (e.key === "ArrowDown") { e.preventDefault(); setSlashSel((i) => (i + 1) % slashItems.length); return; }
                  if (e.key === "ArrowUp") { e.preventDefault(); setSlashSel((i) => (i - 1 + slashItems.length) % slashItems.length); return; }
                  if (e.key === "Tab" || (e.key === "Enter" && !SLASH.some((x) => x.cmd === text.trim().toLowerCase() && !x.arg))) {
                    e.preventDefault(); const x = slashItems[Math.min(slashSel, slashItems.length - 1)]; setText(x.arg ? `${x.cmd} ` : x.cmd); return;
                  }
                }
                if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); void send(); }
              }} />
            {status?.ui?.["voice.enabled"] !== false && <VoiceButton onStart={() => { voiceBase.current = text.trim(); }} onPartial={(t) => setText(`${voiceBase.current ? voiceBase.current + " " : ""}${t}`)} onFinal={(t) => { void send(`${voiceBase.current ? voiceBase.current + " " : ""}${t}`, true); voiceBase.current = ""; }} />}
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

function SlashMenu({ items, sel, onHover, onPick }: { items: typeof SLASH; sel: number; onHover: (i: number) => void; onPick: (x: (typeof SLASH)[number]) => void }) {
  const ref = useExitGhost<HTMLDivElement>("rect");
  return (
    <div className="slash-menu" role="listbox" aria-label="Commands" ref={ref}>
      {items.map((x, i) => (
        <div key={x.cmd} role="option" aria-selected={i === sel} className={`slash-item ${i === sel ? "active" : ""}`} onMouseEnter={() => onHover(i)} onClick={() => onPick(x)}>
          <b>{x.cmd}</b><span className="small muted">{x.help}{x.arg ? ` - ${x.arg}` : ""}</span>
        </div>
      ))}
    </div>
  );
}
