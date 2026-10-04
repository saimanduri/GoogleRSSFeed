import { useCallback, useEffect, useMemo, useState } from "react";
import { SearchBox } from "../components/SearchBox";
import { errText, useApp } from "../app";
import { InlineRename } from "../components/InlineRename";
import { Icon } from "../components/Icon";
import { Badge, Button, Card, Empty, Tabs, Time } from "../components/ui";

type Item = { kind: "chat" | "work"; id: string; title: string; when: string; badge?: string; tone?: string; archived?: boolean; pinned?: boolean };

function bucket(iso: string): string {
  const d = new Date(iso), now = new Date();
  const day = (x: Date) => new Date(x.getFullYear(), x.getMonth(), x.getDate()).getTime();
  const n = Math.round((day(now) - day(d)) / 86400000);
  return n <= 0 ? "Today" : n === 1 ? "Yesterday" : n < 7 ? "Previous 7 days" : n < 31 ? "This month" : d.getFullYear() === now.getFullYear() ? d.toLocaleDateString([], { month: "long" }) : String(d.getFullYear());
}

// Past chats and past work (agent runs) in one timeline, grouped like Claude Code's session history.
export function History() {
  const { call, go, toast } = useApp();
  const [renaming, setRenaming] = useState<string | null>(null);
  const [tab, setTab] = useState<"all" | "chats" | "work">("all");
  const [items, setItems] = useState<Item[] | null>(null);
  const [q, setQ] = useState("");
  const [hits, setHits] = useState<any[] | null>(null);
  const [archived, setArchived] = useState(false);

  const load = useCallback(async () => {
    const [chats, runs] = await Promise.all([
      call<any[]>("chat.list", { archived }).catch(() => []),
      call<any[]>("runs.list", { limit: archived ? 2000 : 200, archived }).catch(() => []),
    ]);
    setItems([
      ...chats.map((c): Item => ({ kind: "chat", id: c.id, title: c.title, when: c.updated_at, archived: !!c.archived, pinned: !!c.pinned })),
      ...runs.map((r): Item => ({ kind: "work", id: r.id, title: r.title || r.summary || r.kind, when: r.started_at || r.created_at, badge: r.status,
        tone: r.status === "COMPLETED" ? "ok" : r.status === "FAILED" ? "danger" : "warn" })),
    ].filter((i) => i.when).sort((a, b) => b.when.localeCompare(a.when)));
  }, [call, archived]);
  useEffect(() => { void load(); }, [load]);
  useEffect(() => {
    if (q.trim().length < 2) { setHits(null); return; }
    const t = setTimeout(() => call<any[]>("history.search", { query: q.trim(), limit: 30 }).then(setHits).catch(() => setHits([])), 200);
    return () => clearTimeout(t);
  }, [q, call]);

  const open = (i: Item) => (i.kind === "chat" ? go("chat", { open: i.id }) : go("activity", { ref: i.id }));
  const groups = useMemo(() => {
    const list = (items ?? []).filter((i) => tab === "all" || (tab === "chats") === (i.kind === "chat"));
    const m = new Map<string, Item[]>();
    list.forEach((i) => { const b = bucket(i.when); m.set(b, [...(m.get(b) ?? []), i]); });
    return [...m.entries()];
  }, [items, tab]);

  return (
    <div className="page history-layout">
      <div className="page-header">
        <div className="grow"><h1>History</h1><div className="muted">Everything you asked and everything the agent did, newest first.</div></div>
        <Button small kind="ghost" icon="archive" onClick={() => setArchived(!archived)}>{archived ? "Hide archived" : "Include archived"}</Button>
      </div>
      <div className="row" style={{ marginBottom: 14 }}>
        <SearchBox className="grow" placeholder="Search all history (type 2+ letters; word beginnings work)…" value={q} onChange={setQ} />
        <Tabs tabs={[["all", "All"], ["chats", "Chats"], ["work", "Work"]]} value={tab} onChange={setTab} />
      </div>
      {hits ? (
        <Card title={`${hits.length} result${hits.length === 1 ? "" : "s"} for “${q}”`}>
          {!hits.length && <Empty icon="search" title="Nothing found" />}
          {hits.map((h, i) => (
            <div key={i} className="history-item" onClick={() => go(h.kind === "file" ? "files" : h.kind === "chat" ? "chat" : "activity", h.kind === "chat" ? { open: h.ref_id } : { ref: h.ref_id })}>
              <Badge>{h.kind}</Badge>
              <div className="grow"><div style={{ fontWeight: 600 }} className="ellipsis">{h.title}</div>
                <div className="small muted ellipsis">{String(h.snippet).replace(/\s+/g, " ")}</div></div>
              <Time iso={h.created_at} smart />
            </div>
          ))}
        </Card>
      ) : items === null ? <div className="skeleton" style={{ height: 240 }} /> : !groups.length ? (
        <Empty icon="history" title="No history yet">Your chats and the work the agent does will show up here.</Empty>
      ) : groups.map(([name, list]) => (
        <div key={name} className="history-group">
          <div className="group-title">{name}</div>
          {list.map((i) => (
            <div key={`${i.kind}${i.id}`} className="history-item" onClick={() => open(i)}>
              <span className="when"><Time iso={i.when} smart /></span>
              <Icon name={i.kind === "chat" ? "chat" : "steps"} size={16} />
              {renaming === i.id && i.kind === "chat"
                ? <div className="grow"><InlineRename value={i.title} label="Chat name" onCancel={() => setRenaming(null)}
                    onSave={async (t) => { setRenaming(null); try { await call("chat.update", { chat_id: i.id, title: t }); toast("Chat renamed", "ok"); void load(); } catch (e: any) { toast(errText(e), "danger"); } }} /></div>
                : <div className="grow ellipsis" style={{ fontWeight: 550 }}>{i.title}</div>}
              {i.kind === "chat" && <span className="row-actions">
                <button className={`btn ghost icon ${i.pinned ? "pin-on" : ""}`} style={{ width: 26, height: 26, padding: 3 }} title={i.pinned ? "Unpin" : "Pin"} aria-label={i.pinned ? "Unpin chat" : "Pin chat"}
                  onClick={async (e) => { e.stopPropagation(); await call("chat.update", { chat_id: i.id, pinned: !i.pinned }); void load(); }}><Icon name="pin" size={14} /></button>
                <button className="btn ghost icon" style={{ width: 26, height: 26, padding: 3 }} title="Rename" aria-label="Rename chat"
                  onClick={(e) => { e.stopPropagation(); setRenaming(i.id); }}><Icon name="edit" size={14} /></button>
              </span>}
              {i.pinned && <Icon name="pin" size={12} />}
              {i.archived && <Badge>archived</Badge>}
              {i.badge && <Badge tone={i.tone}>{i.badge.toLowerCase()}</Badge>}
            </div>
          ))}
        </div>
      ))}
    </div>
  );
}
