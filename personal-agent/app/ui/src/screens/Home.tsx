// Home: a board of small widgets. Every widget can be switched on or off (and ordered) from the Widgets panel on the right; only the
// enabled ones are shown and computed. Mail widgets read Outlook in the background (cached 5 minutes, "Refresh" forces a new reading).
import { ReactNode, useCallback, useEffect, useState } from "react";
import { onEvent } from "../api/gateway";
import { errText, useApp } from "../app";
import { Icon } from "../components/Icon";
import { Badge, Button, bytes, Time, Toggle } from "../components/ui";

type Item = any;

function Shell({ title, icon, tone, onClick, children, wide }: { title: string; icon: string; tone?: string; onClick?: () => void; children: ReactNode; wide?: boolean }) {
  return (
    <div className={`widget ${tone === "warn" ? "warn" : ""} ${wide ? "wide" : ""} ${onClick ? "clickable" : ""}`} onClick={onClick} role={onClick ? "button" : undefined} tabIndex={onClick ? 0 : undefined}
      onKeyDown={(e) => { if (onClick && (e.key === "Enter" || e.key === " ")) { e.preventDefault(); onClick(); } }}>
      <div className="widget-title"><Icon name={icon as any} size={15} /><span className="ellipsis">{title}</span></div>
      {children}
    </div>
  );
}

const STAT = (value: ReactNode, sub?: ReactNode) => (<><div className="stat">{value}</div>{sub && <div className="small muted">{sub}</div>}</>);

function Widget({ id, title, d, reload }: { id: string; title: string; d: Item; reload: (refresh?: boolean) => void }) {
  const { go, call, toast } = useApp();
  const open = d?.go ? () => go(d.go.screen, d.go.params) : undefined;
  const icon: Record<string, string> = { mail_unread: "mail", mail_to_me: "mail", mail_approvals: "approvals", mail_deadlines: "clock", mail_awaiting_reply: "mail", reminders: "reminders", routines_next: "missions",
    approvals: "approvals", attention: "alert", recent_files: "files", storage: "files", memory_recent: "memory", models_health: "model", activity_today: "activity", updates: "info" };
  const ic = icon[id] ?? "info";
  if (!d) return <Shell title={title} icon={ic}><div className="skeleton" style={{ height: 50 }} /></Shell>;
  if (d.state === "off") return <Shell title={title} icon={ic}><div className="small muted">{d.note}</div><Button small onClick={() => go("settings", { section: "connectors" })}>Open Connectors</Button></Shell>;
  if (d.state === "loading") return <Shell title={title} icon={ic}><div className="skeleton" style={{ height: 42 }} /><div className="small faint">{d.note || "Loading..."}</div></Shell>;
  if (d.state === "error") return <Shell title={title} icon={ic}><div className="small" style={{ color: "var(--warn)" }}>Could not read this now.</div><div className="small faint">{d.note}</div><Button small onClick={() => reload(true)}>Try again</Button></Shell>;
  const mailAge = d.age !== undefined ? <div className="small faint">{d.refreshing ? "updating…" : d.age < 60 ? "just now" : `${Math.round(d.age / 60)} min ago`}</div> : null;
  if (id.startsWith("mail_")) return <Shell title={title} icon={ic} tone={d.tone} onClick={open}>{STAT(d.value, d.sub)}{mailAge}</Shell>;
  if (id === "approvals") return <Shell title={title} icon={ic} tone={d.tone} onClick={open}>{STAT(d.value, d.sub)}</Shell>;
  if (id === "reminders") return (
    <Shell title={title} icon={ic} onClick={open}>
      {!d.items.length ? <div className="small muted">No upcoming reminders.</div> : d.items.map((r: Item) => <div key={r.id} className="wrow"><span className="ellipsis grow">{r.text}</span><span className="small faint"><Time iso={r.due_at} smart /></span></div>)}
    </Shell>);
  if (id === "routines_next") return (
    <Shell title={title} icon={ic} onClick={open}>
      {!d.items.length ? <div className="small muted">No active routines. Create one in Missions &amp; Routines or Settings &gt; Email monitoring.</div>
        : d.items.map((r: Item) => <div key={r.id} className="wrow"><span className="ellipsis grow">{String(r.name).replace(/^Email: /, "")}</span><span className="small faint"><Time iso={r.next_run_at} smart /></span></div>)}
    </Shell>);
  if (id === "attention") return (
    <Shell title={title} icon={ic} tone={d.tone}>
      {!d.items.length ? <div className="small" style={{ color: "var(--ok, green)" }}>✓ Nothing needs you right now.</div>
        : d.items.map((a: Item, i: number) => <div key={i} className="wrow clickable" onClick={() => go(a.go.screen, a.go.params)}><span className="grow">{a.text}</span><Icon name="up" size={12} /></div>)}
    </Shell>);
  if (id === "recent_files") return (
    <Shell title={title} icon={ic} onClick={open} wide>
      {!d.items.length ? <div className="small muted">No files yet. Upload some in My Files - they are summarised automatically.</div>
        : d.items.map((f: Item) => <div key={f.id} className="wrow"><span className="ellipsis grow"><b>{f.name}</b> {f.doc_type && <Badge>{f.doc_type}</Badge>} <span className="small faint">{(f.summary || "").slice(0, 80)}</span></span></div>)}
    </Shell>);
  if (id === "storage") { const pct = Math.min(100, Math.round((d.used / (d.quota || 1)) * 100)); return (
    <Shell title={title} icon={ic} onClick={open}>{STAT(`${pct} %`, `${bytes(d.used)} of ${bytes(d.quota)} · ${d.value} files`)}<div className="meter"><div style={{ width: `${pct}%`, background: pct > 80 ? "var(--warn)" : "var(--accent)" }} /></div></Shell>); }
  if (id === "memory_recent") return (
    <Shell title={title} icon={ic} wide>
      {!d.items.length ? <div className="small muted">Nothing learned yet - it grows as you chat, add files and use routines.</div>
        : d.items.map((m: Item) => <div key={m.id} className="wrow"><span className="ellipsis grow" title={m.content}>{m.content}</span>
          <Button small kind="ghost" icon="trash" title="Forget this" onClick={async () => { try { await call("memory.action", { id: m.id, action: "delete" }); toast("Forgotten - it will not be learned again", "ok"); reload(); } catch (e: any) { toast(errText(e), "danger"); } }} /></div>)}
      <Button small kind="ghost" onClick={open}>Open Memory</Button>
    </Shell>);
  if (id === "models_health") return (
    <Shell title={title} icon={ic} onClick={open}>
      <div className="row wrap" style={{ gap: 4 }}>{Object.entries(d.kinds as Record<string, Item>).map(([k, v]) => <Badge key={k} tone={v.ready ? "ok" : "warn"} >{k}{v.ready ? "" : " - none"}</Badge>)}</div>
      <div className="small muted">{d.reachable === null ? "" : d.reachable ? "Model server answers." : "Model server does not answer (is Ollama running?)."}{d.gpu !== null && d.gpu !== undefined ? ` GPU ${Math.round(d.gpu)} %.` : ""}</div>
    </Shell>);
  if (id === "activity_today") return <Shell title={title} icon={ic} onClick={open}>{STAT(`${d.done} done`, `${d.failed} failed · ${Number(d.tokens).toLocaleString()} model tokens today`)}</Shell>;
  if (id === "updates") return (
    <Shell title={title} icon={ic} wide>
      {!d.items.length ? <div className="small muted">Nothing new.</div> : d.items.map((e: Item) => (
        <div key={e.id} className="wrow"><Badge tone={e.severity === "high" ? "danger" : e.severity === "medium" ? "warn" : "info"}>{String(e.kind).replace(/_/g, " ")}</Badge>
          <span className="grow"><b>{e.title}</b>{e.detail && <span className="small muted"> {e.detail}</span>}</span><span className="small faint"><Time iso={e.created_at} /></span>
          <Button small kind="ghost" icon="x" title="Dismiss" onClick={async () => { await call("home.dismiss", { id: e.id }); reload(); }} /></div>))}
    </Shell>);
  return <Shell title={title} icon={ic}><div className="small muted">{JSON.stringify(d).slice(0, 80)}</div></Shell>;
}

export function Home() {
  const { call, go, status, toast } = useApp();
  const [w, setW] = useState<any>(null);
  const [recovery, setRecovery] = useState(true);
  const [panel, setPanel] = useState(false);
  const [busy, setBusy] = useState(false);
  const load = useCallback(async (refresh = false) => {
    try { setW(await call("home.widgets", { refresh })); } catch (e: any) { toast(errText(e), "danger"); }
    call<any>("home.summary").then((d) => setRecovery(d.recovery_key_confirmed !== false)).catch(() => undefined);
  }, [call, toast]);
  useEffect(() => {
    void load();
    const off = onEvent((t) => { if (["home.changed", "tasks.changed", "approvals.changed", "reminders.changed", "home.widgets_changed", "files.changed", "memory.changed", "missions.changed"].includes(t)) void load(); });
    return off; /* eslint-disable-next-line */
  }, []);
  const set = async (enabled: string[]) => {
    setBusy(true);
    try { const r = await call<any>("home.widgets_set", { enabled }); setW((x: any) => ({ ...x, enabled: r.enabled, data: r.data })); } catch (e: any) { toast(errText(e), "danger"); } finally { setBusy(false); }
  };
  if (!w) return <div className="page"><div className="skeleton" style={{ height: 240 }} /></div>;
  const hour = new Date().getHours();
  const greet = hour < 12 ? "Good morning" : hour < 18 ? "Good afternoon" : "Good evening";
  const titleOf = (id: string) => w.catalog.find((c: any) => c.id === id)?.title ?? id;
  const enabled: string[] = w.enabled;
  const groups = Array.from(new Set(w.catalog.map((c: any) => c.group))) as string[];
  const move = (id: string, dir: number) => { const i = enabled.indexOf(id), j = i + dir; if (i < 0 || j < 0 || j >= enabled.length) return; const n = [...enabled]; [n[i], n[j]] = [n[j], n[i]]; void set(n); };
  return (
    <div className={`page home-page ${panel ? "with-panel" : ""}`}>
      <div className="page-header"><div className="grow"><h1>{greet}, {status?.display_name ?? status?.username}</h1><div className="muted">{status?.assistant_name ?? "ChiRAG Agent"} · your day at a glance</div></div>
        <Button small kind="ghost" icon="refresh" onClick={() => void load(true)} title="Read Outlook and everything again">Refresh</Button>
        <Button small kind={panel ? "primary" : undefined} icon="menu" onClick={() => setPanel(!panel)} aria-pressed={panel}>Widgets</Button></div>
      {!recovery && <div className="banner warn"><Icon name="alert" />Your recovery key was not confirmed. Generate a new one in Settings &gt; Account &amp; Security.</div>}
      {status?.needs_pin_setup && <div className="banner warn"><Icon name="alert" />This vault was restored on a new PC. Create a new PIN and recovery key in Settings &gt; Account &amp; Security.
        <Button small onClick={() => go("settings", { section: "account" })}>Open</Button></div>}
      <div className="home-body">
        <div className="widget-grid">
          {!enabled.length && <div className="muted" style={{ padding: 20 }}>No widgets are switched on. Press <b>Widgets</b> to choose what Home shows.</div>}
          {enabled.map((id) => <Widget key={id} id={id} title={titleOf(id)} d={w.data[id]} reload={(r) => void load(r)} />)}
        </div>
        {panel && (
          <aside className="widget-panel" aria-label="Choose widgets">
            <div className="row"><b className="grow">Widgets</b><Button small kind="ghost" icon="x" onClick={() => setPanel(false)} aria-label="Close" /></div>
            <div className="small muted">Switch widgets on or off. Only switched-on widgets are shown. Use the arrows to change their order.</div>
            {groups.map((g) => (
              <div key={g}>
                <div className="group-title">{g}</div>
                {w.catalog.filter((c: any) => c.group === g).map((c: any) => (
                  <div key={c.id} className="panel-row" title={c.about}>
                    <div className="grow"><div style={{ fontWeight: 550 }}>{c.title}</div><div className="small faint">{c.about}</div></div>
                    {enabled.includes(c.id) && <span className="row" style={{ gap: 0 }}><button className="btn ghost icon" aria-label={`Move ${c.title} up`} onClick={() => move(c.id, -1)} disabled={enabled.indexOf(c.id) === 0}><Icon name="up" size={13} /></button>
                      <button className="btn ghost icon" aria-label={`Move ${c.title} down`} onClick={() => move(c.id, 1)} disabled={enabled.indexOf(c.id) === enabled.length - 1}><Icon name="down" size={13} /></button></span>}
                    <Toggle on={enabled.includes(c.id)} label={`Show ${c.title}`} onChange={(v) => void set(v ? [...enabled, c.id] : enabled.filter((x) => x !== c.id))} />
                  </div>))}
              </div>))}
            <div className="row"><Button small disabled={busy} onClick={() => void set(w.catalog.map((c: any) => c.id))}>Show all</Button><Button small disabled={busy} onClick={() => void set([])}>Hide all</Button></div>
          </aside>
        )}
      </div>
    </div>
  );
}
