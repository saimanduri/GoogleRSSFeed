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
      {!d.items.length ? <div className="small muted">No active routines. Create one in Routines or Settings &gt; Email monitoring.</div>
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

// ---------------------------------------------------------------- calm layout: which widget goes where
const KPI = ["mail_unread", "mail_to_me", "mail_awaiting_reply", "mail_approvals", "mail_deadlines", "activity_today", "storage", "models_health"];
const UPCOMING = ["reminders", "routines_next"];
const RECENT = ["updates", "recent_files", "memory_recent"];
const ATTENTION = ["attention", "approvals"];

/** "Needs you now": one row of chips (approvals, attention items, overdue mail deadlines). Hidden behind "All clear" when empty. */
function NeedsYou({ ids, data, mail }: { ids: string[]; data: Record<string, Item>; mail: Record<string, Item> }) {
  const { go } = useApp();
  const chips: { text: string; go?: any; tone: string }[] = [];
  if (ids.includes("approvals") && data.approvals?.value > 0) chips.push({ text: `${data.approvals.value} approval${data.approvals.value === 1 ? "" : "s"} waiting`, go: data.approvals.go, tone: "warn" });
  if (ids.includes("attention")) for (const a of data.attention?.items ?? []) chips.push({ text: a.text, go: a.go, tone: "warn" });
  for (const [k, label] of [["mail_approvals", "mails wait for your approval"], ["mail_deadlines", "mail deadlines within 7 days"]] as const) {
    const m = mail[k];
    if (m && Number(m.value) > 0 && m.tone === "warn") chips.push({ text: `${m.value} ${label}`, go: m.go, tone: "warn" });
  }
  return (
    <div className={`needs-you ${chips.length ? "" : "clear"}`} role="region" aria-label="Needs you now">
      <span className="needs-title"><Icon name={chips.length ? "alert" : "check"} size={15} />{chips.length ? "Needs you now" : "All clear - nothing needs you right now"}</span>
      {chips.map((c, i) => <button key={i} className={`chip ${c.tone}`} onClick={() => c.go && go(c.go.screen, c.go.params)}>{c.text}<Icon name="up" size={11} /></button>)}
    </div>
  );
}

function Tile({ id, title, d }: { id: string; title: string; d: Item }) {
  const { go } = useApp();
  const open = d?.go ? () => go(d.go.screen, d.go.params) : undefined;
  let value: ReactNode = "-", sub: ReactNode = "";
  if (!d || d.state === "loading") { value = <span className="skeleton" style={{ display: "inline-block", width: 40, height: 22 }} />; sub = d?.note ?? ""; }
  else if (d.state === "off") { value = "off"; sub = "Outlook not connected"; }
  else if (d.state === "error") { value = "?"; sub = "could not read now"; }
  else if (id === "activity_today") { value = d.done; sub = `done today${d.failed ? ` · ${d.failed} failed` : ""}`; }
  else if (id === "storage") { const pct = Math.min(100, Math.round((d.used / (d.quota || 1)) * 100)); value = `${pct}%`; sub = `storage · ${bytes(d.used)}`; }
  else if (id === "models_health") { const kinds = Object.values(d.kinds ?? {}) as Item[]; const ready = kinds.filter((k) => k.ready).length; value = `${ready}/${kinds.length}`; sub = d.reachable === false ? "model server not answering" : "model kinds ready"; }
  else { value = d.value; sub = d.sub; }
  const warn = d?.tone === "warn" || (id === "activity_today" && d?.failed > 0) || (id === "models_health" && d?.reachable === false);
  return (
    <button className={`kpi ${warn ? "warn" : ""}`} onClick={open} disabled={!open} title={title}>
      <span className="kpi-label">{title}</span><span className="kpi-value">{value}</span><span className="kpi-sub">{sub}</span>
    </button>
  );
}

/** Reminders and routine runs in ONE time-ordered list. */
function ComingUp({ ids, data }: { ids: string[]; data: Record<string, Item> }) {
  const { go } = useApp();
  const items: { key: string; when: string; text: string; kind: string; go?: any }[] = [];
  if (ids.includes("reminders")) for (const r of data.reminders?.items ?? []) items.push({ key: `r${r.id}`, when: r.due_at, text: r.text, kind: "reminder", go: data.reminders.go });
  if (ids.includes("routines_next")) for (const r of data.routines_next?.items ?? []) items.push({ key: `m${r.id}`, when: r.next_run_at, text: String(r.name).replace(/^Email: /, ""), kind: "routine", go: data.routines_next.go });
  items.sort((a, b) => String(a.when).localeCompare(String(b.when)));
  return (
    <section className="home-col">
      <h2 className="home-h"><Icon name="clock" size={15} />Coming up</h2>
      {!items.length ? <div className="small muted">No reminders or routine runs coming up.</div> : items.slice(0, 8).map((i) => (
        <button key={i.key} className="home-row" onClick={() => i.go && go(i.go.screen, i.go.params)}>
          <span className="home-when"><Time iso={i.when} smart /></span><span className="grow ellipsis">{i.text}</span><Badge>{i.kind}</Badge>
        </button>))}
    </section>
  );
}

function Recently({ ids, data, reload }: { ids: string[]; data: Record<string, Item>; reload: () => void }) {
  const { go, call, toast } = useApp();
  return (
    <section className="home-col">
      <h2 className="home-h"><Icon name="activity" size={15} />Recently</h2>
      {ids.includes("updates") && (data.updates?.items ?? []).slice(0, 5).map((e: Item) => (
        <div key={e.id} className="home-row static"><Badge tone={e.severity === "high" ? "danger" : e.severity === "medium" ? "warn" : "info"}>{String(e.kind).replace(/mission/g, "routine").replace(/_/g, " ")}</Badge>
          <span className="grow ellipsis" title={e.detail}><b>{e.title}</b></span><span className="small faint"><Time iso={e.created_at} /></span>
          <Button small kind="ghost" icon="x" title="Dismiss" onClick={async () => { await call("home.dismiss", { id: e.id }); reload(); }} /></div>))}
      {ids.includes("recent_files") && (data.recent_files?.items ?? []).slice(0, 4).map((f: Item) => (
        <button key={f.id} className="home-row" onClick={() => go("files")}><Icon name="files" size={14} /><span className="grow ellipsis"><b>{f.name}</b> <span className="small faint">{(f.summary || "").slice(0, 70)}</span></span>{f.doc_type && <Badge>{f.doc_type}</Badge>}</button>))}
      {ids.includes("memory_recent") && (data.memory_recent?.items ?? []).slice(0, 3).map((m: Item) => (
        <div key={m.id} className="home-row static"><Icon name="memory" size={14} /><span className="grow ellipsis" title={m.content}>Learned: {m.content}</span>
          <Button small kind="ghost" icon="trash" title="Forget this" onClick={async () => { try { await call("memory.action", { id: m.id, action: "delete" }); toast("Forgotten - it will not be learned again", "ok"); reload(); } catch (er: any) { toast(errText(er), "danger"); } }} /></div>))}
      {!RECENT.some((r) => ids.includes(r) && (data[r]?.items ?? []).length) && <div className="small muted">Nothing new.</div>}
    </section>
  );
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
        <div className="home-main">
          {!enabled.length && <div className="muted" style={{ padding: 20 }}>No widgets are switched on. Press <b>Widgets</b> to choose what Home shows.</div>}
          {enabled.some((id) => ATTENTION.includes(id) || id === "mail_approvals" || id === "mail_deadlines") && <NeedsYou ids={enabled} data={w.data} mail={w.data} />}
          {enabled.some((id) => KPI.includes(id)) && <div className="kpi-row">{enabled.filter((id) => KPI.includes(id)).map((id) => <Tile key={id} id={id} title={titleOf(id)} d={w.data[id]} />)}</div>}
          {(enabled.some((id) => UPCOMING.includes(id)) || enabled.some((id) => RECENT.includes(id))) && (
            <div className="home-cols">
              {enabled.some((id) => UPCOMING.includes(id)) && <ComingUp ids={enabled} data={w.data} />}
              {enabled.some((id) => RECENT.includes(id)) && <Recently ids={enabled} data={w.data} reload={() => void load()} />}
            </div>)}
          {/* any widget without a place in the zones keeps its own card */}
          {enabled.filter((id) => ![...KPI, ...UPCOMING, ...RECENT, ...ATTENTION].includes(id)).length > 0 && <div className="widget-grid">
            {enabled.filter((id) => ![...KPI, ...UPCOMING, ...RECENT, ...ATTENTION].includes(id)).map((id) => <Widget key={id} id={id} title={titleOf(id)} d={w.data[id]} reload={(r) => void load(r)} />)}
          </div>}
        </div>
        {panel && (
          <aside className="widget-panel" aria-label="Choose widgets">
            <div className="row"><b className="grow">Widgets</b><Button small kind="ghost" icon="x" onClick={() => setPanel(false)} aria-label="Close" /></div>
            <div className="small muted">Switch widgets on or off. Home groups them by kind: what needs you, numbers at a glance, what is coming up, and what happened recently. The arrows change the order inside a group.</div>
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
