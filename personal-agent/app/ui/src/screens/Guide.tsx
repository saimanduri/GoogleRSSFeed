import { useMemo, useState } from "react";
import { useApp } from "../app";
import { Icon } from "../components/Icon";
import { Button, Card, Empty } from "../components/ui";
import { GUIDE, GUIDE_AREAS, GuideEntry } from "./guideData";

// Searchable help. Every term typed must start a word somewhere in the entry (so "lock" finds Lock, not "block").
const esc = (t: string) => t.replace(/[^a-z0-9 ]/gi, (c) => `\\${c}`);
const startsWord = (hay: string, t: string) => new RegExp(`(^|[^a-z0-9])${esc(t)}`, "i").test(hay);
export function score(e: GuideEntry, q: string): number {
  const terms = q.toLowerCase().split(/\s+/).filter(Boolean);
  const hay = `${e.title} ${e.what} ${e.how ?? ""} ${e.kw ?? ""} ${e.area}`;
  if (!terms.every((t) => startsWord(hay, t))) return 0;
  return 1 + terms.filter((t) => startsWord(e.title, t)).length * 2 + terms.filter((t) => startsWord(e.kw ?? "", t)).length;
}
export const matches = (e: GuideEntry, q: string) => score(e, q) > 0;

export function Guide() {
  const { go } = useApp();
  const [q, setQ] = useState("");
  const [open, setOpen] = useState<string | null>(null);
  const found = useMemo(() => GUIDE.map((e) => [e, score(e, q)] as const).filter(([, n]) => n > 0).sort((x, y) => y[1] - x[1]).map(([e]) => e), [q]);
  const searching = q.trim().length > 0;
  const open1 = (e: GuideEntry) => go(e.screen!, e.section ? { section: e.section } : undefined);
  const Entry = ({ e }: { e: GuideEntry }) => (
    <div className="guide-entry">
      <button className="guide-head" aria-expanded={searching || open === e.id} onClick={() => setOpen(open === e.id ? null : e.id)}>
        <Icon name={searching || open === e.id ? "down" : "info"} size={15} /><span className="grow">{e.title}</span>
        {searching && <span className="badge">{e.area}</span>}
      </button>
      {(searching || open === e.id) && (
        <div className="guide-body">
          <p>{e.what}</p>
          {e.how && <p className="muted"><b>How:</b> {e.how}</p>}
          {e.screen && <Button small onClick={() => open1(e)}>Open {e.screen === "guide" ? "this screen" : "it"}</Button>}
        </div>
      )}
    </div>
  );
  return (
    <div className="page guide">
      <div className="page-header">
        <div className="grow"><h1>Guide</h1><div className="muted">What each button and screen does. Search for just the one you need.</div></div>
      </div>
      <div className="guide-search">
        <Icon name="search" />
        <input className="input" autoFocus type="search" value={q} onChange={(e) => setQ(e.target.value)} aria-label="Search the guide"
          placeholder='Search, for example "lock", "undo", "theme", "reminder", "STOP ALL"...' />
        {q && <Button kind="ghost" small icon="x" title="Clear search" onClick={() => setQ("")} />}
      </div>
      {searching ? (
        found.length ? (
          <>
            <div className="small muted" style={{ margin: "8px 0" }} role="status">{found.length} result{found.length === 1 ? "" : "s"}</div>
            <Card className="flat">{found.map((e) => <Entry key={e.id} e={e} />)}</Card>
          </>
        ) : <Card><Empty icon="search" title={`Nothing found for "${q}"`}>Try a shorter or different word, such as the name of the button.</Empty></Card>
      ) : (
        GUIDE_AREAS.map((area) => (
          <div key={area} style={{ marginBottom: 16 }}>
            <h3 className="group-title" style={{ paddingLeft: 0 }}>{area}</h3>
            <Card className="flat">{GUIDE.filter((e) => e.area === area).map((e) => <Entry key={e.id} e={e} />)}</Card>
          </div>
        ))
      )}
    </div>
  );
}
