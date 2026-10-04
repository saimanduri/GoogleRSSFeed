// Position rail for long chats (like the thin lines at the right edge in ChatGPT): one tick per question, the tick of the part you are
// reading is dark, hovering or tabbing to the rail opens a list with the first words of every question, a click jumps there.
// Pure view over the messages already on screen: no new data, no backend call.
import { RefObject, useCallback, useEffect, useMemo, useRef, useState } from "react";

export type Turn = { id: string; label: string };
const MAX_TICKS = 40;
export const RAIL_MIN_TURNS = 4;

export function turnLabel(text: string): string {
  const t = String(text ?? "").replace(/\s+/g, " ").trim();
  return t.length > 70 ? `${t.slice(0, 67)}…` : t || "(empty message)";
}

/** Index of the last anchor whose top is at or above `line` (binary search over sorted offsets). */
export function activeIndex(offsets: number[], line: number): number {
  let lo = 0, hi = offsets.length - 1, ans = 0;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (offsets[mid] <= line) { ans = mid; lo = mid + 1; } else hi = mid - 1;
  }
  return ans;
}

export function ChatRail({ turns, container }: { turns: Turn[]; container: RefObject<HTMLElement | null> }) {
  const [active, setActive] = useState(0);
  const offsets = useRef<number[]>([]);
  const timer = useRef<number | undefined>(undefined);
  const last = useRef(0);
  const pop = useRef<HTMLDivElement>(null);
  const ids = useMemo(() => turns.map((t) => t.id).join("|"), [turns]);

  const measure = useCallback(() => {
    const c = container.current;
    if (!c) return;
    const top = c.getBoundingClientRect().top;
    offsets.current = turns.map((t) => {
      const el = c.querySelector<HTMLElement>(`[data-turn="${CSS.escape(t.id)}"]`);
      return el ? el.getBoundingClientRect().top - top + c.scrollTop : Number.POSITIVE_INFINITY;
    });
  }, [container, turns]);

  const update = useCallback(() => {
    const c = container.current;
    if (!c || !offsets.current.length) return;
    const atEnd = c.scrollTop + c.clientHeight >= c.scrollHeight - 6;
    setActive(atEnd ? offsets.current.length - 1 : activeIndex(offsets.current, c.scrollTop + c.clientHeight * 0.35));
  }, [container]);

  useEffect(() => {
    const c = container.current;
    if (!c) return;
    measure(); update();
    // throttled to ~20 updates a second (a timer, not requestAnimationFrame, so it also runs while the window is hidden or being tested)
    const onScroll = () => {
      const wait = 50 - (Date.now() - last.current);
      if (wait <= 0) { last.current = Date.now(); update(); return; }
      window.clearTimeout(timer.current);
      timer.current = window.setTimeout(() => { last.current = Date.now(); update(); }, wait);
    };
    c.addEventListener("scroll", onScroll, { passive: true });
    const ro = new ResizeObserver(() => { measure(); update(); });   // streaming answers and window resizes move the anchors
    ro.observe(c);
    Array.from(c.children).forEach((ch) => ro.observe(ch));
    return () => { c.removeEventListener("scroll", onScroll); ro.disconnect(); window.clearTimeout(timer.current); };
    // eslint-disable-next-line
  }, [ids, container]);

  const jump = (i: number) => {
    const c = container.current;
    if (!c) return;
    measure();
    const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    c.scrollTo({ top: Math.max(0, (offsets.current[i] ?? 0) - 12), behavior: reduce ? "auto" : "smooth" });
    setActive(i);
  };

  if (turns.length < RAIL_MIN_TURNS) return null;
  const bucket = Math.max(1, Math.ceil(turns.length / MAX_TICKS));
  const ticks = Array.from({ length: Math.ceil(turns.length / bucket) }, (_, k) => k);
  const activeTick = Math.floor(active / bucket);
  const keyNav = (e: React.KeyboardEvent) => {
    if (e.key !== "ArrowDown" && e.key !== "ArrowUp") return;
    e.preventDefault();
    const next = Math.min(turns.length - 1, Math.max(0, active + (e.key === "ArrowDown" ? 1 : -1)));
    jump(next);
    pop.current?.querySelectorAll<HTMLElement>(".rail-item")[next]?.focus();
  };
  return (
    <nav className="chat-rail" aria-label="Conversation outline" onKeyDown={keyNav}
      onMouseEnter={() => pop.current?.querySelector(".rail-item.active")?.scrollIntoView({ block: "nearest" })}>
      <div className="rail-ticks" aria-hidden="true">
        {ticks.map((k) => <span key={k} className={`rail-tick ${k === activeTick ? "active" : ""}`} onClick={() => jump(k * bucket)} />)}
      </div>
      <div className="rail-pop" ref={pop}>
        <div className="rail-head">{turns.length} questions</div>
        {turns.map((t, i) => (
          <button key={t.id} className={`rail-item ${i === active ? "active" : ""}`} aria-current={i === active ? "true" : undefined}
            aria-label={`Go to question ${i + 1}: ${t.label}`} onClick={() => jump(i)}>
            <span className="rail-n">{i + 1}</span><span className="ellipsis">{t.label}</span>
          </button>
        ))}
      </div>
    </nav>
  );
}
