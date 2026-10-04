// A search / filter box with a clear (x) button: one click (or Esc) empties it so you can type something new without backspacing.
import { CSSProperties, KeyboardEvent, useRef } from "react";

export function SearchBox({ value, onChange, placeholder, onEnter, className = "", style, label, autoFocus }: {
  value: string; onChange: (v: string) => void; placeholder?: string; onEnter?: () => void; className?: string; style?: CSSProperties; label?: string; autoFocus?: boolean;
}) {
  const ref = useRef<HTMLInputElement>(null);
  const clear = () => { onChange(""); ref.current?.focus(); };
  const key = (e: KeyboardEvent) => { if (e.key === "Escape" && value) { e.stopPropagation(); clear(); } else if (e.key === "Enter") onEnter?.(); };
  return (
    <div className={`search-box ${className}`} style={style}>
      <input ref={ref} className="input" value={value} placeholder={placeholder} aria-label={label ?? placeholder} autoFocus={autoFocus}
        onChange={(e) => onChange(e.target.value)} onKeyDown={key} />
      {value && <button type="button" className="search-clear" aria-label="Clear search" title="Clear (Esc)" onClick={clear}>✕</button>}
    </div>
  );
}
