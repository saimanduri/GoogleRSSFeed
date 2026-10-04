// Edit a name in place (chat title ...): Enter or leaving the box saves, Esc cancels. Empty names are not saved.
import { useEffect, useRef, useState } from "react";

export function InlineRename({ value, onSave, onCancel, label = "Name" }: { value: string; onSave: (v: string) => void; onCancel: () => void; label?: string }) {
  const [v, setV] = useState(value);
  const ref = useRef<HTMLInputElement>(null);
  const done = useRef(false);
  useEffect(() => { ref.current?.focus(); ref.current?.select(); }, []);
  const finish = (save: boolean) => {
    if (done.current) return;
    done.current = true;
    const t = v.trim();
    if (save && t && t !== value) onSave(t); else onCancel();
  };
  return (
    <input ref={ref} className="input inline-rename" aria-label={label} value={v} maxLength={120} spellCheck={false}
      onChange={(e) => setV(e.target.value)} onClick={(e) => e.stopPropagation()} onDoubleClick={(e) => e.stopPropagation()}
      onKeyDown={(e) => { e.stopPropagation(); if (e.key === "Enter") finish(true); else if (e.key === "Escape") finish(false); }}
      onBlur={() => finish(true)} />
  );
}
