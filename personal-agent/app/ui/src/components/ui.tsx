import { ReactNode, useEffect, useState } from "react";
import { Icon } from "./Icon";

export function Button(p: { children?: ReactNode; onClick?: () => void; kind?: "primary" | "danger" | "ghost"; small?: boolean; icon?: string;
  disabled?: boolean; title?: string; busy?: boolean; type?: "button" | "submit" }) {
  return (
    <button type={p.type ?? "button"} className={`btn ${p.kind ?? ""} ${p.small ? "sm" : ""} ${!p.children ? "icon" : ""}`} onClick={p.onClick}
      disabled={p.disabled || p.busy} title={p.title} aria-label={p.title}>
      {p.busy ? <span className="spinner" /> : p.icon ? <Icon name={p.icon} size={p.small ? 15 : 17} /> : null}
      {p.children}
    </button>
  );
}

export function Card({ title, children, actions, className, onClick, icon }: { title?: ReactNode; children?: ReactNode; actions?: ReactNode;
  className?: string; onClick?: () => void; icon?: string }) {
  return (
    <div className={`card ${onClick ? "clickable" : ""} ${className ?? ""}`} onClick={onClick}>
      {(title || actions) && (
        <div className="card-title">
          {icon && <Icon name={icon} />}
          <div className="grow">{title}</div>
          {actions}
        </div>
      )}
      {children}
    </div>
  );
}

export function Badge({ children, tone }: { children: ReactNode; tone?: string }) {
  return <span className={`badge ${tone ?? ""}`}>{children}</span>;
}

const SENS = ["PUBLIC", "INTERNAL", "CONFIDENTIAL", "RESTRICTED"];
export function Sensitivity({ level }: { level: number | string | null | undefined }) {
  const name = typeof level === "number" ? SENS[level] ?? "PUBLIC" : (level ?? "PUBLIC");
  return <span className={`badge sens-${name}`} title="Data sensitivity">{name}</span>;
}

export function Toggle({ on, onChange, disabled, label }: { on: boolean; onChange: (v: boolean) => void; disabled?: boolean; label?: string }) {
  return <button role="switch" aria-checked={on} aria-label={label} className={`toggle ${on ? "on" : ""}`} disabled={disabled} onClick={() => onChange(!on)} />;
}

export function Field({ label, help, error, children }: { label?: ReactNode; help?: ReactNode; error?: ReactNode; children: ReactNode }) {
  return (
    <div className="field">
      {label && <label>{label}</label>}
      {children}
      {help && <div className="help">{help}</div>}
      {error && <div className="err">{error}</div>}
    </div>
  );
}

export function Tabs<T extends string>({ tabs, value, onChange }: { tabs: [T, string][]; value: T; onChange: (v: T) => void }) {
  return (
    <div className="tabs" role="tablist">
      {tabs.map(([k, label]) => (
        <button key={k} role="tab" aria-selected={value === k} className={`tab ${value === k ? "active" : ""}`} onClick={() => onChange(k)}>{label}</button>
      ))}
    </div>
  );
}

export function Empty({ icon = "sparkle", title, children }: { icon?: string; title: string; children?: ReactNode }) {
  return (
    <div className="empty">
      <div className="big"><Icon name={icon} size={34} /></div>
      <div style={{ fontWeight: 650, color: "var(--text-2)" }}>{title}</div>
      {children}
    </div>
  );
}

export function Modal({ title, children, onClose, actions, wide }: { title: ReactNode; children: ReactNode; onClose?: () => void; actions?: ReactNode; wide?: boolean }) {
  useEffect(() => {
    const k = (e: KeyboardEvent) => e.key === "Escape" && onClose?.();
    window.addEventListener("keydown", k);
    return () => window.removeEventListener("keydown", k);
  }, [onClose]);
  return (
    <div className="overlay" onMouseDown={(e) => e.target === e.currentTarget && onClose?.()}>
      <div className={`modal ${wide ? "wide" : ""}`} role="dialog" aria-modal="true">
        <div className="row" style={{ marginBottom: 12 }}>
          <h2 className="grow" style={{ margin: 0 }}>{title}</h2>
          {onClose && <Button kind="ghost" icon="x" title="Close" onClick={onClose} />}
        </div>
        {children}
        {actions && <div className="modal-actions">{actions}</div>}
      </div>
    </div>
  );
}

export function Time({ iso, relative = true }: { iso?: string | null; relative?: boolean }) {
  if (!iso) return <span className="faint">-</span>;
  const d = new Date(iso);
  const diff = (Date.now() - d.getTime()) / 1000;
  let text = d.toLocaleString();
  if (relative) {
    const a = Math.abs(diff);
    const fmt = (n: number, u: string) => `${Math.round(n)} ${u}${Math.round(n) === 1 ? "" : "s"}`;
    const s = a < 60 ? "just now" : a < 3600 ? fmt(a / 60, "min") : a < 86400 ? fmt(a / 3600, "hour") : a < 604800 ? fmt(a / 86400, "day") : d.toLocaleDateString();
    text = a < 60 || a >= 604800 ? s : diff > 0 ? `${s} ago` : `in ${s}`;
  }
  return <time dateTime={iso} title={d.toLocaleString()}>{text}</time>;
}

export function ChipInput({ value, onChange, placeholder }: { value: string[]; onChange: (v: string[]) => void; placeholder?: string }) {
  const [t, setT] = useState("");
  const add = () => { const v = t.trim(); if (v && !value.includes(v)) onChange([...value, v]); setT(""); };
  return (
    <div className="chip-input">
      {value.map((v) => (
        <span className="chip" key={v}>{v}<button aria-label={`remove ${v}`} onClick={() => onChange(value.filter((x) => x !== v))}>✕</button></span>
      ))}
      <input value={t} placeholder={placeholder ?? "Add and press Enter"} onChange={(e) => setT(e.target.value)}
        onKeyDown={(e) => { if (e.key === "Enter" || e.key === ",") { e.preventDefault(); add(); } if (e.key === "Backspace" && !t && value.length) onChange(value.slice(0, -1)); }}
        onBlur={add} />
    </div>
  );
}

export function Spinner() { return <span className="spinner" />; }

export function bytes(n: number) {
  if (!n) return "0 B";
  const u = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.min(u.length - 1, Math.floor(Math.log(n) / Math.log(1024)));
  return `${(n / 1024 ** i).toFixed(i ? 1 : 0)} ${u[i]}`;
}
