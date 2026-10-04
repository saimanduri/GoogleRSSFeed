import { errText, useApp } from "../app";
import { FONTS, SIZES, fontInstalled } from "../fonts";
import { IconUpload } from "./IconUpload";
import { Card } from "./ui";

const THEMES: [string, string, string][] = [
  ["system", "System", "Follows Windows light/dark"],
  ["time_of_day", "Day & night", "Light by day, dark at night (PC clock)"],
  ["light", "Light", "Clean and bright"], ["dark", "Dark", "Easy on the eyes"],
  ["aurora", "Aurora", "Night blue, teal and violet"], ["ocean", "Ocean", "Airy light blue"],
  ["forest", "Forest", "Deep green and lime"], ["sunset", "Sunset", "Warm orange and pink"],
];
const BACKGROUNDS: [string, string][] = [["off", "Off"], ["aurora", "Aurora glow"], ["bubbles", "Bubbles"], ["waves", "Waves"], ["stars", "Starfield"]];

const ACCENTS: [string, string, string][] = [["theme", "Theme default", ""], ["blue", "Blue", "#2563eb"], ["teal", "Teal", "#0d9488"],
  ["emerald", "Emerald", "#16a34a"], ["violet", "Violet", "#7c3aed"], ["graphite", "Graphite", "#64748b"]];

// Preview palette for "system" / "time_of_day" cards
const PREVIEW: Record<string, string> = { system: "light", time_of_day: "dark" };

export function ThemePicker() {
  const { status, call, toast, refresh } = useApp();
  const cur = status?.ui?.["ui.theme"] ?? "system";
  const bg = status?.ui?.["ui.background"] ?? "off";
  const accent = status?.ui?.["ui.accent"] ?? "theme";
  const font = status?.ui?.["ui.font"] ?? "windows";
  const size = status?.ui?.["ui.font_size"] ?? "medium";
  const set = async (key: string, value: string) => {
    try { await call("settings.apply", { changes: { [key]: value } }); await refresh(); } catch (e: any) { toast(errText(e), "danger"); }
  };
  return (
    <Card title="Appearance">
      <div className="theme-grid" role="radiogroup" aria-label="Theme">
        {THEMES.map(([id, label, help]) => (
          <button key={id} role="radio" aria-checked={cur === id} className={`theme-card ${cur === id ? "selected" : ""}`} onClick={() => set("ui.theme", id)} title={help}>
            <div className="theme-preview" data-theme={PREVIEW[id] ?? id}><div className="bar" /><div className="bubble" /><div className="bubble me" /></div>
            <div className="theme-label"><span>{label}</span>{cur === id && <span>✓</span>}</div>
          </button>
        ))}
      </div>
      <div style={{ fontWeight: 600, margin: "4px 0 4px" }}>Pictures</div>
      <IconUpload settingKey="ui.assistant_icon" title="Assistant icon" help="Shown in the top bar. PNG, JPEG or WebP from this PC; it is shrunk to 128 px." fallback="🛡" />
      <IconUpload settingKey="ui.user_icon" title="Your picture" help="Shown next to your name in the left menu." fallback={(status?.display_name ?? status?.username ?? "?").slice(0, 1).toUpperCase()} />
      <div style={{ fontWeight: 600, margin: "10px 0 8px" }}>Accent colour</div>
      <div className="bg-grid" role="radiogroup" aria-label="Accent colour">
        {ACCENTS.map(([id, label, color]) => (
          <button key={id} role="radio" aria-checked={accent === id} className={`chip-choice ${accent === id ? "selected" : ""}`} onClick={() => set("ui.accent", id)}>
            <span className="swatch" style={color ? { background: color } : undefined} />{label}
          </button>
        ))}
      </div>
      <div style={{ fontWeight: 600, margin: "4px 0 8px" }}>Text size</div>
      <div className="bg-grid" role="radiogroup" aria-label="Text size">
        {SIZES.map(([id, label]) => (
          <button key={id} role="radio" aria-checked={size === id} className={`chip-choice ${size === id ? "selected" : ""}`} onClick={() => set("ui.font_size", id)}>{label}</button>
        ))}
      </div>
      <div style={{ fontWeight: 600, margin: "10px 0 8px" }}>Font</div>
      <div className="font-grid" role="radiogroup" aria-label="Font">
        {FONTS.map((f) => {
          const ok = fontInstalled(f.family);
          return (
            <button key={f.id} role="radio" aria-checked={font === f.id} className={`font-card ${font === f.id ? "selected" : ""} ${ok ? "" : "missing"}`} onClick={() => set("ui.font", f.id)}
              title={ok ? f.note : `${f.label} is not installed on this PC - the app will use a similar font until it is`}>
              <span className="font-name" style={{ fontFamily: f.stack }}>{f.label}{font === f.id ? " ✓" : ""}</span>
              <span className="font-sample" style={{ fontFamily: f.stack }}>The quick brown fox 0123 · नमस्ते</span>
              <span className="font-note">{ok ? f.note : "not installed"}</span>
            </button>
          );
        })}
      </div>
      <div style={{ fontWeight: 600, margin: "10px 0 8px" }}>Animated background</div>
      <div className="bg-grid" role="radiogroup" aria-label="Animated background">
        {BACKGROUNDS.map(([id, label]) => (
          <button key={id} role="radio" aria-checked={bg === id} className={`chip-choice ${bg === id ? "selected" : ""}`} onClick={() => set("ui.background", id)}>{label}</button>
        ))}
      </div>
      <div className="small muted">Animation stops automatically with “Reduce motion” and when Windows asks apps to reduce animation. Day and night times can be set below.</div>
    </Card>
  );
}
