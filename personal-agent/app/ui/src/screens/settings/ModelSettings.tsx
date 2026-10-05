// AI models, kept apart on purpose: Chat models (answer and act), Voice models (speech to text), Vision models (read images and
// scanned pages) and Embedding models (memory search). Each kind has its own list, its own default and its own "Add" button, and the
// gateway refuses to put a model into the wrong kind or to use it for a role that is not its own. Every model must pass "Test model"
// (done automatically in the background after adding) before it can be a default.
import { ReactNode, useEffect, useState } from "react";
import { onEvent, pickFile } from "../../api/gateway";
import { errText, useApp } from "../../app";
import { Badge, Button, Card, Field, Modal } from "../../components/ui";

type Kind = "chat" | "stt" | "vision" | "embedding";

const SECTIONS: { kind: Kind; title: string; blurb: string; addLabel: string; roles: [string, string][] }[] = [
  { kind: "chat", title: "Chat models", addLabel: "Add chat model", blurb: "The models that think, answer and run your routines (for example Gemma, Qwen coder, Llama).",
    roles: [["standard", "Standard (chat, routines)"], ["fast", "Fast (summaries)"], ["reasoning", "Reasoning"]] },
  { kind: "stt", title: "Voice models (speech to text)", addLabel: "Add voice model", blurb: "Turn what you say into text (for example Qwen3-ASR, Whisper). Used by the microphone button; they cannot chat.",
    roles: [["stt", "Voice input"]] },
  { kind: "vision", title: "Vision models (images and scanned pages)", addLabel: "Add vision model", blurb: "Read pictures, photos of documents and scanned PDFs (for example Qwen3-VL, Qwen2.5-VL). Used automatically when you upload an image or a scan.",
    roles: [["vision", "Images and scans"]] },
  { kind: "embedding", title: "Embedding models (memory search)", addLabel: "Add embedding model", blurb: "Help the assistant find things in your memory and files. Optional; they cannot chat.",
    roles: [["embedding", "Memory search"]] },
];

export function ModelSettings({ generic }: { generic: ReactNode }) {
  const { call, toast } = useApp();
  const [d, setD] = useState<any>(null);
  const [report, setReport] = useState<any>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [tune, setTune] = useState<any>(null);
  const load = () => call("llm.models").then(setD).catch(() => undefined);
  useEffect(() => { void load(); /* eslint-disable-next-line */ }, []);
  // a model added with "Add" is tested in the background: refresh when it finishes (event) and keep polling as a fallback
  useEffect(() => onEvent((topic, data) => {
    if (topic !== "llm.models_changed") return;
    void load();
    if (data?.state === "passed") toast("Model tested and ready - you can use it now", "ok");
    else if (data?.state === "failed") toast("Model did not pass its test - press Test model on its row for details", "warn");
    // eslint-disable-next-line
  }), []);
  const anyTesting = !!d?.models?.some((m: any) => m.testing);
  useEffect(() => { if (!anyTesting) return; const t = setInterval(() => void load(), 4000); return () => clearInterval(t); /* eslint-disable-next-line */ }, [anyTesting]);
  if (!d) return <div className="skeleton" style={{ height: 200 }} />;
  const test = async (id: string) => { setBusy(id); try { setReport(await call("llm.test", { model_id: id })); void load(); } catch (e: any) { toast(errText(e), "danger"); } finally { setBusy(null); } };
  return (
    <div className="col">
      {SECTIONS.map((sec) => {
        const models = d.models.filter((m: any) => (m.kind ?? "chat") === sec.kind);
        return (
          <Card key={sec.kind} title={sec.title}>
            <div className="small muted" style={{ marginBottom: 8 }}>{sec.blurb}</div>
            {!models.length && <div className="muted small">None yet. Use "{sec.addLabel}" below.</div>}
            {models.length > 0 && <table className="table"><tbody>{models.map((m: any) => (
              <tr key={m.id}>
                <td><b>{m.name}</b><div className="small faint">{m.provider}{m.endpoint ? ` · ${m.endpoint}` : ""}{m.model_name ? ` · ${m.model_name}` : ""}</div>
                  {m.sha256 && <div className="small faint mono">sha256 {m.sha256.slice(0, 16)}…</div>}
                  {(sec.kind === "chat" || sec.kind === "vision") && m.effective && <div className="small muted">
                    Context {m.effective.context_length.toLocaleString()} tokens{m.effective.context_from === "default" ? " (default)" : ""} · Temperature {m.effective.temperature}{m.effective.temperature_from === "default" ? " (default)" : ""}
                    {" "}<button className="link-btn" onClick={() => setTune(m)}>Adjust</button></div>}</td>
                <td><Badge tone={m.location === "remote" ? "danger" : m.location === "builtin" ? "ok" : "warn"}>{m.isolation}</Badge></td>
                <td>{m.testing ? <Badge tone="warn">testing…</Badge> : m.tested ? <Badge tone="ok">tested</Badge> : <Badge tone="warn">not tested</Badge>}</td>
                <td style={{ textAlign: "right" }}><Button small busy={busy === m.id || !!m.testing} onClick={() => test(m.id)}>Test model</Button>
                  <Button small kind="ghost" icon="trash" title="Remove this model" onClick={async () => { if (confirm("Remove this model?")) { await call("llm.remove", { model_id: m.id }); void load(); } }} /></td>
              </tr>))}</tbody></table>}
            {sec.roles.map(([r, label]) => (
              <div key={r} className="setting-row"><div>Default for: {label}</div>
                <select className="input" value={d.roles[r] ?? ""} onChange={async (e) => { try { await call("llm.set_role", { role: r, model_id: e.target.value }); void load(); } catch (er: any) { toast(errText(er), "danger"); } }}>
                  <option value="">(automatic)</option>
                  {models.map((m: any) => <option key={m.id} value={m.id}>{m.name}{m.tested ? "" : " (not tested)"}</option>)}
                </select></div>))}
            <details className="add-model" style={{ marginTop: 10 }}>
              <summary style={{ cursor: "pointer", fontWeight: 600 }}>{sec.addLabel}</summary>
              <div style={{ marginTop: 10 }}><ModelAdder kind={sec.kind} addLabel={sec.addLabel} onAdded={load} builtin={d.builtin_runtime} /></div>
            </details>
          </Card>
        );
      })}
      <h2>Runtime options</h2>
      {generic}
      {tune && <TuneModel m={tune} onClose={() => setTune(null)} onSaved={() => { setTune(null); void load(); }} />}
      {report && (
        <Modal title={report.passed ? "Model passed" : "Model did not pass"} onClose={() => setReport(null)}>
          <div className="list">{report.checks.map((c: any) => (
            <div key={c.name} className="list-item"><Badge tone={c.ok ? "ok" : c.gate === false ? "warn" : "danger"}>{c.ok ? "pass" : "fail"}</Badge>
              <div className="grow"><div className="mono small">{c.name}</div><div className="small faint">{c.note ?? c.error ?? ""}</div></div></div>))}</div>
          {report.exposure?.checked && <div className={`banner ${report.exposure.at_risk ? "danger" : "ok"} small`} style={{ marginTop: 10 }}>
            {report.exposure.at_risk ? "This runtime answers browser requests or is reachable from your network - AT RISK." : "Listener check: not reachable from browsers or the network."}</div>}
          <p className="small faint" style={{ marginTop: 10 }}>Red-team checks measure whether the model tries to follow injected instructions. The gateway blocks such actions regardless.</p>
        </Modal>
      )}
    </div>
  );
}

const CONTEXT_PRESETS = [4096, 8192, 16384, 32768, 65536, 131072];

/** Context length + temperature of ONE model. Empty = use the defaults in "Runtime options" below. */
function TuneModel({ m, onClose, onSaved }: { m: any; onClose: () => void; onSaved: () => void }) {
  const { call, toast } = useApp();
  const [ctx, setCtx] = useState<string>(m.context_length ? String(m.context_length) : "");
  const [temp, setTemp] = useState<string>(m.temperature === null || m.temperature === undefined ? "" : String(m.temperature));
  const [busy, setBusy] = useState(false);
  const save = async (reset = false) => {
    setBusy(true);
    try {
      await call("llm.update", { model_id: m.id, context_length: reset || !ctx ? null : Number(ctx), temperature: reset || temp === "" ? null : Number(temp) });
      toast(reset ? "Back to the defaults" : "Saved - used from the next message", "ok");
      onSaved();
    } catch (e: any) { toast(errText(e), "danger"); } finally { setBusy(false); }
  };
  const t = temp === "" ? m.effective.temperature : Number(temp);
  return (
    <Modal title={`Adjust ${m.name}`} onClose={onClose} actions={<>
      <Button kind="ghost" onClick={() => save(true)} disabled={busy}>Use defaults</Button>
      <Button kind="primary" busy={busy} onClick={() => save(false)}>Save</Button></>}>
      <Field label="Context length (tokens)" help={`How much text the model can look at in one go: your question, the conversation, files and tool results. Larger = handles long documents and mail threads but needs more memory (VRAM) and is slower. Empty = default (${m.effective.context_from === "default" ? m.effective.context_length.toLocaleString() : "Runtime options"}).${m.provider === "openai" ? " For vLLM / LM Studio / Run:ai the server's own limit applies; this value keeps requests inside it." : ""}`}>
        <div className="row wrap" style={{ gap: 6, marginBottom: 6 }}>{CONTEXT_PRESETS.map((n) => (
          <Button key={n} small kind={Number(ctx) === n ? "primary" : "ghost"} onClick={() => setCtx(String(n))}>{n >= 1024 ? `${n / 1024}k` : n}</Button>))}</div>
        <input className="input" type="number" min={1024} max={1048576} step={1024} placeholder="default" value={ctx} onChange={(e) => setCtx(e.target.value)} />
      </Field>
      <Field label={`Temperature: ${Number.isFinite(t) ? t : "-"}`} help="Lower = steadier, factual, repeatable (good for routines, finance, tool use; 0-0.3). Higher = more varied wording (0.7-1.0). Empty = default.">
        <input type="range" min={0} max={2} step={0.05} value={Number.isFinite(t) ? t : 0.2} onChange={(e) => setTemp(e.target.value)} aria-label="Temperature" style={{ width: "100%" }} />
        <div className="row" style={{ justifyContent: "space-between" }}><span className="small faint">0 steady</span>
          <button className="link-btn small" onClick={() => setTemp("")}>use default</button><span className="small faint">2 creative</span></div>
      </Field>
    </Modal>
  );
}

const KIND_WORD: Record<Kind, string> = { chat: "a chat model", stt: "a voice (speech-to-text) model", vision: "a vision model", embedding: "an embedding model" };

/** Add form for ONE kind of model. The kind is fixed by the card it sits in, so the lists can never get mixed up. */
export function ModelAdder({ onAdded, builtin, compact, kind = "chat", addLabel = "Add model" }: { onAdded: () => void; builtin?: boolean; compact?: boolean; kind?: Kind; addLabel?: string }) {
  const { toast, call } = useApp();
  const [p, setP] = useState<any>({ provider: "ollama", endpoint: "http://127.0.0.1:11434", kind });
  const [found, setFound] = useState<{ name: string; kind: string; vision: boolean }[]>([]);
  const [busy, setBusy] = useState(false);
  const [note, setNote] = useState("");
  const presets: Record<string, any> = { ollama: { endpoint: "http://127.0.0.1:11434" }, openai: { endpoint: "http://127.0.0.1:8000/v1" }, builtin: { endpoint: "" }, dev_mock: {} };
  const set = (k: string, v: any) => setP({ ...p, [k]: v });
  // which discovered models belong to this card: voice and embedding models only show up under their own kind
  const fits = (f: { kind: string; vision: boolean }) => (kind === "stt" ? f.kind === "stt" : kind === "embedding" ? f.kind === "embedding" : kind === "vision" ? f.kind === "chat" && f.vision : f.kind === "chat");
  const shown = found.filter(fits);
  const hidden = found.length - shown.length;
  const choose = async (name: string) => {
    setP((q: any) => ({ ...q, model_name: name, kind }));
    setNote("");
    if (!name || p.provider !== "ollama") return;
    try {
      const r = await call<any>("llm.inspect", { provider: p.provider, endpoint: p.endpoint, model_name: name, api_key: p.api_key });
      const ok = fits({ kind: r.kind, vision: !!r.vision });
      if (!ok) setNote(r.kind === "stt" ? "This is a voice model - add it under Voice models." : r.kind === "embedding" ? "This is an embedding model - add it under Embedding models."
        : kind === "vision" ? "This model does not report image understanding - choose a vision model such as qwen2.5vl or qwen3-vl." : "This model cannot be added here.");
      else setNote(`Detected: ${KIND_WORD[kind]}.`);
    } catch { /* the choice stays manual; the gateway checks again when adding */ }
  };
  const add = async () => {
    setBusy(true);
    try {
      await call("llm.add", { model: { ...p, kind, name: p.name || p.model_name || p.provider } });
      toast("Model added. It is being tested in the background - it becomes ready by itself.", "ok");
      onAdded();
    } catch (e: any) { toast(errText(e), "danger"); } finally { setBusy(false); }
  };
  return (
    <div className="col">
      <Field label="Where does the model run?">
        <select className="input" value={p.provider} onChange={(e) => setP({ ...p, provider: e.target.value, kind, ...presets[e.target.value] })}>
          <option value="ollama">Ollama on this PC</option>
          <option value="openai">OpenAI-compatible server (vLLM, LM Studio, LocalAI, Run:ai...)</option>
          {kind !== "stt" && kind !== "vision" && <option value="builtin">Built-in runtime (llama.cpp, .gguf file){builtin === false ? " - not installed" : ""}</option>}
          <option value="dev_mock">Developer mock (dev mode only)</option>
        </select>
      </Field>
      {(p.provider === "ollama" || p.provider === "openai") && (
        <div className="grid-2">
          <Field label="Endpoint URL" help="Loopback (127.0.0.1) keeps data on this PC. Other hosts are REMOTE and need your password.">
            <input className="input" value={p.endpoint} onChange={(e) => set("endpoint", e.target.value)} />
          </Field>
          <Field label="API key (optional)" help="Stored in your vault and bound to this model only.">
            <input className="input" type="password" value={p.api_key ?? ""} onChange={(e) => set("api_key", e.target.value)} />
          </Field>
          <Field label="Model name">
            <div className="row"><input className="input" value={p.model_name ?? ""} onChange={(e) => set("model_name", e.target.value)} onBlur={(e) => { if (e.target.value) void choose(e.target.value); }} />
              <Button onClick={async () => { try { const r = await call<any>("llm.discover", { provider: p.provider, endpoint: p.endpoint, api_key: p.api_key }); setFound(r.details ?? r.models.map((n: string) => ({ name: n, kind: "chat", vision: false }))); toast(`Found ${r.models.length} models`, "ok"); } catch (e: any) { toast(errText(e), "danger"); } }}>Discover</Button></div>
          </Field>
          <Field label="Display name"><input className="input" value={p.name ?? ""} onChange={(e) => set("name", e.target.value)} /></Field>
        </div>
      )}
      {found.length > 0 && (
        <div className="col" style={{ gap: 4 }}>
          <div className="row" style={{ flexWrap: "wrap", gap: 6 }}>
            {shown.map((f) => <Button key={f.name} small kind={p.model_name === f.name ? "primary" : "ghost"} onClick={() => void choose(f.name)}>{f.name}</Button>)}
            {!shown.length && <span className="small muted">No {KIND_WORD[kind]} found on this server.</span>}
          </div>
          {hidden > 0 && <div className="small faint">{hidden} other model{hidden === 1 ? "" : "s"} on the server belong to the other kinds and are not shown here.</div>}
        </div>
      )}
      {note && <div className="small faint">{note}</div>}
      {p.provider === "builtin" && (
        <Field label="GGUF model file" help="The file's SHA-256 is recorded and verified before every load. Only .gguf files are accepted.">
          <div className="row"><input className="input" value={p.path ?? ""} onChange={(e) => set("path", e.target.value)} />
            <Button onClick={async () => { const f = await pickFile({ filters: [{ name: "GGUF model", extensions: ["gguf"] }] }); if (f) set("path", String(f)); }}>Browse</Button></div>
        </Field>
      )}
      <div className="row"><div className="spacer" /><Button kind={compact ? undefined : "primary"} busy={busy} onClick={add}>{addLabel}</Button></div>
    </div>
  );
}
