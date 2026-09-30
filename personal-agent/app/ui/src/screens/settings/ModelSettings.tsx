// AI models: built-in llama.cpp (GGUF), Ollama, vLLM, LM Studio, Run:ai or any OpenAI-compatible endpoint, plus
// speech-to-text and embedding models. Every model must pass "Test model" before it can be a default.
import { ReactNode, useEffect, useState } from "react";
import { pickFile } from "../../api/gateway";
import { errText, useApp } from "../../app";
import { Badge, Button, Card, Field, Modal } from "../../components/ui";

const ROLES = [["standard", "Standard (chat, missions)"], ["fast", "Fast (summaries)"], ["reasoning", "Reasoning"], ["vision", "Vision"],
  ["embedding", "Embeddings (memory search)"], ["stt", "Speech-to-text (voice)"]];

export function ModelSettings({ generic }: { generic: ReactNode }) {
  const { call, toast } = useApp();
  const [d, setD] = useState<any>(null);
  const [report, setReport] = useState<any>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const load = () => call("llm.models").then(setD).catch(() => undefined);
  useEffect(() => { void load(); /* eslint-disable-next-line */ }, []);
  if (!d) return <div className="skeleton" style={{ height: 200 }} />;
  const test = async (id: string) => { setBusy(id); try { setReport(await call("llm.test", { model_id: id })); void load(); } catch (e: any) { toast(errText(e), "danger"); } finally { setBusy(null); } };
  return (
    <div className="col">
      <Card title="Models">
        {!d.models.length && <div className="muted small">No models yet. Add one below.</div>}
        <table className="table"><tbody>{d.models.map((m: any) => (
          <tr key={m.id}>
            <td><b>{m.name}</b><div className="small faint">{m.provider}{m.endpoint ? ` · ${m.endpoint}` : ""}{m.model_name ? ` · ${m.model_name}` : ""}</div>
              {m.sha256 && <div className="small faint mono">sha256 {m.sha256.slice(0, 16)}…</div>}</td>
            <td><Badge>{m.kind}</Badge></td>
            <td><Badge tone={m.location === "remote" ? "danger" : m.location === "builtin" ? "ok" : "warn"}>{m.isolation}</Badge></td>
            <td>{m.tested ? <Badge tone="ok">tested</Badge> : <Badge tone="warn">not tested</Badge>}</td>
            <td style={{ textAlign: "right" }}><Button small busy={busy === m.id} onClick={() => test(m.id)}>Test model</Button>
              <Button small kind="ghost" icon="trash" onClick={async () => { if (confirm("Remove this model?")) { await call("llm.remove", { model_id: m.id }); void load(); } }} /></td>
          </tr>))}</tbody></table>
      </Card>
      <Card title="Default model per role">
        {ROLES.map(([r, label]) => (
          <div key={r} className="setting-row"><div>{label}</div>
            <select className="input" value={d.roles[r] ?? ""} onChange={async (e) => { try { await call("llm.set_role", { role: r, model_id: e.target.value }); void load(); } catch (er: any) { toast(errText(er), "danger"); } }}>
              <option value="">(automatic)</option>
              {d.models.filter((m: any) => (r === "stt" ? m.kind === "stt" : r === "embedding" ? m.kind === "embedding" : m.kind === "chat")).map((m: any) => <option key={m.id} value={m.id}>{m.name}{m.tested ? "" : " (not tested)"}</option>)}
            </select></div>))}
      </Card>
      <Card title="Add a model"><ModelAdder onAdded={load} builtin={d.builtin_runtime} /></Card>
      <h2>Runtime options</h2>
      {generic}
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

export function ModelAdder({ onAdded, builtin, compact }: { onAdded: () => void; builtin?: boolean; compact?: boolean }) {
  const { toast } = useApp();
  const { call } = useApp();
  const [p, setP] = useState<any>({ provider: "ollama", endpoint: "http://127.0.0.1:11434", kind: "chat" });
  const [found, setFound] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);
  const presets: Record<string, any> = {
    ollama: { endpoint: "http://127.0.0.1:11434" }, openai: { endpoint: "http://127.0.0.1:8000/v1" }, builtin: { endpoint: "" }, dev_mock: {},
  };
  const set = (k: string, v: any) => setP({ ...p, [k]: v });
  const add = async () => {
    setBusy(true);
    try {
      await call("llm.add", { model: { ...p, name: p.name || p.model_name || p.provider } });
      toast("Model added - run 'Test model' before making it a default", "ok");
      onAdded();
    } catch (e: any) { toast(errText(e), "danger"); } finally { setBusy(false); }
  };
  return (
    <div className="col">
      <div className="grid-2">
        <Field label="Where does the model run?">
          <select className="input" value={p.provider} onChange={(e) => setP({ ...p, provider: e.target.value, ...presets[e.target.value] })}>
            <option value="ollama">Ollama on this PC</option>
            <option value="openai">OpenAI-compatible server (vLLM, LM Studio, LocalAI, Run:ai...)</option>
            <option value="builtin">Built-in runtime (llama.cpp, .gguf file){builtin === false ? " - not installed" : ""}</option>
            <option value="dev_mock">Developer mock (dev mode only)</option>
          </select>
        </Field>
        <Field label="Model type">
          <select className="input" value={p.kind} onChange={(e) => set("kind", e.target.value)}>
            <option value="chat">Chat / agent</option><option value="stt">Speech-to-text (Whisper etc.)</option><option value="embedding">Embeddings</option>
          </select>
        </Field>
      </div>
      {(p.provider === "ollama" || p.provider === "openai") && (
        <div className="grid-2">
          <Field label="Endpoint URL" help="Loopback (127.0.0.1) keeps data on this PC. Other hosts are REMOTE and need your password.">
            <input className="input" value={p.endpoint} onChange={(e) => set("endpoint", e.target.value)} />
          </Field>
          <Field label="API key (optional)" help="Stored in your vault and bound to this model only.">
            <input className="input" type="password" value={p.api_key ?? ""} onChange={(e) => set("api_key", e.target.value)} />
          </Field>
          <Field label="Model name">
            <div className="row"><input className="input" list="found-models" value={p.model_name ?? ""} onChange={(e) => set("model_name", e.target.value)} />
              <datalist id="found-models">{found.map((f) => <option key={f} value={f} />)}</datalist>
              <Button onClick={async () => { try { const r = await call<any>("llm.discover", { provider: p.provider, endpoint: p.endpoint, api_key: p.api_key }); setFound(r.models); toast(`Found ${r.models.length} models`, "ok"); } catch (e: any) { toast(errText(e), "danger"); } }}>Discover</Button></div>
          </Field>
          <Field label="Display name"><input className="input" value={p.name ?? ""} onChange={(e) => set("name", e.target.value)} /></Field>
        </div>
      )}
      {p.provider === "builtin" && (
        <Field label="GGUF model file" help="The file's SHA-256 is recorded and verified before every load. Only .gguf files are accepted.">
          <div className="row"><input className="input" value={p.path ?? ""} onChange={(e) => set("path", e.target.value)} />
            <Button onClick={async () => { const f = await pickFile({ filters: [{ name: "GGUF model", extensions: ["gguf"] }] }); if (f) set("path", String(f)); }}>Browse</Button></div>
        </Field>
      )}
      <div className="row"><div className="spacer" /><Button kind={compact ? undefined : "primary"} busy={busy} onClick={add}>Add model</Button></div>
    </div>
  );
}
