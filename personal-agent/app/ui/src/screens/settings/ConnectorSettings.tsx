import { ReactNode, useEffect, useState } from "react";
import { openExternal } from "../../api/gateway";
import { errText, useApp } from "../../app";
import { Badge, Button, Card, Time, Toggle } from "../../components/ui";

export function ConnectorSettings({ generic }: { generic: ReactNode }) {
  const { call, toast } = useApp();
  const [d, setD] = useState<any>(null);
  const load = () => call("connectors.list").then(setD).catch(() => undefined);
  useEffect(() => { void load(); /* eslint-disable-next-line */ }, []);
  const set = async (id: string, flags: any) => { try { await call("connectors.set", { connector: id, ...flags }); void load(); } catch (e: any) { toast(errText(e), "danger"); } };
  if (!d) return <div className="skeleton" style={{ height: 200 }} />;
  return (
    <div className="col">
      <Card>
        <div className="row"><div className="grow"><b>Pause all connectors</b><div className="small muted">Instantly stops every connector in chat and missions.</div></div>
          <Toggle on={d.pause_all} label="Pause all connectors" onChange={async (v) => { await call("connectors.pause_all", { paused: v }); void load(); }} /></div>
      </Card>
      {d.connectors.map((c: any) => (
        <Card key={c.id} title={<span>{c.label} <Badge tone={c.enabled && c.connection_ok ? "ok" : c.enabled ? "warn" : ""}>{c.enabled ? (c.connection_ok ? "on" : "on - not ready") : "off"}</Badge></span>}
          actions={<Toggle on={c.enabled} label={`${c.label} on/off`} onChange={(v) => set(c.id, { enabled: v })} />}>
          <p className="small muted">{c.description}</p>
          {!c.connection_ok && c.connection_reason && <div className="banner warn small">{c.connection_reason}</div>}
          <div className="row wrap small">
            <span className="row">Use in chat <Toggle on={c.use_chat} onChange={(v) => set(c.id, { use_chat: v })} /></span>
            <span className="row">Use in routines <Toggle on={c.use_missions} onChange={(v) => set(c.id, { use_missions: v })} /></span>
            <span className="faint">Last used: <Time iso={c.last_used_at} /></span>
          </div>
          {c.manifest && <div className="small faint" style={{ marginTop: 6 }}>Declares: sends to {c.manifest.destinations?.join(", ")} · data: {c.manifest.data_types?.join(", ")} · effects: {c.manifest.side_effects?.join(", ")}</div>}
          {c.id === "m365" && (
            <div className="col" style={{ marginTop: 10 }}>
              <div className="small"><b>Permissions requested:</b> {(c.granted_scopes_plain ?? []).join(" · ")}</div>
              <div className="row">
                <Button small kind="primary" onClick={async () => { try { const r = await call<any>("connectors.m365_sign_in", {}); await openExternal(r.authorize_url); toast("Finish signing in in your browser (within 2 minutes)", "info"); } catch (e: any) { toast(errText(e), "danger"); } }}>Sign in with Microsoft</Button>
                <Button small onClick={async () => { if (confirm("Disconnect and delete the stored token?")) { await call("connectors.disconnect", { connector: "m365", delete_data: confirm("Also delete data imported from Microsoft 365 (attachments, derived memories)?") }); void load(); } }}>Disconnect</Button>
              </div>
              <div className="small faint">Uses your organisation's (or your own) Entra ID app registration: public client, redirect URI http://localhost. Enter its Client ID and Tenant ID below.</div>
            </div>
          )}
          {c.id === "web" && <div className="small" style={{ marginTop: 8 }}>Search provider: <b>{c.search_provider}</b> {c.search_ready ? <Badge tone="ok">ready</Badge> : <Badge tone="warn">needs a provider + API key bound to web.search (Secrets)</Badge>}
            <Button small onClick={async () => { try { const r = await call<any>("web.test_search", {}); toast(`Web search works (${r.provider}, ${r.results} result, ${r.ms} ms)`, "ok"); } catch (e: any) { toast(errText(e), "danger"); } }}>Test web search</Button></div>}
          {c.dependent_missions?.length > 0 && <div className="small faint">Used by missions: {c.dependent_missions.map((m: any) => m.name).join(", ")}</div>}
        </Card>
      ))}
      <h2>Connector options</h2>
      {generic}
    </div>
  );
}
