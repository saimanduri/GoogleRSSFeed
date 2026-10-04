import { useEffect, useState } from "react";
import { useApp } from "../../app";
import { Icon } from "../../components/Icon";
import { AccountSettings } from "./AccountSettings";
import { ConnectorSettings } from "./ConnectorSettings";
import { GenericGroup } from "./GenericGroup";
import { BackupSettings, DiagnosticsSettings, EmergencySettings, LogsExtras, PrivacySettings, RulesExtras, ToolsExtras, UpdatesSettings } from "./Misc";
import { EmailMonitoring } from "./EmailMonitoring";
import { ModelSettings } from "./ModelSettings";
import { ThemePicker } from "../../components/ThemePicker";

const ICONS: Record<string, string> = { account: "shield", connectors: "plug", model: "model", autonomy: "activity", rules: "approvals", approvals: "check",
  tools: "steps", web: "search", files: "files", memory: "memory", notifications: "reminders", logs: "archive", backup: "download", emergency: "alert",
  updates: "refresh", privacy: "lock", diagnostics: "info", ui: "sparkle", emailmon: "mail" };

export function Settings() {
  const { call, route } = useApp();
  const [schema, setSchema] = useState<any>(null);
  const [section, setSection] = useState<string>(route.params?.section ?? "account");
  const load = () => call("settings.describe").then(setSchema).catch(() => undefined);
  useEffect(() => { void load(); /* eslint-disable-next-line */ }, []);
  useEffect(() => { if (route.params?.section) setSection(route.params.section); }, [route.params?.section]);
  if (!schema) return <div className="page"><div className="skeleton" style={{ height: 300 }} /></div>;
  const group = schema.groups.find((g: any) => g.id === section);
  const generic = <GenericGroup schema={schema} group={section} onChanged={load} />;
  return (
    <div className="settings-layout">
      <nav className="settings-nav" aria-label="Settings">
        <div className="faint small" style={{ padding: "4px 12px 8px", fontWeight: 700, letterSpacing: ".06em" }}>SETTINGS</div>
        {schema.groups.map((g: any) => (
          <button key={g.id} className={`nav-item ${section === g.id ? "active" : ""}`} onClick={() => setSection(g.id)}>
            <span className="icon"><Icon name={ICONS[g.id] ?? "settings"} size={16} /></span>{g.label}
          </button>
        ))}
      </nav>
      <div className="page" key={section}>
        <div className="page-header"><h1>{group?.label}</h1></div>
        {section === "account" && <AccountSettings generic={generic} tab={route.params?.tab} />}
        {section === "connectors" && <ConnectorSettings generic={generic} />}
        {section === "model" && <ModelSettings generic={generic} />}
        {section === "backup" && <BackupSettings generic={generic} />}
        {section === "emergency" && <EmergencySettings generic={generic} />}
        {section === "privacy" && <PrivacySettings />}
        {section === "diagnostics" && <DiagnosticsSettings />}
        {section === "updates" && <UpdatesSettings generic={generic} />}
        {section === "logs" && <>{generic}<LogsExtras /></>}
        {section === "tools" && <>{generic}<ToolsExtras /></>}
        {section === "rules" && <>{generic}<RulesExtras reload={load} /></>}
        {["autonomy", "approvals", "web", "files", "memory", "notifications"].includes(section) && generic}
        {section === "ui" && <><ThemePicker />{generic}</>}
        {section === "emailmon" && <EmailMonitoring generic={generic} onChanged={load} />}
      </div>
    </div>
  );
}
