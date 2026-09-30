// Browser-preview mock of pa-gateway. Used ONLY when the UI runs outside the Tauri app (npm run dev),
// so the interface can be designed and tested on any OS. It is never bundled into security decisions.
type Listener = (topic: string, data: any) => void;
let emit: Listener = () => {};
export function mockListen(fn: Listener) {
  emit = fn;
}

const now = () => new Date().toISOString();
let seq = 0;
const id = (p: string) => `${p}_${Date.now().toString(16)}${(seq++).toString(16)}`;

const S: any = {
  state: "SETUP_REQUIRED",
  username: "",
  chats: [] as any[],
  messages: {} as Record<string, any[]>,
  runs: [] as any[],
  steps: {} as Record<string, any[]>,
  approvals: [] as any[],
  reminders: [] as any[],
  missions: [] as any[],
  files: [] as any[],
  memories: [] as any[],
  secrets: [] as any[],
  models: [] as any[],
  settings: {} as Record<string, any>,
  kill: { pause_agent: false, stop_tasks: false, disable_connectors: false, disable_web: false, disable_sandbox: false, stop_all: false },
  connectors: [
    { id: "outlook_local", label: "Local Outlook (classic)", description: "Read and search classic Outlook on this PC.", enabled: false, use_chat: true, use_missions: true, connection_ok: true, scopes: [], dependent_missions: [] },
    { id: "m365", label: "Microsoft 365 / Exchange Online", description: "Mail and calendar via Microsoft Graph.", enabled: false, use_chat: true, use_missions: true, connection_ok: false, connection_reason: "Not connected", scopes: [], dependent_missions: [], granted_scopes_plain: ["Read your basic profile", "Read your mail", "Read your calendar", "Stay signed in"] },
    { id: "web", label: "Web search + fetch", description: "Search the public web and fetch pages through the egress filter.", enabled: true, use_chat: true, use_missions: true, connection_ok: true, scopes: [], dependent_missions: [], search_provider: "none" },
  ],
};

const SCHEMA = {
  groups: [
    ["account", "Account & Security"], ["connectors", "Connectors"], ["model", "AI Model"], ["autonomy", "Autonomy & Budgets"],
    ["rules", "Rules & Safety"], ["approvals", "Approvals"], ["tools", "Tools & Skills"], ["web", "Web Access"], ["files", "Files & Storage"],
    ["memory", "Memory"], ["notifications", "Notifications"], ["logs", "Logs & SIEM"], ["backup", "Backup & Restore"],
    ["emergency", "Emergency Stop"], ["updates", "Updates"], ["privacy", "Privacy & Data"], ["diagnostics", "Diagnostics & About"], ["ui", "Appearance & Voice"],
  ].map(([id, label]) => ({ id, label })),
  settings: [
    { key: "security.auto_lock_minutes", group: "account", label: "Auto-lock after idle (minutes)", type: "int", default: 10, value: 10, min: 1, max: 60, options: [], help: "Auto-lock cannot be switched off.", risk: "A longer timeout leaves the app open for longer.", loosen: "up", stepup: false, floor: false },
    { key: "security.lock_on_windows_lock", group: "account", label: "Lock when Windows locks", type: "bool", default: true, value: true, options: [], help: "", risk: "", loosen: "false", stepup: false, floor: false },
    { key: "web.fetch_any_site", group: "web", label: "Fetch any site", type: "bool", default: false, value: false, options: [], help: "", risk: "The agent may fetch any public website.", loosen: "true", stepup: false, floor: false },
    { key: "web.allowlist", group: "web", label: "Allowed domains", type: "list", default: [], value: ["wikipedia.org", "github.com"], options: [], help: "", risk: "", loosen: "list_add", stepup: false, floor: false },
    { key: "autonomy.profile", group: "autonomy", label: "Autonomy profile", type: "enum", default: "cautious", value: "cautious", options: ["cautious", "balanced"], help: "", risk: "", loosen: ["cautious", "balanced"], stepup: false, floor: false },
    { key: "budget.task.tool_calls", group: "autonomy", label: "Per task: max tool calls", type: "int", default: 100, value: 100, min: 1, max: 2000, options: [], help: "", risk: "", loosen: "up", stepup: false, floor: false },
    { key: "notifications.content_level", group: "notifications", label: "Notification content", type: "enum", default: "notify", value: "notify", options: ["notify", "summary"], help: "", risk: "", loosen: ["notify", "summary"], stepup: false, floor: false },
    { key: "ui.theme", group: "ui", label: "Theme", type: "enum", default: "system", value: "system", options: ["system", "light", "dark"], help: "", risk: "", loosen: null, stepup: false, floor: false },
  ],
};

function status() {
  const base: any = { state: S.state, username: S.username, dev_mode: true, password_wait: 0, pin_available: S.state === "UI_LOCKED", recovery_wait: 0 };
  if (S.state === "UNLOCKED")
    Object.assign(base, {
      tasks_running: S.runs.filter((r: any) => r.status === "RUNNING").length,
      approvals_pending: S.approvals.filter((a: any) => a.status === "PENDING").length,
      killswitch: { levels: S.kill, any: Object.values(S.kill).some(Boolean), labels: {} },
      needs_pin_setup: false, unread_notifications: 0,
      ui: { "ui.theme": S.settings["ui.theme"] ?? "system", "ui.text_scale": 100, "ui.reduce_motion": false, "chat.show_steps": true, "voice.enabled": true, "security.auto_lock_minutes": 10, "emergency.hotkey": "Ctrl+Alt+Shift+S" },
    });
  return base;
}

function step(runId: string, type: string, title: string, status = "done", detail: any = {}) {
  const s = { id: id("stp"), run_id: runId, seq: (S.steps[runId]?.length ?? 0) + 1, type, title, status, detail, started_at: now(), duration_ms: Math.floor(Math.random() * 400) };
  (S.steps[runId] ||= []).push(s);
  emit("run.step", { run_id: runId, step_id: s.id, seq: s.seq, type, title, status, detail });
}

async function simulateChat(chatId: string, text: string, runId: string) {
  const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));
  step(runId, "input", "Your message", "done", { text });
  await wait(300);
  step(runId, "llm", "Thinking (standard model)", "done", {});
  if (/remind me/i.test(text)) {
    step(runId, "thought", "Reasoning", "done", { text: "The user wants a reminder; proposing one for confirmation." });
    step(runId, "tool", "reminders.propose", "running", {});
    step(runId, "policy", "Policy: REQUIRE_APPROVAL", "done", { reasons: ["this action needs your confirmation"] });
    const a = { id: id("apr"), tool: "reminders.propose", kind: "reminder", payload: { text: text.replace(/remind me (to )?/i, ""), due_at: new Date(Date.now() + 86400000).toISOString().slice(0, 16) }, payload_hash: "sha256:mock", destination: null, sensitivity: 1, risk: "low", reason: "this action needs your confirmation", requires_password: 0, status: "PENDING", created_at: now(), expires_at: new Date(Date.now() + 86400000).toISOString(), chat_id: chatId, run_id: runId };
    S.approvals.push(a);
    step(runId, "approval", "Waiting for your approval: reminders.propose", "waiting", { approval_id: a.id });
    emit("approvals.changed", { id: a.id, status: "PENDING", chat_id: chatId });
    return;
  }
  const answer = `Here's what I can tell you about **${text.slice(0, 60)}**.\n\n- This is the browser preview (mock gateway).\n- In the real app, every step you see here is logged and policy-checked.`;
  for (let i = 0; i < answer.length; i += 12) {
    emit("llm.delta", { run_id: runId, delta: answer.slice(i, i + 12) });
    await wait(30);
  }
  emit("llm.delta", { run_id: runId, delta: "", done: true });
  finish(chatId, runId, answer);
}

function finish(chatId: string, runId: string, answer: string) {
  S.messages[chatId].push({ id: id("msg"), chat_id: chatId, role: "assistant", content: answer, run_id: runId, sources: ["model"], sensitivity: 1, created_at: now() });
  const r = S.runs.find((x: any) => x.id === runId);
  if (r) { r.status = "COMPLETED"; r.ended_at = now(); r.summary = answer; }
  emit("chat.message", { chat_id: chatId, run_id: runId });
  emit("run.finished", { run_id: runId, status: "COMPLETED" });
}

export async function mockCall(method: string, p: any): Promise<any> {
  await new Promise((r) => setTimeout(r, 60));
  switch (method) {
    case "session.status": return status();
    case "session.touch": return { ok: true };
    case "setup.preflight": return { windows: false, platform: "browser", version: "Browser preview", tpm: { usable: false, detail: "mock" }, tpm_ok: true, bitlocker: "unknown", sandbox: "UNAVAILABLE", data_folder: "%LOCALAPPDATA%\\PersonalAgent", cloud_synced: false, disk_free_gb: 120, gpu: ["Mock GPU"], dev_mode: true };
    case "setup.check_password": { const pw = p.password as string; const score = Math.min(4, Math.floor(pw.length / 4)); return { strength: { score, label: ["very weak", "weak", "fair", "good", "strong"][score], feedback: [] }, errors: pw.length < 12 ? ["Password must be at least 12 characters"] : [] }; }
    case "setup.check_pin": return { errors: /^(\d)\1+$/.test(p.pin) || p.pin === "123456" ? ["PIN is too common"] : p.pin.length < 6 ? ["PIN must be 6-12 characters"] : [] };
    case "setup.create": S.state = "UNLOCKED"; S.username = p.username; return { recovery_key: "7K2QD-9XM4R-A1B2C-D3E4F-G5H6J-K7M8N-P9Q0R-S1T2V", confirm_groups: [1, 5], protector: "software" };
    case "setup.confirm_recovery": return { ok: true };
    case "auth.sign_in": if (p.password.length < 4) throw { code: "auth_failed", message: "wrong username or password" }; S.state = "UNLOCKED"; S.username = p.username; return status();
    case "auth.quick_unlock": S.state = "UNLOCKED"; return status();
    case "auth.lock": S.state = "UI_LOCKED"; emit("session.changed", status()); return status();
    case "auth.sign_out": S.state = "SIGNED_OUT"; return status();
    case "auth.step_up": return { category: p.category, method: p.method, minutes: 5 };
    case "auth.forgot_password": S.state = "UNLOCKED"; return { recovery_key: "NEW01-KEY02-ABCDE-FGHJK-MNPQR-STVWX-YZ012-34567", confirm_groups: [0, 3] };
    case "home.summary": return { events: [{ id: "h1", kind: "mission_done", severity: "info", title: "Morning mail summary finished", detail: "Output saved to My Files.", created_at: now() }], completed: [], failed: [], waiting: [], outcome_unknown: [], files_created: [], memories_proposed: S.memories.filter((m: any) => m.status === "PROPOSED"), approvals_pending: S.approvals.filter((a: any) => a.status === "PENDING").length, reminders_today: S.reminders, security: [], memory_review: null, budget: { used: { tokens: 12345, web_requests: 3 }, limits: { tokens: 2000000, web_requests: 150, egress_bytes: 1048576, runtime_seconds: 14400 } }, recovery_key_confirmed: true };
    case "chat.list": return S.chats;
    case "chat.create": { const c = { id: id("chat"), title: "New chat", created_at: now(), updated_at: now(), hwm: 0, allow_tools: 1 }; S.chats.unshift(c); S.messages[c.id] = []; return { id: c.id }; }
    case "chat.get": { const chat = S.chats.find((c: any) => c.id === p.chat_id); return { chat: { ...chat, sensitivity: "INTERNAL", sources: [] }, messages: S.messages[p.chat_id] ?? [], pending_approvals: S.approvals.filter((a: any) => a.chat_id === p.chat_id && a.status === "PENDING"), running: [] }; }
    case "chat.send": {
      const chat = S.chats.find((c: any) => c.id === p.chat_id);
      if (chat.title === "New chat") chat.title = p.text.slice(0, 60);
      S.messages[p.chat_id].push({ id: id("msg"), chat_id: p.chat_id, role: "user", content: p.text, created_at: now(), sources: [] });
      const runId = id("run");
      S.runs.unshift({ id: runId, kind: "chat", title: p.text.slice(0, 80), chat_id: p.chat_id, status: "RUNNING", started_at: now(), hwm: 1, tokens_in: 800, tokens_out: 120, tool_calls: 0 });
      emit("run.started", { run_id: runId, chat_id: p.chat_id });
      void simulateChat(p.chat_id, p.text, runId);
      return { run_id: runId, task_id: id("task") };
    }
    case "chat.update": case "chat.delete": return { ok: true };
    case "runs.list": return S.runs;
    case "runs.get": return { ...S.runs.find((r: any) => r.id === p.run_id), steps: S.steps[p.run_id] ?? [] };
    case "approvals.list": return S.approvals.filter((a: any) => a.status === (p.status ?? "PENDING"));
    case "approvals.decide": {
      const a = S.approvals.find((x: any) => x.id === p.approval_id);
      a.status = p.approve ? "APPROVED" : "DENIED";
      emit("approvals.changed", { id: a.id, status: a.status, chat_id: a.chat_id });
      if (p.approve && a.tool === "reminders.propose") {
        S.reminders.push({ id: id("rem"), text: a.payload.text, due_at: a.payload.due_at, status: "SCHEDULED", timezone: "local" });
        step(a.run_id, "approval", "Approval approved: reminders.propose", "done", {});
        step(a.run_id, "tool", "reminders.propose", "done", { preview: "Reminder scheduled." });
        finish(a.chat_id, a.run_id, `Done - I'll remind you: **${a.payload.text}** on ${a.payload.due_at.replace("T", " ")}.`);
      } else if (!p.approve) finish(a.chat_id, a.run_id, "OK, I won't do that.");
      return a;
    }
    case "reminders.list": return S.reminders;
    case "reminders.create": S.reminders.push({ id: id("rem"), text: p.text, due_at: p.due_at, status: "SCHEDULED" }); return { id: "x" };
    case "reminders.action": S.reminders = S.reminders.filter((r: any) => r.id !== p.id || p.action === "snooze"); return { ok: true };
    case "missions.list": return S.missions;
    case "missions.create": { const m = { id: id("msn"), ...p.mission, status: "DRAFT", schedule_text: typeof p.mission.schedule === "string" ? p.mission.schedule : JSON.stringify(p.mission.schedule), allowed_tools: p.mission.allowed_tools ?? [], warnings: ["Runs only while this PC is on and you stay signed in to Windows."] }; S.missions.push(m); return { id: m.id }; }
    case "missions.activate": S.missions.find((m: any) => m.id === p.mission_id).status = "ACTIVE"; return { ok: true };
    case "missions.set_status": S.missions.find((m: any) => m.id === p.mission_id).status = p.status; return { ok: true };
    case "missions.describe": return { name: "Morning mail summary", objective: p.text, schedule: { type: "cron", cron: "30 7 * * 1-5" }, timezone: "Europe/Berlin", allowed_tools: ["m365.search_mail", "notify.user"], notification_level: "notify", output_format: "markdown", missed_run_policy: "RUN_ONCE", kind: "routine" };
    case "missions.parse_schedule": return { schedule: { type: "cron", cron: "30 7 * * 1-5" }, next_run: new Date(Date.now() + 3600e3).toISOString() };
    case "missions.run_now": return { task_id: "t" };
    case "tasks.list": return S.runs.map((r: any) => ({ id: r.id, objective: r.title, state: r.status, trigger_type: "USER", updated_at: r.started_at, usage: { tool_calls: 1, tokens: 900 }, limits: { tool_calls: 100, tokens: 200000 } }));
    case "files.list": return { files: S.files, storage: { used_bytes: 1234567, quota_bytes: 10737418240, count: S.files.length, quarantined: 0 } };
    case "files.upload": { const f = { id: id("file"), name: p.name ?? String(p.path).split(/[\\/]/).pop(), folder: "/", size_bytes: 2048, status: "READY", sensitivity: 1, source: "upload", created_at: now(), tags_json: [] }; S.files.unshift(f); return f; }
    case "files.preview": return { ...S.files.find((f: any) => f.id === p.file_id), text: "Extracted text preview (mock)." };
    case "memory.list": return S.memories;
    case "memory.add": S.memories.push({ id: id("mem"), type: p.type ?? "preference", content: p.content, trust: "TRUSTED", status: "ACTIVE", source: "user", created_at: now(), provenance: [] }); return { id: "m" };
    case "memory.about_me": return { stated: S.memories.filter((m: any) => m.trust === "TRUSTED"), inferred: S.memories.filter((m: any) => m.trust === "INFERRED") };
    case "memory.review": return { new: [], stale: [], conflicts: [], proposed: [] };
    case "secrets.list": return S.secrets;
    case "secrets.create": S.secrets.push({ id: id("sec"), ...p.item, value: undefined, bindings: p.bindings ?? [], updated_at: now(), version: 1 }); return { id: "s" };
    case "secrets.reveal": return { ...S.secrets.find((s: any) => s.id === p.id), value: "mock-secret-value", hide_after_seconds: 20 };
    case "secrets.generate": return { value: Array.from(crypto.getRandomValues(new Uint8Array(15))).map((b) => "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789!@#$%"[b % 62]).join("") };
    case "secrets.health": return { weak: [], reused: [], old: [] };
    case "secrets.binding_targets": return ["web.search"];
    case "connectors.list": return { connectors: S.connectors, pause_all: false };
    case "connectors.set": Object.assign(S.connectors.find((c: any) => c.id === p.connector), p); return { ok: true };
    case "llm.models": return { models: S.models, roles: {}, builtin_runtime: false };
    case "llm.add": S.models.push({ id: id("mdl"), name: p.model.name || p.model.model_name || p.model.provider, provider: p.model.provider, kind: p.model.kind ?? "chat", endpoint: p.model.endpoint, tested: 0, isolation: "Reduced isolation", location: "loopback" }); return { id: "m" };
    case "llm.test": { const m = S.models.find((x: any) => x.id === p.model_id); m.tested = 1; return { passed: true, checks: [{ name: "follows_instructions", ok: true }, { name: "json_action_protocol", ok: true }, { name: "attack_attempt_rate", ok: true, note: "0/3" }] }; }
    case "llm.discover": return { models: ["llama3.1:8b", "qwen2.5:14b", "whisper-large-v3"] };
    case "settings.describe": return { groups: SCHEMA.groups, settings: SCHEMA.settings.map((s: any) => ({ ...s, value: S.settings[s.key] ?? s.value })) };
    case "settings.classify": return { normalized: p.changes, loosening: Object.keys(p.changes).filter((k) => k === "web.fetch_any_site" && p.changes[k]).map((k) => ({ key: k, label: "Fetch any site", before: false, after: true, risk: "The agent may fetch any public website." })), stepup: [] };
    case "settings.begin_loosen": return { token: "tok", delay_seconds: 10, loosening: [] };
    case "settings.apply": Object.assign(S.settings, p.changes); emit("settings.changed", {}); return { applied: Object.keys(p.changes), loosened: [] };
    case "settings.rules_plain": return { rules: [{ rule: "Default deny: any action not explicitly allowed is refused.", floor: true }, { rule: "Sending email always needs your approval.", floor: true }], policy_version: "local-1.0.0+user-0" };
    case "settings.history": return [];
    case "killswitch.state": return { levels: S.kill, any: Object.values(S.kill).some(Boolean), labels: {} };
    case "killswitch.activate": S.kill[p.level] = true; emit("killswitch.changed", { levels: S.kill, any: true }); return { levels: S.kill, any: true };
    case "killswitch.release": Object.keys(S.kill).forEach((k) => (S.kill[k] = false)); emit("killswitch.changed", { levels: S.kill, any: false }); return { levels: S.kill, any: false };
    case "activity.events": return { events: [{ event_id: "e1", sequence: 1, timestamp: now(), category: "authentication", event_type: "auth.signin", severity: "info" }, { event_id: "e2", sequence: 2, timestamp: now(), category: "authorization", event_type: "policy.decision", tool: "web.search", policy_decision: "ALLOW", severity: "info" }], integrity: { ok: true, events: 2 } };
    case "logs.verify": return { ok: true, events: 2 };
    case "logs.status": return { path: "%LOCALAPPDATA%\\PersonalAgent\\logs\\agent-security.jsonl", segments: [], siem: { enabled: false }, integrity: { ok: true } };
    case "posture.run": return [{ id: "dev_mode", title: "Developer mode", status: "high", detail: "Browser preview" }, { id: "bitlocker", title: "BitLocker drive encryption", status: "ok", detail: "System drive is encrypted." }, { id: "sandbox", title: "Python sandbox", status: "warn", detail: "Standard isolation (AppContainer)." }];
    case "history.search": return [];
    case "notifications.list": return [];
    case "tools.catalog": return { tools: [], sandbox: { strength: "UNAVAILABLE", label: "Unavailable", help: "" } };
    case "skills.list": return [];
    case "backup.status": return { folder: "", last: null, files: [], scheduled_enabled: false, warn: true };
    case "privacy.data_map": return [];
    case "diagnostics.health": return { gateway: { ok: true, uptime_s: 100 }, core: { ok: true }, model: {}, sandbox: { strength: "UNAVAILABLE" }, metrics: {} };
    case "about": return { name: "Personal Agent - Desktop Edition", version: "0.1.0", build: "mock", licences: [] };
    case "updates.status": return { current: "0.1.0", auto_check: true, available: null, note: "mock" };
    case "account.signin_history": return [];
    case "voice.transcribe": return { text: "remind me to call the dentist tomorrow at 9" };
    case "ui.open_link": return { screen: "home" };
    default: return {};
  }
}
