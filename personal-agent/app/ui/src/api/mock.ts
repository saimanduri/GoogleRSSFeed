// Browser-preview mock of pa-gateway. Used ONLY when the UI runs outside the Tauri app (npm run dev),
// so the interface can be designed and tested on any OS. It is never bundled into security decisions.
import schemaJson from "./settings_schema.json";
type Listener = (topic: string, data: any) => void;
let emit: Listener = () => {};
export function mockListen(fn: Listener) {
  emit = fn;
}

const now = () => new Date().toISOString();
const ago = (h: number) => new Date(Date.now() - h * 3600_000).toISOString();
let seq = 0;
const id = (p: string) => `${p}_${Date.now().toString(16)}${(seq++).toString(16)}`;

const S: any = {
  state: "SETUP_REQUIRED",
  username: "",
  display_name: "",
  assistant_name: "Personal Agent",
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

// generated from pa_gateway/settings_schema.py by scripts/gen_mock_schema.py (a test keeps it current)
const SCHEMA: { groups: any[]; settings: any[] } = schemaJson as any;

function status() {
  const base: any = { state: S.state, username: S.username, display_name: S.display_name || S.username, assistant_name: S.assistant_name, dev_mode: true, password_wait: 0, pin_available: S.state === "UI_LOCKED", recovery_wait: 0 };
  if (S.state === "UNLOCKED")
    Object.assign(base, {
      tasks_running: S.runs.filter((r: any) => r.status === "RUNNING").length,
      approvals_pending: S.approvals.filter((a: any) => a.status === "PENDING").length, proposals_pending: 0, outlook_unseen: (S as any).unseen ?? 1,
      killswitch: { levels: S.kill, any: Object.values(S.kill).some(Boolean), labels: {} },
      needs_pin_setup: false, unread_notifications: 0,
      ui: { "ui.theme": S.settings["ui.theme"] ?? "system", "ui.background": S.settings["ui.background"] ?? "off", "ui.accent": S.settings["ui.accent"] ?? "theme", "ui.font": S.settings["ui.font"] ?? "windows", "ui.font_size": S.settings["ui.font_size"] ?? "medium", "ui.assistant_icon": S.settings["ui.assistant_icon"] ?? "", "ui.user_icon": S.settings["ui.user_icon"] ?? "", "ui.day_starts": "07:00", "ui.night_starts": "19:00", "ui.text_scale": 100, "ui.reduce_motion": false, "chat.show_steps": false, "voice.enabled": true, "security.auto_lock_minutes": 10, "emergency.hotkey": "Ctrl+Alt+Shift+S" },
    });
  return base;
}

// Browser preview only: open http://127.0.0.1:5173/?demo to start signed in with sample chats and history.
function seedDemo() {
  const ago = (h: number) => new Date(Date.now() - h * 3600_000).toISOString();
  S.state = "UNLOCKED"; S.username = "sai"; S.display_name = "Sai"; S.assistant_name = "Manduri";
  const titles: [string, number, number, string][] = [["Plan for the week", 1, 1, ""], ["Summarise unread mail", 5, 0, ""], ["Dentist reminder", 27, 0, ""],
    ["Local LLM news", 50, 0, "Research"], ["Budget spreadsheet help", 100, 0, "Research"], ["Old trip notes", 400, 0, ""]];
  titles.forEach(([title, h, pinned, folder], i) => {
    const c = { id: `chat_demo${i}`, title, created_at: ago(h), updated_at: ago(h), hwm: 0, allow_tools: 1, pinned, folder };
    S.chats.push(c);
    S.messages[c.id] = Array.from({ length: i === 0 ? 40 : 2 }, (_, k) => ({ id: `m${i}_${k}`, chat_id: c.id, role: k % 2 ? "assistant" : "user",
      content: k % 2 ? "Here is a longer answer with **bold text**, a list:\n\n- first point\n- second point\n- third point\n\nAnd more explanation so the message is tall enough to scroll." : `Question number ${k / 2 + 1} about ${title}`,
      created_at: ago(h - k * 0.01), sources: ["model only"], sensitivity: 0 }));
  });
  ["Morning mail summary", "Weekly report", "Reminder fired", "Web search: LLM news"].forEach((t, i) =>
    S.runs.push({ id: `run_demo${i}`, kind: i === 1 ? "mission" : "chat", status: i === 2 ? "FAILED" : "COMPLETED", title: t, started_at: ago(2 + i * 30), created_at: ago(2 + i * 30) }));
}
function seedDemoFiles() {
  const mk = (id: string, name: string, status: string, extra: any) => ({ id, name, folder: "/Identity", status, status_reason: null, sensitivity: 2, size_bytes: 120000, created_at: ago(3), in_knowledge: 0, tags_json: [], source: "upload", sha256: "ab12cd34".repeat(8), sniffed_type: "pdf", scan_json: { antivirus: { engine: "defender", clean: true } }, ...extra });
  S.files.push(mk("file_pan", "PAN card.pdf", "READY", { meta_full: { title: "PAN card of Sai", doc_type: "PAN card", summary: "Income tax identity card issued to Sai. The number is in the file, not here.", keywords: ["pan", "income tax", "identity"], personal_data: ["PAN number", "date of birth"], status: "READY", edited: false, model: "qwen3-coder", note: null }, meta: { doc_type: "PAN card", summary: "Income tax identity card issued to Sai.", status: "READY" } }));
  S.files.push(mk("file_inv", "Invoice 4711.png", "READY", { sensitivity: 1, meta_full: { title: "Invoice 4711", doc_type: "Invoice", summary: "Tax invoice for Sharma Traders, total Rs 60,000.", keywords: ["invoice", "sharma"], personal_data: [], status: "READY", edited: true, model: null, note: "edited by you" }, meta: { doc_type: "Invoice", summary: "Tax invoice for Sharma Traders.", status: "READY" } }));
  S.files.push(mk("file_held", "contract.docx", "REJECTED", { sensitivity: 1, status_reason: "not scanned: Microsoft Defender is turned off (another antivirus may be active), so the file cannot be scanned", scan_json: { antivirus: { unavailable: true } } }));
}
if (typeof location !== "undefined" && new URLSearchParams(location.search).has("demo")) { seedDemo(); seedDemoFiles(); }

function step(runId: string, type: string, title: string, status = "done", detail: any = {}) {
  for (const prev of S.steps[runId] ?? []) if (prev.status === "running" && type !== "policy") prev.status = "done";
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
  const r = await mockImpl(method, p);
  return r === undefined ? r : structuredClone(r); // like the real IPC: every response is a fresh copy
}

async function mockImpl(method: string, p: any): Promise<any> {
  switch (method) {
    case "session.status": return status();
    case "session.touch": return { ok: true };
    case "setup.preflight": return { windows: false, platform: "browser", version: "Browser preview", tpm: { usable: false, detail: "mock" }, tpm_ok: true, bitlocker: "unknown", sandbox: "UNAVAILABLE", data_folder: "%LOCALAPPDATA%\\PersonalAgent", cloud_synced: false, disk_free_gb: 120, gpu: ["Mock GPU"], dev_mode: true };
    case "setup.check_password": { const pw = p.password as string; const score = Math.min(4, Math.floor(pw.length / 4)); return { strength: { score, label: ["very weak", "weak", "fair", "good", "strong"][score], feedback: [] }, errors: pw.length < 12 ? ["Password must be at least 12 characters"] : [] }; }
    case "setup.check_pin": return { errors: /^(\d)\1+$/.test(p.pin) || p.pin === "123456" ? ["PIN is too common"] : p.pin.length < 6 ? ["PIN must be 6-12 characters"] : [] };
    case "account.set_profile": S.display_name = p.display_name; S.assistant_name = p.assistant_name; emit("session.changed", status()); return { display_name: S.display_name, assistant_name: S.assistant_name };
    case "setup.create": S.state = "UNLOCKED"; S.username = p.username; S.display_name = p.display_name || p.username; S.assistant_name = p.assistant_name || "Personal Agent"; return { recovery_key: "7K2QD-9XM4R-A1B2C-D3E4F-G5H6J-K7M8N-P9Q0R-S1T2V", confirm_groups: [1, 5], protector: "software" };
    case "setup.confirm_recovery": return { ok: true };
    case "auth.sign_in": if (p.password.length < 4) throw { code: "auth_failed", message: "wrong username or password" }; S.state = "UNLOCKED"; S.username = p.username; return status();
    case "auth.quick_unlock": S.state = "UNLOCKED"; return status();
    case "auth.lock": S.state = "UI_LOCKED"; emit("session.changed", status()); return status();
    case "auth.sign_out": S.state = "SIGNED_OUT"; return status();
    case "auth.step_up": return { category: p.category, method: p.method, minutes: 5 };
    case "auth.forgot_password": S.state = "UNLOCKED"; return { recovery_key: "NEW01-KEY02-ABCDE-FGHJK-MNPQR-STVWX-YZ012-34567", confirm_groups: [0, 3] };
    case "home.dismiss": ((S as any).dismissed ??= []).push(p.id); return { ok: true };
    case "home.widgets": case "home.widgets_set": {
      const CAT: [string, string, string, string][] = [["updates", "Assistant", "Updates from the assistant", "Finished routines, files created and other notes."],
        ["mail_unread", "Mail", "Mail: unread today", "Unread mail received today."], ["mail_to_me", "Mail", "Mail: addressed to me", "Mail where you are in To."],
        ["mail_approvals", "Mail", "Mail: waiting for my approval", "Unanswered approval requests, last 7 days."], ["mail_deadlines", "Mail", "Mail: deadlines", "Deadlines within 7 days or overdue."],
        ["mail_awaiting_reply", "Mail", "Mail: waiting for a reply", "Sent mail nobody has answered."], ["reminders", "Planner", "Upcoming reminders", "Your next reminders."],
        ["routines_next", "Planner", "Next routine runs", "When routines run next."], ["approvals", "Attention", "Approvals waiting", "Actions, routines and memories waiting for you."],
        ["attention", "Attention", "Needs your attention", "Failed work, quarantine, expired shares, reminders."], ["recent_files", "Files", "Recent files", "Files lately added, with summaries."],
        ["storage", "Files", "My Files storage", "Quota used."], ["memory_recent", "Assistant", "What I learned lately", "Newest memories - forget any."],
        ["models_health", "System", "AI models and PC", "Which models are ready."], ["activity_today", "System", "Activity today", "Work finished in 24 hours."]];
      const DEF = ["updates", "mail_unread", "mail_to_me", "mail_approvals", "mail_deadlines", "reminders", "approvals", "attention", "routines_next", "recent_files", "models_health", "activity_today"];
      if (method === "home.widgets_set") S.settings["home.widgets"] = (p.enabled as string[]).filter((i) => CAT.some((c) => c[0] === i));
      const en: string[] = S.settings["home.widgets"] ?? DEF;
      const mail = (value: number, sub: string, tone?: string) => ({ state: "ok", value, sub, tone, age: 40, go: { screen: "outlook" } });
      const D: Record<string, any> = {
        updates: { state: "ok", items: S.home?.events ?? [{ id: "e1", kind: "mission_done", severity: "info", title: "Morning mail summary finished", detail: "Output saved to My Files.", created_at: ago(1) }], go: { screen: "activity" } },
        mail_unread: mail(45, "of 120 received today"), mail_to_me: mail(30, "in To today, 12 in CC"), mail_approvals: mail(4, "unanswered, last 7 days", "warn"), mail_deadlines: mail(2, "due within 7 days or overdue", "warn"),
        mail_awaiting_reply: mail(3, "sent mail without a reply (7 days)"),
        reminders: { state: "ok", items: [{ id: "r1", text: "Call the dentist", due_at: ago(-60) }, { id: "r2", text: "Budget review with finance", due_at: ago(-300) }], go: { screen: "reminders" } },
        routines_next: { state: "ok", items: [{ id: "m1", name: "Email: Hourly inbox check", next_run_at: ago(-30) }, { id: "m2", name: "Morning digest", next_run_at: ago(-600) }], go: { screen: "missions" } },
        approvals: { state: "ok", value: 2, sub: "1 actions, 1 routines, 0 memories", tone: "warn", go: { screen: "approvals" } },
        attention: { state: "ok", value: 2, tone: "warn", items: [{ kind: "files", text: "15 files in quarantine", go: { screen: "files" } }, { kind: "share", text: "1 shared folder approval expired (ask again in the chat)", go: { screen: "chat" } }] },
        recent_files: { state: "ok", items: [{ id: "f1", name: "PAN card.pdf", doc_type: "PAN card", summary: "Income tax identity card issued to Sai." }, { id: "f2", name: "Invoice 4711.png", doc_type: "Invoice", summary: "Invoice for Sharma Traders." }], go: { screen: "files" } },
        storage: { state: "ok", used: 1500000, quota: 10737418240, value: 15, go: { screen: "files" } },
        memory_recent: { state: "ok", value: 3, items: [{ id: "mm1", content: "Prefers short bullet-point reports", source: "learned:chat", created_at: ago(5) }, { id: "mm2", content: "My Files has 'PAN card.pdf' (in /Identity): PAN card. Contains: PAN number (values are in the file, not here).", source: "learned:file", created_at: ago(9) }], go: { screen: "memory" } },
        models_health: { state: "ok", kinds: { chat: { ready: true, name: "qwen3-coder" }, voice: { ready: true, name: "qwen3-asr" }, vision: { ready: false, name: "" }, embedding: { ready: false, name: "" } }, total: 3, reachable: true, gpu: 12, go: { screen: "settings", params: { section: "model" } } },
        activity_today: { state: "ok", done: 20, failed: 3, tokens: 124000, go: { screen: "activity" } },
      };
      return { catalog: CAT.map(([id, group, title, about]) => ({ id, group, title, about })), enabled: en, data: Object.fromEntries(en.map((i) => [i, D[i]])) };
    }
    case "home.summary": return { events: [{ id: "h1", kind: "mission_done", severity: "info", title: "Morning mail summary finished", detail: "Output saved to My Files.", created_at: now() }].filter((e: any) => !((S as any).dismissed ?? []).includes(e.id)), completed: [], failed: [], waiting: [], outcome_unknown: [], files_created: [], memories_proposed: S.memories.filter((m: any) => m.status === "PROPOSED"), approvals_pending: S.approvals.filter((a: any) => a.status === "PENDING").length, reminders_today: S.reminders, security: [], memory_review: null, budget: { used: { tokens: 12345, web_requests: 3 }, limits: { tokens: 2000000, web_requests: 150, egress_bytes: 1048576, runtime_seconds: 14400 } }, recovery_key_confirmed: true };
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
    case "chat.update": { const c = S.chats.find((x: any) => x.id === p.chat_id); if (c) { for (const k of ["pinned", "folder", "title", "archived", "allow_tools"]) if (p[k] !== undefined) c[k] = typeof p[k] === "boolean" ? +p[k] : p[k]; if (p.title !== undefined && !String(p.title).trim()) throw { code: "invalid_request", message: "a chat name cannot be empty" }; } return { ok: true }; }
    case "chat.delete": S.chats = S.chats.filter((c: any) => c.id !== p.chat_id); delete S.messages[p.chat_id]; return { ok: true };
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
    case "missions.create": { const m = { id: id("msn"), ...p.mission, status: "DRAFT", schedule_text: typeof p.mission.schedule === "string" ? p.mission.schedule : JSON.stringify(p.mission.schedule), allowed_tools: p.mission.allowed_tools ?? [], warnings: ["Runs only while this PC is on and you stay signed in to Windows.", ...((p.mission.allowed_tools ?? []).includes("web.search") ? ["Web search needs a search provider and its API key (Settings > Web Access / Secrets). Without it this mission cannot search and will report an error each time it runs.", "emergency stop is active"] : [])] }; S.missions.push(m); return { id: m.id }; }
    case "missions.activate": S.missions.find((m: any) => m.id === p.mission_id).status = "ACTIVE"; return { ok: true };
    case "missions.set_status": S.missions.find((m: any) => m.id === p.mission_id).status = p.status; return { ok: true };
    case "missions.describe": return { name: "Morning mail summary", objective: p.text, schedule: { type: "cron", cron: "30 7 * * 1-5" }, timezone: "Europe/Berlin", allowed_tools: ["m365.search_mail", "notify.user"], notification_level: "notify", output_format: "markdown", missed_run_policy: "RUN_ONCE", kind: "routine" };
    case "localfiles.list": return ((S as any).lf ??= []).filter((g: any) => g.chat_id === p.chat_id);
    case "localfiles.grant": { const name = String(p.path).split(/[\\/]/).pop() || "file"; const g = { id: id("lf"), chat_id: p.chat_id, name, ext: "." + (name.split(".").pop() ?? ""), kind: /xlsx|csv|tsv/i.test(name) ? "table" : "text", size: 98_000_000, sensitivity: "INTERNAL", created_at: now(), folder: "C:\\Users\\you\\Desktop", indexed: false }; ((S as any).lf ??= []).push(g); emit("localfiles.changed", {}); return g; }
    case "localfiles.folder_info": return { path: p.path, name: String(p.path).split(/[\/]/).pop() || p.path, files: 12, subfolders: 3, subfolder_files: 40, subfolder_names: ["2026", "Archive", "Drafts"], total_mb: 85.2, truncated: false };
    case "localfiles.grant_folder": { const name = String(p.path).split(/[\/]/).pop() || "folder"; const g = { id: id("lf"), chat_id: p.chat_id, name, ext: "", scope: "folder", recursive: !!p.include_subfolders, expired: false, kind: "folder", size: 0, sensitivity: "CONFIDENTIAL", folder: p.path, created_at: now() }; ((S as any).lf ??= []).push(g); emit("localfiles.changed", {}); return g; }
    case "localfiles.reapprove": { const g = ((S as any).lf ?? []).find((x: any) => x.id === p.grant_id); if (g) { g.expired = false; g.recursive = !!p.include_subfolders; } emit("localfiles.changed", {}); return g; }
    case "localfiles.allow_subfolders": { const g = ((S as any).lf ?? []).find((x: any) => x.id === p.grant_id); if (g) g.recursive = true; (S as any).lfreq = []; emit("localfiles.changed", {}); return g; }
    case "localfiles.requests": return ((S as any).lfreq ??= []).filter((r: any) => r.chat_id === p.chat_id);
    case "localfiles.deny_request": (S as any).lfreq = ((S as any).lfreq ?? []).filter((r: any) => r.id !== p.request_id); return { ok: true };
    case "files.release_unscanned": { const f = S.files.find((x: any) => x.id === p.file_id); if (f) { f.status = "READY"; f.status_reason = "NOT antivirus-scanned (no antivirus could scan it)"; } return f; }
    case "files.meta_update": return { title: p.title, doc_type: p.doc_type, summary: p.summary, keywords: p.keywords, personal_data: [], status: "READY", edited: true, model: null, note: "edited by you" };
    case "files.analyse": return { started: true };
    case "files.reread": return { ok: true };
    case "localfiles.revoke": (S as any).lf = ((S as any).lf ?? []).filter((g: any) => g.id !== p.grant_id); emit("localfiles.changed", {}); return { ok: true };
    case "system.usage": {
      const g: any = (S as any).gpu ?? ((S as any).gpu = { h: Array.from({ length: 40 }, () => 3), v: 3 });
      g.v = Math.max(0, Math.min(100, g.v + (Math.random() - 0.45) * 30)); g.h = [...g.h.slice(-59), g.v];
      return { available: true, util: g.v, vram_mb: 6200, history: g.h };
    }
    case "network.logs": {
      const t = (min: number) => new Date(Date.now() - min * 60e3).toISOString();
      const rows = [
        { id: "n1", ts: t(2), component: "llm", method: "POST", scheme: "http", host: "127.0.0.1", port: 11434, path: "/v1/chat/completions", status: 200, outcome: "ok", reason: "", bytes_out: 5100, bytes_in: 2200, duration_ms: 8400, ip: "", loopback: 1, purpose: "model chat (ollama: llama3.1)", tool: "", task_id: "t1" },
        { id: "n2", ts: t(35), component: "web", method: "GET", scheme: "https", host: "en.wikipedia.org", port: 443, path: "/wiki/Reserve_Bank_of_India", status: 200, outcome: "ok", reason: "", bytes_out: 0, bytes_in: 88000, duration_ms: 640, ip: "208.80.154.224", loopback: 0, purpose: "", tool: "web.fetch", task_id: "t2" },
        { id: "n3", ts: t(36), component: "web", method: "GET", scheme: "https", host: "news.example.org", port: 443, path: "/ai/today", status: null, outcome: "blocked", reason: "domain_not_allowed: domain is not on the allowlist (Settings > Web Access)", bytes_out: 0, bytes_in: 0, duration_ms: 3, ip: "", loopback: 0, purpose: "", tool: "web.fetch", task_id: "t2" },
        { id: "n4", ts: t(90), component: "m365", method: "GET", scheme: "https", host: "graph.microsoft.com", port: 443, path: "/v1.0/me/messages", status: 200, outcome: "ok", reason: "", bytes_out: 0, bytes_in: 41000, duration_ms: 420, ip: "", loopback: 0, purpose: "Microsoft Graph", tool: "m365.search_mail", task_id: "t3" },
      ];
      return { rows, total: rows.length, retention_days: 14, summary: { requests: 4, bytes_out: 5100, bytes_in: 131200, blocked: 1, errors: 0, local: 1,
        hosts: [["127.0.0.1", "llm", 1, 1], ["en.wikipedia.org", "web", 1, 0], ["news.example.org", "web", 1, 0], ["graph.microsoft.com", "m365", 1, 0]].map(([h, c, n, l]: any, i) => ({ host: h, requests: n, bytes_out: 0, bytes_in: 0, blocked: h === "news.example.org" ? 1 : 0, last_ts: rows[i].ts, component: c, loopback: l })),
        by_day: [{ day: new Date(Date.now() - 86400e3).toISOString().slice(0, 10), requests: 6, blocked: 0 }, { day: new Date().toISOString().slice(0, 10), requests: 4, blocked: 1 }], by_component: [] } };
    }
    case "emailskills.list": {
      const base = [["inbox_hourly", "Hourly inbox check", "Every hour (08:00-20:00, Mon-Sat): sorts the last hour's mail into approval / deadline / urgent / questions / information.", "Every hour between 08:00 and 20:59 on Mon, Tue, Wed, Thu, Fri, Sat"],
        ["approvals", "Emails waiting for my approval", "Finds mail that asks for your approval, reads it and tells you what exactly is asked.", "Every 2 hours between 08:00 and 18:59 on Mon, Tue, Wed, Thu, Fri, Sat"],
        ["deadlines", "Deadline radar", "Mail whose deadline is overdue or within 3 days.", "At 09:00 and 15:00 on weekdays"],
        ["morning_brief", "Morning briefing", "Overnight mail in numbers and today's meetings.", "At 08:30 on Mon-Sat"],
        ["vip_alert", "VIP sender alert", "Mail from the people you list in the last hour.", "Every hour between 08:00 and 20:59"]];
      const st: any = (S as any).skills ?? ((S as any).skills = {});
      if (p.mark_seen) (S as any).unseen = 0;
      return { skills: base.map(([id, title, description, sched]) => ({ id, title, description, needs_vips: id === "vip_alert", tools: [], enabled: !!st[id], mission_id: st[id] ? "msn_" + id : null, status: st[id] ? "ACTIVE" : "OFF", schedule_text: sched, next_run_at: st[id] ? new Date(Date.now() + 3600e3).toISOString() : null, last_run_at: null,
        last_state: st[id] ? "COMPLETED" : null, last_at: now(), last_error: null, last_nothing: false,
        last_result: st[id] ? "**2 need your approval**\n\n- **Budget revision FY27** - Finance (To) - approval sought for a 4% increase; deadline 3 Oct. Worth reading in full.\n- **Vendor onboarding** - Procurement (CC) - FYI only." : "" })), outlook_installed: true, usable: true, reason: "", connector_enabled: true, vips: [], show_nav: true };
    }
    case "emailskills.set": { ((S as any).skills ??= {})[p.skill] = !!p.enabled; (S as any).unseen = p.enabled ? 1 : 0; return { ok: true }; }
    case "emailskills.run": return { task_id: id("task") };
    case "missions.parse_schedule": return { schedule: { type: "cron", cron: String(p.text) }, text: String(p.text), next_run: new Date(Date.now() + 3600e3).toISOString() };
    case "missions.run_now": return { task_id: "t" };
    case "tasks.list": return S.runs.map((r: any) => ({ id: r.id, objective: r.title, state: r.status, trigger_type: "USER", updated_at: r.started_at, usage: { tool_calls: 1, tokens: 900 }, limits: { tool_calls: 100, tokens: 200000 } }));
    case "files.list": return { files: S.files, storage: { used_bytes: 1234567, quota_bytes: 10737418240, count: S.files.length, quarantined: 0 } };
    case "files.upload": { const f = { id: id("file"), name: p.name ?? String(p.path).split(/[\\/]/).pop(), folder: "/", size_bytes: 2048, status: "READY", sensitivity: 1, source: "upload", created_at: now(), tags_json: [] }; S.files.unshift(f); return f; }
    case "files.preview": { const f: any = S.files.find((x: any) => x.id === p.file_id); return { ...f, meta: f?.meta_full ?? null, text: "Extracted text preview (mock)." }; }
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
    case "web.test_search": return { ok: true, provider: "exa", results: 1, ms: 420 };
    case "llm.inspect": return { kind: /asr|whisper/i.test(p.model_name) ? "stt" : /embed/i.test(p.model_name) ? "embedding" : "chat", vision: /vl/i.test(p.model_name), capabilities: [], detail: "" };
    case "llm.discover": return { models: ["llama3.1:8b", "qwen2.5:14b", "whisper-large-v3", "qwen2.5vl:7b"], details: [{ name: "llama3.1:8b", kind: "chat", vision: false }, { name: "qwen2.5:14b", kind: "chat", vision: false }, { name: "whisper-large-v3", kind: "stt", vision: false }, { name: "qwen2.5vl:7b", kind: "chat", vision: true }] };
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
    case "diagnostics.resources": {
      const j = (b: number) => Math.max(0, Math.min(100, b + (Math.random() - 0.5) * 6));
      const lv = (x: number) => (x >= 90 ? "critical" : x >= 75 ? "warn" : "ok");
      const cpu = j(34), mem = j(92), gpu = j(61), disk = 71;
      return { red_at: 90, orange_at: 75, processes: [{ name: "pa-gateway.exe", pid: 4120, memory_mb: 182 }, { name: "pa-core.exe", pid: 4188, memory_mb: 96 }, { name: "pa-ui.exe", pid: 3904, memory_mb: 141 }],
        meters: [{ label: "CPU", pct: cpu, app_pct: 3.1, detail: "16 logical processors", app_detail: "this app", level: lv(cpu) },
          { label: "Memory", pct: mem, app_pct: 1.3, detail: "29.4 of 31.7 GB used", app_detail: "this app 419 MB", level: lv(mem) },
          { label: "GPU", pct: gpu, app_pct: null, detail: "whole PC · 9120 MB video memory in use", app_detail: "", level: lv(gpu) },
          { label: "Storage", pct: disk, app_pct: 0.2, detail: "268.1 GB free of 931 GB on the data drive", app_detail: "app data 1840 MB", level: lv(disk) }] };
    }
    case "diagnostics.health": return { gateway: { ok: true, uptime_s: 100 }, core: { ok: true }, model: {}, sandbox: { strength: "UNAVAILABLE" }, metrics: { tasks: { COMPLETED: 12, FAILED: 1 }, approvals: { PENDING: 1, APPROVED: 7 } } };
    case "about": return { name: "Personal Agent - Desktop Edition", version: "0.1.1", build: "mock", licences: [] };
    case "updates.status": return { current: "0.1.0", auto_check: true, available: null, note: "mock" };
    case "account.signin_history": return [];
    case "voice.transcribe": return { text: "remind me to call the dentist tomorrow at 9" };
    case "ui.open_link": return { screen: "home" };
    default: return {};
  }
}
