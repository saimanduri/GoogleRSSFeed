# Architecture

## 1. Processes and trust boundaries (spec 2.1)

```
 Windows 11 - your account (standard user; no admin at runtime)
 ┌──────────────────────────────────────────────────────────────────────────────────────┐
 │ pa-ui.exe  (Tauri 2 + WebView2, React UI, tray, STOP ALL hotkey, toasts)              │
 │   holds no long-term secrets · strict CSP · cannot navigate away · no FS/shell APIs   │
 │        │ named pipe \\.\pipe\PersonalAgent-<random>  (user-only DACL, NETWORK denied, │
 │        │ reject remote clients, per-launch UI token, server-PID check, client image)  │
 │        ▼                                                                             │
 │ pa-gateway.exe  (SECURITY CORE - the only process with keys and network)             │
 │   vault · auth/step-up · policy engine · tool gateway · DLP · egress filter ·         │
 │   approvals · budgets · kill switch · unified log (+SIEM) · connectors · files ·      │
 │   LLM access · scheduler · backups · posture · session log / transcripts              │
 │     │ pipe (core token,     │ stdin/stdout        │ stdin/stdout   │ spawns            │
 │     │ core PID check)       ▼                     ▼                ▼                   │
 │  pa-core.exe          pa-parser.exe        pa-outlook-worker   Windows Sandbox /       │
 │  agent loop, context  (Job Object:         (Outlook COM,       AppContainer+Job:       │
 │  from session log,    mem/time limits)     read-only + AI      python.run              │
 │  skills, summaries    one doc per process  drafts)                                     │
 │                                                                                        │
 │  llama-server.exe (built-in runtime): 127.0.0.1, random port, 256-bit key known only    │
 │  to pa-gateway. Firewall: ONLY pa-gateway.exe may connect outbound.                     │
 └──────────────────────────────────────────────────────────────────────────────────────┘
                      │ HTTPS (allowlisted, SSRF-checked, DLP-checked)
                      ▼
         Microsoft Graph · web search API · allowlisted websites · (optional) remote model endpoint
```

Differences from the spec diagram are listed in [DEVIATIONS.md](DEVIATIONS.md). The most visible one:
pa-core calls models **through the gateway** (`llm.complete`) instead of talking to pa-llm directly, so
that "nothing reaches the model unless it was logged first" (39.5) is enforced in one place and pa-core
never needs the runtime's API key.

## 2. The security spine in one request

```
user types in Chat ─► ui: chat.send ─► gateway: store message, session log (TRUSTED), run + task(QUEUED)
pa-core worker ─► work.next ─► task.context (built ONLY from the session log)
             ─► llm.complete ─► gateway verifies every message is logged, logs the full request, calls the model,
                                streams tokens to the UI (llm.delta), logs the response
             ─► model returns {"action":"tool", ...}
             ─► tools.invoke ─► ToolGateway (spec 7.2):
                   kill switch → schema → caller owns task → connector effective state → resource scope →
                   argument constraints → data-flow + policy (default deny) → budget → DLP → approval
                   (payload-hash bound; task WAITING_FOR_APPROVAL; card in chat) → re-check everything →
                   audit "about to execute" (fail closed) → outbox (idempotency) → execute →
                   discard if cancelled/connector off → classify sensitivity → DLP on result → audit result →
                   session log (UNTRUSTED, labelled <data>) → return
             ─► ... loop ... ─► {"action":"final"} ─► task.finish ─► chat message, run finished, notification
```
Every step above is also emitted as a **run step** (`run.step` event) - that is the live "Steps" timeline
in Chat and Activity (last 100 requests; older archived but searchable).

## 3. Module map

| Spec area | Module(s) |
|---|---|
| 4 Identity, keys | `pa_gateway/vault/*`, `auth/*`, `app.py` (setup/sign_in/quick_unlock/forgot_password/step_up) |
| 5 UI / Settings | `app/ui/src/**`, `settings_schema.py`, `settings.py`, `ipc/api_ui.py` |
| 6 Secrets | `secrets_store.py`, `dlp/dlp.py` (keyed fingerprints) |
| 7 Tool gateway, policy | `tools/gateway.py`, `policy/engine.py`, `policy/tools_registry.py` |
| 8-11 Connectors | `connectors/service.py`, `web.py`, `m365.py`, `outlook_local.py`, `pa_workers/outlook` |
| 12 Prompt injection | `tools/injection.py`, `pa_core/prompts.py`, labelling in `tools/gateway.py` |
| 13 Sensitivity | `pa_common/sensitivity.py`, task/run/chat high-water marks |
| 14 Egress + DLP | `egress/http.py`, `dlp/dlp.py` |
| 15 Files | `files/checks.py`, `files/store.py`, `pa_workers/parser` |
| 16 Missions | `agentdata/missions.py`, `agentdata/schedule.py`, scheduler loop in `app.py` |
| 17 Tasks | `agentdata/tasks.py`, outbox in `app.py` + `tools/gateway.py` |
| 18 Budgets | `budgets.py` |
| 19 Sandbox | `sandbox.py`, `pa_workers/sandbox/appcontainer.py` |
| 20 LLM | `llm/service.py`, `llm/runtime.py`, `llm/modeltest.py`, `llm/mock.py` |
| 21 Memory | `agentdata/memory.py` |
| 22 / 39.6 Skills | `agentdata/skills.py` |
| 23 / 39.14 Approvals | `approvals.py`, `ui/src/components/ApprovalCard.tsx` |
| 24 Kill switch | `killswitch.py`, `Gateway._on_killswitch`, tray + hotkey in `src-tauri/src/main.rs` |
| 25 Unified log | `audit/writer.py`, `audit/redact.py`, `audit/siem.py` |
| 26 Notifications | `agentdata/home.py`, toasts in the Tauri shell |
| 27 Backup | `backup.py` |
| 39.1 Listener hardening | `ipc/pipe_server.py`, `llm/runtime.py`, `llm/service.py::exposure_check`, m365 loopback |
| 39.2 No config via links | `api_ui.py::ui_open_link` |
| 39.3 Method allowlists | `ipc/dispatch.py` + `@rpc(roles=...)` |
| 39.4 Extensions | `extensions.py`, connector manifests, skill validator |
| 39.5 Session log | `agentdata/sessionlog.py`, `pa_core/context.py` |
| 39.7 Summaries | `pa_core/context.py`, `api_core.session_append` |
| 39.8 Sub-agents | `tools/builtin.py::subtask`, `TaskService.raise_hwm` |
| 39.10 Posture | `posture.py`, Settings > Account & Security > Security posture |
| 39.12 History search | `agentdata/history.py` (FTS5) |
| 39.13 Memory review | `MemoryService.review/about_me` |
| 39.15 Plain-words missions | `schedule.parse_plain`, `MissionService.describe_to_form` |

## 4. State machines

**Session** (`auth/session.py`): `SETUP_REQUIRED → UNLOCKED ⇄ UI_LOCKED`, `* → SIGNED_OUT` (keys wiped) on
sign-out / Windows sign-out / sleep without BitLocker / "pause missions while locked" / STOP ALL is *not*
a sign-out. Auto-lock after idle is enforced by the gateway (`_housekeeping`), not the UI.

**Task** (`agentdata/tasks.py`): `QUEUED → RUNNING → (WAITING_FOR_APPROVAL ⇄ RUNNING) → COMPLETED | FAILED |
CANCELLED | TIMED_OUT | OUTCOME_UNKNOWN`; `WAITING_FOR_RESOURCE`, `PAUSED` (kill switch hold), `SUSPENDED`.
Terminal states are final; late results are discarded.

**Mission**: `DRAFT → ACTIVE ⇄ PAUSED`, `SUSPENDED` (connector turned off), `COMPLETED` (one-off done),
`CANCELLED`. Only the user activates/widens/resumes.

## 5. Threads (pa-gateway)
- pipe accept loop + one thread per connection; RPCs run on a 48-thread pool
- `scheduler` (every 15 s): approvals expiry, reminders, missions, daily posture/retention, backups
- `housekeeping` (every 5 s): idle auto-lock
- `core-supervisor`: spawns/restarts pa-core with backoff
- `win-session`: WTS lock/logoff + power notifications (Windows)
- per-file pipeline threads; SIEM forwarder; llama-server log watcher

## 6. Data at rest
See [DATABASE.md](DATABASE.md) and [SECRETS_HANDLING.md](SECRETS_HANDLING.md).
`%LOCALAPPDATA%\PersonalAgent\` (ACL: you + SYSTEM): `vault.header`, `db\agent.db` (SQLCipher),
`files\*.bin` (AES-GCM chunks), `logs\agent-security*.jsonl`, `state\auth_state.json`, `run\gateway.json`
(per-launch rendezvous, deleted on exit), `tmp\` (wiped per run), `snapshots\` (pre-update).

## 7. Portable layout (0.1.3, no installer / no admin)
```
PersonalAgent-Portable\
  pa-ui.exe                  Tauri shell; starts the gateway when none is running
  python\python.exe          python.org embeddable 3.12 (PSF-signed), python312._pth = ".", "..\app", Lib\site-packages, import site
  python\Lib\site-packages\ hash-pinned wheels from requirements.lock
  app\pa_common|pa_gateway|pa_core|pa_workers   sources; buildinfo.py has RELEASE_BUILD and PORTABLE_BUILD = True
```
`pa_common/portable.py::portable_root()` is only non-None when PORTABLE_BUILD is set **and** the interpreter is
`<root>\python\python.exe`. The IPC server then accepts the UI only as `<root>\pa-ui.exe` and the core only as
`<root>\python\python.exe` with the exact spawned pid. Firewall enforcement does not apply (no admin); Posture
reports it. Dev runs from a venv start workers from the base interpreter (the venv `python.exe` is a launcher
that would break the exact-pid check).

## 8. UI structure additions (0.1.4)
`themes.css` (tokens per `[data-theme]`), `extras.css` (backdrop, floating buttons, slash menu, meter, history),
`components/Backdrop.tsx` (CSS-only animation), `ThemePicker.tsx`, `UsageMeter.tsx`, `screens/History.tsx`.
Settings `ui.theme`, `ui.background`, `ui.day_starts`, `ui.night_starts`; chats have `pinned` and `folder` (DB v2).

## 9. Added in 0.1.7
- `pa_gateway/netlog.py` - central network log (web egress client callback, Microsoft 365 token/Graph calls, model calls); contextvar carries the tool/task.
- `pa_gateway/sysmon.py` - GPU meter (Windows PDH counters, sampled only while the window polls `system.usage`).
- `pa_gateway/localfiles.py` + `pa_workers/parser/tables.py` - local files read in place: grant per chat, isolated reader, SQLite index, declarative queries (no SQL from the model).
- `pa_gateway/agentdata/email_skills.py` + `pa_workers/outlook/mailops.py|mailscan.py` - Outlook skills and deterministic mail flags; dates are formatted with the Windows short-date pattern.

### Agent loop details (0.1.6-0.1.7)
Chat/mission task -> `pa-core` AgentLoop: (1) mission `prefetch` tool calls run first when present (deterministic data gathering through the normal tool gate), (2) model call with a JSON action schema
(`reasoning_effort: none` for Ollama), (3) lenient parse (`actions.py`; tool call shapes, rejects empty/placeholder answers), (4) tool via the gateway, (5) final answer. Empty model replies raise `model_empty`.
The network log context (`netlog.context(tool, task, run)`) wraps tool execution so every request is attributed.
