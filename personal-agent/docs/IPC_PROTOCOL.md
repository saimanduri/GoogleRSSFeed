# IPC protocol

## Transport
- Windows named pipe `\\.\pipe\PersonalAgent-<24 random hex>` created by pa-gateway with
  `FILE_FLAG_FIRST_PIPE_INSTANCE` (anti-squatting), `PIPE_REJECT_REMOTE_CLIENTS`, byte mode, overlapped I/O.
- DACL: current user (read/write) + SYSTEM; **NETWORK logon SID denied**; no Everyone / Users entries (tested).
- Rendezvous: `%LOCALAPPDATA%\PersonalAgent\run\gateway.json` = `{pipe, ui_token, pid}` (ACL: user + SYSTEM),
  rewritten every launch, deleted on exit. pa-core never uses it: it receives `{pipe, token, gateway_pid}` on stdin.
- Clients open the pipe with `SECURITY_SQOS_PRESENT | SECURITY_IDENTIFICATION` (no impersonation) and check
  `GetNamedPipeServerProcessId == pid` (refuse a squatter).
- Server checks the client: `GetNamedPipeClientProcessId` → `QueryFullProcessImageNameW`; release builds require
  the exact executable (`pa-ui.exe` / `pa-core.exe`) inside the install folder (+ Authenticode when
  `SIGNED_BUILD`); pa-core must also be the exact PID the gateway spawned. 5 failed handshakes → Home security event.

## Framing (`pa_common/protocol.py`)
`uint32 little-endian length` + UTF-8 JSON; max 16 MiB.

| Message | Shape |
|---|---|
| hello (client → gateway, first frame) | `{"type":"hello","role":"ui"|"core","token":"…","protocol":1}` → `{"type":"res","id":0,"ok":true}` |
| request | `{"type":"req","id":<n>,"method":"ns.name","params":{…}}` |
| response | `{"type":"res","id":<n>,"ok":true,"result":…}` or `{"ok":false,"error":{"code","message","details"}}` |
| event (gateway → ui) | `{"type":"evt","topic":"…","data":{…}}` |

## Roles and method allowlists (spec 39.3) - `pa_gateway/ipc/dispatch.py`
Each method is registered with `@rpc(name, roles=…, state=…, stepup=…)`. A role calling a method not in its
list gets `method_not_allowed` and a `ipc.method_denied` audit event (High for pa-core). There is no
"full access" token.

**core** (`ipc/api_core.py`) - bound to a task the worker leased:
`work.next`, `task.context`, `llm.complete`, `tools.invoke`, `session.append` (only summary/plan/skill.used/note),
`run.step` (thought/plan/info/retry/error), `task.heartbeat`, `task.finish`.
pa-core has **no** settings, secrets, connectors, skills activation, approvals.decide or kill-switch methods (tested).

**ui** (`ipc/api_ui.py`) - everything user-facing, grouped: `session.*`, `setup.*`, `auth.*`, `ui.open_link`,
`account.*`, `posture.*`, `settings.*`, `home.*`, `notifications.*`, `chat.*`, `runs.*`, `tasks.*`,
`approvals.*`, `missions.*`, `reminders.*`, `files.*`, `memory.*`, `secrets.*`, `connectors.*`, `llm.*`,
`voice.transcribe`, `tools.catalog`, `skills.*`, `activity.*`, `logs.*`, `history.search`, `killswitch.*`,
`backup.*`, `privacy.*`, `diagnostics.*`, `about`, `updates.status`.

`state`: `any` (works signed out: status, setup, sign-in), `unlocked` (UI usable), `keys` (vault open even if
the UI is locked - pa-core methods and `killswitch.activate`, which must always work).

Step-up categories (`stepup=`): `secrets`, `export`, `security_settings`, `approvals_high`, `connectors`,
`backup_restore`, `transcripts`, `skills`, `updates`. On `step_up_required` the UI prompts for PIN/password
and retries automatically (`app.tsx::call`). `password_required` prompts for the password and retries with it.

## Event topics (gateway → ui)
`session.changed`, `run.started`, `run.step`, `run.finished`, `llm.delta`, `chat.message`, `tasks.changed`,
`approvals.changed`, `missions.changed`, `reminders.changed`, `reminder.fired`, `files.changed`,
`connectors.changed`, `settings.changed`, `killswitch.changed`, `home.changed`, `notify`, `security.event`,
`skills.changed`. The Tauri shell also emits `gw.status` (connected/disconnected) and `gw.local` (hotkey).

## Workers
pa-parser, pa-outlook-worker and the sandbox do **not** connect to the pipe. They talk only to the gateway
over their own stdin/stdout (one JSON request per line / one response), started with a scrubbed environment
inside a Job Object.

## Additions (0.1.2 - 0.1.4)
- Client identity in **portable** builds: `ui` must be `<root>\pa-ui.exe`; `core` must be `<root>\python\python.exe` and
  the exact pid the gateway spawned. Dev (source) runs from a venv start the core from the base interpreter so the
  exact-pid rule still holds; release (frozen) builds are unchanged.
- `chat.update` accepts `pinned` (bool) and `folder` (text, max 40, no control characters or `<`/`>`).
- `session.status.ui` also carries `ui.background`, `ui.day_starts`, `ui.night_starts`.

Added methods (role `ui`, see docs/ACTION_CATALOG.md for the generated list): `emailskills.list/set/run`, `network.logs`, `system.usage`, `localfiles.grant/list/revoke`.
`task.context` (role core) now also carries `local_files` (id, name, kind, size, label - never paths), `now_local`, and `mission.prefetch`. `llm.complete` accepts `action_schema`.
