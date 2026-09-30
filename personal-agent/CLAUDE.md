# CLAUDE.md - working on Personal Agent

Read this before changing anything. It is written for Claude Code / Codex and for humans.

## What this is
A single-user, local-first AI agent for **Windows 11**, built to the spec in
`docs/spec/personal_desktop_agent_spec_v1_1.txt` (the source of truth; section numbers like "spec 7.2"
in code comments refer to it). Security components are built before the capabilities they protect.

## Non-negotiable rules (spec 35 - never break these)
1. Vetted crypto only (`cryptography`, `argon2-cffi`, SQLCipher, CNG). Never implement primitives.
2. Never log, print, return over IPC to pa-core, or send: secrets, PINs, passwords, recovery keys, tokens.
3. pa-core never gets network access or vault/secret access. Only pa-gateway talks to the network.
4. No shell/command tool for the model. Model-written code runs only in the sandbox (`python.run`).
5. Never bypass Outlook's Object Model Guard.
6. No TCP listener except the model runtime on 127.0.0.1 (random port + key) and the 120-second one-shot
   M365 OAuth loopback callback. The UI talks over a named pipe.
7. Every setting has a GUI control; defaults live in code (`pa_gateway/settings_schema.py`), user values
   in the encrypted DB. The agent has no tool that reads or changes settings.
8. Default deny for tools, connectors, domains. Fail closed when policy, logging, vault or isolation fails.
9. Tests for every security-sensitive change, in the same change.
10. Mock adapters (dev mock model, software TPM) only in developer mode; `RELEASE_BUILD` refuses them.
11. No telemetry leaves the PC.
12. Links/URIs/notifications may only open a screen (`ui.open_link`); never change settings or send tokens.
13. Each IPC role gets only its methods (`@rpc(..., roles=...)`). Never add settings/secrets/approval/
    kill-switch methods to the `core` role.
14. Nothing reaches a model unless it is in the session log first (`SessionLog.verify_messages_logged`).
15. Skills are declarative JSON; never execute code from a skill. No self-modifying skills.
16. Skills, summaries and sub-agents can never widen rights or drop limits.

## Layout
```
app/pa_common/     shared, no secrets: protocol framing, paths, sensitivity, dev-mode flags, pipe client
app/pa_gateway/    SECURITY CORE (the only process with keys + network)
  app.py           Gateway object: wiring, sign-in state machine, scheduler, core supervisor, kill switch effects
  vault/           crypto wrappers, key hierarchy (vault.py), TPM/software PIN protector, recovery key
  auth/            password/PIN rules, brute-force counters, session + step-up
  db/              SQLCipher wrapper + schema (append-only migrations)
  audit/           unified HMAC-chained security log, redaction, SIEM forwarding
  policy/          tool definitions (tools_registry.py) + deterministic policy engine
  tools/           ToolGateway (the 7.2 invocation sequence), built-in tool executors, injection heuristics
  connectors/      framework + web, m365 (Graph), outlook_local (COM worker client)
  dlp/ egress/     data-loss prevention; SSRF-safe egress filter
  files/           quarantine -> validate -> Defender scan -> parse (pa-parser) pipeline
  llm/             model registry/router/providers, llama.cpp runtime manager, mock model, model tests
  agentdata/       runs+steps, session log, tasks, missions/schedules, reminders, memory, skills, history, home
  ipc/             dispatcher (roles, validation), api_ui.py, api_core.py, named-pipe server, local client
  settings*.py     settings schema + tighten/loosen service
  approvals.py budgets.py killswitch.py secrets_store.py sandbox.py backup.py posture.py workers.py
app/pa_core/       agent loop, context builder (from session log only), prompts, JSON action parser
app/pa_workers/    parser (documents), outlook (COM), sandbox/appcontainer (AppContainer launcher)
app/ui/            React + TypeScript (src/) and Tauri 2 Rust shell (src-tauri/)
tests/             unit/, integration/, windows/ (Windows-only), redteam/ (deterministic gate)
installer/windows/ firewall rules, dev install, uninstall
scripts/           build.ps1 (bundle), check_env.py, pyinstaller/
```

## How to run things
- Tests: `python -m pytest -q` (conftest sets `PA_DEV_MODE=1`, fast KDF, in-process core).
  Windows-only tests are marked `@pytest.mark.windows`; hardware-dependent ones `@pytest.mark.laptop`
  (run with `PA_LAPTOP_TESTS=1`). Red-team repetitions: `PA_REDTEAM_N` (CI uses 20).
- End-to-end smoke: `cd app && python -m pa_gateway --selftest`.
- Lint: `ruff check app tests --select E,F,W,B --ignore E501,B905,B007,B904`.
- UI: `cd app/ui && npm install && npm run build` (type-check + bundle); `npm run dev` for a browser
  preview with the mock gateway (`src/api/mock.ts`); `npx tauri dev` for the real shell on Windows.
- CI (GitHub Actions, windows-latest): see `.github/workflows/`. Check it after every push.

## Conventions
- Python 3.12, type hints, small functions, docstrings that cite the spec section.
- Timestamps: UTC ISO strings via `pa_common.timeutil.now_iso()/to_iso()` (lexicographically sortable).
- IDs: `pa_common.ids.new_id(prefix)`.
- Errors surfaced to the UI: raise `PAError(message, code=...)`; codes are stable strings the UI switches on
  (`step_up_required`, `password_required`, `policy_denied`, ...).
- Every security-relevant action writes an audit event (`gw.audit.write(event_type, category, **metadata)`):
  metadata only; forbidden keys are redacted automatically.
- New setting: add an `S(...)` entry in `settings_schema.py` with the correct `loosen` direction; the UI
  renders it automatically. If it can weaken the security floor, tag it `("floor",)`.
- New tool: define it in `policy/tools_registry.py` (schema + side effect + risk), implement the executor,
  register it, add policy tests and a red-team case if it has side effects.
- New connector: subclass `ConnectorAdapter`, declare a manifest, register tools, register in `Gateway._open`.
- DB change: append a new migration in `db/schema.py`; never edit released ones; update docs/DATABASE.md.
- UI: every RPC goes through `useApp().call()` which handles step-up and password prompts. Keep the mock
  (`src/api/mock.ts`) roughly in sync so browser previews keep working.

## Keep these docs current
Update PROGRESS.md, PENDING_WORK.md, VERSION_HISTORY.md and TESTS.md in the same change as the code.
