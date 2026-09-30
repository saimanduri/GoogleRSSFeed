# Personal Agent - Desktop Edition (Windows 11)

A private, local-first AI agent for **one person on one Windows 11 PC**, built to
[`docs/spec/personal_desktop_agent_spec_v1_1.txt`](docs/spec/personal_desktop_agent_spec_v1_1.txt).

The model **proposes**; the gateway **decides and executes**; Windows **enforces the walls**; the
security log **remembers**; **you** have the final say, one STOP button away.

| What you get | Where |
|---|---|
| One desktop window (Tauri + WebView2): Home, Chat, Missions & Routines, Reminders, Tasks, Approvals, My Files, Memory, Secrets, Activity, **Settings** | `app/ui` |
| Security core `pa-gateway` (vault, auth, policy, tool gateway, DLP, egress filter, approvals, budgets, kill switch, unified log) | `app/pa_gateway` |
| Agent runtime `pa-core` (no keys, no network) | `app/pa_core` |
| Worker processes: file parser, Outlook COM worker, sandbox launcher | `app/pa_workers` |
| Tests: unit, integration, Windows-only, red-team gate | `tests/` |
| Installer scripts (firewall rules, logon tasks) | `installer/windows` |
| Windows CI (tests + UI build + bundle) | `.github/workflows/windows-ci.yml` |

Screenshots (browser preview with the built-in mock gateway): [`docs/screenshots/`](docs/screenshots).

## Quick start on your Windows laptop

### A. Easiest: download the bundle built by GitHub Actions
1. On GitHub open **Actions -> personal-agent-windows -> latest green run -> Artifacts ->
   `PersonalAgent-windows-bundle`** and download it.
2. Unzip, open **PowerShell as Administrator** in the folder and run
   `Set-ExecutionPolicy -Scope Process Bypass; .\installer\install-dev.ps1`
   (installs to `C:\Program Files\PersonalAgent`, adds the firewall rules and the logon tasks).
3. Start `pa-gateway.exe`, then `pa-ui.exe`. The first-run wizard guides you.
4. Connect a model: Settings > AI Model (Ollama on this PC is the quickest: `ollama pull llama3.1:8b`).

### B. From source (for development / code changes)
Prerequisites: Windows 11, **Python 3.12**, **Node 20+**, **Rust (stable, MSVC)**, Git.

```powershell
cd personal-agent
py -3.12 -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
python scripts\check_env.py                 # native library smoke test
python -m pytest -q                         # all tests (Windows-only tests run on Windows)
cd app; python -m pa_gateway --selftest     # end-to-end smoke test with the mock model
```

Run the app in developer mode (software TPM stand-in allowed, mock model available - **never use real data**):
```powershell
$env:PA_DEV_MODE = "1"; $env:PA_DATA_DIR = "$env:LOCALAPPDATA\PersonalAgent-dev"
cd app; python -m pa_gateway                # terminal 1: the gateway (named pipe server)
cd app\ui; npm install; $env:PA_DATA_DIR = "$env:LOCALAPPDATA\PersonalAgent-dev"; npx tauri dev   # terminal 2
```
UI-only work on any OS: `cd app/ui && npm install && npm run dev` then open http://127.0.0.1:5173
(uses a mock gateway; nothing is real).

## Documentation map
| File | Purpose |
|---|---|
| [CLAUDE.md](CLAUDE.md) | Instructions for AI coding assistants (and humans) changing this code |
| [PROGRESS.md](PROGRESS.md) | What is implemented, per spec section, with status |
| [PENDING_WORK.md](PENDING_WORK.md) | What is not done yet / known gaps, prioritised |
| [TESTS.md](TESTS.md) | Automated tests, what CI covers, and the **laptop test checklist** |
| [VERSION_HISTORY.md](VERSION_HISTORY.md) | Changelog |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Processes, trust boundaries, data flow, module map |
| [docs/SECRETS_HANDLING.md](docs/SECRETS_HANDLING.md) | Key hierarchy, vault, TPM/PIN, recovery, memory hygiene |
| [docs/DATABASE.md](docs/DATABASE.md) | Every table and column, migrations, retention |
| [docs/IPC_PROTOCOL.md](docs/IPC_PROTOCOL.md) | Named-pipe protocol, roles, method allowlists |
| [docs/LOG_SCHEMA.md](docs/LOG_SCHEMA.md) | Security log format, integrity chain, SIEM |
| [docs/THREAT_MODEL.md](docs/THREAT_MODEL.md) | Assets, attackers, mitigations, honest limits |
| [docs/USER_GUIDE.md](docs/USER_GUIDE.md) | "How your data is protected" + everyday use |
| [docs/RECOVERY_GUIDE.md](docs/RECOVERY_GUIDE.md) | Forgot password / PIN / recovery key, restore |
| [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) | Dev setup, conventions, how to add a tool/connector/setting |
| [docs/BUILD_AND_RELEASE.md](docs/BUILD_AND_RELEASE.md) | Bundling, signing, installer, CI |
| [docs/CONNECTORS.md](docs/CONNECTORS.md) | Outlook, Microsoft 365, Web; adding new connectors |
| [docs/DEVIATIONS.md](docs/DEVIATIONS.md) | Where and why the implementation differs from the spec |
| [docs/SPEC_TRACEABILITY.md](docs/SPEC_TRACEABILITY.md) | Spec section -> code -> test map |
