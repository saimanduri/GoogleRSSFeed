# Development

## Setup (Windows 11)
```powershell
winget install Python.Python.3.12 OpenJS.NodeJS.LTS Rustlang.Rustup Git.Git
rustup default stable-msvc      # needs "Desktop development with C++" (VS Build Tools) for the linker
cd personal-agent
py -3.12 -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
python scripts\check_env.py
python -m pytest -q
cd app\ui; npm install
```
Linux/macOS can run the Python tests (Windows-only tests skip) and the UI in the browser preview, which is
how most of v0.1.0 was developed; the product itself targets Windows 11 only.

## Running in developer mode
```powershell
$env:PA_DEV_MODE = "1"                                   # allows software TPM stand-in + mock model
$env:PA_DATA_DIR = "$env:LOCALAPPDATA\PersonalAgent-dev" # keep dev data separate
cd app; python -m pa_gateway                             # gateway + pa-core (as a python.exe child)
# second terminal
cd app\ui; $env:PA_DATA_DIR = "$env:LOCALAPPDATA\PersonalAgent-dev"; npx tauri dev
```
Useful variables (ignored in release builds): `PA_TEST_FAST_KDF=1` (tiny Argon2 cost - tests only),
`PA_CORE_INPROCESS=1` (run pa-core as threads in the gateway), `PA_SANDBOX_FORCE=STRONG|STANDARD|UNAVAILABLE`,
`PA_SANDBOX_PYTHON`, `PA_LLAMA_SERVER`, `PA_DEBUG=1` (print handler tracebacks), `PA_GATEWAY_CMD` (Tauri dev
starts the gateway with this command if it is not running).

Dev-only switch `PA_FORCE_SOFTWARE_PROTECTOR=1` (dev mode only): never touch the real TPM. The test suite sets it
(wrong-PIN tests would otherwise exhaust your TPM's per-user authorization budget - Windows then blocks all TPM
key creation for hours, System event 23 / error 0x80290409).

Browser-only UI work: `cd app/ui; npm run dev` → http://127.0.0.1:5173 uses `src/api/mock.ts`.

## Recipes
**Add a setting** - `pa_gateway/settings_schema.py`: add `S(key, group, label, type, default, loosen, min, max,
options, help, risk)`. Pick `loosen` carefully (which direction weakens security). Floor-bound enums get
`tags=("floor",)`. Read it with `gw.settings.get(key)`. The Settings UI renders it automatically.

**Add a tool** - `policy/tools_registry.py`: pydantic args model + `ToolDef(name, version, connector, side_effect,
risk, output_source, args, description, …)`. Implement `fn(args, ctx) -> ToolResult` (return the correct
`sensitivity` and `source`), register with `gw.tools.register(name, fn)` (built-ins in `tools/builtin.py`).
Add policy tests; add a red-team case in `tests/redteam/corpus.json` if it has side effects.

**Add a connector** - subclass `connectors.service.ConnectorAdapter` (id, label, manifest, `connection_valid`,
`register_tools`, `disconnect`), register in `Gateway._open`. Network calls must use `EgressClient` with an
explicit allowlist. Add its domains to `CONNECTOR_DOMAINS` so web.fetch cannot bypass it when it is off.

**Add an RPC method** - `ipc/api_ui.py` (or `api_core.py` - think twice): `@rpc("ns.name", roles=(UI,),
state="unlocked", stepup=None)`; validate every param with `P`. Add it to `src/api/mock.ts` if the UI uses it.

**Add a DB table/column** - append `(2, """...""")` to `MIGRATIONS` in `db/schema.py` (statements end with
`;\n`), update docs/DATABASE.md and VERSION_HISTORY.md.

## Portable build and the laptop smoke test
```powershell
cd app\ui; npx tauri build --no-bundle; cd ..\..          # pa-ui.exe
.\scripts\build-portable.ps1                               # dist\PersonalAgent-Portable (downloads + verifies embeddable Python)
python scripts\laptop_smoke.py                              # throwaway dev vault, REAL TPM + Ollama, ~20 checks
python scripts\laptop_smoke.py --software                   # same without touching the TPM
python scripts\laptop_smoke.py --wrong-pin                  # adds ONE wrong PIN - do not repeat runs (TPM budget)
```
Browser preview with sample data: `cd app\ui; npm run dev` then open `http://127.0.0.1:5173/?demo`
(signed in, sample chats, history). Stop other gateways first (single-instance lock) before running the pipe tests.

**E2E catalogue** - add an RPC: add a step to `tests/e2e/scenarios.json` (id, ui, call, params, expect, audit...) and the
UI control to `tests/e2e/ui_actions.json`, then `python scripts/gen_action_catalog.py` and `python scripts/e2e_runner.py`.
Use `--learn` to see the response keys and the security-log events a step produced (author `expect`/`audit` from it),
`E2E_GATEWAY_LOG=<file>` to keep the gateway's stderr (PA_DEBUG) and `PA_DEBUG_DUMP=<seconds>` (dev mode) to dump all
thread stacks periodically - this is how the pipe accept-loop bug was found.

**Add a theme** - add a `[data-theme="name"]` token block to `app/ui/src/themes.css`, add the name to the
`ui.theme` options in `settings_schema.py`, `ThemePicker.tsx`, `mock.ts` and the `/theme` list in `Chat.tsx`.
**Add a slash command** - append to `SLASH` in `screens/Chat.tsx` and handle it in `runSlash`.

## Style
- ruff (`E,F,W,B`), 110 columns. Type hints. Docstrings cite spec sections.
- UI: function components, hooks, no global state library; CSS tokens in `styles.css`; icons in `Icon.tsx`.
- Never add dependencies that download or execute code at runtime.

**Developer run without the TPM**: `powershell -ExecutionPolicy Bypass -File scripts\run-dev.ps1 [-Fresh]` starts the built pa-ui.exe with a source gateway in developer mode (software PIN protector, data in %LOCALAPPDATA%\PersonalAgent-dev, Defender scan skipped).

## Scripts added in 0.1.5-0.1.7
- `scripts/e2e_runner.py` (187 steps; `--learn`, `--tpm`, `--only`), `scripts/gen_action_catalog.py`, `scripts/laptop_smoke.py`, `scripts/run-dev.ps1`.
- `scripts/live_outlook_check.py [model] [skill ...]` - real Outlook + real Ollama in a throwaway developer gateway; prints counts and timings only (`LIVE_SHOW=n` prints the start of a result, `LIVE_STEPS=1` lists steps, `LIVE_VIPS="A;B"` sets VIP names).
- `scripts/perf_tables.py [rows]` - generates a ~100 MB .xlsx and times indexing and queries (not part of the test suite).
- Test seams: `PA_SKIP_DEFENDER=1` (dev only), conftest isolates tests from the antivirus; `tests/helpers_xlsx.py` writes minimal .xlsx files.
- Browser preview: `cd app/ui; npx vite --port 5199 --strictPort`, open `/?demo`. The built-in preview pane reports `visibilityState = hidden` (no animation frames); override it in the page to test polling UI.
