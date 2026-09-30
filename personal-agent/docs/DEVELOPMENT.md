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

## Style
- ruff (`E,F,W,B`), 110 columns. Type hints. Docstrings cite spec sections.
- UI: function components, hooks, no global state library; CSS tokens in `styles.css`; icons in `Icon.tsx`.
- Never add dependencies that download or execute code at runtime.
