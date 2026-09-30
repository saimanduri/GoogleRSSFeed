# Build and release

## CI (GitHub Actions, `windows-latest`)
Workflow: `.github/workflows/windows-ci.yml` (inside this folder when it is its own repo) or
`.github/workflows/personal-agent-windows.yml` in the parent repo.

| Job | Does | Output |
|---|---|---|
| `python` | env check, ruff, pytest (incl. Windows + red-team N=20), selftest | `test-results` (JUnit + `requirements.lock`) |
| `ui` | `npm install`, `npm run build` (tsc + vite), `tauri build --no-bundle` | `pa-ui` (pa-ui.exe) |
| `bundle` | PyInstaller (one folder, 4 executables), copies pa-ui.exe + installer scripts | `PersonalAgent-windows-bundle` |

Download the bundle: GitHub → Actions → the run → *Artifacts*.

## Local build (Windows)
```powershell
cd app\ui; npm install; npx tauri build --no-bundle     # → app\ui\src-tauri\target\release\pa-ui.exe
cd ..\..; .\scripts\build.ps1 -OutDir dist\PersonalAgent  # PyInstaller → dist\PersonalAgent
Copy-Item app\ui\src-tauri\target\release\pa-ui.exe dist\PersonalAgent\
```
`build.ps1` flags: default = release mode (no dev shortcuts), unsigned; `-Dev` = developer bundle;
`-Signed` = require Authenticode on IPC clients (only after signing!). The script restores
`pa_common/buildinfo.py` afterwards so release flags never stay in the source tree.

Bundle layout (all executables in ONE folder - firewall rules and IPC client checks use exact paths):
```
PersonalAgent\
  pa-ui.exe  pa-gateway.exe  pa-core.exe  pa-parser.exe  pa-outlook-worker.exe  _internal\
  installer\ (firewall-rules.ps1, install-dev.ps1, uninstall.ps1)   llm-runtime\   README-FIRST.txt
  [optional] llama-server.exe   sandbox-python\python.exe
```

## Install
`installer\install-dev.ps1` (elevated): copies to `C:\Program Files\PersonalAgent` (write-protected for
standard users), creates outbound-block firewall rules for every component except pa-gateway, and registers
two per-user logon tasks (gateway, tray UI). Uninstall: `installer\uninstall.ps1`.

## Release checklist (future, see PENDING_WORK P1)
1. Tag `vX.Y.Z`, update VERSION_HISTORY.md.
2. CI builds with `-Signed` after signing all executables (signtool, timestamped).
3. Tauri WiX MSI with custom actions (firewall rules, logon tasks), signed.
4. SBOM (CycloneDX) + dependency audit attached to the release.
5. Signed update manifest (Ed25519) published; app verifies before offering the update.
