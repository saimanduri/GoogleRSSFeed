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

## Portable (copy-and-run, no admin) - 0.1.3
```powershell
cd app\ui; npx tauri build --no-bundle; cd ..\..
.\scripts\build-portable.ps1 -OutDir dist\PersonalAgent-Portable   # downloads + verifies python.org embeddable Python
```
Copy the folder anywhere and double-click `pa-ui.exe` (it starts the gateway itself). The bundled `python.exe` is signed
by the Python Software Foundation, so Windows Smart App Control accepts it; the PyInstaller `pa-gateway.exe` etc. are
unsigned and ARE blocked by Smart App Control (observed on Windows 11 Home). Release-strict flags apply (no mock model,
TPM required). Limitation: no firewall rules without admin; Posture shows a High finding. Third-party `.pyd` wheels are
unsigned too and can be blocked on some SAC machines (reputation based) - signing everything remains PENDING_WORK 2.

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

## Installing a new bundle over a running install (learned 2026-10-01)
Latest bundle (2026-10-02, includes pa-ui.exe): `dist\PersonalAgent-0.1.10` - build it into a NEW folder (`scriptsuild.ps1 -OutDir dist\PersonalAgent-0.1.10`) so a folder you are installing from is never overwritten.
The logon tasks restart a killed gateway/tray after one minute and Windows keeps DLLs locked briefly, so a plain `Stop-Process` can leave `libcrypto-3.dll` in use (`Copy-Item` fails). Do, in an administrator PowerShell:
`Stop-ScheduledTask` + `Disable-ScheduledTask` for "PersonalAgent Gateway" and "PersonalAgent Tray"; kill `pa-ui pa-gateway pa-core pa-outlook-worker pa-parser`; wait 5 s and check none is left;
`.\installer\install-dev.ps1` from `dist\PersonalAgent` (it re-registers both tasks enabled); `Start-ScheduledTask "PersonalAgent Gateway"`; start `pa-ui.exe`.
Build order: `npx tauri build --no-bundle` (needs Smart App Control off), `scripts\build.ps1 -OutDir dist\PersonalAgent` (copy `pa-ui.exe` into it), `scripts\build-portable.ps1`.
Hidden imports for the PyInstaller spec now include `win32pdh` and `pypdf`.
