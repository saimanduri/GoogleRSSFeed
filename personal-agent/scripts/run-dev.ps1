<#
  Developer test run WITHOUT the TPM and WITHOUT your real data.
  - gateway runs from source in developer mode (software PIN protector, mock model available, fast key derivation)
  - data lives in %LOCALAPPDATA%\PersonalAgent-dev (separate from the real %LOCALAPPDATA%\PersonalAgent)
  - the same pa-ui.exe window is used (a "DEV MODE" badge is shown). Never enter real secrets here.
  Usage:  powershell -ExecutionPolicy Bypass -File scripts\run-dev.ps1 [-Fresh]
  -Fresh deletes ONLY the dev data folder first (asks for confirmation) so the first-run wizard shows again.
#>
param([switch]$Fresh)
$root = Split-Path -Parent $PSScriptRoot
$py = Join-Path $root ".venv\Scripts\python.exe"
$ui = Join-Path $root "app\ui\src-tauri\target\release\pa-ui.exe"
if (-not (Test-Path $py)) { throw "venv not found: $py" }
if (-not (Test-Path $ui)) { throw "pa-ui.exe not built: cd app\ui; npx tauri build --no-bundle" }
$data = Join-Path $env:LOCALAPPDATA "PersonalAgent-dev"
if ($Fresh -and (Test-Path $data)) {
  $a = Read-Host "Delete the DEV data folder $data ? (y/N)"
  if ($a -eq "y") { Remove-Item -Recurse -Force $data }
}
Get-Process pa-ui -ErrorAction SilentlyContinue | Stop-Process -Force
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like "*pa_gateway*" } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
$env:PA_DEV_MODE = "1"
$env:PA_FORCE_SOFTWARE_PROTECTOR = "1"
$env:PA_TEST_FAST_KDF = "1"
$env:PA_SKIP_DEFENDER = "1"
$env:PA_DATA_DIR = $data
$env:PYTHONPATH = Join-Path $root "app"
$env:PA_GATEWAY_CMD = "$py -m pa_gateway --data-dir $data"
Start-Process $ui
Write-Host "Started in DEVELOPER mode. Data: $data"
