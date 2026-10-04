<#
  Build the Windows bundle (run on Windows, from the personal-agent folder, in a Python 3.12 venv with
  requirements-dev.txt installed). CI runs this in the 'bundle' job.

    .\scripts\build.ps1 -OutDir dist\PersonalAgent            # release-mode, unsigned bundle (default)
    .\scripts\build.ps1 -OutDir dist\PersonalAgent -Dev       # developer bundle (mock model, software TPM allowed)
    .\scripts\build.ps1 -OutDir dist\PersonalAgent -Signed    # after code signing: IPC requires Authenticode

  pa-ui.exe is built separately with `npx tauri build --no-bundle` in app\ui (or downloaded from CI) and copied
  into the same folder.
#>
param([string]$OutDir = "dist\PersonalAgent", [switch]$Dev, [switch]$Signed)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$hash = (git rev-parse --short HEAD 2>$null); if (-not $hash) { $hash = "local" }
# PowerShell names are case-insensitive: never reuse $Dev/$Signed for locals
$releaseFlag = if ($Dev) { "False" } else { "True" }
$signedFlag = if ($Signed) { "True" } else { "False" }
$bi = "app\pa_common\buildinfo.py"
$orig = Get-Content $bi -Raw
try {
  $new = $orig -replace "RELEASE_BUILD = \w+", "RELEASE_BUILD = $releaseFlag" -replace "SIGNED_BUILD = \w+", "SIGNED_BUILD = $signedFlag" -replace 'BUILD_HASH = ".*"', "BUILD_HASH = `"$hash`""
  Set-Content $bi $new -NoNewline
  pyinstaller scripts\pyinstaller\personal-agent.spec --noconfirm --clean --distpath build\pyi-dist --workpath build\pyi-work
  if ($LASTEXITCODE -ne 0) { throw "PyInstaller failed ($LASTEXITCODE)" }
} finally {
  Set-Content $bi $orig -NoNewline   # never leave release flags in the source tree
}
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
Copy-Item -Recurse -Force build\pyi-dist\PersonalAgent\* $OutDir
New-Item -ItemType Directory -Force -Path "$OutDir\installer" | Out-Null
Copy-Item -Force installer\windows\*.ps1 "$OutDir\installer\"
Copy-Item -Force installer\windows\*.ico "$OutDir\installer\" -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path "$OutDir\llm-runtime" | Out-Null
@"
ChiRAG Agent - Windows bundle ($hash, $(if ($Dev) {'DEVELOPER'} else {'release mode, unsigned'}))

1. Copy this folder somewhere, open PowerShell AS ADMINISTRATOR in it and run:
       Set-ExecutionPolicy -Scope Process Bypass; .\installer\install-dev.ps1
   (installs to C:\Program Files\PersonalAgent, creates firewall rules and logon tasks)
2. Start "pa-gateway.exe", then "pa-ui.exe" (or sign out and in again).
3. Optional: put llama-server.exe (llama.cpp, Windows build) into this folder to use the built-in runtime,
   or connect Ollama / vLLM / LM Studio in Settings > AI Model.
See TESTS.md in the source zip for the laptop test checklist.
"@ | Set-Content "$OutDir\README-FIRST.txt"
Write-Output "Bundle ready: $OutDir"
