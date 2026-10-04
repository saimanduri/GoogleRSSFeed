<#
  Build the PORTABLE folder: copy it anywhere (no installer, no administrator rights) and double-click pa-ui.exe.
  pa-ui.exe starts the gateway through the bundled python.org embeddable Python (signed by the Python Software
  Foundation), so Windows Smart App Control accepts it without our own code-signing certificate.

    .\scripts\build-portable.ps1 [-OutDir dist\PersonalAgent-Portable]

  Needs: a built pa-ui.exe (cd app\ui; npx tauri build --no-bundle), Python 3.12 + internet for the first build.
  Release-strict: RELEASE_BUILD = True (no mock model, no software TPM), PORTABLE_BUILD = True.
  Limitation: without administrator rights there are no firewall rules - see Security Posture "Portable mode".
#>
param([string]$OutDir = "dist\PersonalAgent-Portable")
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$PyVersion = "3.12.10"
$PyZipUrl = "https://www.python.org/ftp/python/$PyVersion/python-$PyVersion-embed-amd64.zip"
$PyZipSha256 = "4ACBED6DD1C744B0376E3B1CF57CE906F9DC9E95E68824584C8099A63025A3C3"
$uiExe = "app\ui\src-tauri\target\release\pa-ui.exe"
if (-not (Test-Path $uiExe)) { throw "pa-ui.exe not built: cd app\ui; npx tauri build --no-bundle" }
if (-not (Test-Path .venv\Scripts\python.exe)) { throw "create .venv first (py -3.12 -m venv .venv; pip install -r requirements-dev.txt)" }

if (Test-Path $OutDir) { Remove-Item -Recurse -Force $OutDir }
New-Item -ItemType Directory -Force -Path $OutDir, "$OutDir\python", "$OutDir\app" | Out-Null

# 1. embeddable Python: verify pinned hash AND the Python Software Foundation signature
$zip = Join-Path $env:TEMP "python-$PyVersion-embed-amd64.zip"
if (-not (Test-Path $zip)) { Invoke-WebRequest $PyZipUrl -OutFile $zip }
if ((Get-FileHash $zip -Algorithm SHA256).Hash -ne $PyZipSha256) { Remove-Item $zip; throw "embeddable Python hash mismatch" }
Expand-Archive $zip "$OutDir\python" -Force
$sig = Get-AuthenticodeSignature "$OutDir\python\python.exe"
if ($sig.Status -ne "Valid" -or $sig.SignerCertificate.Subject -notmatch "Python Software Foundation") { throw "python.exe is not signed by the PSF" }
@("python312.zip", ".", "..\app", "Lib\site-packages", "import site") | Set-Content "$OutDir\python\python312._pth" -Encoding ascii

# 2. dependencies from the hash-pinned lock file (binary wheels only)
.\.venv\Scripts\python.exe -m pip install --quiet --disable-pip-version-check --require-hashes --only-binary=:all: `
    --target "$OutDir\python\Lib\site-packages" -r requirements.lock
if ($LASTEXITCODE -ne 0) { throw "pip install failed" }

# 3. application code + release/portable flags (the source tree is never modified)
foreach ($pkg in "pa_common", "pa_gateway", "pa_core", "pa_workers") {
    Copy-Item -Recurse -Force "app\$pkg" "$OutDir\app\$pkg"
}
Get-ChildItem "$OutDir\app" -Recurse -Directory -Filter __pycache__ | Remove-Item -Recurse -Force
$bi = "$OutDir\app\pa_common\buildinfo.py"
$hash = (git rev-parse --short HEAD 2>$null); if (-not $hash) { $hash = "local" }
(Get-Content $bi -Raw) -replace "RELEASE_BUILD = \w+", "RELEASE_BUILD = True" -replace "PORTABLE_BUILD = \w+", "PORTABLE_BUILD = True" `
    -replace 'BUILD_HASH = ".*"', "BUILD_HASH = `"$hash`"" | Set-Content $bi -NoNewline

# 4. UI + readme
Copy-Item $uiExe "$OutDir\pa-ui.exe"
@"
ChiRAG Agent - PORTABLE ($hash)

Start: double-click pa-ui.exe. It starts the background gateway itself. No installer, no administrator rights.
Needs: Windows 11 with TPM 2.0 (setup refuses to continue without it), WebView2 (included in Windows 11).
Models: Settings > AI Model > Ollama at http://127.0.0.1:11434 (or any OpenAI-compatible local server).
Your data lives in %LOCALAPPDATA%\PersonalAgent (not in this folder). To move to another PC use Backup / Restore.

Security note: without administrator rights Windows firewall rules cannot be created, so pa-core and the workers
are not blocked from the network by Windows. The Security Posture page shows this as a High finding. Install with
installer\install-dev.ps1 (admin) for full isolation.
Smart App Control: pa-ui.exe is unsigned; if your PC blocks it, see docs/BUILD_AND_RELEASE.md.
"@ | Set-Content "$OutDir\README-FIRST.txt"

# 5. self-check with the bundled interpreter only (no environment variables)
Push-Location $OutDir
$env:PYTHONPATH = ""
& .\python\python.exe -c "import pa_gateway.app, pa_core.agent, pa_common.portable as p; assert p.portable_root(), 'not portable'; print('portable import check OK')"
$rc = $LASTEXITCODE
Pop-Location
if ($rc -ne 0) { throw "portable import check failed" }
Write-Output "Portable folder ready: $OutDir"
