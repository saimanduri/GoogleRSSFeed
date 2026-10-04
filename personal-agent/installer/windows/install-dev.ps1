<#
  Developer/portable install (no MSI): copies the CI-built bundle to Program Files, creates the firewall
  rules and a per-user logon task. Run from an elevated PowerShell in the unzipped bundle folder.
  The signed MSI (future) performs the same steps.
#>
# The script sits in <bundle>\installer\, so the bundle root is its parent folder.
param([string]$Source = (Split-Path -Parent $PSScriptRoot), [string]$InstallDir = "$env:ProgramFiles\PersonalAgent")
$ErrorActionPreference = "Stop"
if (-not (Test-Path (Join-Path $Source "pa-gateway.exe"))) { throw "pa-gateway.exe not found in $Source - run this script from the bundle folder" }
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
Copy-Item -Recurse -Force (Join-Path $Source "*") $InstallDir
# Program Files is write-protected for standard users (inherited ACL) - binaries cannot be tampered with.
& (Join-Path $InstallDir "installer\firewall-rules.ps1") -InstallDir $InstallDir
$user = "$env:USERDOMAIN\$env:USERNAME"
$action = New-ScheduledTaskAction -Execute (Join-Path $InstallDir "pa-gateway.exe")
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $user
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit 0 -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
Register-ScheduledTask -TaskName "PersonalAgent Gateway" -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null
$uiAction = New-ScheduledTaskAction -Execute (Join-Path $InstallDir "pa-ui.exe") -Argument "--tray"
Register-ScheduledTask -TaskName "PersonalAgent Tray" -Action $uiAction -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null
# Desktop and Start-menu shortcuts with the ChiRAG icon (easy access; the window opens the already running app)
try {
  $ws = New-Object -ComObject WScript.Shell
  $ico = Join-Path $InstallDir "installer\ChiRAG.ico"
  foreach ($dir in @([Environment]::GetFolderPath("Desktop"), (Join-Path ([Environment]::GetFolderPath("Programs")) ""))) {
    if (-not $dir -or -not (Test-Path $dir)) { continue }
    $lnk = $ws.CreateShortcut((Join-Path $dir "ChiRAG Agent.lnk"))
    $lnk.TargetPath = Join-Path $InstallDir "pa-ui.exe"
    $lnk.WorkingDirectory = $InstallDir
    $lnk.Description = "ChiRAG Agent - private local-first AI agent"
    if (Test-Path $ico) { $lnk.IconLocation = "$ico,0" }
    $lnk.Save()
  }
} catch { Write-Warning "Could not create the shortcuts: $_" }
Write-Output "Installed to $InstallDir. Sign out and in again, or start pa-gateway.exe and pa-ui.exe now."
