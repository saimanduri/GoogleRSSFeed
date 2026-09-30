<#
  Developer/portable install (no MSI): copies the CI-built bundle to Program Files, creates the firewall
  rules and a per-user logon task. Run from an elevated PowerShell in the unzipped bundle folder.
  The signed MSI (future) performs the same steps.
#>
param([string]$Source = $PSScriptRoot, [string]$InstallDir = "$env:ProgramFiles\PersonalAgent")
$ErrorActionPreference = "Stop"
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
Write-Output "Installed to $InstallDir. Sign out and in again, or start pa-gateway.exe and pa-ui.exe now."
