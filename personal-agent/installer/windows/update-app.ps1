<#
  ONE command to install or update ChiRAG Agent from a bundle folder (the folder this script sits in, one level up).
  Run it in a PowerShell opened AS ADMINISTRATOR (right-click Start > Terminal (Admin)):

      powershell -ExecutionPolicy Bypass -File "<bundle folder>\installer\update-app.ps1"

  It: stops the app and its two startup tasks, makes sure no program is still running, copies the new files to
  C:\Program Files\PersonalAgent (your data in %LOCALAPPDATA%\PersonalAgent is NOT touched), re-creates the firewall rules, startup tasks and
  Desktop/Start-menu shortcuts, then starts the app. Nothing is deleted except the old program files that are replaced.
#>
param([string]$Source = (Split-Path -Parent $PSScriptRoot))
$ErrorActionPreference = "Stop"

$admin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $admin) { Write-Host "This needs an ADMINISTRATOR PowerShell. Right-click Start > Terminal (Admin), then run the same command again." -ForegroundColor Red; exit 1 }
if (-not (Test-Path (Join-Path $Source "pa-gateway.exe"))) { Write-Host "pa-gateway.exe not found in $Source - run this script from inside the bundle's installer folder." -ForegroundColor Red; exit 1 }

Write-Host "1/5 Stopping the app..." -ForegroundColor Cyan
foreach ($t in "PersonalAgent Gateway", "PersonalAgent Tray") {
  Stop-ScheduledTask -TaskName $t -ErrorAction SilentlyContinue
  Disable-ScheduledTask -TaskName $t -ErrorAction SilentlyContinue | Out-Null
}
$names = "pa-ui", "pa-gateway", "pa-core", "pa-outlook-worker", "pa-parser"
for ($i = 0; $i -lt 6; $i++) {
  Get-Process $names -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
  Start-Sleep -Seconds 2
  if (-not (Get-Process $names -ErrorAction SilentlyContinue)) { break }
}
if (Get-Process $names -ErrorAction SilentlyContinue) { Write-Host "A program is still running. Close it in Task Manager and run this command again." -ForegroundColor Red; exit 1 }

Write-Host "2/5 Installing the new version from $Source ..." -ForegroundColor Cyan
& (Join-Path $Source "installer\install-dev.ps1") -Source $Source

Write-Host "3/5 Starting the app..." -ForegroundColor Cyan
Enable-ScheduledTask -TaskName "PersonalAgent Gateway" -ErrorAction SilentlyContinue | Out-Null
Enable-ScheduledTask -TaskName "PersonalAgent Tray" -ErrorAction SilentlyContinue | Out-Null
Start-ScheduledTask -TaskName "PersonalAgent Gateway"
Start-Sleep -Seconds 6
Write-Host "4/5 Opening the window..." -ForegroundColor Cyan
Start-Process (Join-Path "$env:ProgramFiles\PersonalAgent" "pa-ui.exe")
Write-Host "5/5 Done. ChiRAG Agent is updated. Use the 'ChiRAG Agent' icon on your Desktop from now on." -ForegroundColor Green
