param([string]$InstallDir = "$env:ProgramFiles\PersonalAgent", [switch]$KeepData)
$ErrorActionPreference = "Continue"
Unregister-ScheduledTask -TaskName "PersonalAgent Gateway" -Confirm:$false
Unregister-ScheduledTask -TaskName "PersonalAgent Tray" -Confirm:$false
Get-NetFirewallRule -DisplayName "PersonalAgent-Block-*" | Remove-NetFirewallRule
Stop-Process -Name pa-ui, pa-gateway, pa-core, llama-server -Force -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force $InstallDir
if (-not $KeepData) { Write-Output "Your encrypted data remains in $env:LOCALAPPDATA\PersonalAgent - delete it from the app (Privacy & Data) or manually." }

foreach ($d in @([Environment]::GetFolderPath("Desktop"), [Environment]::GetFolderPath("Programs"))) { Remove-Item -Force (Join-Path $d "ChiRAG Agent.lnk") -ErrorAction SilentlyContinue }
