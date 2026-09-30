<#
  Windows Defender Firewall per-program rules (spec 2.1, 14.2).
  ONLY pa-gateway.exe may connect outbound. Every other component is blocked.
  Run elevated (the MSI runs it once at install; Settings > Security Posture > Repair re-runs it).
#>
param([string]$InstallDir = "$env:ProgramFiles\PersonalAgent")
$ErrorActionPreference = "Stop"
$blocked = @("pa-core.exe", "pa-parser.exe", "pa-outlook-worker.exe", "llama-server.exe", "pa-sandbox-runner.exe",
             "sandbox-python\python.exe", "sandbox-python\pythonw.exe")
foreach ($exe in $blocked) {
    $path = Join-Path $InstallDir $exe
    $name = "PersonalAgent-Block-" + (Split-Path $exe -Leaf)
    if ($exe -like "sandbox-python*") { $name = "PersonalAgent-Block-sandbox-" + (Split-Path $exe -Leaf) }
    Get-NetFirewallRule -DisplayName $name -ErrorAction SilentlyContinue | Remove-NetFirewallRule
    New-NetFirewallRule -DisplayName $name -Direction Outbound -Program $path -Action Block -Profile Any `
        -Description "Personal Agent: this component must never reach the network" | Out-Null
    Write-Output "blocked outbound: $path"
}
