"""Security Posture page (spec 39.10, 5.3 "Security checks", 14.2).

Each check: {id, title, status: ok|warn|high|info|unknown, detail, fix (optional action id)}.
Runs at start, daily, after settings changes and on demand; results are logged; High findings go to Home.
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
from datetime import timedelta
from typing import Any

from pa_common.devmode import dev_mode
from pa_common.paths import looks_cloud_synced
from pa_common.timeutil import now_iso, parse_iso, utcnow

from .vault.protector import tpm_status

FIREWALL_RULE_PREFIX = "PersonalAgent-Block-"
BLOCKED_PROGRAMS = ("pa-core.exe", "pa-parser.exe", "pa-outlook-worker.exe", "llama-server.exe", "pa-sandbox-runner.exe")
NOWIN = getattr(subprocess, "CREATE_NO_WINDOW", 0)


def _ps(cmd: str, timeout: int = 20) -> str:
    try:
        r = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", cmd], capture_output=True,
                           timeout=timeout, creationflags=NOWIN)
        return r.stdout.decode(errors="ignore").strip()
    except (OSError, subprocess.TimeoutExpired):
        return ""


def bitlocker_status() -> str:
    if sys.platform != "win32":
        return "unknown"
    # Works without admin rights (shell property); 1 = on, 2 = off, 3 = suspended?, 5 = decrypting...
    v = _ps("(New-Object -ComObject Shell.Application).NameSpace($env:SystemDrive).Self.ExtendedProperty('System.Volume.BitLockerProtection')")
    return {"1": "on", "3": "on", "5": "on", "2": "off", "0": "off"}.get(v, "unknown")


def defender_status() -> dict[str, Any]:
    if sys.platform != "win32":
        return {"available": False}
    out = _ps("Get-MpComputerStatus | Select-Object AMServiceEnabled,RealTimeProtectionEnabled,AntivirusSignatureAge | ConvertTo-Json")
    try:
        return {"available": True, **json.loads(out)}
    except ValueError:
        return {"available": False}


def firewall_rules_present() -> dict[str, bool]:
    if sys.platform != "win32":
        return {}
    out = _ps(f"Get-NetFirewallRule -DisplayName '{FIREWALL_RULE_PREFIX}*' -ErrorAction SilentlyContinue | "
              "Where-Object {{ $_.Enabled -eq 'True' -and $_.Action -eq 'Block' -and $_.Direction -eq 'Outbound' }} | "
              "Select-Object -ExpandProperty DisplayName")
    names = set(out.splitlines())
    return {p: f"{FIREWALL_RULE_PREFIX}{p}" in names for p in BLOCKED_PROGRAMS}


def live_outbound_test() -> dict[str, Any]:
    """Start pa-core.exe in self-test mode: it tries to connect out and must FAIL (packaged builds only)."""
    if not getattr(sys, "frozen", False):
        return {"tested": False, "reason": "developer mode: all components run as python.exe, per-program firewall rules cannot apply"}
    exe = os.path.join(os.path.dirname(sys.executable), "pa-core.exe")
    try:
        r = subprocess.run([exe, "--selftest-network"], capture_output=True, timeout=30, creationflags=NOWIN)
        return {"tested": True, "blocked": r.returncode == 0, "detail": r.stdout.decode(errors="ignore")[:200]}
    except (OSError, subprocess.TimeoutExpired) as e:
        return {"tested": False, "reason": str(e)}


def data_acl_ok(path: str) -> dict[str, Any]:
    if sys.platform != "win32":
        return {"ok": None, "detail": "not Windows"}
    try:
        r = subprocess.run(["icacls", path], capture_output=True, timeout=15, creationflags=NOWIN)
    except (OSError, subprocess.TimeoutExpired):
        return {"ok": None, "detail": "icacls failed"}
    out = r.stdout.decode(errors="ignore")
    bad = [line.strip() for line in out.splitlines()[1:] if any(x in line for x in ("Everyone", "Users:", "Authenticated Users", "BUILTIN\\Users"))]
    inherited = "(I)" in out
    return {"ok": not bad and not inherited, "detail": "; ".join(bad) or ("inherits permissions" if inherited else "user + SYSTEM only")}


def run_posture(gw) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []

    def add(cid: str, title: str, status: str, detail: str, fix: str | None = None) -> None:
        checks.append({"id": cid, "title": title, "status": status, "detail": detail, "fix": fix})

    if dev_mode():
        add("dev_mode", "Developer mode", "high", "Developer mode is ON: software key protector, unsigned components and the "
            "mock model may be used. Never use developer mode with real data.")
    tpm = tpm_status()
    prot = gw.vault.header.data.get("reset", {}).get("protector", {}).get("kind") if gw.vault.header.data else None
    add("tpm", "TPM 2.0", "ok" if tpm["usable"] and prot == "tpm" else "high",
        f"{tpm['detail']}; PIN protector: {prot or 'none'}" + (" (software stand-in - PIN can be brute-forced offline)" if prot == "software" else ""))
    bl = bitlocker_status()
    add("bitlocker", "BitLocker drive encryption", {"on": "ok", "off": "warn"}.get(bl, "unknown"),
        {"on": "System drive is encrypted.", "off": "BitLocker is off: anyone with the disk can read Windows' files. Turn it on in "
         "Settings > Privacy & security > Device encryption / BitLocker."}.get(bl, "Could not determine BitLocker status."))
    dfd = defender_status()
    add("defender", "Microsoft Defender", "ok" if dfd.get("RealTimeProtectionEnabled") else ("warn" if dfd.get("available") else "unknown"),
        "Real-time protection on." if dfd.get("RealTimeProtectionEnabled") else "Real-time protection is off or unknown.")
    fw = firewall_rules_present()
    if fw:
        missing = [k for k, v in fw.items() if not v]
        add("firewall", "Firewall rules (only the gateway may go online)", "ok" if not missing else "high",
            "All outbound-block rules present." if not missing else f"Missing rules for: {', '.join(missing)}", "repair_firewall" if missing else None)
    else:
        add("firewall", "Firewall rules", "unknown", "Not checked on this platform.")
    live = live_outbound_test()
    add("firewall_live", "Live outbound test from blocked components",
        "ok" if live.get("blocked") else ("warn" if not live.get("tested") else "high"),
        live.get("detail") or live.get("reason", ""))
    root = str(gw.paths.root)
    add("data_folder_sync", "Data folder not in a cloud-synced folder", "high" if looks_cloud_synced(gw.paths.root) else "ok", root)
    acl = data_acl_ok(root)
    add("data_acl", "Data folder permissions", "ok" if acl["ok"] else ("unknown" if acl["ok"] is None else "high"), acl["detail"],
        "repair_acl" if acl["ok"] is False else None)
    rt = gw.runtime
    add("llm_listener", "Local model listener", "ok",
        "Built-in runtime: 127.0.0.1 only, random port and 256-bit key per launch; only the gateway holds the key." if rt.available()
        else "Built-in runtime not installed.")
    for m in gw.llm.models():
        if m["location"] == "loopback":
            exp = gw.llm.exposure_check(m)
            add(f"runtime_{m['id']}", f"External runtime: {m['name']}", "high" if exp.get("at_risk") else "warn",
                "Reachable from the network or answers browser requests - AT RISK." if exp.get("at_risk")
                else "Reduced isolation: any local program can call it (no per-launch key).")
        elif m["location"] == "remote":
            add(f"runtime_{m['id']}", f"Remote model: {m['name']}", "warn", "Chat content is sent to this server over the network.")
    sb = gw.sandbox.describe()
    add("sandbox", "Python sandbox", {"STRONG": "ok", "STANDARD": "warn", "UNAVAILABLE": "info"}[sb["strength"]], sb["label"] + ". " + sb["help"])
    loose = gw.settings.looser_than_defaults()
    add("settings", "Settings looser than 'Cautious' defaults", "ok" if not loose else "warn",
        "None." if not loose else ", ".join(f"{x['label']}: {x['value']}" for x in loose[:10]))
    tainted = gw.db.scalar("SELECT count(*) FROM skills WHERE tainted=1 AND status='ACTIVE'") or 0
    add("skills", "Skills with tainted origin", "ok" if not tainted else "warn", f"{tainted} active skill(s) with tainted origin.")
    health = gw.secrets.health()
    n = len(health["weak"]) + len(health["reused"]) + len(health["old"])
    add("secrets", "Secrets health", "ok" if not n else "warn",
        f"weak: {len(health['weak'])}, reused: {len(health['reused'])}, older than a year: {len(health['old'])}")
    fast = gw.db.scalar("SELECT count(*) FROM approvals WHERE decision_ms IS NOT NULL AND decision_ms < 2000 AND status IN ('APPROVED','USED')") or 0
    add("approvals", "Quick approvals pattern", "ok" if fast < 3 else "warn", f"{fast} approvals accepted in under 2 seconds.")
    ver = gw.last_log_verify or {}
    add("log_chain", "Security log integrity", "ok" if ver.get("ok") else "high", json.dumps(ver)[:300] if ver else "Not verified yet.", "verify_log")
    last = gw.db.scalar("SELECT value FROM meta WHERE key='backup.last'")
    if not last:
        add("backup", "Backups", "warn", "No backup yet (Settings > Backup & Restore).")
    else:
        age = utcnow() - parse_iso(last)
        add("backup", "Backups", "ok" if age < timedelta(days=14) else "warn", f"Last backup: {last}")
    untested = [m["name"] for m in gw.llm.models() if not m["tested"]]
    add("models", "Models tested", "ok" if not untested else "info", "All tested." if not untested else f"Not tested: {', '.join(untested)}")
    pin_state = gw.auth_state.data
    if pin_state.get("pin_disabled"):
        add("pin", "PIN quick unlock", "warn", "PIN disabled after repeated failures until your next password sign-in.")
    try:
        socket.getaddrinfo("localhost", None)
    except OSError:
        pass
    gw.audit.write("posture.checked", "posture", high=sum(1 for c in checks if c["status"] == "high"),
                   warn=sum(1 for c in checks if c["status"] == "warn"))
    return [{**c, "checked_at": now_iso()} for c in checks]
