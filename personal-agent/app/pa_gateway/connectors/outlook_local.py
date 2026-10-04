"""Local classic Outlook connector (spec 9).

pa-gateway spawns pa-outlook-worker on demand (same user, Job Object, firewall-blocked) and talks to
it over its stdin/stdout only. The worker exposes a FIXED set of read operations (+ optional AI draft)
and never receives prompts or model output. It never suppresses or auto-clicks the Outlook Object
Model Guard; if Outlook shows a security prompt the call waits for the user or times out.
"""
from __future__ import annotations

import base64
import json
import subprocess
import sys
import threading
import time
from typing import Any

from pa_common.sensitivity import Sensitivity

from ..tools.base import ExecContext, ToolFailed, ToolResult, ToolUnavailable
from ..workers import JobLimits, popen_limited, scrubbed_env, worker_command
from .service import ConnectorAdapter

IDLE_EXIT = 60
OP_TIMEOUT = 90


def outlook_classic_installed() -> bool:
    if sys.platform != "win32":
        return False
    try:
        import winreg
        with winreg.OpenKey(winreg.HKEY_CLASSES_ROOT, r"Outlook.Application\CLSID"):
            return True
    except OSError:
        return False


class OutlookLocalConnector(ConnectorAdapter):
    id = "outlook_local"
    label = "Local Outlook (classic)"
    description = "Read and search classic Outlook for Windows on this PC, including PST archives (read-only)."
    manifest = {"destinations": ["none (local only)"], "data_types": ["mail", "calendar"],
                "side_effects": ["read", "AI drafts (optional)"], "secrets": []}

    def __init__(self, gw):
        super().__init__(gw)
        self._proc: subprocess.Popen | None = None
        self._job = None
        self._lock = threading.RLock()
        self._last = 0.0
        threading.Thread(target=self._reaper, daemon=True, name="outlook-reaper").start()

    def connection_valid(self) -> tuple[bool, str]:
        if sys.platform != "win32":
            return False, "Local Outlook requires Windows"
        if not outlook_classic_installed():
            return False, "Classic Outlook is not installed (the new Outlook has no Object Model - use Microsoft 365 instead)"
        return True, ""

    def terminate(self) -> None:
        with self._lock:
            if self._proc and self._proc.poll() is None:
                self._proc.kill()
            self._proc, self._job = None, None

    def _reaper(self) -> None:
        while True:
            time.sleep(10)
            if self._proc and time.time() - self._last > IDLE_EXIT:
                self.terminate()

    def _call(self, op: str, **params: Any) -> dict[str, Any]:
        s = self.gw.settings
        cfg = {"start_when_needed": bool(s.get("outlook.start_when_needed")), "include_pst": bool(s.get("outlook.include_pst")),
               "denylist": s.get("outlook.folder_denylist"), "allowlist": s.get("outlook.folder_allowlist"),
               "max_items": int(s.get("outlook.max_items")), "max_body": int(s.get("outlook.max_body_kb")) * 1024,
               "max_attachment": int(s.get("outlook.max_attachment_mb")) * 1024 * 1024,
               "date_range_days": int(s.get("outlook.date_range_days")), "highest_label": s.get("outlook.highest_label"),
               "drafts": bool(s.get("outlook.enable_drafts"))}
        with self._lock:
            if self._proc is None or self._proc.poll() is not None:
                self._proc, self._job = popen_limited(worker_command("pa_workers.outlook"), cwd=self.gw.paths.tmp_dir,
                                                      env=scrubbed_env(tmp=self.gw.paths.tmp_dir),
                                                      limits=JobLimits(memory_mb=1024, max_processes=4))
            self._last = time.time()
            req = json.dumps({"op": op, "params": params, "config": cfg}) + "\n"
            assert self._proc.stdin and self._proc.stdout
            self._proc.stdin.write(req.encode())
            self._proc.stdin.flush()
            result: dict[str, Any] = {}

            def read():
                line = self._proc.stdout.readline()  # type: ignore[union-attr]
                result["line"] = line

            t = threading.Thread(target=read, daemon=True)
            t.start()
            t.join(OP_TIMEOUT)
            if t.is_alive():
                self.terminate()
                raise ToolUnavailable("Outlook did not respond (a security prompt may be waiting for you in Outlook)")
        try:
            res = json.loads(result.get("line") or b"{}")
        except ValueError as e:
            raise ToolFailed("Outlook worker returned invalid data") from e
        if not res.get("ok"):
            if res.get("unavailable"):
                raise ToolUnavailable(res.get("error", "Outlook is not available"))
            raise ToolFailed(res.get("error", "Outlook operation failed"))
        self.gw.connectors.touch("outlook_local")
        return res["result"]

    def _sens(self, label: int | None) -> int:
        base = Sensitivity[self.gw.settings.get("sensitivity.default.mail")]
        if label and label >= 2:  # olPrivate=2, olConfidential=3
            base = Sensitivity.max(base, Sensitivity.CONFIDENTIAL)
        return int(base)

    def register_tools(self, tg) -> None:
        tg.register("outlook_local.list_folders", lambda a, c: self._simple("list_folders", a))
        tg.register("outlook_local.search", lambda a, c: self._simple("search", a))
        tg.register("outlook_local.digest", lambda a, c: self._simple("digest", a))
        tg.register("outlook_local.mail_stats", lambda a, c: self._simple("mail_stats", a))
        tg.register("outlook_local.awaiting_reply", lambda a, c: self._simple("awaiting_reply", a))
        tg.register("outlook_local.get_message", lambda a, c: self._simple("get_message", a))
        tg.register("outlook_local.calendar_read", lambda a, c: self._simple("calendar_read", a))
        tg.register("outlook_local.get_attachment", self.get_attachment)
        tg.register("outlook_local.create_draft", self.create_draft)

    def _simple(self, op: str, args: dict[str, Any]) -> ToolResult:
        r = self._call(op, **args)
        return ToolResult(r.get("text", ""), self._sens(r.get("max_sensitivity")), "outlook_local",
                          {k: v for k, v in r.items() if k in ("count", "entry_id")})

    def get_attachment(self, args: dict[str, Any], ctx: ExecContext) -> ToolResult:
        r = self._call("get_attachment", **args)
        f = self.gw.files.ingest(name=r["name"], data=base64.b64decode(r["data_b64"]), source="outlook_local",
                                 sensitivity=self._sens(r.get("sensitivity")), folder="/Mail attachments", run_async=False)
        return ToolResult(f"Attachment saved to My Files as {f['id']} ({f['name']}); status: {f['status']}",
                          self._sens(r.get("sensitivity")), "outlook_local", {"file_id": f["id"], "status": f["status"]})

    def create_draft(self, args: dict[str, Any], ctx: ExecContext) -> ToolResult:
        if not ctx.approved:
            raise ToolFailed("drafts require approval")
        r = self._call("create_draft", **args)
        return ToolResult(r.get("text", "Draft created."), 0, "outlook_local", {"entry_id": r.get("entry_id")},
                          bytes_out=len(args["body"].encode()))
