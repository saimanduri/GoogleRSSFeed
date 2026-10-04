"""The Gateway: owns keys, database, policy and network. Wires every service together and runs the
sign-in state machine (spec 4.5-4.7), background scheduling and the pa-core supervisor.

Thread model: IPC connections and RPC handlers run on threads; every service is thread-safe via the
DB lock and its own locks. Nothing here ever returns a secret value to pa-core.
"""
from __future__ import annotations

import json
import os
import secrets
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable

from pa_common.devmode import dev_mode
from pa_common.winpaths import system32
from pa_common.errors import AuthError, LockedError, PAError
from pa_common.ids import new_id
from pa_common.paths import DataPaths, looks_cloud_synced
from pa_common.timeutil import now_iso

from . import backup as backup_mod
from .agentdata.history import HistoryService
from .agentdata.home import HomeService
from .agentdata.memory import MemoryService
from .agentdata.email_skills import EmailSkills
from .localfiles import LocalFiles
from .vision import VisionReader
from .agentdata.memory_learn import MemoryLearner
from .files.insights import FileInsights
from .agentdata.home_widgets import HomeWidgets
from .netlog import NetLog
from .sysmon import GpuMonitor
from .agentdata.missions import MissionService
from .agentdata.reminders import ReminderService
from .agentdata.runs import RunService
from .agentdata.sessionlog import SessionLog
from .agentdata.skills import SkillService
from .agentdata.tasks import TaskService
from .approvals import ApprovalService
from .audit.siem import SiemForwarder
from .audit.writer import AuditWriter
from .auth import passwords
from .auth.session import SETUP_REQUIRED, SIGNED_OUT, UI_LOCKED, UNLOCKED, SessionManager
from .auth.state import AuthState
from .budgets import BudgetService
from .connectors.m365 import M365Connector
from .connectors.outlook_local import OutlookLocalConnector
from .connectors.service import ConnectorService
from .connectors.web import WebConnector
from .db.database import Database
from .dlp.dlp import DLPEngine
from .files.store import FilesService
from .killswitch import KillSwitch
from .llm.runtime import BuiltinRuntime
from .llm.service import LLMService
from .policy.engine import PolicyEngine
from .posture import run_posture
from .sandbox import SandboxService
from .secrets_store import SecretsService
from .settings import SettingsService
from .tools.builtin import register_builtin_tools
from .tools.gateway import ToolGateway
from .vault import recovery
from .vault.protector import ProtectorError, TpmLockedOut, default_protector, tpm_friendly_message
from .vault.vault import HeaderTampered, UnlockedKeys, Vault, WrongPassword, WrongRecovery

TICK_SECONDS = 15


class Gateway:
    def __init__(self, paths: DataPaths, *, start_core: bool = True, egress_client=None):
        self.paths = paths
        paths.ensure()
        secure_data_folder(paths.root)
        self.restored = backup_mod.apply_staged_restore(paths.root)
        self.audit = AuditWriter(paths.logs_dir, paths.state_dir / "audit.spool")
        self.siem = SiemForwarder()
        self.audit.add_listener(self.siem.on_event)
        self.audit.add_listener(self._on_audit_event)
        self.auth_state = AuthState(paths.auth_state)
        self.vault = Vault(paths.vault_header)
        self.session = SessionManager()
        self.keys: UnlockedKeys | None = None
        self.db: Database | None = None
        self._listeners: list[Callable[[str, dict], None]] = []
        self._start_core = start_core
        self._egress_client = egress_client
        self._threads_stop = threading.Event()
        self._pending_rk: str | None = None
        self.last_log_verify: dict[str, Any] | None = None
        self.core_supervisor: CoreSupervisor | None = None
        self.pipe_server = None
        self.rpc = None
        self.debug = os.environ.get("PA_DEBUG") == "1"
        self._lock = threading.RLock()
        self.started_at = time.time()
        if looks_cloud_synced(paths.root) and not dev_mode():
            raise SystemExit("The data folder is inside a cloud-synced folder. Refusing to start (spec 2.5).")
        if self.vault.header.exists():
            try:
                self.vault.load()
                self.session.set_state(SIGNED_OUT)
            except HeaderTampered:
                self.session.set_state(SIGNED_OUT)
                self.audit.write("vault.header_unreadable", "security", severity="critical")
        else:
            self.session.set_state(SETUP_REQUIRED)
        self.audit.write("gateway.started", "lifecycle", pid=os.getpid(), dev_mode=dev_mode(), restored=self.restored)
        threading.Thread(target=self._housekeeping, daemon=True, name="housekeeping").start()

    # ================================================================== events
    def add_listener(self, fn: Callable[[str, dict], None]) -> None:
        self._listeners.append(fn)

    def emit(self, topic: str, data: dict) -> None:
        for fn in list(self._listeners):
            try:
                fn(topic, data)
            except Exception:  # noqa: BLE001
                pass

    def _on_audit_event(self, ev: dict) -> None:
        if ev.get("severity") in ("high", "critical", "medium"):
            self.emit("security.event", {"event_type": ev.get("event_type"), "severity": ev.get("severity"), "ts": ev.get("timestamp")})

    def home_event(self, kind: str, severity: str, title: str, detail: str = "", ref_id: str | None = None) -> None:
        if self.db is not None:
            self.home.event(kind, severity, title, detail, ref_id)

    def notify(self, kind: str, title: str, body: str | None, sensitivity: int, screen: str | None, ref_id: str | None, **named: Any) -> None:
        if self.db is not None:
            if kind == "reminder":
                sensitivity = 0  # reminders show the user's own words (still hidden on the lock screen)
            self.home.notify(kind, title, body, sensitivity, screen, ref_id, **named)

    # ================================================================== state
    @property
    def state(self) -> str:
        return self.session.state

    def require_unlocked(self) -> None:
        if self.session.state != UNLOCKED or self.db is None:
            raise LockedError("the app is locked")

    def require_keys(self) -> None:
        if self.db is None or self.keys is None:
            raise LockedError("signed out")

    def status(self) -> dict[str, Any]:
        profile = self.vault.profile if self.vault.header.data else {"display_name": "", "assistant_name": "ChiRAG Agent"}
        out: dict[str, Any] = {"state": self.session.state, "username": self.vault.username if self.vault.header.data else "",
                               "display_name": profile["display_name"], "assistant_name": profile["assistant_name"],
                               "dev_mode": dev_mode(), "password_wait": self.auth_state.password_wait_seconds(),
                               "pin_available": self._pin_available(), "recovery_wait": self.auth_state.recovery_wait_seconds()}
        if self.db is not None and self.session.state == UNLOCKED:
            running = self.db.scalar("SELECT count(*) FROM tasks WHERE state IN ('RUNNING','WAITING_FOR_APPROVAL')") or 0
            out.update({"tasks_running": running, "approvals_pending": self.approvals.pending_count(),
                        "proposals_pending": self.db.scalar("SELECT count(*) FROM missions WHERE status='DRAFT' AND proposed_by='agent'") or 0,
                        "outlook_unseen": self.emailskills.unseen(),
                        "killswitch": self.killswitch.state(), "needs_pin_setup": self._needs_pin_setup(),
                        "unread_notifications": self.db.scalar("SELECT count(*) FROM notifications WHERE read=0") or 0,
                        "ui": {k: self.settings.get(k) for k in ("ui.theme", "ui.background", "ui.accent", "ui.show_outlook_nav", "ui.show_gpu_meter", "ui.assistant_icon", "ui.user_icon", "ui.day_starts", "ui.night_starts", "ui.font", "ui.font_size", "memory.auto_learn", "home.widgets", "ui.text_scale", "ui.reduce_motion", "chat.show_steps",
                                                                 "voice.enabled", "security.auto_lock_minutes", "emergency.hotkey")}})
        return out

    def _pin_available(self) -> bool:
        return (self.session.state == UI_LOCKED and not self.auth_state.data.get("pin_disabled")
                and (self.settings.get("security.pin_quick_unlock") if self.db is not None else False)
                and self.session.quick_unlock_allowed(self.settings.get("security.quick_unlock_max_hours") if self.db is not None else 24))

    def _needs_pin_setup(self) -> bool:
        return (self.paths.state_dir / "restored_needs_pin").exists()

    # ================================================================== setup (wizard steps 1-4)
    def preflight(self) -> dict[str, Any]:
        from .posture import bitlocker_status
        from .vault.protector import tpm_status
        tpm = tpm_status()
        usage = shutil.disk_usage(self.paths.root)
        return {"windows": sys.platform == "win32", "platform": sys.platform, "version": _windows_version(),
                "tpm": tpm, "tpm_ok": tpm["usable"] or dev_mode(), "bitlocker": bitlocker_status(),
                "sandbox": _sandbox_probe(),
                "data_folder": str(self.paths.root), "cloud_synced": looks_cloud_synced(self.paths.root),
                "disk_free_gb": round(usage.free / 1e9, 1), "gpu": _gpu_info(), "dev_mode": dev_mode()}

    def setup(self, username: str, password: str, pin: str, allow_letters: bool = False,
              display_name: str = "", assistant_name: str = "") -> dict[str, Any]:
        with self._lock:
            if self.session.state != SETUP_REQUIRED:
                raise PAError("already set up", code="invalid_state")
            errs = passwords.validate_username(username) + passwords.validate_password(password, username, pin) + \
                passwords.validate_pin(pin, allow_letters, password)
            display_name = display_name.strip() or username
            assistant_name = assistant_name.strip() or "ChiRAG Agent"
            errs += passwords.validate_profile_name(display_name, "Your name") + \
                passwords.validate_profile_name(assistant_name, "Assistant name")
            if errs:
                raise PAError("; ".join(errs), code="invalid_request", errors=errs)
            try:
                protector = default_protector()
            except ProtectorError as e:
                raise PAError(str(e), code="tpm_unavailable") from e
            try:
                keys, rk = self.vault.create(username, password, pin, protector, display_name, assistant_name)
            except ProtectorError as e:
                self.audit.write("setup.protector_failed", "authentication", severity="medium", error=type(e).__name__)
                raise PAError(tpm_friendly_message(e), code="tpm_throttled" if "0x80290409" in str(e) else "tpm_unavailable") from e
            self.auth_state.data["rk_confirmed"] = False
            self.auth_state.data["pin_allow_letters"] = allow_letters
            self.auth_state.password_ok()
            self.audit.set_key(keys.key("K_log"))
            self.audit.write("setup.completed", "authentication", protector=protector.kind)
            self._open(keys)
            self._pending_rk = rk
            return {"recovery_key": rk, "confirm_groups": recovery.pick_confirmation_groups(), "protector": protector.kind}

    def confirm_recovery(self, answers: dict[str, str]) -> None:
        self.require_unlocked()
        if not self._pending_rk:
            raise PAError("nothing to confirm", code="invalid_state")
        groups = recovery.groups(self._pending_rk)
        for idx, val in answers.items():
            if recovery.normalize_group(val) != groups[int(idx)]:
                raise PAError(f"group {int(idx) + 1} does not match - check what you wrote down", code="recovery_mismatch")
        self._pending_rk = None
        self.auth_state.data["rk_confirmed"] = True
        self.auth_state.save()
        self.audit.write("recovery_key.confirmed", "authentication")

    # ================================================================== sign-in / lock
    def sign_in(self, username: str, password: str) -> dict[str, Any]:
        with self._lock:
            if self.session.state == SETUP_REQUIRED:
                raise PAError("setup required", code="setup_required")
            wait = self.auth_state.password_wait_seconds()
            if wait > 0:
                raise AuthError(f"too many attempts - wait {int(wait) + 1} s", code="rate_limited", wait=wait)
            try:
                if username.strip().lower() != self.vault.username.lower():
                    raise WrongPassword("wrong username or password")
                keys = self.vault.unlock_with_password(password)
            except (WrongPassword, HeaderTampered) as e:
                self.auth_state.password_failed()
                self.audit.write("auth.signin_failed", "authentication", severity="medium", method="password",
                                 reason="tampered" if isinstance(e, HeaderTampered) else "bad_credentials")
                self._signin_history("password", False)
                raise AuthError(str(e) if isinstance(e, HeaderTampered) else "wrong username or password",
                                wait=self.auth_state.password_wait_seconds()) from e
            if self.session.state == UI_LOCKED and self.keys is not None:
                keys.wipe()  # already open; password only re-verifies presence
                self.session.signed_in(via_password=True)
                self.auth_state.password_ok()
                self.audit.write("auth.unlocked", "authentication", method="password")
                self._signin_history("unlock_password", True)
                self.emit("session.changed", self.status())
                return self.status()
            self.auth_state.password_ok()
            self.audit.set_key(keys.key("K_log"))
            self.audit.write("auth.signin", "authentication", method="password")
            self._open(keys)
            self._signin_history("password", True)
            for n in self.auth_state.pop_notices():
                self.home_event("security_notice", "high", n["text"])
            return self.status()

    def _signin_history(self, kind: str, ok: bool, detail: str = "") -> None:
        if self.db is not None:
            self.db.insert("signin_history", {"id": new_id("sih"), "ts": now_iso(), "kind": kind, "success": int(ok), "detail": detail})

    def quick_unlock(self, pin: str) -> dict[str, Any]:
        with self._lock:
            if not self._pin_available():
                raise AuthError("PIN unlock is not available - use your password", code="pin_unavailable")
            try:
                ok = self.vault.verify_pin(pin)
            except TpmLockedOut as e:
                self.audit.write("auth.pin_tpm_lockout", "authentication", severity="high")
                raise AuthError(str(e), code="tpm_lockout") from e
            except ProtectorError as e:
                raise AuthError(f"PIN unavailable: {e}", code="pin_unavailable") from e
            if not ok:
                self.auth_state.pin_failed()
                self.audit.write("auth.pin_failed", "authentication", severity="medium",
                                 failures=self.auth_state.data["pin_failures"])
                self._signin_history("pin", False)
                raise AuthError("wrong PIN" + (" - PIN disabled until you sign in with your password"
                                               if self.auth_state.data.get("pin_disabled") else ""))
            self.auth_state.pin_ok()
            self.session.signed_in(via_password=False)
            self.audit.write("auth.quick_unlock", "authentication", method="pin")
            self._signin_history("pin", True)
            self.emit("session.changed", self.status())
            return self.status()

    def lock_ui(self, reason: str = "user") -> None:
        with self._lock:
            if self.session.state != UNLOCKED:
                return
            if self.db is not None and self.settings.get("security.pause_missions_when_locked"):
                self.sign_out(f"locked ({reason}) with 'pause missions while locked' on")
                return
            self.session.set_state(UI_LOCKED)
            self.audit.write("auth.locked", "authentication", reason=reason)
            self.emit("session.changed", self.status())

    def sign_out(self, reason: str = "user") -> None:
        """Wipe keys from memory; missions pause until the next password sign-in (spec 4.7)."""
        with self._lock:
            if self.keys is None:
                self.session.set_state(SIGNED_OUT if self.vault.header.exists() else SETUP_REQUIRED)
                return
            self.audit.write("auth.signout", "authentication", reason=reason)
            self._close()
            self.session.set_state(SIGNED_OUT)
            self.emit("session.changed", self.status())

    def step_up(self, category: str, method: str, secret: str) -> dict[str, Any]:
        self.require_unlocked()
        if method == "password":
            self.verify_password(secret)
        elif method == "pin":
            if not self.settings.get("security.pin_quick_unlock") or self.auth_state.data.get("pin_disabled"):
                raise AuthError("PIN step-up unavailable - use your password", code="pin_unavailable")
            try:
                ok = self.vault.verify_pin(secret)
            except ProtectorError as e:
                raise AuthError(str(e), code="pin_unavailable") from e
            if not ok:
                self.auth_state.pin_failed()
                self.audit.write("auth.stepup_failed", "authentication", method="pin", category=category, severity="medium")
                raise AuthError("wrong PIN")
            self.auth_state.pin_ok()
        else:
            raise PAError("bad method", code="invalid_request")
        self.session.grant_stepup(category, float(self.settings.get("security.stepup_minutes")), method)
        self.audit.write("auth.stepup", "authentication", method=method, category=category)
        return {"category": category, "method": method, "minutes": self.settings.get("security.stepup_minutes")}

    def verify_password(self, password: str) -> None:
        wait = self.auth_state.password_wait_seconds()
        if wait > 0:
            raise AuthError(f"too many attempts - wait {int(wait) + 1} s", code="rate_limited", wait=wait)
        try:
            k = self.vault.unlock_with_password(password)
            k.wipe()
        except WrongPassword as e:
            self.auth_state.password_failed()
            self.audit.write("auth.password_check_failed", "authentication", severity="medium")
            raise AuthError("wrong password") from e
        self.auth_state.data["password_failures"] = 0
        self.auth_state.save()

    # ================================================================== reset / change flows (4.6)
    def forgot_password(self, pin: str, recovery_key: str, new_password: str) -> dict[str, Any]:
        with self._lock:
            wait = self.auth_state.recovery_wait_seconds()
            if wait > 0:
                raise AuthError(f"recovery is disabled for {int(wait / 60) + 1} more minutes", code="rate_limited", wait=wait)
            errs = passwords.validate_password(new_password, self.vault.username, pin)
            if errs:
                raise PAError("; ".join(errs), code="invalid_request", errors=errs)
            try:
                keys = self.vault.unlock_with_pin_and_recovery(pin, recovery_key)
            except (WrongRecovery, ProtectorError, HeaderTampered, ValueError) as e:
                self.auth_state.recovery_failed()
                self.audit.write("auth.recovery_failed", "authentication", severity="high")
                raise AuthError("PIN or recovery key rejected") from e
            self.auth_state.recovery_ok()
            self.vault.change_password(keys, new_password)
            new_rk = self.vault.rotate_recovery_key(keys, pin)
            self.auth_state.password_ok()
            self.auth_state.add_notice("Your password was reset with your PIN and recovery key. A new recovery key was issued.")
            self.audit.set_key(keys.key("K_log"))
            self.audit.write("auth.password_reset", "authentication", severity="high", method="pin+recovery_key")
            self._open(keys)
            self.approvals.void_all("password reset")
            for n in self.auth_state.pop_notices():
                self.home_event("security_notice", "high", n["text"])
            self._pending_rk = new_rk
            return {"recovery_key": new_rk, "confirm_groups": recovery.pick_confirmation_groups()}

    def change_password(self, current: str, new: str) -> None:
        self.require_unlocked()
        self.verify_password(current)
        errs = passwords.validate_password(new, self.vault.username)
        if errs:
            raise PAError("; ".join(errs), code="invalid_request", errors=errs)
        assert self.keys
        self.vault.change_password(self.keys, new)
        self.audit.write("auth.password_changed", "authentication", severity="medium")

    def set_new_pin(self, password: str, new_pin: str, recovery_key: str | None) -> dict[str, Any]:
        """Forgot PIN / restored on a new PC: new TPM key; needs the recovery key (or issues a new one)."""
        self.require_unlocked()
        self.verify_password(password)
        errs = passwords.validate_pin(new_pin, bool(self.auth_state.data.get("pin_allow_letters")), password)
        if errs:
            raise PAError("; ".join(errs), code="invalid_request", errors=errs)
        assert self.keys
        protector = default_protector()
        result: dict[str, Any] = {}
        if recovery_key:
            try:
                self.vault.set_pin(self.keys, new_pin, recovery_key, protector)
            except WrongRecovery as e:
                raise AuthError("recovery key rejected") from e
        else:
            rk = self.vault.rotate_recovery_key(self.keys, new_pin, protector)
            self._pending_rk = rk
            result = {"recovery_key": rk, "confirm_groups": recovery.pick_confirmation_groups()}
        self.auth_state.data.update(pin_failures=0, pin_disabled=False)
        self.auth_state.save()
        (self.paths.state_dir / "restored_needs_pin").unlink(missing_ok=True)
        self.audit.write("auth.pin_changed", "authentication", severity="medium", new_recovery_key=not recovery_key)
        return result

    def new_recovery_key(self, password: str, pin: str) -> dict[str, Any]:
        self.require_unlocked()
        self.verify_password(password)
        if not self.vault.verify_pin(pin):
            self.auth_state.pin_failed()
            raise AuthError("wrong PIN")
        assert self.keys
        rk = self.vault.rotate_recovery_key(self.keys, pin)
        self._pending_rk = rk
        self.audit.write("auth.recovery_key_rotated", "authentication", severity="medium")
        return {"recovery_key": rk, "confirm_groups": recovery.pick_confirmation_groups()}

    def set_profile(self, display_name: str, assistant_name: str) -> dict[str, str]:
        """Your name + the assistant's name (no password needed: not security-relevant, but logged)."""
        self.require_unlocked()
        display_name, assistant_name = display_name.strip(), assistant_name.strip()
        errs = passwords.validate_profile_name(display_name, "Your name") + \
            passwords.validate_profile_name(assistant_name, "Assistant name")
        if errs:
            raise PAError("; ".join(errs), code="invalid_request", errors=errs)
        before = self.vault.profile
        assert self.keys
        self.vault.set_profile(self.keys, display_name, assistant_name)
        self.audit.write("profile.changed", "configuration", display_name_changed=before["display_name"] != display_name,
                         assistant_name_changed=before["assistant_name"] != assistant_name)
        self.emit("session.changed", self.status())
        return self.vault.profile

    def change_username(self, password: str, username: str) -> None:
        self.require_unlocked()
        self.verify_password(password)
        errs = passwords.validate_username(username)
        if errs:
            raise PAError(errs[0], code="invalid_request")
        assert self.keys
        self.vault.rename(self.keys, username)
        self.audit.write("auth.username_changed", "authentication")

    def delete_everything(self, password: str, confirmation: str) -> None:
        self.require_unlocked()
        if confirmation.strip() != "DELETE EVERYTHING":
            raise PAError("type DELETE EVERYTHING to confirm", code="confirmation_required")
        self.verify_password(password)
        self.audit.write("privacy.delete_everything", "privacy", severity="critical")
        self._close()
        self.vault.destroy()  # crypto-erase first: without the VMK every blob is unreadable
        for d in (self.paths.db_dir, self.paths.files_dir, self.paths.tmp_dir, self.paths.snapshots_dir):
            shutil.rmtree(d, ignore_errors=True)
        self.paths.ensure()
        from .auth.state import DEFAULTS
        self.auth_state.data = dict(DEFAULTS)
        self.auth_state.save()
        self.session.set_state(SETUP_REQUIRED)
        self.emit("session.changed", self.status())

    # ================================================================== open / close
    def _open(self, keys: UnlockedKeys) -> None:
        self.keys = keys
        self.db = Database(self.paths.db_file, keys.key("K_db"))
        self.settings = SettingsService(self.db, self.audit, on_change=self._settings_changed)
        self.history = HistoryService(self.db)
        self.home = HomeService(self.db, self.emit, self.settings)
        self.policy = PolicyEngine(self.settings.get, lambda: str(self.db.scalar("SELECT count(*) FROM settings_history") or 0))
        self.dlp = DLPEngine(keys.key("K_dlp"), self.settings.get)
        self.audit.set_secret_scanner(self.dlp.contains_secret)
        self.secrets = SecretsService(self.db, keys, self.audit, self.dlp)
        self.secrets.load_dlp()
        self.killswitch = KillSwitch(self.db, self.audit)
        self.killswitch.add_listener(self._on_killswitch)
        self.budgets = BudgetService(self.db, self.settings, self.audit)
        self.sessionlog = SessionLog(self.db)
        self.runs = RunService(self.db, self.emit, self.settings, self.history)
        self.tasks = TaskService(self.db, self.audit, self.emit)
        self.tasks.set_blocked_fn(self.killswitch.agent_blocked)
        self.approvals = ApprovalService(self.db, self.audit, self.settings, self.emit, self.home_event)
        self.tools = ToolGateway(self)
        self.files = FilesService(self)
        self.runtime = BuiltinRuntime(self)
        self.llm = LLMService(self)
        self.memory = MemoryService(self.db, self.audit, self.settings, embed=self._embed_safe)
        self.netlog = NetLog(self)
        self.gpu = GpuMonitor()
        self.vision = VisionReader(self)
        self.memory_learner = MemoryLearner(self)
        self.insights = FileInsights(self)
        self.widgets = HomeWidgets(self)
        self.web_grants: dict[str, str] = {}      # chat id -> app session in which the user allowed web access for that chat
        self.localfiles = LocalFiles(self)
        self.missions = MissionService(self)
        self.emailskills = EmailSkills(self)
        self.reminders = ReminderService(self)
        self.skills = SkillService(self)
        self.sandbox = SandboxService(self)
        self.connectors = ConnectorService(self)
        self.connectors.register(WebConnector(self, self._egress_client))
        self.connectors.register(M365Connector(self))
        self.connectors.register(OutlookLocalConnector(self))
        register_builtin_tools(self)
        self._configure_audit()
        self.session.signed_in(via_password=True)
        self.last_log_verify = self.audit.verify(keys.key("K_log"))
        if not self.last_log_verify.get("ok"):
            self.home_event("log_integrity", "high", "Security log integrity check failed", json.dumps(self.last_log_verify))
            self.audit.write("log.verify_failed", "audit", severity="critical", **{k: v for k, v in self.last_log_verify.items() if k != "ok"})
        recovered = self.tasks.recover_after_restart()
        if recovered:
            self.audit.write("tasks.recovered", "task", count=recovered)
        self._threads_stop.clear()
        threading.Thread(target=self._scheduler_loop, daemon=True, name="scheduler").start()
        if self._start_core:
            self.core_supervisor = CoreSupervisor(self)
            self.core_supervisor.start()
        self.emit("session.changed", self.status())

    def _close(self) -> None:
        self._threads_stop.set()
        if self.core_supervisor:
            self.core_supervisor.stop()
            self.core_supervisor = None
        if self.db is not None:
            try:
                self.runtime.stop()
                self.sandbox.terminate()
                self.connectors.adapters["outlook_local"].terminate()  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001
                pass
            self.audit.set_secret_scanner(None)
            self.db.close()
            self.db = None
        if self.keys is not None:
            self.keys.wipe()
            self.keys = None
        self.audit.set_key(None)
        self._pending_rk = None

    def shutdown(self) -> None:
        self.audit.write("gateway.stopping", "lifecycle")
        self._close()
        self._threads_stop.set()

    def _embed_safe(self, text: str):
        return self.llm.embed(text)

    def _configure_audit(self) -> None:
        s = self.settings
        self.audit.rotate_bytes = int(s.get("logs.rotate_mb")) * 1024 * 1024
        self.audit.rotate_days = int(s.get("logs.rotate_days"))
        self.audit.retention_days = int(s.get("logs.retention_days"))
        self.siem.configure({"enabled": s.get("siem.enabled"), "transport": s.get("siem.transport"), "url": s.get("siem.url"),
                             "host": s.get("siem.host"), "port": s.get("siem.port"), "fingerprint": s.get("siem.fingerprint"),
                             "format": s.get("siem.format")})

    def _settings_changed(self, changes: dict[str, Any]) -> None:
        if any(k.startswith(("logs.", "siem.")) for k in changes):
            self._configure_audit()
        self.emit("settings.changed", {"keys": list(changes)})

    # ================================================================== kill switch effects (spec 24)
    def _on_killswitch(self, level: str, active: bool) -> None:
        if not active:
            if not self.killswitch.agent_blocked():
                self.tasks.release_held()
                if self.core_supervisor is None and self._start_core:
                    self.core_supervisor = CoreSupervisor(self)
                    self.core_supervisor.start()
            self.emit("killswitch.changed", self.killswitch.state())
            return
        if level in ("stop_tasks", "stop_all", "pause_agent"):
            self.approvals.void_all(f"emergency stop: {level}")
            self.tasks.hold_queued(f"emergency stop: {level}")
        if level in ("stop_tasks", "stop_all"):
            self.tasks.cancel_running(f"emergency stop: {level}")
        if level in ("disable_sandbox", "stop_all", "stop_tasks"):
            self.sandbox.terminate()
        if level in ("disable_connectors", "stop_all", "stop_tasks"):
            self.connectors.adapters["outlook_local"].terminate()  # type: ignore[attr-defined]
        if level == "stop_all":
            if self.core_supervisor:
                self.core_supervisor.stop()
                self.core_supervisor = None
            self.runtime.stop()
        self.home_event("killswitch", "high", f"Emergency stop: {level.replace('_', ' ')}", "Release it in Settings > Emergency Stop.")
        self.emit("killswitch.changed", self.killswitch.state())

    # ================================================================== outbox (idempotency, 17.3)
    def outbox_get(self, key: str) -> dict[str, Any] | None:
        return self.db.one("SELECT * FROM outbox WHERE idem_key=?", (key,)) if self.db else None

    def outbox_put(self, key: str, task_id: str, tool: str, phash: str, state: str, result: dict | None = None) -> None:
        self.db.execute("INSERT INTO outbox(idem_key,task_id,tool,payload_hash,state,result_json,created_at,updated_at) "
                        "VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(idem_key) DO UPDATE SET state=excluded.state, "
                        "result_json=excluded.result_json, updated_at=excluded.updated_at",
                        (key, task_id, tool, phash, state, json.dumps(result or {}), now_iso(), now_iso()))

    def skills_note_denial(self, task: dict[str, Any]) -> None:
        run = task.get("session_id")
        if not run or self.db is None:
            return
        ids = [r["meta_json"] for r in self.db.all("SELECT meta_json FROM session_events WHERE session_id=? AND kind='skill.used'", (run,))]
        skill_ids = [json.loads(m).get("skill_id") for m in ids if m]
        if skill_ids:
            self.skills.note_denial([s for s in skill_ids if s])

    # ================================================================== background loops
    def _scheduler_loop(self) -> None:
        last_daily = 0.0
        last_backup_check = 0.0
        while not self._threads_stop.wait(TICK_SECONDS):
            if self.db is None:
                return
            try:
                self.approvals.expire_old()
                self.reminders.tick()
                if not self.killswitch.agent_blocked():
                    self.missions.tick()
                now = time.time()
                if now - last_daily > 24 * 3600:
                    last_daily = now
                    self._daily()
                if now - last_backup_check > 3600:
                    last_backup_check = now
                    self._scheduled_backup()
            except Exception as e:  # noqa: BLE001 - never let the scheduler die
                try:
                    self.audit.write("scheduler.error", "lifecycle", error=type(e).__name__, severity="medium")
                except Exception:  # noqa: BLE001
                    pass

    def _daily(self) -> None:
        results = run_posture(self)
        for c in results:
            if c["status"] == "high":
                self.home_event("posture", "high", f"Security check: {c['title']}", c["detail"])
        self.memory.apply_retention()
        self.skills.retire_unused()
        days = int(self.settings.get("retention.transcripts_days"))
        from datetime import timedelta

        from pa_common.timeutil import to_iso, utcnow
        self.sessionlog.purge_older_than(to_iso(utcnow() - timedelta(days=days)))
        last = self.db.scalar("SELECT value FROM meta WHERE key='backup.last'")
        if not last:
            self.home_event("backup", "medium", "No backup yet", "Set up backups in Settings > Backup & Restore.")

    def _scheduled_backup(self) -> None:
        """Scheduled backups use a key derived from the password when backups were set up
        (Argon2id(password, salt) wrapped with K_backup_local), so they restore with that password."""
        sched = self.settings.get("backup.schedule")
        folder = self.settings.get("backup.folder")
        if sched == "off" or not folder:
            return
        from datetime import timedelta

        from pa_common.timeutil import parse_iso, utcnow
        last = self.db.scalar("SELECT value FROM meta WHERE key='backup.last'")
        if last and utcnow() - parse_iso(last) < timedelta(days=1 if sched == "daily" else 7):
            return
        kek = self.backup_kek()
        if kek is None:
            self.home_event("backup_due", "medium", "Backup is due",
                            "Open Settings > Backup & Restore and click 'Back up now' (your password is needed once).")
            return
        try:
            backup_mod.create_backup(self, None, Path(folder), bool(self.settings.get("backup.include_logs")), kek=kek)
            backup_mod.prune(Path(folder), int(self.settings.get("backup.keep")))
        except Exception as e:  # noqa: BLE001
            self.home_event("backup_failed", "high", "Scheduled backup failed", str(e)[:300])

    def enable_scheduled_backups(self, password: str) -> None:
        self.require_unlocked()
        self.verify_password(password)
        salt, params, kek = backup_mod.derive_kek(password)
        from .vault.crypto import aead_encrypt, b64e
        wrapped = aead_encrypt(self.keys.key("K_backup_local"), kek, b"pa/backup-kek")  # type: ignore[union-attr]
        self.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('backup.kek',?)",
                        (json.dumps({"salt": b64e(salt), "params": params.to_dict(), "wrapped": b64e(wrapped)}),))
        self.audit.write("backup.scheduled_enabled", "backup")

    def backup_kek(self):
        raw = self.db.scalar("SELECT value FROM meta WHERE key='backup.kek'") if self.db else None
        if not raw:
            return None
        from .vault.crypto import Argon2Params, aead_decrypt, b64d
        d = json.loads(raw)
        kek = aead_decrypt(self.keys.key("K_backup_local"), b64d(d["wrapped"]), b"pa/backup-kek")  # type: ignore[union-attr]
        return b64d(d["salt"]), Argon2Params.from_dict(d["params"]), kek

    def _housekeeping(self) -> None:
        """Idle auto-lock (cannot be disabled; spec 5.4.1)."""
        while True:
            time.sleep(5)
            try:
                if self.session.state == UNLOCKED and self.db is not None:
                    limit = float(self.settings.get("security.auto_lock_minutes")) * 60
                    if self.session.idle_seconds() > limit:
                        self.lock_ui("idle")
            except Exception:  # noqa: BLE001
                pass

    # ================================================================== misc
    def verify_log(self) -> dict[str, Any]:
        self.require_keys()
        self.last_log_verify = self.audit.verify(self.keys.key("K_log"))  # type: ignore[union-attr]
        self.audit.write("log.verified", "audit", ok=self.last_log_verify["ok"], events=self.last_log_verify.get("events"))
        return self.last_log_verify

    def posture(self) -> list[dict[str, Any]]:
        self.require_unlocked()
        return run_posture(self)

    def new_core_token(self) -> str:
        return secrets.token_urlsafe(32)


class CoreSupervisor:
    """Starts pa-core (restart with backoff on crash; spec 30). STOP ALL terminates it."""

    def __init__(self, gw: Gateway):
        self.gw = gw
        self.proc: subprocess.Popen | None = None
        self.token = gw.new_core_token()
        self._stop = threading.Event()
        self.inprocess = os.environ.get("PA_CORE_INPROCESS") == "1" or sys.platform != "win32"
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True, name="core-supervisor")
        self._thread.start()

    def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            started = time.time()
            try:
                if self.inprocess:
                    from pa_core.agent import CoreRuntime

                    from .ipc.local import LocalClient
                    rt = CoreRuntime(lambda wid: LocalClient(self.gw.rpc, "core", wid, self.token), stop=self._stop)
                    rt.run()
                else:
                    self._spawn_and_wait()
            except Exception as e:  # noqa: BLE001
                try:
                    self.gw.audit.write("core.crashed", "lifecycle", error=type(e).__name__, severity="medium")
                except Exception:  # noqa: BLE001
                    pass
            if self._stop.is_set():
                break
            if time.time() - started > 60:
                backoff = 1.0
            self._stop.wait(backoff)
            backoff = min(backoff * 2, 60)

    def _spawn_and_wait(self) -> None:
        from .posture import require_firewall_rules
        from .workers import scrubbed_env, worker_command
        try:
            require_firewall_rules()
        except PAError as e:
            self.gw.audit.write("core.start_blocked", "security", reason="firewall_rules_missing", severity="high")
            self.gw.home_event("firewall_missing", "high", "The agent cannot start: firewall rules are missing",
                               "Open Settings > Security Posture and choose Repair.")
            self._stop.wait(30)
            raise e
        server = self.gw.pipe_server
        assert server is not None
        self.proc = subprocess.Popen(worker_command("pa_core"), stdin=subprocess.PIPE, env=scrubbed_env(),
                                     cwd=str(self.gw.paths.tmp_dir), creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        server.expect_core_pid(self.proc.pid, self.token)
        assert self.proc.stdin
        self.proc.stdin.write(json.dumps({"pipe": server.pipe_name, "token": self.token, "gateway_pid": os.getpid()}).encode() + b"\n")
        self.proc.stdin.close()
        self.gw.audit.write("core.started", "lifecycle", pid=self.proc.pid)
        self.proc.wait()

    def stop(self) -> None:
        self._stop.set()
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(5)
            except subprocess.TimeoutExpired:
                self.proc.kill()


# ------------------------------------------------------------------------------ platform helpers
def secure_data_folder(root: Path) -> None:
    """Data folder ACL: current user + SYSTEM full control, inheritance removed (spec 2.5)."""
    if sys.platform != "win32":
        try:
            os.chmod(root, 0o700)
        except OSError:
            pass
        return
    user = os.environ.get("USERNAME", "")
    domain = os.environ.get("USERDOMAIN", "")
    who = f"{domain}\\{user}" if domain else user
    subprocess.run([system32("icacls.exe"), str(root), "/inheritance:r", "/grant:r", f"{who}:(OI)(CI)F", "*S-1-5-18:(OI)(CI)F", "/Q"],
                   capture_output=True, check=False, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))


def _windows_version() -> str:
    if sys.platform != "win32":
        return "n/a"
    v = sys.getwindowsversion()
    return f"Windows {'11' if v.build >= 22000 else '10'} (build {v.build})"


def _gpu_info() -> list[str]:
    if sys.platform != "win32":
        return []
    from .posture import _ps
    out = _ps("Get-CimInstance Win32_VideoController | Select-Object -ExpandProperty Name")
    return [x for x in out.splitlines() if x.strip()][:4]


def _sandbox_probe() -> str:
    from .sandbox import WSB_EXE
    if sys.platform != "win32":
        return "UNAVAILABLE"
    if WSB_EXE.exists():
        return "STRONG"
    try:
        from pa_workers.sandbox import appcontainer
        return "STANDARD" if appcontainer.available() else "UNAVAILABLE"
    except Exception:  # noqa: BLE001
        return "UNAVAILABLE"
