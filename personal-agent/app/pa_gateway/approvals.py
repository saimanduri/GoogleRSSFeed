"""Approvals (spec 23, 39.14).

- bound to SHA-256 of the exact canonical payload + destination; any change voids it; single use
- expiry: default 24 h; high risk 1 h (Settings)
- high risk needs step-up (PIN by default, password option); CONFIDENTIAL external writes need the password
- granted only inside the unlocked app (the only caller is the pa-ui IPC role)
- fatigue: per-task request rate limit; warning if approvals are accepted in < N s repeatedly
- "Edit and re-propose" creates a NEW approval with a new hash; the waiting tool call follows it
"""
from __future__ import annotations

import hashlib
import json
import threading
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Callable

from pa_common.errors import PAError
from pa_common.ids import new_id
from pa_common.timeutil import now_iso, parse_iso, to_iso, utcnow

PENDING = "PENDING"
APPROVED = "APPROVED"
DENIED = "DENIED"
EXPIRED = "EXPIRED"
VOIDED = "VOIDED"
USED = "USED"
REPLACED = "REPLACED"

MAX_REQUESTS_PER_TASK_PER_HOUR = 20


def payload_hash(tool: str, payload: dict[str, Any], destination: str | None) -> str:
    body = json.dumps({"tool": tool, "payload": payload, "destination": destination or ""},
                      sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "sha256:" + hashlib.sha256(body.encode("utf-8")).hexdigest()


@dataclass
class _Waiter:
    event: threading.Event = field(default_factory=threading.Event)
    outcome: str = ""
    approval_id: str = ""


class ApprovalService:
    def __init__(self, db, audit, settings, emit: Callable[[str, dict], None], home_event: Callable[..., None]):
        self.db = db
        self.audit = audit
        self.settings = settings
        self.emit = emit
        self.home_event = home_event
        self._waiters: dict[str, _Waiter] = {}
        self._lock = threading.RLock()
        self._fast_accepts: list[float] = []

    # ------------------------------------------------------------------ create
    def create(self, *, tool: str, payload: dict[str, Any], destination: str | None, sensitivity: int, risk: str,
               reason: str, requires_password: bool, task_id: str | None, run_id: str | None, chat_id: str | None,
               mission_id: str | None, kind: str = "tool") -> dict[str, Any]:
        if task_id:
            since = to_iso(utcnow() - timedelta(hours=1))
            n = self.db.scalar("SELECT count(*) FROM approvals WHERE task_id=? AND created_at>?", (task_id, since))
            if n >= MAX_REQUESTS_PER_TASK_PER_HOUR:
                raise PAError("too many approval requests from this task", code="approval_rate_limited")
        hours = self.settings.get("approvals.high_risk_expiry_hours" if risk == "high" else "approvals.expiry_hours")
        row = {
            "id": new_id("apr"), "task_id": task_id, "run_id": run_id, "chat_id": chat_id, "mission_id": mission_id,
            "kind": kind, "tool": tool, "payload_json": json.dumps(payload, ensure_ascii=False),
            "payload_hash": payload_hash(tool, payload, destination), "destination": destination,
            "sensitivity": int(sensitivity), "risk": risk, "reason": reason, "requires_password": int(requires_password),
            "status": PENDING, "created_at": now_iso(),
            "expires_at": to_iso(utcnow() + timedelta(hours=float(hours))),
        }
        self.audit.write("approval.requested", "approval", approval_id=row["id"], tool=tool, task_id=task_id,
                         payload_hash=row["payload_hash"], risk=risk, sensitivity=int(sensitivity), destination=destination)
        self.db.insert("approvals", row)
        with self._lock:
            self._waiters[row["id"]] = _Waiter(approval_id=row["id"])
        self.emit("approvals.changed", {"id": row["id"], "status": PENDING, "chat_id": chat_id, "run_id": run_id})
        return self.get(row["id"]) or row

    def get(self, approval_id: str) -> dict[str, Any] | None:
        r = self.db.one("SELECT * FROM approvals WHERE id=?", (approval_id,))
        if r:
            r["payload"] = json.loads(r.pop("payload_json"))
        return r

    def list(self, status: str | None = PENDING, limit: int = 200) -> list[dict[str, Any]]:
        self.expire_old()
        if status:
            rows = self.db.all("SELECT * FROM approvals WHERE status=? ORDER BY created_at DESC LIMIT ?", (status, limit))
        else:
            rows = self.db.all("SELECT * FROM approvals ORDER BY created_at DESC LIMIT ?", (limit,))
        for r in rows:
            r["payload"] = json.loads(r.pop("payload_json"))
        return rows

    def pending_count(self) -> int:
        return int(self.db.scalar("SELECT count(*) FROM approvals WHERE status=?", (PENDING,)) or 0)

    # ------------------------------------------------------------------ decide (UI only)
    def decide(self, approval_id: str, approve: bool, *, stepup_ok: bool, password_ok: bool,
               shown_hash: str, opened_at_ms: int | None = None, edited_payload: dict[str, Any] | None = None) -> dict[str, Any]:
        a = self.get(approval_id)
        if not a:
            raise PAError("approval not found", code="not_found")
        if a["status"] != PENDING:
            raise PAError(f"approval is {a['status'].lower()}", code="approval_not_pending")
        if parse_iso(a["expires_at"]) < utcnow():
            self._finish(a, EXPIRED)
            raise PAError("approval expired", code="approval_expired")
        # The UI must echo the hash of what it displayed; recompute from stored payload (spec 23: bound to payload)
        recomputed = payload_hash(a["tool"], a["payload"], a["destination"])
        if shown_hash != a["payload_hash"] or recomputed != a["payload_hash"]:
            raise PAError("the action changed since it was shown; review it again", code="approval_hash_mismatch")
        decision_ms = None
        if opened_at_ms:
            decision_ms = max(0, int(time.time() * 1000) - int(opened_at_ms))
        if edited_payload is not None:
            new = self.create(tool=a["tool"], payload=edited_payload, destination=a["destination"],
                              sensitivity=a["sensitivity"], risk=a["risk"], reason=a["reason"] + " (edited by you)",
                              requires_password=bool(a["requires_password"]), task_id=a["task_id"], run_id=a["run_id"],
                              chat_id=a["chat_id"], mission_id=a["mission_id"], kind=a["kind"])
            self._finish(a, REPLACED, replaced_by=new["id"])
            return new
        if approve:
            if a["risk"] == "high" and not stepup_ok:
                raise PAError("re-authenticate to approve high-risk actions", code="step_up_required", category="approvals_high")
            if a["requires_password"] and not password_ok:
                raise PAError("your password is required for this approval", code="password_required")
            self._fatigue_check(decision_ms)
        self._finish(a, APPROVED if approve else DENIED, decision_ms=decision_ms)
        return self.get(approval_id) or a

    def _fatigue_check(self, decision_ms: int | None) -> None:
        if decision_ms is None:
            return
        limit = float(self.settings.get("approvals.fast_warning_seconds")) * 1000
        now = time.time()
        if decision_ms < limit:
            self._fast_accepts = [t for t in self._fast_accepts if now - t < 600] + [now]
            if len(self._fast_accepts) >= 3:
                self._fast_accepts.clear()
                self.audit.write("approval.fatigue_warning", "approval", severity="medium")
                self.home_event("approval_fatigue", "medium", "Approvals are being accepted very quickly",
                                "Take a moment to read each request. Fast approvals are a common way mistakes slip through.")

    def _finish(self, a: dict[str, Any], status: str, decision_ms: int | None = None, replaced_by: str | None = None) -> None:
        self.db.update("approvals", "id", a["id"], {"status": status, "decided_at": now_iso(), "decision_ms": decision_ms})
        self.audit.write(f"approval.{status.lower()}", "approval", approval_id=a["id"], tool=a["tool"],
                         payload_hash=a["payload_hash"], decision_ms=decision_ms, replaced_by=replaced_by)
        with self._lock:
            w = self._waiters.get(a["id"])
            if w and replaced_by:
                # the waiting tool call now follows the new approval
                self._waiters[replaced_by] = w
                w.approval_id = replaced_by
            elif w:
                w.outcome = status
                w.event.set()
        self.emit("approvals.changed", {"id": a["id"], "status": status, "chat_id": a.get("chat_id"), "run_id": a.get("run_id")})

    # ------------------------------------------------------------------ waiting (tool gateway)
    def wait(self, approval_id: str, timeout: float, cancelled: Callable[[], bool]) -> tuple[str, str]:
        """Block until decided. Returns (status, final_approval_id)."""
        with self._lock:
            w = self._waiters.setdefault(approval_id, _Waiter(approval_id=approval_id))
        deadline = time.time() + timeout
        while time.time() < deadline:
            if w.event.wait(0.25):
                return w.outcome, w.approval_id
            if cancelled():
                a = self.get(w.approval_id)
                if a and a["status"] == PENDING:
                    self._finish(a, VOIDED)
                return VOIDED, w.approval_id
            a = self.get(w.approval_id)
            if a and a["status"] != PENDING:
                return a["status"], w.approval_id
            if a and parse_iso(a["expires_at"]) < utcnow():
                self._finish(a, EXPIRED)
                return EXPIRED, w.approval_id
        a = self.get(w.approval_id)
        return (a["status"] if a else EXPIRED), w.approval_id

    def consume(self, approval_id: str, tool: str, payload: dict[str, Any], destination: str | None) -> None:
        """Single use: the executed payload must hash to the approved hash."""
        a = self.get(approval_id)
        if not a or a["status"] != APPROVED:
            raise PAError("approval not valid", code="approval_invalid")
        if payload_hash(tool, payload, destination) != a["payload_hash"]:
            raise PAError("payload differs from the approved one", code="approval_hash_mismatch")
        self.db.update("approvals", "id", approval_id, {"status": USED})

    def void_all(self, reason: str) -> int:
        rows = self.db.all("SELECT * FROM approvals WHERE status=?", (PENDING,))
        for r in rows:
            r["payload"] = json.loads(r.pop("payload_json"))
            self._finish(r, VOIDED)
        if rows:
            self.audit.write("approval.voided_all", "approval", count=len(rows), reason=reason)
        return len(rows)

    def expire_old(self) -> None:
        now = now_iso()
        for r in self.db.all("SELECT * FROM approvals WHERE status=? AND expires_at<?", (PENDING, now)):
            r["payload"] = json.loads(r.pop("payload_json"))
            self._finish(r, EXPIRED)
