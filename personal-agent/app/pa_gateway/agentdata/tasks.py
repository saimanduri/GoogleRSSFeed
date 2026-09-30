"""Durable tasks and queue (spec 17). SQLite (WAL, SQLCipher) is the system of record; every state
transition is persisted in a transaction together with a transition record.

States: CREATED, PLANNING, QUEUED, RUNNING, WAITING_FOR_RESOURCE, WAITING_FOR_APPROVAL, SUSPENDED,
VERIFYING, OUTCOME_UNKNOWN, COMPLETED, FAILED, CANCELLED, TIMED_OUT (+ PAUSED for kill switch holds).
"""
from __future__ import annotations

import json
import threading
from datetime import timedelta
from typing import Any, Callable

from pa_common.errors import PAError
from pa_common.ids import new_id
from pa_common.timeutil import now_iso, to_iso, utcnow

ACTIVE = ("RUNNING", "PLANNING", "VERIFYING", "WAITING_FOR_APPROVAL")
TERMINAL = ("COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT", "OUTCOME_UNKNOWN")
ALL_STATES = ("CREATED", "PLANNING", "QUEUED", "RUNNING", "WAITING_FOR_RESOURCE", "WAITING_FOR_APPROVAL", "SUSPENDED",
              "VERIFYING", "OUTCOME_UNKNOWN", "COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT", "PAUSED")
PRIORITY_CHAT = 1
PRIORITY_SUBTASK = 3
PRIORITY_MISSION = 5
LEASE_SECONDS = 120


class TaskService:
    def __init__(self, db, audit, emit: Callable[[str, dict], None]):
        self.db = db
        self.audit = audit
        self.emit = emit
        self._lock = threading.RLock()
        self._cancel: dict[str, threading.Event] = {}
        self._done: dict[str, threading.Event] = {}
        self._wakeup = threading.Condition()

    # ------------------------------------------------------------------ create / read
    def create(self, *, objective: str, trigger: str, run_id: str | None, chat_id: str | None = None,
               mission_id: str | None = None, allowed_tools: list[str] | None = None, budget: dict | None = None,
               parent_task_id: str | None = None, depth: int = 0, priority: int = PRIORITY_MISSION,
               hwm: int = 0, model_id: str | None = None, session_id: str | None = None) -> dict[str, Any]:
        tid = new_id("task")
        now = now_iso()
        row = {"id": tid, "run_id": run_id, "session_id": session_id or chat_id or run_id or tid, "chat_id": chat_id, "mission_id": mission_id, "parent_task_id": parent_task_id,
               "depth": depth, "trigger_type": trigger, "objective": objective, "state": "QUEUED", "priority": priority,
               "allowed_tools_json": allowed_tools if allowed_tools is not None else None, "budget_json": budget or {},
               "usage_json": {}, "hwm": hwm, "model_id": model_id, "created_at": now, "updated_at": now}
        if row["allowed_tools_json"] is None:
            row["allowed_tools_json"] = "null"
        with self.db.tx():
            self.db.insert("tasks", row)
            self.db.insert("task_transitions", {"id": new_id("tt"), "task_id": tid, "from_state": None, "to_state": "QUEUED",
                                                "reason": "created", "ts": now})
        self.audit.write("task.created", "task", task_id=tid, mission_id=mission_id, trigger_type=trigger,
                         parent_task_id=parent_task_id, depth=depth)
        self._cancel[tid] = threading.Event()
        self._done[tid] = threading.Event()
        self.emit("tasks.changed", {"task_id": tid, "state": "QUEUED"})
        with self._wakeup:
            self._wakeup.notify_all()
        return self.get(tid)  # type: ignore[return-value]

    def get(self, task_id: str) -> dict[str, Any] | None:
        return self.db.one("SELECT * FROM tasks WHERE id=?", (task_id,))

    def allowed_tools(self, task: dict[str, Any]) -> list[str] | None:
        return json.loads(task.get("allowed_tools_json") or "null")

    def list(self, states: tuple[str, ...] | None = None, limit: int = 200) -> list[dict[str, Any]]:
        if states:
            qs = ",".join("?" for _ in states)
            return self.db.all(f"SELECT * FROM tasks WHERE state IN ({qs}) ORDER BY updated_at DESC LIMIT ?", (*states, limit))
        return self.db.all("SELECT * FROM tasks ORDER BY updated_at DESC LIMIT ?", (limit,))

    def transitions(self, task_id: str) -> list[dict[str, Any]]:
        return self.db.all("SELECT * FROM task_transitions WHERE task_id=? ORDER BY ts", (task_id,))

    # ------------------------------------------------------------------ state changes
    def transition(self, task_id: str, to: str, reason: str = "", **fields: Any) -> None:
        if to not in ALL_STATES:
            raise PAError("bad task state", code="invalid_request")
        with self._lock, self.db.tx():
            t = self.get(task_id)
            if not t:
                raise PAError("task not found", code="not_found")
            if t["state"] in TERMINAL and to != t["state"]:
                return  # terminal states are final (late results discarded)
            changes = {"state": to, "updated_at": now_iso(), **fields}
            if to in ("WAITING_FOR_RESOURCE", "WAITING_FOR_APPROVAL", "SUSPENDED", "PAUSED"):
                changes.setdefault("wait_reason", reason)
            self.db.update("tasks", "id", task_id, changes)
            self.db.insert("task_transitions", {"id": new_id("tt"), "task_id": task_id, "from_state": t["state"],
                                                "to_state": to, "reason": reason[:500], "ts": now_iso()})
        self.audit.write("task.transition", "task", task_id=task_id, from_state=t["state"], to_state=to,
                         reason=reason[:200], mission_id=t.get("mission_id"))
        if to in TERMINAL:
            ev = self._done.setdefault(task_id, threading.Event())
            ev.set()
        self.emit("tasks.changed", {"task_id": task_id, "state": to, "reason": reason[:200]})
        if to == "QUEUED":
            with self._wakeup:
                self._wakeup.notify_all()

    def raise_hwm(self, task_id: str, level: int) -> int:
        with self._lock:
            t = self.get(task_id)
            if not t:
                return level
            new = max(int(t["hwm"]), int(level))
            if new != t["hwm"]:
                self.db.update("tasks", "id", task_id, {"hwm": new})
            if t.get("parent_task_id"):  # sub-agent reads raise the parent's level (spec 39.8)
                self.raise_hwm(t["parent_task_id"], new)
            return new

    # ------------------------------------------------------------------ queue (pa-core workers)
    def claim(self, worker: str, background_slots: int, blocked: bool, timeout: float = 20.0) -> dict[str, Any] | None:
        """Long-poll for the next task. Chat (interactive) tasks always go first (spec 18)."""
        deadline = utcnow() + timedelta(seconds=timeout)
        while True:
            if not blocked:
                with self._lock, self.db.tx():
                    running_bg = int(self.db.scalar(
                        "SELECT count(*) FROM tasks WHERE state='RUNNING' AND priority>=?", (PRIORITY_MISSION,)) or 0)
                    row = self.db.one(
                        "SELECT * FROM tasks WHERE state='QUEUED' AND (priority<? OR ?<?) ORDER BY priority, created_at LIMIT 1",
                        (PRIORITY_MISSION, running_bg, background_slots))
                    if row:
                        lease = to_iso(utcnow() + timedelta(seconds=LEASE_SECONDS))
                        self.db.update("tasks", "id", row["id"], {"state": "RUNNING", "lease_owner": worker,
                                                                  "lease_until": lease, "updated_at": now_iso()})
                        self.db.insert("task_transitions", {"id": new_id("tt"), "task_id": row["id"], "from_state": "QUEUED",
                                                            "to_state": "RUNNING", "reason": f"claimed by {worker}", "ts": now_iso()})
                        self._cancel.setdefault(row["id"], threading.Event())
                        self.emit("tasks.changed", {"task_id": row["id"], "state": "RUNNING"})
                        return self.get(row["id"])
            remaining = (deadline - utcnow()).total_seconds()
            if remaining <= 0:
                return None
            with self._wakeup:
                self._wakeup.wait(min(remaining, 2.0))
            if callable(getattr(self, "_blocked_fn", None)):
                blocked = self._blocked_fn()  # type: ignore[attr-defined]

    def set_blocked_fn(self, fn: Callable[[], bool]) -> None:
        self._blocked_fn = fn

    def heartbeat(self, task_id: str, worker: str) -> None:
        t = self.get(task_id)
        if not t or t.get("lease_owner") != worker:
            raise PAError("task is not leased by this worker", code="not_owner")
        self.db.update("tasks", "id", task_id, {"lease_until": to_iso(utcnow() + timedelta(seconds=LEASE_SECONDS))})

    def require_owner(self, task_id: str, worker: str) -> dict[str, Any]:
        t = self.get(task_id)
        if not t or t.get("lease_owner") != worker:
            raise PAError("task is not leased by this caller", code="not_owner")
        return t

    def recover_after_restart(self) -> int:
        """After crash/restart/sign-in: tasks that were RUNNING go back to QUEUED (spec 17.2).
        Side effects are protected by the outbox (idempotency) so re-running cannot duplicate them."""
        rows = self.db.all("SELECT id FROM tasks WHERE state IN ('RUNNING','PLANNING','VERIFYING','WAITING_FOR_APPROVAL')")
        for r in rows:
            self.transition(r["id"], "QUEUED", "recovered after restart", lease_owner=None, lease_until=None)
        return len(rows)

    # ------------------------------------------------------------------ cancellation
    def cancel_event(self, task_id: str) -> threading.Event:
        return self._cancel.setdefault(task_id, threading.Event())

    def is_cancelled(self, task_id: str | None) -> bool:
        return bool(task_id) and self.cancel_event(task_id).is_set()  # type: ignore[arg-type]

    def cancel(self, task_id: str, reason: str, state: str = "CANCELLED") -> None:
        self.cancel_event(task_id).set()
        for child in self.db.all("SELECT id FROM tasks WHERE parent_task_id=? AND state NOT IN ('COMPLETED','FAILED','CANCELLED','TIMED_OUT','OUTCOME_UNKNOWN')", (task_id,)):
            self.cancel(child["id"], "parent cancelled", state)
        self.transition(task_id, state, reason)

    def cancel_running(self, reason: str) -> int:
        rows = self.list(("RUNNING", "PLANNING", "VERIFYING", "WAITING_FOR_APPROVAL"))
        for r in rows:
            self.cancel(r["id"], reason)
        return len(rows)

    def hold_queued(self, reason: str) -> None:
        for r in self.list(("QUEUED",)):
            self.transition(r["id"], "PAUSED", reason)

    def release_held(self) -> None:
        for r in self.list(("PAUSED",)):
            self.transition(r["id"], "QUEUED", "kill switch released")

    def wait_done(self, task_id: str, timeout: float, cancelled: Callable[[], bool]) -> dict[str, Any] | None:
        ev = self._done.setdefault(task_id, threading.Event())
        end = utcnow() + timedelta(seconds=timeout)
        while utcnow() < end:
            if ev.wait(0.5):
                break
            t = self.get(task_id)
            if t and t["state"] in TERMINAL:
                break
            if cancelled():
                self.cancel(task_id, "parent cancelled")
                break
        return self.get(task_id)
