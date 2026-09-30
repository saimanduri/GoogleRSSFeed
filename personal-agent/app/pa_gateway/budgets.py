"""Budgets per task and per day (spec 18). On exhaustion the task stops, state is saved and a report
is shown on Home. Interactive chat has priority over missions on the local model (fixed ON)."""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime
from typing import Any

from pa_common.errors import PAError
from pa_common.timeutil import parse_iso, utcnow

# usage key -> (per-task setting, per-day setting, unit scale)
LIMITS: dict[str, tuple[str | None, str | None, float]] = {
    "tokens": ("budget.task.tokens", "budget.daily.tokens", 1),
    "tool_calls": ("budget.task.tool_calls", None, 1),
    "web_requests": ("budget.task.web_requests", "budget.daily.web_requests", 1),
    "files": ("budget.task.files", None, 1),
    "egress_bytes": ("budget.task.egress_kb", "budget.daily.egress_kb", 1024),
    "external_writes": ("budget.task.external_writes", None, 1),
    "subtasks": ("budget.task.subtasks", None, 1),
    "steps": ("budget.task.steps", None, 1),
    "retries": ("budget.task.retries", None, 1),
}


class BudgetExceeded(PAError):
    code = "budget_exhausted"


class BudgetService:
    def __init__(self, db, settings, audit):
        self.db = db
        self.settings = settings
        self.audit = audit
        self._lock = threading.RLock()

    @staticmethod
    def today() -> str:
        return datetime.now().strftime("%Y-%m-%d")  # local day

    def task_limits(self, task: dict[str, Any]) -> dict[str, float]:
        override = json.loads(task.get("budget_json") or "{}")
        out: dict[str, float] = {}
        for key, (task_setting, _, scale) in LIMITS.items():
            if task_setting:
                base = float(self.settings.get(task_setting)) * scale
                # a mission may only NARROW its budget, never widen beyond settings
                out[key] = min(base, float(override[key])) if key in override else base
        out["runtime_seconds"] = min(float(self.settings.get("budget.task.runtime_minutes")) * 60,
                                     float(override.get("runtime_seconds", 1e12)))
        return out

    def daily_used(self, key: str) -> float:
        return float(self.db.scalar("SELECT value FROM budget_usage WHERE day=? AND key=?", (self.today(), key)) or 0)

    def check(self, task: dict[str, Any] | None, key: str, amount: float = 1) -> None:
        """Raise BudgetExceeded if adding `amount` would exceed the per-task or per-day limit."""
        with self._lock:
            if task is not None:
                usage = json.loads(task.get("usage_json") or "{}")
                limits = self.task_limits(task)
                if key in limits and usage.get(key, 0) + amount > limits[key]:
                    raise BudgetExceeded(f"task budget for {key} exhausted", key=key, scope="task")
                started = parse_iso(task["created_at"])
                if (utcnow() - started).total_seconds() > limits["runtime_seconds"]:
                    raise BudgetExceeded("task runtime budget exhausted", key="runtime", scope="task")
            _, daily_setting, scale = LIMITS.get(key, (None, None, 1))
            if daily_setting and self.daily_used(key) + amount > float(self.settings.get(daily_setting)) * scale:
                raise BudgetExceeded(f"daily budget for {key} exhausted", key=key, scope="day")
            if self.daily_used("runtime_seconds") > float(self.settings.get("budget.daily.runtime_hours")) * 3600:
                raise BudgetExceeded("daily runtime budget exhausted", key="runtime", scope="day")

    def add(self, task_id: str | None, key: str, amount: float = 1) -> None:
        with self._lock, self.db.tx():
            if task_id:
                row = self.db.one("SELECT usage_json FROM tasks WHERE id=?", (task_id,))
                if row:
                    usage = json.loads(row["usage_json"] or "{}")
                    usage[key] = usage.get(key, 0) + amount
                    self.db.update("tasks", "id", task_id, {"usage_json": usage})
            self.db.execute("INSERT INTO budget_usage(day,key,value) VALUES(?,?,?) "
                            "ON CONFLICT(day,key) DO UPDATE SET value=value+excluded.value", (self.today(), key, amount))

    def summary(self) -> dict[str, Any]:
        rows = self.db.all("SELECT key, value FROM budget_usage WHERE day=?", (self.today(),))
        used = {r["key"]: r["value"] for r in rows}
        limits = {k: float(self.settings.get(d)) * s for k, (_, d, s) in LIMITS.items() if d}
        limits["runtime_seconds"] = float(self.settings.get("budget.daily.runtime_hours")) * 3600
        return {"day": self.today(), "used": used, "limits": limits, "ts": time.time()}
