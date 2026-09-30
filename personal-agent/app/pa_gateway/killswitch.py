"""Kill switch / emergency stop (spec 24).

Levels: pause_agent, stop_tasks, disable_connectors, disable_web, disable_sandbox, stop_all.
State is persisted in the DB and held in memory; the tool gateway reads it before every step and
tool call (new calls blocked immediately - well under the 2 s target). Activation cancels
in-flight calls, voids approvals, terminates the sandbox / Outlook worker and (STOP ALL) pa-core
and the model runtime. Release requires the password.
"""
from __future__ import annotations

import json
import threading
from typing import Any, Callable

from pa_common.timeutil import now_iso

LEVELS = ("pause_agent", "stop_tasks", "disable_connectors", "disable_web", "disable_sandbox", "stop_all")
LABELS = {
    "pause_agent": "Pause agent (no new steps; queued tasks held)",
    "stop_tasks": "Stop all tasks (running tasks cancelled, queue held)",
    "disable_connectors": "Disable all connectors",
    "disable_web": "Disable web access",
    "disable_sandbox": "Disable Python sandbox",
    "stop_all": "STOP ALL (everything, incl. agent runtime and model)",
}


class KillSwitch:
    def __init__(self, db, audit):
        self.db = db
        self.audit = audit
        self._lock = threading.RLock()
        self._state: dict[str, Any] = {k: False for k in LEVELS}
        self._listeners: list[Callable[[str, bool], None]] = []
        row = db.one("SELECT value FROM meta WHERE key='killswitch'")
        if row:
            self._state.update(json.loads(row["value"]))

    def add_listener(self, fn: Callable[[str, bool], None]) -> None:
        self._listeners.append(fn)

    def state(self) -> dict[str, Any]:
        with self._lock:
            return {"levels": dict(self._state), "labels": LABELS, "any": any(self._state[k] for k in LEVELS)}

    def active(self, level: str) -> bool:
        with self._lock:
            return bool(self._state.get("stop_all")) or bool(self._state.get(level))

    def agent_blocked(self) -> bool:
        return self.active("pause_agent") or self.active("stop_tasks")

    def _persist(self) -> None:
        self.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('killswitch',?)", (json.dumps(self._state),))

    def activate(self, level: str, source: str) -> None:
        if level not in LEVELS:
            raise ValueError("unknown kill switch level")
        with self._lock:
            self._state[level] = True
            self._state["activated_at"] = now_iso()
            # persist + log before notifying so that the state survives a crash mid-stop
            self._persist()
        self.audit.write("killswitch.activated", "killswitch", level=level, source=source, severity="high")
        for fn in list(self._listeners):
            try:
                fn(level, True)
            except Exception:  # noqa: BLE001
                pass

    def release(self, level: str | None, source: str) -> None:
        with self._lock:
            for k in (LEVELS if level in (None, "all") else (level,)):
                self._state[k] = False
            self._persist()
        self.audit.write("killswitch.released", "killswitch", level=level or "all", source=source, severity="medium")
        for fn in list(self._listeners):
            try:
                fn(level or "all", False)
            except Exception:  # noqa: BLE001
                pass
