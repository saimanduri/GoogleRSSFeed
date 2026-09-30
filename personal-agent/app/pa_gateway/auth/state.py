"""Brute-force counters that must survive restarts and be readable BEFORE unlock (spec 4.7).

Not secret. Tampering could only reset delays; Argon2id and the TPM's own lockout still apply.
"""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any

DEFAULTS: dict[str, Any] = {
    "password_failures": 0,
    "password_next_allowed": 0.0,
    "pin_failures": 0,
    "pin_disabled": False,
    "recovery_failures": 0,
    "recovery_locked_until": 0.0,
    "last_password_signin": 0.0,
    "notices": [],
}


class AuthState:
    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.RLock()
        self.data = dict(DEFAULTS)
        if path.exists():
            try:
                self.data.update(json.loads(path.read_text("utf-8")))
            except (OSError, ValueError):
                pass

    def save(self) -> None:
        with self._lock:
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.data), "utf-8")
            os.replace(tmp, self.path)

    # -- password: exponential delay after 3 failures (1,2,4,8... s, max 5 min)
    def password_wait_seconds(self) -> float:
        return max(0.0, float(self.data["password_next_allowed"]) - time.time())

    def password_failed(self) -> None:
        with self._lock:
            n = int(self.data["password_failures"]) + 1
            self.data["password_failures"] = n
            if n >= 3:
                self.data["password_next_allowed"] = time.time() + min(2 ** (n - 3), 300)
            self.save()

    def password_ok(self) -> None:
        with self._lock:
            self.data.update(password_failures=0, password_next_allowed=0.0, pin_failures=0, pin_disabled=False,
                             last_password_signin=time.time())
            self.save()

    # -- PIN: app disables PIN after 5 consecutive failures until next password sign-in
    def pin_failed(self) -> None:
        with self._lock:
            self.data["pin_failures"] = int(self.data["pin_failures"]) + 1
            if self.data["pin_failures"] >= 5:
                self.data["pin_disabled"] = True
            self.save()

    def pin_ok(self) -> None:
        with self._lock:
            self.data["pin_failures"] = 0
            self.save()

    # -- recovery: 5 failures -> disabled for 1 hour
    def recovery_wait_seconds(self) -> float:
        return max(0.0, float(self.data["recovery_locked_until"]) - time.time())

    def recovery_failed(self) -> None:
        with self._lock:
            n = int(self.data["recovery_failures"]) + 1
            self.data["recovery_failures"] = n
            if n >= 5:
                self.data["recovery_locked_until"] = time.time() + 3600
                self.data["recovery_failures"] = 0
            self.save()

    def recovery_ok(self) -> None:
        with self._lock:
            self.data.update(recovery_failures=0, recovery_locked_until=0.0)
            self.save()

    def add_notice(self, text: str) -> None:
        with self._lock:
            self.data.setdefault("notices", []).append({"text": text, "ts": time.time()})
            self.save()

    def pop_notices(self) -> list[dict]:
        with self._lock:
            n = self.data.get("notices", [])
            self.data["notices"] = []
            self.save()
            return n
