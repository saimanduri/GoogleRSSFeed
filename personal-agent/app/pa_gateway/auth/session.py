"""Sign-in state machine, idle auto-lock and step-up grants (spec 4.5, 4.7).

States:
  SETUP_REQUIRED  no vault yet -> first-run wizard
  SIGNED_OUT      keys NOT in memory -> password required (cold start)
  UI_LOCKED       keys in gateway memory (missions keep running); UI needs PIN (or password)
  UNLOCKED        UI usable
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field

SETUP_REQUIRED = "SETUP_REQUIRED"
SIGNED_OUT = "SIGNED_OUT"
UI_LOCKED = "UI_LOCKED"
UNLOCKED = "UNLOCKED"

# step-up categories (spec 4.5)
STEPUP_CATEGORIES = {
    "secrets": "Reveal or copy a stored secret",
    "export": "Export data",
    "security_settings": "Change security settings",
    "approvals_high": "Approve a high-risk action",
    "connectors": "Connect or disconnect a connector",
    "backup_restore": "Restore a backup",
    "transcripts": "View full transcripts",
    "skills": "Activate a skill",
    "updates": "Install an update",
}


@dataclass
class SessionState:
    state: str = SIGNED_OUT
    last_activity: float = field(default_factory=time.time)
    last_password: float = 0.0
    stepups: dict[str, tuple[float, str]] = field(default_factory=dict)  # category -> (expires, method)


class SessionManager:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.s = SessionState()

    @property
    def state(self) -> str:
        return self.s.state

    def set_state(self, state: str) -> None:
        with self._lock:
            self.s.state = state
            if state != UNLOCKED:
                self.s.stepups.clear()

    def signed_in(self, via_password: bool = True) -> None:
        with self._lock:
            self.s.state = UNLOCKED
            self.s.last_activity = time.time()
            if via_password:
                self.s.last_password = time.time()

    def touch(self) -> None:
        self.s.last_activity = time.time()

    def idle_seconds(self) -> float:
        return time.time() - self.s.last_activity

    def quick_unlock_allowed(self, max_age_hours: float) -> bool:
        return self.s.state == UI_LOCKED and (time.time() - self.s.last_password) < max_age_hours * 3600

    def grant_stepup(self, category: str, minutes: float, method: str) -> None:
        with self._lock:
            self.s.stepups[category] = (time.time() + minutes * 60, method)

    def has_stepup(self, category: str, require_password: bool = False) -> bool:
        with self._lock:
            g = self.s.stepups.get(category)
            if not g or g[0] < time.time():
                return False
            return g[1] == "password" if require_password else True

    def clear_stepups(self) -> None:
        with self._lock:
            self.s.stepups.clear()
