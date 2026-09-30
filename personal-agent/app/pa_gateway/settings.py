"""Settings service: defaults in code, user values in the encrypted DB, tighten/loosen rules (spec 5.5)."""
from __future__ import annotations

import hashlib
import json
import secrets
import threading
import time
from typing import Any, Callable

from pa_common.errors import PAError, StepUpRequired, ValidationError
from pa_common.ids import new_id
from pa_common.timeutil import now_iso

from .settings_schema import BALANCED_OVERRIDES, BY_KEY, GROUPS, SETTINGS, coerce, exceeds_floor, is_loosening

LOOSEN_DELAY_SECONDS = 10
LOOSEN_TOKEN_TTL = 600


class SettingsService:
    def __init__(self, db, audit, on_change: Callable[[dict[str, Any]], None] | None = None):
        self.db = db
        self.audit = audit
        self._cache: dict[str, Any] = {}
        self._lock = threading.RLock()
        self._loosen_intents: dict[str, tuple[float, str]] = {}
        self._on_change = on_change
        self.reload()

    def reload(self) -> None:
        with self._lock:
            self._cache = {r["key"]: json.loads(r["value_json"]) for r in self.db.all("SELECT key, value_json FROM settings")}

    def get(self, key: str) -> Any:
        spec = BY_KEY[key]
        with self._lock:
            return self._cache.get(key, spec.default)

    def __getitem__(self, key: str) -> Any:
        return self.get(key)

    def all(self) -> dict[str, Any]:
        return {s.key: self.get(s.key) for s in SETTINGS}

    def describe(self) -> dict[str, Any]:
        """Schema + values for the generic Settings UI."""
        return {
            "groups": [{"id": g, "label": label} for g, label in GROUPS],
            "settings": [{
                "key": s.key, "group": s.group, "label": s.label, "type": s.type, "default": s.default,
                "value": self.get(s.key), "min": s.min, "max": s.max, "options": list(s.options), "help": s.help,
                "risk": s.risk, "loosen": s.loosen, "stepup": s.stepup, "floor": "floor" in s.tags,
            } for s in SETTINGS if not s.hidden],
        }

    # ------------------------------------------------------------------ changes
    @staticmethod
    def _changes_hash(changes: dict[str, Any]) -> str:
        return hashlib.sha256(json.dumps(changes, sort_keys=True).encode()).hexdigest()

    def classify(self, changes: dict[str, Any]) -> dict[str, Any]:
        """Validate a change set and report which items loosen security (UI shows the risk text)."""
        normalized: dict[str, Any] = {}
        loosening: list[dict[str, Any]] = []
        stepup: list[str] = []
        for key, value in changes.items():
            spec = BY_KEY.get(key)
            if spec is None:
                raise ValidationError(f"unknown setting {key}")
            try:
                v = coerce(spec, value)
            except ValueError as e:
                raise ValidationError(str(e)) from e
            if exceeds_floor(spec, v):
                raise PAError(f"{spec.label}: cannot be set looser than the security floor", code="floor_violation")
            normalized[key] = v
            before = self.get(key)
            if is_loosening(spec, before, v):
                loosening.append({"key": key, "label": spec.label, "before": before, "after": v,
                                  "risk": spec.risk or "This makes the agent less restrictive."})
            if spec.stepup and v != before:
                stepup.append(key)
        return {"normalized": normalized, "loosening": loosening, "stepup": stepup}

    def begin_loosen(self, changes: dict[str, Any]) -> dict[str, Any]:
        """Step 1 of a loosening change: returns a token usable only after the 10-second read delay."""
        info = self.classify(changes)
        token = secrets.token_urlsafe(24)
        with self._lock:
            self._loosen_intents[token] = (time.time(), self._changes_hash(info["normalized"]))
        return {"token": token, "delay_seconds": LOOSEN_DELAY_SECONDS, "loosening": info["loosening"]}

    def apply(self, changes: dict[str, Any], *, password_ok: bool = False, loosen_token: str | None = None,
              stepup_ok: bool = False, actor: str = "user") -> dict[str, Any]:
        """Apply a change set. Tightening is immediate. Loosening needs password + a matured loosen token."""
        if actor != "user":
            raise PAError("only the user can change settings", code="forbidden")  # spec 5.5: agent has no settings tool
        info = self.classify(changes)
        norm = info["normalized"]
        if info["stepup"] and not stepup_ok:
            raise StepUpRequired("changing endpoints needs re-authentication", category="security_settings")
        if info["loosening"]:
            if not password_ok:
                raise PAError("loosening a setting requires your password", code="password_required",
                              loosening=info["loosening"])
            with self._lock:
                intent = self._loosen_intents.pop(loosen_token or "", None)
            if intent is None:
                raise PAError("confirmation missing or expired", code="loosen_confirmation_required")
            started, h = intent
            age = time.time() - started
            if h != self._changes_hash(norm):
                raise PAError("changes differ from what was confirmed", code="loosen_confirmation_required")
            if age < LOOSEN_DELAY_SECONDS:
                raise PAError("please read the warning first", code="loosen_too_fast", wait=LOOSEN_DELAY_SECONDS - age)
            if age > LOOSEN_TOKEN_TTL:
                raise PAError("confirmation expired", code="loosen_confirmation_required")
        loosening_keys = {x["key"] for x in info["loosening"]}
        with self._lock, self.db.tx():
            for key, v in norm.items():
                before = self.get(key)
                if before == v:
                    continue
                direction = "loosen" if key in loosening_keys else ("tighten" if BY_KEY[key].loosen is not None else "neutral")
                self.audit.write("settings.changed", "configuration", setting=key, before=_safe(before),
                                 after=_safe(v), direction=direction, severity="medium" if direction == "loosen" else "info")
                self.db.execute("INSERT OR REPLACE INTO settings(key, value_json, updated_at) VALUES (?,?,?)",
                                (key, json.dumps(v), now_iso()))
                self.db.insert("settings_history", {"id": new_id("sh"), "key": key, "before_json": json.dumps(before),
                                                    "after_json": json.dumps(v), "direction": direction, "ts": now_iso()})
                self._cache[key] = v
        if self._on_change:
            self._on_change(norm)
        return {"applied": list(norm), "loosened": sorted(loosening_keys)}

    def profile_changes(self, profile: str) -> dict[str, Any]:
        changes: dict[str, Any] = {"autonomy.profile": profile}
        src = BALANCED_OVERRIDES if profile == "balanced" else {k: BY_KEY[k].default for k in BALANCED_OVERRIDES}
        changes.update(src)
        return changes

    def looser_than_defaults(self) -> list[dict[str, Any]]:
        out = []
        for s in SETTINGS:
            v = self.get(s.key)
            if is_loosening(s, s.default, v):
                out.append({"key": s.key, "label": s.label, "default": s.default, "value": v})
        return out

    def history(self, limit: int = 200) -> list[dict[str, Any]]:
        return self.db.all("SELECT * FROM settings_history ORDER BY ts DESC LIMIT ?", (limit,))


def _safe(v: Any) -> Any:
    if isinstance(v, str) and len(v) > 120:
        return v[:120] + "..."
    return v
