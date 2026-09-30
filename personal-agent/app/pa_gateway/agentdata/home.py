"""Home "While you were away" events and in-app notifications (spec 5.2, 26)."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from pa_common.ids import new_id
from pa_common.sensitivity import Sensitivity
from pa_common.timeutil import now_iso


class HomeService:
    def __init__(self, db, emit: Callable[[str, dict], None], settings):
        self.db = db
        self.emit = emit
        self.settings = settings

    def event(self, kind: str, severity: str, title: str, detail: str = "", ref_id: str | None = None) -> None:
        eid = new_id("home")
        self.db.insert("home_events", {"id": eid, "kind": kind, "severity": severity, "title": title[:300],
                                       "detail": detail[:2000], "ref_id": ref_id, "created_at": now_iso()})
        self.emit("home.changed", {"id": eid, "severity": severity})

    def events(self, include_dismissed: bool = False, limit: int = 200) -> list[dict[str, Any]]:
        q = "SELECT * FROM home_events" + ("" if include_dismissed else " WHERE dismissed=0") + " ORDER BY created_at DESC LIMIT ?"
        return self.db.all(q, (limit,))

    def dismiss(self, event_id: str) -> None:
        self.db.update("home_events", "id", event_id, {"dismissed": 1})

    # ---------------------------------------------------------------- notifications
    def _quiet(self) -> bool:
        start, end = self.settings.get("notifications.quiet_start"), self.settings.get("notifications.quiet_end")
        if not start or not end:
            return False
        now = datetime.now().strftime("%H:%M")
        return (start <= now < end) if start < end else (now >= start or now < end)

    def notify(self, kind: str, title: str, body: str | None, sensitivity: int, screen: str | None, ref_id: str | None) -> None:
        """Content-level rules: default 'notify' shows only a generic line; 'summary' never includes
        CONFIDENTIAL content. Toasts are private (no lock-screen content) and carry no actions (spec 26)."""
        level = self.settings.get("notifications.content_level")
        shown_body = None
        if level == "summary" and body and sensitivity < Sensitivity.CONFIDENTIAL:
            shown_body = body[:200]
        nid = new_id("ntf")
        self.db.insert("notifications", {"id": nid, "kind": kind, "title": title[:120], "body": shown_body,
                                         "sensitivity": int(sensitivity), "screen": screen, "ref_id": ref_id,
                                         "created_at": now_iso()})
        toast = bool(self.settings.get("notifications.toasts")) and not self._quiet()
        # screen/ref_id only open a screen in the app; links never carry parameters that change anything (39.2)
        self.emit("notify", {"id": nid, "kind": kind, "title": title[:120], "body": shown_body, "screen": screen,
                             "toast": toast, "private": sensitivity > Sensitivity.PUBLIC})

    def notifications(self, limit: int = 100) -> list[dict[str, Any]]:
        return self.db.all("SELECT * FROM notifications ORDER BY created_at DESC LIMIT ?", (limit,))

    def mark_read(self, nid: str | None = None) -> None:
        if nid:
            self.db.update("notifications", "id", nid, {"read": 1})
        else:
            self.db.execute("UPDATE notifications SET read=1")
