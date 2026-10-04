"""Reminders (user requirement: "talk and ask it to remind me on a date; remind me after my confirmation").

Flow: the user speaks/types "remind me to call the dentist on Friday at 9" -> the agent calls
reminders.propose -> an approval card appears in chat ("Allow once / Deny") -> only after the user
confirms is the reminder SCHEDULED -> at the due time the gateway raises a Windows notification,
a Home entry and an in-app banner (like Claude's "task done" notifications).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from pa_common.errors import PAError
from pa_common.ids import new_id
from pa_common.timeutil import now_iso, parse_iso, to_iso, utcnow

from .missions import local_tz


class ReminderService:
    def __init__(self, gw):
        self.gw = gw
        self.db = gw.db

    @staticmethod
    def to_utc(local_iso: str, tz: str) -> datetime:
        dt = datetime.fromisoformat(local_iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo(tz))
        return dt.astimezone(timezone.utc)

    def create(self, text: str, due_local: str, tz: str | None = None, source: str = "user", run_id: str | None = None) -> str:
        tz = tz or local_tz()
        try:
            due = self.to_utc(due_local, tz)
        except (ValueError, KeyError) as e:
            raise PAError("could not understand the reminder date/time", code="invalid_request") from e
        if due < utcnow() - timedelta(minutes=1):
            raise PAError("that time is in the past", code="invalid_request")
        rid = new_id("rem")
        self.db.insert("reminders", {"id": rid, "text": text[:500], "due_at": to_iso(due), "timezone": tz,
                                     "status": "SCHEDULED", "source": source, "run_id": run_id, "created_at": now_iso()})
        self.gw.audit.write("reminder.scheduled", "reminder", reminder_id=rid, source=source, due_at=to_iso(due))
        self.gw.emit("reminders.changed", {"id": rid})
        return rid

    def list(self, include_done: bool = False) -> list[dict[str, Any]]:
        q = "SELECT * FROM reminders" + ("" if include_done else " WHERE status IN ('SCHEDULED','FIRED')") + " ORDER BY due_at"
        return self.db.all(q)

    def cancel(self, rid: str) -> None:
        self.db.update("reminders", "id", rid, {"status": "CANCELLED"})
        self.gw.audit.write("reminder.cancelled", "reminder", reminder_id=rid)
        self.gw.emit("reminders.changed", {"id": rid})

    def dismiss(self, rid: str) -> None:
        self.db.update("reminders", "id", rid, {"status": "DISMISSED"})
        self.gw.emit("reminders.changed", {"id": rid})

    def snooze(self, rid: str, minutes: int) -> None:
        r = self.db.one("SELECT * FROM reminders WHERE id=?", (rid,))
        if not r:
            raise PAError("reminder not found", code="not_found")
        self.db.update("reminders", "id", rid, {"status": "SCHEDULED", "due_at": to_iso(utcnow() + timedelta(minutes=max(1, minutes)))})
        self.gw.emit("reminders.changed", {"id": rid})

    def tick(self) -> int:
        n = 0
        for r in self.db.all("SELECT * FROM reminders WHERE status='SCHEDULED' AND due_at <= ?", (now_iso(),)):
            self.db.update("reminders", "id", r["id"], {"status": "FIRED", "fired_at": now_iso()})
            late = (utcnow() - parse_iso(r["due_at"])).total_seconds() > 600
            # Reminder text is the user's own words -> shown in the toast only if the content level allows it
            self.gw.notify("reminder", "Reminder" + (" (missed while away)" if late else ""), r["text"], 1, "reminders", r["id"],
                           subject=r["text"], status="Reminder - missed while you were away" if late else "Reminder")
            self.gw.home_event("reminder", "info", f"Reminder: {r['text'][:120]}", "", r["id"])
            self.gw.emit("reminder.fired", {"id": r["id"], "text": r["text"], "late": late})
            self.gw.audit.write("reminder.fired", "reminder", reminder_id=r["id"], late=late)
            n += 1
        return n
