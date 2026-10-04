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

    def notify(self, kind: str, title: str, body: str | None, sensitivity: int, screen: str | None, ref_id: str | None, *,
               subject: str | None = None, status: str | None = None, record: bool = True) -> None:
        """Content-level rules: default 'notify' shows only a generic line; 'summary' never includes
        CONFIDENTIAL content. Toasts are private (no lock-screen content) and carry no actions (spec 26).

        `subject` is the NAME of what the notification is about (reminder text, routine name, chat name) and `status` a short
        fixed line ("Finished", "Needs your approval"). With 'Show names in Windows notifications' on, the Windows toast is
        titled with the subject and carries the status as its text - like Claude's own notifications. Names only, never mail or
        document content. `record=False` shows a toast without adding a row to the notification list (chat answers)."""
        from ..files.checks import clean_label
        level = self.settings.get("notifications.content_level")
        shown_body = None
        if level == "summary" and body and sensitivity < Sensitivity.CONFIDENTIAL:
            shown_body = body[:200]
        subj = clean_label(subject or "")[:60]
        stat = clean_label(status or "")[:100]
        list_title = f"{title}: {subj}" if subj else title
        if subj and self.settings.get("notifications.show_names"):
            toast_title, toast_body = subj, " - ".join(x for x in (stat, shown_body if kind != "chat" else None) if x) or None
        else:
            toast_title, toast_body = title, shown_body
        nid = None
        if record:
            nid = new_id("ntf")
            self.db.insert("notifications", {"id": nid, "kind": kind, "title": list_title[:120], "body": shown_body,
                                             "sensitivity": int(sensitivity), "screen": screen, "ref_id": ref_id,
                                             "created_at": now_iso()})
        toast = bool(self.settings.get("notifications.toasts")) and not self._quiet()
        # screen/ref_id only open a screen in the app; links never carry parameters that change anything (39.2)
        self.emit("notify", {"id": nid, "kind": kind, "title": list_title[:120], "body": shown_body, "screen": screen, "ref_id": ref_id if kind == "chat" else None,
                             "toast": toast, "toast_title": toast_title[:120], "toast_body": (toast_body or "")[:300] or None,
                             "private": sensitivity > Sensitivity.PUBLIC})

    def notifications(self, limit: int = 100) -> list[dict[str, Any]]:
        return self.db.all("SELECT * FROM notifications ORDER BY created_at DESC LIMIT ?", (limit,))

    def mark_read(self, nid: str | None = None) -> None:
        if nid:
            self.db.update("notifications", "id", nid, {"read": 1})
        else:
            self.db.execute("UPDATE notifications SET read=1")
