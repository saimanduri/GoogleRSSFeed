"""Email monitoring skills: ready-made routines for classic Outlook that the user switches on and off.

Each skill is a routine template (declarative: a schedule, an instruction and a fixed list of READ-ONLY tools).
Switching a skill on creates (or re-activates) a normal routine tagged with `template_id`; switching it off pauses
it. Nothing here can send, delete or move mail, and the tool list can never widen beyond what is written below.
The mail itself is pre-classified by deterministic code in the Outlook worker (approval wording, deadlines,
urgency, questions, replied-or-not); the language model only reads the flagged messages and explains them.
"""
from __future__ import annotations

import json
from typing import Any

from pa_common.errors import PAError

from .schedule import describe

NOTHING = "NOTHING_NEW"

_COMMON = (
    "You are checking the user's classic Outlook mailbox (read-only). Work only from tool results; never invent senders, "
    "dates or content. Times are the user's local time. Be brief: one line per message. If there is nothing to report, "
    f"your final answer must be exactly {NOTHING}. Do not ask the user questions; this runs in the background. The first data-gathering tool "
    "call(s) described below have ALREADY been made for you and their results are in this conversation: do not repeat them, analyse them "
    "(you may still call outlook_local.get_message to read a message in full)."
)

TOOLS_MAIL = ["outlook_local.digest", "outlook_local.get_message", "time.now"]


def _t(id, title, description, schedule, objective, tools, fmt="digest", needs_vips=False, pre=None):  # noqa: A002
    return {"id": id, "title": title, "description": description, "schedule": schedule, "objective": f"{_COMMON}\n\n{objective}",
            "tools": tools, "output_format": fmt, "needs_vips": needs_vips, "pre": pre or []}


TEMPLATES: list[dict[str, Any]] = [
    _t("inbox_hourly", "Hourly inbox check",
       "Every hour (08:00-20:00, Mon-Sat): checks the mail that arrived in the last hour, sorts it into needs approval / deadline / urgent / "
       "questions for you / just information, and highlights what is worth reading.",
       "0 8-20 * * 1-6",
       "Call outlook_local.digest with since_minutes=65 and max_results=50. Group the messages under these headings (a message can appear "
       "once, in the first heading that applies): NEEDS MY APPROVAL (flag APPROVAL), DEADLINE (flag DEADLINE:date), URGENT, QUESTIONS FOR ME, "
       "FOR INFORMATION. For every APPROVAL message call outlook_local.get_message, read it fully and say in one sentence what exactly "
       "approval is sought for and whether it is worth reading in full. Give each item as: sender - subject - one-line point. End with the "
       "counts per heading. Show messages where 'you are in: To' before CC/other.",
       TOOLS_MAIL, pre=[("outlook_local.digest", {"since_minutes": 65, "max_results": 50})]),
    _t("approvals", "Emails waiting for my approval",
       "Every 2 hours (08:00-18:00, Mon-Sat): finds mail from the last 5 days that asks for your approval and that you have not replied to, "
       "reads each one fully and tells you what exactly is asked.",
       "0 8-18/2 * * 1-6",
       "Call outlook_local.digest with only_flag='approval', unanswered_only=true, since_minutes=7200 and max_results=20. For each message call "
       "outlook_local.get_message and write: who asks, what exactly approval is sought for (amount, scope, date, authority if stated), the "
       "deadline if any, what the attachments are, and your one-line view on whether the user can approve straight away or needs more "
       "information. Order by deadline, then by age (oldest first). Never say that anything was approved or rejected.",
       TOOLS_MAIL, pre=[("outlook_local.digest", {"only_flag": "approval", "unanswered_only": True, "since_minutes": 7200, "max_results": 20})]),
    _t("deadlines", "Deadline radar",
       "Twice a day (09:00 and 15:00, Mon-Fri): highlights mail from the last 14 days that has a deadline that is overdue or within 3 days.",
       "0 9,15 * * 1-5",
       "Call outlook_local.digest with only_flag='deadline', deadline_within_days=3, since_minutes=20160 and max_results=40 (the tool already keeps "
       "only mail whose deadline is overdue, today or within 3 days). If it lists 0 messages, your final answer is exactly NOTHING_NEW. Otherwise "
       "list them sorted by date as: DEADLINE date - sender - subject - what is due - whether the user already replied.",
       TOOLS_MAIL, pre=[("outlook_local.digest", {"only_flag": "deadline", "deadline_within_days": 3, "since_minutes": 20160, "max_results": 40})]),
    _t("morning_brief", "Morning briefing",
       "08:30 Mon-Sat: overnight mail in numbers, the important items that need you, and today's meetings.",
       "30 8 * * 1-6",
       "1) Call outlook_local.mail_stats with since_minutes=840. 2) Call outlook_local.digest with since_minutes=840, only_flagged=true and "
       "max_results=20. 3) Call time.now and outlook_local.calendar_read for today (start 00:00, end 23:59, local time). Write: one line of "
       "numbers (received, unread, in To), then 'Needs you' (approvals, deadlines, urgent), then 'Today's meetings' with times.",
       ["outlook_local.mail_stats", "outlook_local.digest", "outlook_local.get_message", "outlook_local.calendar_read", "time.now"],
       pre=[("outlook_local.mail_stats", {"since_minutes": 840}), ("outlook_local.digest", {"since_minutes": 840, "only_flagged": True, "max_results": 20}),
            ("outlook_local.calendar_read", {"start": "{TODAY_START}", "end": "{TODAY_END}"})]),
    _t("meeting_prep", "Meeting preparation",
       "08:45 Mon-Fri: for each of today's meetings, finds the related recent mail so you walk in prepared.",
       "45 8 * * 1-5",
       "Call time.now and outlook_local.calendar_read for today (start 00:00, end 23:59). For each meeting call outlook_local.search with "
       "query set to the most distinctive 2-3 words of the meeting subject, since_minutes=20160 and max_results=3. Write per meeting: time, "
       "subject, then up to 3 related mails (sender, date, one-line point). Skip meetings with no related mail.",
       ["outlook_local.calendar_read", "outlook_local.search", "outlook_local.get_message", "time.now"],
       pre=[("outlook_local.calendar_read", {"start": "{TODAY_START}", "end": "{TODAY_END}"})]),
    _t("followups", "Follow-up tracker",
       "16:00 Mon-Fri: lists mail you sent in the last 7 days that nobody has answered yet (older than 2 days).",
       "0 16 * * 1-5",
       "Call outlook_local.awaiting_reply with days=7 and min_age_hours=48 and max_results=25. List each as: sent date - to whom - subject - days "
       "waiting. Put the oldest first. Add a one-line suggestion for the three oldest (for example 'send a reminder'), but do not draft or send anything.",
       ["outlook_local.awaiting_reply", "outlook_local.get_message", "time.now"], pre=[("outlook_local.awaiting_reply", {"days": 7, "min_age_hours": 48, "max_results": 25})]),
    _t("reply_needed", "Mail that needs my reply",
       "Every 2 hours (09:00-17:00, Mon-Fri): finds questions addressed to you (you are in To) from the last 2 days that you have not answered.",
       "0 9-17/2 * * 1-5",
       "Call outlook_local.digest with only_flag='question', unanswered_only=true, since_minutes=2880 and max_results=30. Keep only messages "
       "where 'you are in: To'. For each give: sender - subject - the question in one line - how old. Most recent first.",
       TOOLS_MAIL, pre=[("outlook_local.digest", {"only_flag": "question", "unanswered_only": True, "since_minutes": 2880, "max_results": 30})]),
    _t("vip_alert", "VIP sender alert",
       "Every hour (08:00-20:00, Mon-Sat): mail from the people you list (boss, key clients...) in the last hour. Add their names below.",
       "0 8-20 * * 1-6",
       "The VIP names are: {VIPS}. For each name call outlook_local.digest with sender=<that name>, since_minutes=65 and max_results=10. "
       "List every message as: VIP - subject - one-line point - flags. If a VIP message has APPROVAL or DEADLINE flags read it with "
       "outlook_local.get_message and say what is asked.",
       TOOLS_MAIL, needs_vips=True, pre=[("outlook_local.digest", {"sender": "{VIP}", "since_minutes": 65, "max_results": 10})]),
    _t("weekly_summary", "Weekly wrap-up",
       "Friday 16:30: the week's mail in numbers, the threads that still need you, and what is waiting for a reply.",
       "30 16 * * 5",
       "1) outlook_local.mail_stats with since_minutes=10080. 2) outlook_local.digest with only_flagged=true, unanswered_only=true, "
       "since_minutes=10080 and max_results=30. 3) outlook_local.awaiting_reply with days=7. Write: numbers; 'Still open for you' (top 10); "
       "'Waiting on others' (top 5); and one sentence on how the week compared with a normal week only if you can tell from the data.",
       ["outlook_local.mail_stats", "outlook_local.digest", "outlook_local.awaiting_reply", "outlook_local.get_message", "time.now"],
       pre=[("outlook_local.mail_stats", {"since_minutes": 10080}), ("outlook_local.digest", {"only_flagged": True, "unanswered_only": True, "since_minutes": 10080, "max_results": 30}),
            ("outlook_local.awaiting_reply", {"days": 7})]),
    _t("cleanup_report", "Inbox clean-up report",
       "Monday 09:00: who sends you the most mail, so you can unsubscribe, filter or delegate.",
       "0 9 * * 1",
       "Call outlook_local.mail_stats with since_minutes=10080 and top_senders=15. List the top senders with counts and the share of the total, "
       "and suggest for each whether it looks like a newsletter / automated notice (candidate for a rule or unsubscribe) or a person. "
       "Do not change anything.",
       ["outlook_local.mail_stats", "time.now"], fmt="table", pre=[("outlook_local.mail_stats", {"since_minutes": 10080, "top_senders": 15})]),
]
BY_ID = {t["id"]: t for t in TEMPLATES}


class EmailSkills:
    def __init__(self, gw):
        self.gw = gw
        self.db = gw.db

    # ------------------------------------------------------------------ helpers
    def _vips(self) -> list[str]:
        return [str(v).strip() for v in (self.gw.settings.get("emailskills.vips") or []) if str(v).strip()]

    def render(self, mission: dict[str, Any]) -> str:
        """Objective text at run time (VIP names are read from settings so edits apply to the next run)."""
        text = mission["objective"]
        if "{VIPS}" in text:
            text = text.replace("{VIPS}", ", ".join(f'"{v}"' for v in self._vips()) or "(none set)")
        return text

    def prefetch(self, mission: dict[str, Any]) -> list[dict[str, Any]]:
        """Tool calls the application makes ITSELF before the model starts (so the model only has to explain real data).
        Dates are filled in at run time in the user's local time; VIP skills get one call per VIP name."""
        from datetime import datetime  # noqa: PLC0415
        t = BY_ID.get(mission.get("template_id") or "")
        if not t:
            return []
        now = datetime.now()
        fill = {"{TODAY_START}": now.strftime("%Y-%m-%dT00:00:00"), "{TODAY_END}": now.strftime("%Y-%m-%dT23:59:59")}
        out: list[dict[str, Any]] = []
        for tool, args in t["pre"]:
            if "{VIP}" in json.dumps(args):
                for name in self._vips()[:8]:
                    out.append({"tool": tool, "args": {k: (name if v == "{VIP}" else v) for k, v in args.items()}})
                continue
            out.append({"tool": tool, "args": {k: fill.get(v, v) if isinstance(v, str) else v for k, v in args.items()}})
        return out

    def _row(self, tid: str) -> dict[str, Any] | None:
        return self.db.one("SELECT * FROM missions WHERE template_id=? AND status!='CANCELLED' ORDER BY created_at DESC", (tid,))

    def _last(self, mid: str) -> dict[str, Any] | None:
        return self.db.one("SELECT id, state, result, updated_at, error FROM tasks WHERE mission_id=? AND parent_task_id IS NULL "
                           "ORDER BY created_at DESC", (mid,))

    # ------------------------------------------------------------------ api
    def status(self) -> dict[str, Any]:
        conn = self.gw.connectors
        usable, reason = conn.usable("outlook_local", "mission")
        from ..connectors.outlook_local import outlook_classic_installed
        return {"outlook_installed": outlook_classic_installed(), "usable": usable, "reason": reason,
                "connector_enabled": bool(conn.row("outlook_local")["enabled"]), "vips": self._vips(),
                "show_nav": bool(self.gw.settings.get("ui.show_outlook_nav"))}

    def list(self, mark_seen: bool = False) -> dict[str, Any]:
        items = []
        for t in TEMPLATES:
            m = self._row(t["id"])
            last = self._last(m["id"]) if m else None
            sched = json.loads(m["schedule_json"]) if m else {"type": "cron", "cron": t["schedule"]}
            res = (last or {}).get("result") or ""
            items.append({
                "id": t["id"], "title": t["title"], "description": t["description"], "needs_vips": t["needs_vips"],
                "tools": t["tools"], "enabled": bool(m and m["status"] == "ACTIVE"), "mission_id": m["id"] if m else None,
                "status": m["status"] if m else "OFF", "schedule_text": describe(sched), "next_run_at": m["next_run_at"] if m else None,
                "last_run_at": m["last_run_at"] if m else None, "last_state": last["state"] if last else None,
                "last_at": last["updated_at"] if last else None, "last_error": last["error"] if last else None,
                "last_result": "" if res.strip() == NOTHING else res[:20000], "last_nothing": res.strip() == NOTHING,
            })
        if mark_seen:
            self.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('outlook.last_seen',?)", (self._now(),))
            self.gw.emit("emailskills.changed", {})
        return {"skills": items, **self.status()}

    @staticmethod
    def _now() -> str:
        from pa_common.timeutil import now_iso
        return now_iso()

    def unseen(self) -> int:
        """Results of email skills finished since the Outlook screen was last opened (nothing-new runs do not count)."""
        seen = self.db.one("SELECT value FROM meta WHERE key='outlook.last_seen'")
        since = seen["value"] if seen else ""
        return int(self.db.scalar(
            "SELECT count(*) FROM tasks t JOIN missions m ON m.id=t.mission_id WHERE m.template_id IS NOT NULL AND t.parent_task_id IS NULL "
            "AND t.state='COMPLETED' AND t.updated_at>? AND COALESCE(t.result,'')!='' AND TRIM(t.result)!=?", (since, NOTHING)) or 0)

    def set_enabled(self, tid: str, enabled: bool) -> dict[str, Any]:
        t = BY_ID.get(tid)
        if not t:
            raise PAError("unknown email skill", code="not_found")
        m = self._row(tid)
        if not enabled:
            if m and m["status"] == "ACTIVE":
                self.gw.missions.set_status(m["id"], "PAUSED", "email skill switched off")
            self.gw.audit.write("emailskill.disabled", "mission", skill=tid)
            self.gw.emit("emailskills.changed", {})
            return {"ok": True}
        st = self.status()
        if not st["outlook_installed"]:
            raise PAError("Classic Outlook is not installed on this PC (the new Outlook cannot be read).", code="needs_setup")
        if not st["connector_enabled"]:
            raise PAError("Turn on Local Outlook first: Settings > Connectors > Local Outlook (classic), with 'Use in missions' on.", code="needs_setup")
        if not st["usable"]:
            raise PAError(st["reason"] or "Local Outlook is not usable for routines yet (Settings > Connectors).", code="needs_setup")
        if t["needs_vips"] and not self._vips():
            raise PAError("Add at least one VIP name first (Email monitoring > VIP names).", code="needs_setup")
        if m is None:
            mid = self.gw.missions.create({"name": f"Email: {t['title']}", "objective": t["objective"], "schedule": {"type": "cron", "cron": t["schedule"]},
                                           "allowed_tools": t["tools"], "kind": "routine", "output_format": t["output_format"],
                                           "notification_level": "notify", "missed_run_policy": "SKIP", "template_id": t["id"]})
            m = self.gw.missions.get(mid)
        self.gw.missions.activate(m["id"])
        self.gw.audit.write("emailskill.enabled", "mission", skill=tid, mission_id=m["id"])
        self.gw.emit("emailskills.changed", {})
        return {"ok": True, "mission_id": m["id"]}

    def run_now(self, tid: str) -> dict[str, Any]:
        if tid not in BY_ID:
            raise PAError("unknown email skill", code="not_found")
        m = self._row(tid)
        if m is None:
            self.set_enabled(tid, True)
            m = self._row(tid)
        return {"task_id": self.gw.missions.run_now(m["id"], "USER", "run now")}
