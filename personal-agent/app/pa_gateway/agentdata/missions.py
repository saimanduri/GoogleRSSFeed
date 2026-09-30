"""Missions and Routines (spec 16, 39.15; user requirement "Claude Routines").

A ROUTINE is a mission with a simple recurring prompt ("every weekday at 7:30 summarise important
mail"). Both share one model. Only the user creates, activates, widens or resumes a mission; the
agent can only PROPOSE one (status DRAFT, proposed_by=agent). Missions may only narrow settings.
"""
from __future__ import annotations

import json
import random
from datetime import timedelta
from typing import Any

from pa_common.errors import PAError
from pa_common.ids import new_id
from pa_common.timeutil import now_iso, parse_iso, to_iso, utcnow

from ..policy.tools_registry import BY_NAME, EXTERNAL_WRITE
from . import schedule as sch

STATUSES = ("DRAFT", "ACTIVE", "PAUSED", "SUSPENDED", "WAITING_APPROVAL", "COMPLETED", "FAILED", "CANCELLED")
MISSED = ("SKIP", "RUN_ONCE", "RUN_ALL")
MAX_CATCHUP = 5


def local_tz() -> str:
    try:
        from tzlocal import get_localzone_name  # type: ignore[import-not-found]
        return get_localzone_name()
    except Exception:  # noqa: BLE001
        return "UTC"


class MissionService:
    def __init__(self, gw):
        self.gw = gw
        self.db = gw.db

    # ------------------------------------------------------------------ CRUD
    def _normalize(self, d: dict[str, Any]) -> dict[str, Any]:
        tz = d.get("timezone") or local_tz()
        schedule = d.get("schedule")
        if isinstance(schedule, str):
            parsed = sch.parse_plain(schedule)
            if parsed is None:
                parts = schedule.split()
                parsed = {"type": "cron", "cron": schedule} if len(parts) == 5 else None
            if parsed is None:
                raise PAError("could not understand the schedule; try 'every weekday at 07:30' or a cron expression",
                              code="invalid_schedule")
            schedule = parsed
        try:
            sch.validate(schedule, tz)
        except (sch.ScheduleError, ValueError, KeyError) as e:
            raise PAError(str(e), code="invalid_schedule") from e
        tools = [t for t in d.get("allowed_tools", []) if t in BY_NAME]
        disabled = set(self.gw.settings.get("tools.disabled") or [])
        if disabled & set(tools):
            raise PAError(f"mission uses disabled tools: {', '.join(sorted(disabled & set(tools)))}", code="invalid_request")
        connectors = sorted({BY_NAME[t].connector for t in tools if BY_NAME[t].connector in ("web", "m365", "outlook_local")})
        missed = d.get("missed_run_policy", "RUN_ONCE")
        if missed not in MISSED:
            raise PAError("invalid missed-run policy", code="invalid_request")
        budget = {k: v for k, v in (d.get("budget") or {}).items() if isinstance(v, (int, float)) and v >= 0}
        return {"name": str(d.get("name") or "Untitled")[:120], "objective": str(d.get("objective") or "")[:8000],
                "schedule_json": schedule, "timezone": tz, "allowed_tools_json": tools, "allowed_connectors_json": connectors,
                "overnight": int(bool(d.get("overnight", True))), "budget_json": budget, "model_id": d.get("model_id") or None,
                "output_format": d.get("output_format", "markdown")[:40], "notification_level": d.get("notification_level", "notify")[:20],
                "missed_run_policy": missed, "resource_wait_minutes": int(d.get("resource_wait_minutes", 60)),
                "kind": "routine" if d.get("kind") == "routine" else "mission"}

    def create(self, d: dict[str, Any], proposed_by: str = "user") -> str:
        if not str(d.get("objective") or "").strip():
            raise PAError("objective is required", code="invalid_request")
        row = self._normalize(d)
        mid = new_id("msn")
        row.update({"id": mid, "status": "DRAFT", "proposed_by": proposed_by, "created_at": now_iso(), "updated_at": now_iso()})
        self.db.insert("missions", row)
        self.gw.audit.write("mission.created", "mission", mission_id=mid, proposed_by=proposed_by, kind=row["kind"],
                            tools=row["allowed_tools_json"])
        self.gw.emit("missions.changed", {"mission_id": mid})
        return mid

    def update(self, mid: str, d: dict[str, Any]) -> dict[str, Any]:
        m = self.get(mid)
        row = self._normalize({**self._as_input(m), **d})
        widened = set(row["allowed_tools_json"]) - set(json.loads(m["allowed_tools_json"]))
        if m["status"] == "ACTIVE" and widened:
            row["status"] = "PAUSED"  # widening an active mission requires re-activation (user) and step-up
        row["updated_at"] = now_iso()
        self.db.update("missions", "id", mid, row)
        self.gw.audit.write("mission.updated", "mission", mission_id=mid, widened=sorted(widened))
        if self.get(mid)["status"] == "ACTIVE":
            self._schedule_next(mid)
        self.gw.emit("missions.changed", {"mission_id": mid})
        return {"widened": sorted(widened), "status": self.get(mid)["status"]}

    @staticmethod
    def _as_input(m: dict[str, Any]) -> dict[str, Any]:
        return {"name": m["name"], "objective": m["objective"], "schedule": json.loads(m["schedule_json"]), "timezone": m["timezone"],
                "allowed_tools": json.loads(m["allowed_tools_json"]), "overnight": bool(m["overnight"]),
                "budget": json.loads(m["budget_json"]), "model_id": m["model_id"], "output_format": m["output_format"],
                "notification_level": m["notification_level"], "missed_run_policy": m["missed_run_policy"],
                "resource_wait_minutes": m["resource_wait_minutes"], "kind": m["kind"]}

    def get(self, mid: str) -> dict[str, Any]:
        m = self.db.one("SELECT * FROM missions WHERE id=?", (mid,))
        if not m:
            raise PAError("mission not found", code="not_found")
        return m

    def list(self) -> list[dict[str, Any]]:
        out = []
        for m in self.db.all("SELECT * FROM missions WHERE status!='CANCELLED' ORDER BY created_at DESC"):
            m["schedule"] = json.loads(m.pop("schedule_json"))
            m["schedule_text"] = sch.describe(m["schedule"])
            m["allowed_tools"] = json.loads(m.pop("allowed_tools_json"))
            m["allowed_connectors"] = json.loads(m.pop("allowed_connectors_json"))
            m["budget"] = json.loads(m.pop("budget_json"))
            m["warnings"] = self.warnings(m)
            out.append(m)
        return out

    def warnings(self, m: dict[str, Any]) -> list[str]:
        w = ["Runs only while this PC is on and you stay signed in to Windows (locked is fine)."]
        if "outlook_local" in m["allowed_connectors"]:
            w.append("Needs classic Outlook to be open (or 'Start Outlook when needed').")
        for c in m["allowed_connectors"]:
            ok, reason = self.gw.connectors.usable(c, "mission")
            if not ok:
                w.append(reason)
        return w

    def uses_write_tools(self, m: dict[str, Any]) -> bool:
        return any(BY_NAME[t].side_effect == EXTERNAL_WRITE for t in json.loads(m["allowed_tools_json"]) if t in BY_NAME)

    def set_status(self, mid: str, status: str, reason: str = "") -> None:
        if status not in STATUSES:
            raise PAError("bad status", code="invalid_request")
        self.db.update("missions", "id", mid, {"status": status, "updated_at": now_iso()})
        self.gw.audit.write("mission.status", "mission", mission_id=mid, status=status, reason=reason[:200])
        if status == "ACTIVE":
            self._schedule_next(mid)
        self.gw.emit("missions.changed", {"mission_id": mid})

    def activate(self, mid: str) -> None:
        """User only (RPC layer enforces the role; write-tool missions need a password step-up there)."""
        m = self.get(mid)
        if m["status"] not in ("DRAFT", "PAUSED", "SUSPENDED", "FAILED", "COMPLETED"):
            raise PAError(f"mission is {m['status']}", code="invalid_state")
        self.set_status(mid, "ACTIVE", "activated by user")

    def _schedule_next(self, mid: str, after=None) -> None:
        m = self.get(mid)
        nxt = sch.next_run(json.loads(m["schedule_json"]), m["timezone"], after or utcnow())
        self.db.update("missions", "id", mid, {"next_run_at": to_iso(nxt) if nxt else None})

    # ------------------------------------------------------------------ running
    def run_now(self, mid: str, trigger: str = "SCHEDULE", note: str = "") -> str:
        m = self.get(mid)
        run_id = self.gw.runs.start("mission", f"{m['name']}" + (f" ({note})" if note else ""), mission_id=mid)
        objective = m["objective"] + (f"\n\nOutput format: {m['output_format']}." if m["output_format"] else "")
        self.gw.sessionlog.append(run_id, "user.message", objective, role="user", source="user", trust="TRUSTED")
        t = self.gw.tasks.create(objective=objective, trigger=trigger, run_id=run_id, mission_id=mid,
                                 allowed_tools=json.loads(m["allowed_tools_json"]), budget=json.loads(m["budget_json"]),
                                 priority=5, model_id=m["model_id"], session_id=run_id)
        self.gw.runs.attach_task(run_id, t["id"])
        self.db.update("missions", "id", mid, {"last_run_at": now_iso(), "last_task_id": t["id"]})
        return t["id"]

    def due(self) -> list[dict[str, Any]]:
        return self.db.all("SELECT * FROM missions WHERE status='ACTIVE' AND next_run_at IS NOT NULL AND next_run_at <= ?", (now_iso(),))

    def tick(self) -> int:
        """Fire due missions; apply the missed-run policy for runs missed while the PC was off/locked."""
        fired = 0
        for m in self.due():
            sched = json.loads(m["schedule_json"])
            missed_count = 0
            cursor = parse_iso(m["next_run_at"])
            now = utcnow()
            while cursor <= now and missed_count < 1000:
                missed_count += 1
                nxt = sch.next_run(sched, m["timezone"], cursor)
                if nxt is None:
                    break
                cursor = nxt
            policy = m["missed_run_policy"]
            runs = 1 if missed_count <= 1 else {"SKIP": 0, "RUN_ONCE": 1, "RUN_ALL": min(missed_count, MAX_CATCHUP)}[policy]
            for i in range(runs):
                self.run_now(m["id"], "SCHEDULE", "catch-up" if missed_count > 1 else "")
                fired += 1
            if missed_count > 1:
                self.gw.audit.write("mission.catch_up", "mission", mission_id=m["id"], missed=missed_count, ran=runs, policy=policy)
            nxt = sch.next_run(sched, m["timezone"], now)
            # jitter catch-up scheduling slightly so many missions do not start at once
            if nxt and missed_count > 1:
                nxt += timedelta(seconds=random.randint(0, 90))
            changes: dict[str, Any] = {"next_run_at": to_iso(nxt) if nxt else None}
            if nxt is None and sched.get("type") == "once":
                changes["status"] = "COMPLETED"
            self.db.update("missions", "id", m["id"], changes)
        return fired

    def on_event(self, event: str, info: dict[str, Any]) -> int:
        """LOCAL_EVENT (new file) / EXTERNAL_EVENT (new mail) triggers, rate-limited (spec 16.3)."""
        fired = 0
        limit = int(self.gw.settings.get("triggers.rate_per_hour"))
        since = to_iso(utcnow() - timedelta(hours=1))
        for m in self.db.all("SELECT * FROM missions WHERE status='ACTIVE'"):
            sched = json.loads(m["schedule_json"])
            if sched.get("type") != "event" or sched.get("event") != event:
                continue
            if event == "new_mail":
                allowed = [s.lower() for s in (self.gw.settings.get("triggers.mail.allowed_senders") or [])]
                sender = str(info.get("sender", "")).lower()
                if not allowed or not any(sender == a or sender.endswith("@" + a.lstrip("@")) for a in allowed):
                    continue
            recent = int(self.db.scalar("SELECT count(*) FROM tasks WHERE mission_id=? AND created_at>?", (m["id"], since)) or 0)
            if recent >= limit:
                self.gw.audit.write("trigger.rate_limited", "mission", mission_id=m["id"])
                continue
            trig = "EXTERNAL_EVENT" if event == "new_mail" else "LOCAL_EVENT"
            self.run_now(m["id"], trig, event.replace("_", " "))
            fired += 1
        return fired

    # ------------------------------------------------------------------ plain words (39.15)
    def describe_to_form(self, text: str) -> dict[str, Any]:
        """Fill the mission form from a description. Nothing is saved until the user confirms."""
        form: dict[str, Any] = {"name": text.strip()[:60], "objective": text.strip(), "schedule": sch.parse_plain(text),
                                "timezone": local_tz(), "allowed_tools": [], "notification_level": "notify",
                                "output_format": "markdown", "missed_run_policy": "RUN_ONCE", "kind": "routine"}
        low = text.lower()
        if "mail" in low or "email" in low:
            form["allowed_tools"] += ["m365.search_mail", "m365.get_message"] if self.gw.connectors.usable("m365", "mission")[0] \
                else ["outlook_local.search", "outlook_local.get_message"]
        if "news" in low or "research" in low or "web" in low:
            form["allowed_tools"] += ["web.search", "web.fetch"]
        if "calendar" in low or "meeting" in low:
            form["allowed_tools"].append("m365.calendar_read" if self.gw.connectors.usable("m365", "mission")[0] else "outlook_local.calendar_read")
        if "report" in low or "save" in low or "file" in low:
            form["allowed_tools"].append("files.write")
        form["allowed_tools"].append("notify.user")
        disabled = set(self.gw.settings.get("tools.disabled") or [])
        form["allowed_tools"] = [t for t in dict.fromkeys(form["allowed_tools"]) if t not in disabled]
        if form["schedule"] is None:
            form["schedule"] = {"type": "cron", "cron": "0 9 * * *"}
            form["schedule_note"] = "Could not find a schedule in your description; defaulted to every day at 09:00."
        return form
