"""Executors for built-in (non-connector) tools. Each runs only after the full gateway sequence."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from pa_common.sensitivity import Sensitivity, Trust

from .base import ExecContext, ToolFailed, ToolResult


def register_builtin_tools(gw) -> None:
    tg = gw.tools

    def files_list(a: dict[str, Any], c: ExecContext) -> ToolResult:
        rows = gw.files.list(folder=None if a["folder"] in ("", "/") else a["folder"], query=a["query"], include_rejected=False)
        sens = max([int(r["sensitivity"]) for r in rows] or [0])
        text = "\n".join(f"- {r['id']} | {r['folder']}{'' if r['folder'].endswith('/') else '/'}{r['name']} | {r['size_bytes']} bytes | "
                         f"{Sensitivity(r['sensitivity']).name} | {r['status']}" for r in rows[:200]) or "No files."
        # listing names/metadata only: sensitivity of names is the max label (file names can be sensitive too)
        return ToolResult(text, sens, "files", {"count": len(rows)})

    def files_read(a: dict[str, Any], c: ExecContext) -> ToolResult:
        text, row = gw.files.text(a["file_id"], a["max_chars"])
        hidden = json.loads(row["hidden_json"] or "[]")
        gw.budgets.add(c.task["id"], "files")
        return ToolResult(text, int(row["sensitivity"]), "files", {"file_id": row["id"], "name": row["name"]}, hidden=hidden)

    def files_write(a: dict[str, Any], c: ExecContext) -> ToolResult:
        # derived items inherit the highest input level (spec 13.2)
        f = gw.files.ingest(name=a["name"] if "." in a["name"] else a["name"] + ".md", data=a["content"].encode("utf-8"),
                            source="agent", sensitivity=c.hwm, folder=a["folder"], run_async=False)
        return ToolResult(f"Saved {f['name']} to My Files ({f['id']}, {f['status']}).", c.hwm, "files", {"file_id": f["id"]},
                          trust=Trust.TRUSTED)

    def notify_user(a: dict[str, Any], c: ExecContext) -> ToolResult:
        gw.notify("agent", a["title"], a["message"], c.hwm, "home", c.task["id"])
        return ToolResult("Notification shown.", 0, "notify", {}, trust=Trust.TRUSTED)

    def history_search(a: dict[str, Any], c: ExecContext) -> ToolResult:
        hits = gw.history.search(a["query"], a["limit"])
        sens = 0
        for h in hits:
            if h["kind"] == "file":
                f = gw.files.get(h["ref_id"])
                sens = max(sens, int(f["sensitivity"]) if f else 0)
            elif h["kind"] == "run":
                r = gw.db.one("SELECT hwm FROM runs WHERE id=?", (h["ref_id"],))
                sens = max(sens, int(r["hwm"]) if r else 0)
            elif h["kind"] == "chat":
                m = gw.db.one("SELECT sensitivity FROM chat_messages WHERE id=?", (h["ref_id"],))
                sens = max(sens, int(m["sensitivity"]) if m else 0)
        text = "\n".join(f"- [{h['kind']}] {h['title']} ({h['created_at'][:10]}): {h['snippet']}" for h in hits) or "No matches."
        return ToolResult(text, sens, "history", {"count": len(hits)})

    def memory_search(a: dict[str, Any], c: ExecContext) -> ToolResult:
        hits = gw.memory.search(a["query"], a["limit"])
        sens = max([int(h["sensitivity"]) for h in hits] or [0])
        text = "\n".join(f"- ({h['trust']}, {h['type']}) {h['content']}" for h in hits) or "Nothing remembered about that."
        return ToolResult(text, sens, "memory", {"count": len(hits)}, trust=Trust.TRUSTED)

    def memory_propose(a: dict[str, Any], c: ExecContext) -> ToolResult:
        tainted = gw.sessionlog.has_untrusted_instructions_risk(c.task["session_id"])
        res = gw.memory.propose(a["content"], a["type"], a["importance"], task=c.task, sensitivity=c.hwm, tainted=tainted,
                                provenance=gw.sessionlog.sources(c.task["session_id"]))
        msg = {"proposed": "Proposed - the user will confirm it in Memory > Proposed.",
               "active": "Saved (low-importance preference auto-confirmed).", "duplicate": "Already remembered.",
               "rejected": f"Not saved: {res.get('reason')}", "memory_disabled": "Memory is turned off."}[res["status"]]
        return ToolResult(msg, 0, "memory", res, trust=Trust.TRUSTED)

    def reminders_propose(a: dict[str, Any], c: ExecContext) -> ToolResult:
        if not c.approved:
            raise ToolFailed("reminders are scheduled only after the user confirms")
        rid = gw.reminders.create(a["text"], a["due_at"], a.get("timezone"), source="agent", run_id=c.run_id)
        r = gw.db.one("SELECT due_at FROM reminders WHERE id=?", (rid,))
        return ToolResult(f"Reminder scheduled for {a['due_at']} (local time).", 0, "reminders", {"reminder_id": rid, "due_at_utc": r["due_at"]},
                          trust=Trust.TRUSTED)

    def missions_propose(a: dict[str, Any], c: ExecContext) -> ToolResult:
        mid = gw.missions.create({"name": a["name"], "objective": a["objective"], "schedule": a["schedule"],
                                  "allowed_tools": a["tools"], "kind": "routine"}, proposed_by="agent")
        gw.home_event("mission_proposed", "info", f"Proposed routine: {a['name']}", "Review and activate it in Missions.", mid)
        return ToolResult(f"Created a DRAFT routine '{a['name']}'. The user must review and activate it in Missions.", 0,
                          "missions", {"mission_id": mid}, trust=Trust.TRUSTED)

    def skills_propose(a: dict[str, Any], c: ExecContext) -> ToolResult:
        res = gw.skills.propose({"name": a["name"], "description": a["description"], "steps": a["steps"], "tools": a["tools"]},
                                source="agent", task=c.task)
        return ToolResult(f"Skill proposal recorded (version {res['version']}{', TAINTED origin' if res['tainted'] else ''}). "
                          "The user reviews it in Settings > Tools & Skills.", 0, "skills", res, trust=Trust.TRUSTED)

    def subtask(a: dict[str, Any], c: ExecContext) -> ToolResult:
        parent = c.task
        # a sub-agent never has more rights than its parent; clean public research starts at PUBLIC
        hwm = 0 if a["public_research"] else int(parent["hwm"])
        allowed = gw.tasks.allowed_tools(parent)
        if a["public_research"]:
            allowed = [t for t in (allowed or ["web.search", "web.fetch", "time.now"]) if t in ("web.search", "web.fetch", "time.now")]
        running = gw.db.scalar("SELECT count(*) FROM tasks WHERE parent_task_id=? AND state IN ('QUEUED','RUNNING')", (parent["id"],)) or 0
        if running >= int(gw.settings.get("tools.subagents_max")):
            raise ToolFailed("too many sub-agents running for this task")
        run_id = gw.runs.start("subtask", a["objective"][:120], chat_id=parent.get("chat_id"), mission_id=parent.get("mission_id"))
        gw.sessionlog.append(run_id, "user.message", a["objective"], role="user", source="agent", trust=Trust.UNTRUSTED
                             if not a["public_research"] else Trust.INFERRED, sensitivity=hwm)
        child = gw.tasks.create(objective=a["objective"], trigger=parent["trigger_type"], run_id=run_id, chat_id=parent.get("chat_id"),
                                mission_id=parent.get("mission_id"), allowed_tools=allowed, parent_task_id=parent["id"],
                                depth=int(parent["depth"]) + 1, priority=3, hwm=hwm, budget=json.loads(parent.get("budget_json") or "{}"),
                                session_id=run_id)
        gw.runs.attach_task(run_id, child["id"])
        gw.budgets.add(parent["id"], "subtasks")
        done = gw.tasks.wait_done(child["id"], 1800, c.cancelled)
        if not done or done["state"] != "COMPLETED":
            raise ToolFailed(f"sub-task ended as {done['state'] if done else 'unknown'}")
        # child reads raise the parent's level (spec 39.8) - enforced in TaskService.raise_hwm; result carries it too
        return ToolResult(done.get("result") or "", int(done["hwm"]), "subagent", {"task_id": child["id"]}, trust=Trust.UNTRUSTED)

    def time_now(a: dict[str, Any], c: ExecContext) -> ToolResult:
        now = datetime.now().astimezone()
        return ToolResult(now.strftime("%A %d %B %Y, %H:%M (%Z, UTC%z)"), 0, "builtin", {"iso": now.isoformat()}, trust=Trust.TRUSTED)

    tg.register("files.list", files_list)
    tg.register("files.read", files_read)
    tg.register("files.write", files_write)
    tg.register("python.run", gw.sandbox.run_tool)
    tg.register("notify.user", notify_user)
    tg.register("history.search", history_search)
    tg.register("memory.search", memory_search)
    tg.register("memory.propose", memory_propose)
    tg.register("reminders.propose", reminders_propose)
    tg.register("missions.propose", missions_propose)
    tg.register("skills.propose", skills_propose)
    tg.register("agent.subtask", subtask)
    tg.register("time.now", time_now)
