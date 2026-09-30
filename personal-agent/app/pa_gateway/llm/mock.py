"""Deterministic developer model (dev mode only). Lets the whole app - chat, tools, approvals,
reminders, missions, the step timeline - be exercised without any real model installed.
It follows the agent's JSON action protocol (see pa_core/prompts.py)."""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta


def _last(messages: list[dict], role: str) -> str:
    for m in reversed(messages):
        if m.get("role") == role:
            return str(m.get("content", ""))
    return ""


def _tool_available(messages: list[dict], name: str) -> bool:
    return any(name in str(m.get("content", "")) for m in messages if m.get("role") == "system")


def _due(text: str) -> str:
    now = datetime.now()
    day = now + timedelta(days=1) if "tomorrow" in text.lower() else now
    m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if m:
        day = datetime.fromisoformat(m.group(1))
    t = re.search(r"\bat (\d{1,2})(?::(\d{2}))?\s*(am|pm)?", text.lower())
    hour, minute = 9, 0
    if t:
        hour, minute = int(t.group(1)), int(t.group(2) or 0)
        if t.group(3) == "pm" and hour < 12:
            hour += 12
    return day.replace(hour=hour, minute=minute, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M")


def mock_complete(messages: list[dict]) -> str:
    system = _last(messages, "system")
    last_user = _last(messages, "user")
    if "MISSION_FORM_JSON" in system:
        return json.dumps({"name": "Morning mail summary", "objective": last_user[:300] or "Summarize important mail",
                           "schedule": {"type": "cron", "cron": "30 7 * * 1-5"}, "tools": ["m365.search_mail", "notify.user"],
                           "notification_level": "notify", "output_format": "markdown"})
    if "SUMMARIZE_CONVERSATION" in system:
        return "Summary of earlier conversation (data only): the user and the agent discussed the topics above."
    if "PLAIN_ANSWER_ONLY" in system:
        return "OK"
    last = messages[-1] if messages else {}
    c = str(last.get("content", ""))
    if "<data source" in c[:300]:
        body = re.sub(r"<[^>]+>", "", c)
        return json.dumps({"thought": "I have the tool result; answering.", "action": "final",
                           "answer": "Here is what I found:\n\n" + body.strip()[:800]})
    if c.startswith("[") and ("denied" in c or "error" in c or "unavailable" in c):
        return json.dumps({"action": "final", "answer": "I could not complete that step: " + c[:300]})
    text = last_user.lower()

    def act(tool: str, args: dict, thought: str) -> str:
        if not _tool_available(messages, tool):
            return json.dumps({"action": "final", "answer": f"(dev model) I would use {tool}, but it is not available here."})
        return json.dumps({"thought": thought, "action": "tool", "tool": tool, "args": args})

    if "remind me" in text:
        what = re.sub(r"(?i)remind me (to )?", "", last_user).strip()
        return act("reminders.propose", {"text": what[:200] or "Reminder", "due_at": _due(last_user)}, "Proposing a reminder.")
    if text.startswith("remember"):
        return act("memory.propose", {"content": last_user[8:].strip(" :,")[:500] or last_user, "type": "preference",
                                      "importance": "normal"}, "Proposing a memory.")
    if "search" in text or "look up" in text or "news" in text:
        q = re.sub(r"(?i)^(please )?(search( the web)?( for)?|look up)\s*", "", last_user).strip()
        return act("web.search", {"query": q[:200] or last_user[:200], "count": 5}, "Searching the web.")
    if text.startswith("fetch ") or "http" in text:
        url = re.search(r"https?://\S+", last_user)
        if url:
            return act("web.fetch", {"url": url.group(0)}, "Fetching the page.")
    if "my files" in text or "list files" in text:
        return act("files.list", {"folder": "/"}, "Listing files.")
    if "calculate" in text or "python" in text:
        expr = re.sub(r"[^0-9+\-*/(). ]", "", last_user) or "1+1"
        return act("python.run", {"code": f"print({expr})"}, "Running Python in the sandbox.")
    if "mail" in text or "email" in text:
        tool = "m365.search_mail" if _tool_available(messages, "m365.search_mail") else "outlook_local.search"
        return act(tool, {"query": "", "max_results": 5}, "Searching mail.")
    if "every" in text and ("day" in text or "morning" in text or "week" in text):
        return act("missions.propose", {"name": "Routine", "objective": last_user, "schedule": last_user[:200]},
                   "Proposing a routine for you to review.")
    if "what time" in text or "date" in text:
        return act("time.now", {}, "Checking the time.")
    return json.dumps({"thought": "Answering directly.", "action": "final",
                       "answer": f"(developer model) You said: {last_user[:500]}"})
