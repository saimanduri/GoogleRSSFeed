"""Home screen widgets: small cards the user switches on and off (Home > Widgets). Only enabled widgets are computed.

Mail widgets read Outlook through the same read-only operations the email skills use (counts only; never mail text), refreshed in the background at
most every 5 minutes so that opening Home is instant. Everything else comes from the local database.
"""
from __future__ import annotations

import re
import threading
import time
from datetime import datetime, timedelta
from typing import Any

from pa_common.timeutil import to_iso, utcnow

# id -> (group, title, what it shows)
CATALOG: dict[str, tuple[str, str, str]] = {
    "mail_unread": ("Mail", "Mail: unread today", "Unread mail in your Inbox received today (classic Outlook)."),
    "mail_to_me": ("Mail", "Mail: addressed to me", "Mail received today where you are in To (and how many in CC)."),
    "mail_approvals": ("Mail", "Mail: waiting for my approval", "Mail from the last 7 days that asks for your approval and that you have not answered."),
    "mail_deadlines": ("Mail", "Mail: deadlines", "Mail from the last 7 days that mentions a deadline within the next 7 days or overdue."),
    "mail_awaiting_reply": ("Mail", "Mail: waiting for a reply", "Mail you sent that nobody has answered yet (follow-ups)."),
    "reminders": ("Planner", "Upcoming reminders", "Your next reminders."),
    "routines_next": ("Planner", "Next routine runs", "When your routines and email checks run next."),
    "approvals": ("Attention", "Approvals waiting", "Actions, proposed routines and proposed memories waiting for your yes or no."),
    "attention": ("Attention", "Needs your attention", "Failed or waiting work, files held in quarantine, expired folder shares, backup and recovery key reminders."),
    "recent_files": ("Files", "Recent files", "Files you added lately, with their automatic summary."),
    "storage": ("Files", "My Files storage", "How much of your storage quota is used."),
    "memory_recent": ("Assistant", "What I learned lately", "The newest things the assistant learned about you - forget any with one click in Memory."),
    "models_health": ("System", "AI models and PC", "Which chat, voice and vision models are ready, whether Ollama answers, GPU load."),
    "updates": ("Assistant", "Updates from the assistant", "Finished routines, files created, reminders that fired and other notes (dismiss any)."),
    "activity_today": ("System", "Activity today", "Finished and failed work in the last 24 hours and today's model tokens."),
}
DEFAULT = ["updates", "mail_unread", "mail_to_me", "mail_approvals", "mail_deadlines", "reminders", "approvals", "attention", "routines_next", "recent_files", "models_health", "activity_today"]
MAIL_TTL = 300
_STATS = re.compile(r"Total received: (\d+) \| unread: (\d+) \| you are in To: (\d+) \| in CC: (\d+)")


class HomeWidgets:
    def __init__(self, gw):
        self.gw = gw
        self._mail: dict[str, Any] = {}
        self._mail_ts = 0.0
        self._mail_busy = False
        self._mail_error = ""
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ settings
    def enabled(self) -> list[str]:
        ids = self.gw.settings.get("home.widgets") or []
        return [i for i in ids if i in CATALOG]

    def set_enabled(self, ids: list[str]) -> list[str]:
        clean: list[str] = []
        for i in ids:
            if i in CATALOG and i not in clean:
                clean.append(i)
        self.gw.settings.apply({"home.widgets": clean})
        return clean

    def catalog(self) -> list[dict[str, str]]:
        return [{"id": k, "group": g, "title": t, "about": a} for k, (g, t, a) in CATALOG.items()]

    # ------------------------------------------------------------------ mail (background, cached)
    def _mail_usable(self) -> tuple[bool, str]:
        try:
            ok, reason = self.gw.connectors.usable("outlook_local", "chat")
        except Exception:  # noqa: BLE001
            return False, "Local Outlook is not available"
        return ok, reason

    def _refresh_mail(self) -> None:
        gw = self.gw
        try:
            c = gw.connectors.adapters["outlook_local"]
            now = datetime.now()
            minutes = max(1, int((now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds() // 60) + 1)
            out: dict[str, Any] = {}
            m = _STATS.search(c._call("mail_stats", since_minutes=minutes, scope="inbox").get("text", ""))
            if m:
                out.update(received=int(m.group(1)), unread=int(m.group(2)), to_me=int(m.group(3)), cc_me=int(m.group(4)))
            out["approvals"] = int(c._call("digest", only_flag="approval", unanswered_only=True, since_minutes=10080, max_results=50).get("count", 0))
            out["deadlines"] = int(c._call("digest", only_flag="deadline", deadline_within_days=7, since_minutes=10080, max_results=50).get("count", 0))
            out["awaiting"] = int(c._call("awaiting_reply", days=7, max_results=50).get("count", 0))
            with self._lock:
                self._mail, self._mail_ts, self._mail_error = out, time.time(), ""
        except Exception as e:  # noqa: BLE001 - Outlook closed, prompt waiting, worker busy...
            with self._lock:
                self._mail_error = str(e)[:160] or type(e).__name__
                self._mail_ts = time.time() - MAIL_TTL + 60          # try again in a minute
        finally:
            with self._lock:
                self._mail_busy = False
            gw.emit("home.widgets_changed", {})

    def mail(self, force: bool = False) -> dict[str, Any]:
        ok, reason = self._mail_usable()
        if not ok:
            return {"state": "off", "note": "Switch on Local Outlook in Settings > Connectors to see your mail numbers."}
        with self._lock:
            stale = force or time.time() - self._mail_ts > MAIL_TTL
            start = stale and not self._mail_busy
            if start:
                self._mail_busy = True
            data, err, ts, busy = dict(self._mail), self._mail_error, self._mail_ts, self._mail_busy
        if start:
            threading.Thread(target=self._refresh_mail, daemon=True, name="home-mail").start()
        if not data:
            return {"state": "error" if err and not busy else "loading", "note": err or "Reading Outlook..."}
        return {"state": "ok", "data": data, "age": int(time.time() - ts), "refreshing": busy, "note": err}

    # ------------------------------------------------------------------ everything
    def data(self, ids: list[str] | None = None, refresh_mail: bool = False) -> dict[str, Any]:
        ids = ids if ids is not None else self.enabled()
        out: dict[str, Any] = {}
        mail = self.mail(refresh_mail) if any(i.startswith("mail_") for i in ids) else None
        for i in ids:
            try:
                out[i] = self._one(i, mail)
            except Exception as e:  # noqa: BLE001 - one broken widget must not blank the screen
                out[i] = {"state": "error", "note": type(e).__name__}
        return out

    def _mail_widget(self, key: str, mail: dict[str, Any]) -> dict[str, Any]:
        if mail["state"] != "ok":
            return {"state": mail["state"], "note": mail.get("note", "")}
        d = mail["data"]
        base = {"state": "ok", "age": mail.get("age", 0), "refreshing": mail.get("refreshing", False), "go": {"screen": "outlook"}}
        if key == "mail_unread":
            return {**base, "value": d.get("unread", 0), "sub": f"of {d.get('received', 0)} received today"}
        if key == "mail_to_me":
            return {**base, "value": d.get("to_me", 0), "sub": f"in To today, {d.get('cc_me', 0)} in CC"}
        if key == "mail_approvals":
            return {**base, "value": d.get("approvals", 0), "sub": "unanswered, last 7 days", "tone": "warn" if d.get("approvals") else "ok"}
        if key == "mail_deadlines":
            return {**base, "value": d.get("deadlines", 0), "sub": "due within 7 days or overdue", "tone": "warn" if d.get("deadlines") else "ok"}
        return {**base, "value": d.get("awaiting", 0), "sub": "sent mail without a reply (7 days)"}

    def _one(self, i: str, mail: dict[str, Any] | None) -> dict[str, Any]:
        gw, db = self.gw, self.gw.db
        if i.startswith("mail_"):
            return self._mail_widget(i, mail or {"state": "loading"})
        if i == "reminders":
            rows = db.all("SELECT id, text, due_at, status FROM reminders WHERE status='SCHEDULED' ORDER BY due_at LIMIT 5")
            return {"state": "ok", "items": rows, "value": int(db.scalar("SELECT count(*) FROM reminders WHERE status='SCHEDULED'") or 0), "go": {"screen": "reminders"}}
        if i == "routines_next":
            rows = db.all("SELECT id, name, next_run_at, template_id FROM missions WHERE status='ACTIVE' AND next_run_at IS NOT NULL ORDER BY next_run_at LIMIT 5")
            return {"state": "ok", "items": rows, "value": int(db.scalar("SELECT count(*) FROM missions WHERE status='ACTIVE'") or 0), "go": {"screen": "missions"}}
        if i == "approvals":
            tools = int(gw.approvals.pending_count())
            routines = int(db.scalar("SELECT count(*) FROM missions WHERE status='DRAFT' AND proposed_by='agent'") or 0)
            mem = int(db.scalar("SELECT count(*) FROM memories WHERE status='PROPOSED'") or 0)
            tot = tools + routines + mem
            return {"state": "ok", "value": tot, "sub": f"{tools} actions, {routines} routines, {mem} memories", "tone": "warn" if tot else "ok", "go": {"screen": "approvals"}}
        if i == "attention":
            since = to_iso(utcnow() - timedelta(days=1))
            items: list[dict[str, Any]] = []
            for r in db.all("SELECT id, objective FROM tasks WHERE state IN ('FAILED','TIMED_OUT') AND updated_at>? ORDER BY updated_at DESC LIMIT 3", (since,)):
                items.append({"kind": "failed", "text": f"Failed: {str(r['objective'])[:70]}", "go": {"screen": "tasks"}})
            for r in db.all("SELECT id, objective, wait_reason FROM tasks WHERE state IN ('WAITING_FOR_RESOURCE','OUTCOME_UNKNOWN') ORDER BY updated_at DESC LIMIT 3"):
                items.append({"kind": "waiting", "text": f"Waiting: {str(r['objective'])[:60]} {('(' + str(r['wait_reason'])[:40] + ')') if r['wait_reason'] else ''}", "go": {"screen": "tasks"}})
            held = int(db.scalar("SELECT count(*) FROM files WHERE deleted=0 AND status='REJECTED'") or 0)
            if held:
                items.append({"kind": "files", "text": f"{held} file{'s' if held != 1 else ''} in quarantine", "go": {"screen": "files"}})
            nonce = gw.session.s.nonce
            expired = int(db.scalar("SELECT count(*) FROM local_grants WHERE session_nonce<>?", (nonce,)) or 0)
            if expired:
                items.append({"kind": "share", "text": f"{expired} shared file/folder approval{'s' if expired != 1 else ''} expired (ask again in the chat)", "go": {"screen": "chat"}})
            if not gw.auth_state.data.get("rk_confirmed", True):
                items.append({"kind": "security", "text": "Recovery key not confirmed", "go": {"screen": "settings", "params": {"section": "account"}}})
            return {"state": "ok", "items": items[:8], "value": len(items), "tone": "warn" if items else "ok"}
        if i == "recent_files":
            rows = db.all("SELECT id, name, folder, created_at, status FROM files WHERE deleted=0 AND status='READY' ORDER BY created_at DESC LIMIT 5")
            for r in rows:
                m = gw.insights.get(r["id"])
                r["doc_type"], r["summary"] = (m["doc_type"], m["summary"]) if m else ("", "")
            return {"state": "ok", "items": rows, "go": {"screen": "files"}}
        if i == "storage":
            s = gw.files.storage()
            return {"state": "ok", "used": s["used_bytes"], "quota": s["quota_bytes"], "value": s["count"], "go": {"screen": "files"}}
        if i == "memory_recent":
            rows = db.all("SELECT id, content, source, created_at FROM memories WHERE status='ACTIVE' ORDER BY created_at DESC LIMIT 5")
            return {"state": "ok", "items": rows, "value": int(db.scalar("SELECT count(*) FROM memories WHERE status='ACTIVE'") or 0), "go": {"screen": "memory"}}
        if i == "models_health":
            ms = gw.llm.models()
            kinds = {}
            for k, role in (("chat", "standard"), ("voice", "stt"), ("vision", "vision"), ("embedding", "embedding")):
                m = gw.llm.role_model(role)
                kinds[k] = {"ready": bool(m and m["tested"]), "name": m["name"] if m else ""}
            m = gw.llm.role_model("standard")
            reachable = None
            if m and m["provider"] in ("ollama", "openai"):
                reachable = gw.llm._reachable(m)
            try:
                gpu = gw.gpu.usage()
            except Exception:  # noqa: BLE001
                gpu = {}
            return {"state": "ok", "kinds": kinds, "total": len(ms), "reachable": reachable, "gpu": gpu.get("util") if isinstance(gpu, dict) else None,
                    "go": {"screen": "settings", "params": {"section": "model"}}}
        if i == "updates":
            ev = gw.home.events()
            return {"state": "ok", "items": ev[:8], "value": len(ev), "go": {"screen": "activity"}}
        if i == "activity_today":
            since = to_iso(utcnow() - timedelta(days=1))
            done = int(db.scalar("SELECT count(*) FROM tasks WHERE state='COMPLETED' AND updated_at>?", (since,)) or 0)
            failed = int(db.scalar("SELECT count(*) FROM tasks WHERE state IN ('FAILED','TIMED_OUT') AND updated_at>?", (since,)) or 0)
            b = gw.budgets.summary()
            return {"state": "ok", "done": done, "failed": failed, "tokens": b.get("used", {}).get("tokens", 0), "go": {"screen": "activity"}}
        return {"state": "error", "note": "unknown widget"}

