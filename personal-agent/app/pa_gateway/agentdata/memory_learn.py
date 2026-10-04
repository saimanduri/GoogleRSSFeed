"""Automatic memory (the assistant learns about you from what you do) - with a strong "forget" control.

Sources, from most to least free-form:
  * your own typed messages in chats (an extraction step with the local model; only YOUR words are shown to it, never tool results, mail
    bodies or web pages, so content from outside cannot plant memories),
  * routines and email-monitoring skills you switch on (deterministic text, no model),
  * files you put in My Files (see files/insights.py: what the file is and where it is - never secret values).
Rules: nothing that looks like a secret or an ID/card/phone/e-mail value is ever stored (DLP + patterns); one memory per fact (near duplicates are
skipped); a memory you delete or reject is remembered as "forgotten" and never learned again; a daily cap; everything shows in Memory with its
source and can be edited, disabled or deleted there. Switch the whole thing off in Settings > Memory (`memory.auto_learn`).
"""
from __future__ import annotations

import hashlib
import json
import re
import threading
import time
from typing import Any

import numpy as np

from pa_common.errors import PAError
from pa_common.ids import new_id
from pa_common.sensitivity import Trust
from pa_common.timeutil import now_iso, to_iso, utcnow

SYSTEM = (
    "You extract LASTING facts about the USER from what the user typed. Reply with ONLY a JSON array of at most 3 objects "
    '{"type":"preference|semantic|project|procedural","content":"one short sentence about the user, third person","importance":"low|normal|high"}. '
    "Include only things the user clearly said about themselves: preferences, role, projects, habits, tools they use, people or teams they work with, goals. "
    "Skip one-off requests, questions, and anything about the content of documents. NEVER include secrets, passwords, ID, PAN, Aadhaar, card or bank numbers, "
    "phone numbers, e-mail addresses or street addresses. If there is nothing lasting, reply []."
)
_NUM = re.compile(r"\d[\d\s\-]{5,}\d")
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
_PAN = re.compile(r"\b[A-Z]{5}\d{4}[A-Z]\b")
_FORBIDDEN = re.compile(r"(password|passcode|\bpin\b|\bcvv\b|api[_ -]?key|secret|token)\s*(is|=|:)", re.I)


def norm(text: str) -> str:
    return re.sub(r"\W+", " ", text.lower()).strip()


def chash(text: str) -> str:
    return hashlib.sha256(norm(text).encode()).hexdigest()


def looks_sensitive(gw, text: str) -> bool:
    """True when a memory text must not be stored: secrets, ID-like numbers, e-mail addresses, DLP findings."""
    if _NUM.search(text) or _EMAIL.search(text) or _PAN.search(text) or _FORBIDDEN.search(text):
        return True
    try:
        return bool(gw.dlp.check_outbound(text)["blocked"])
    except Exception:  # noqa: BLE001
        return True


class MemoryLearner:
    def __init__(self, gw):
        self.gw = gw
        self._busy: set[str] = set()
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ switches
    def enabled(self) -> bool:
        s = self.gw.settings
        return bool(s.get("memory.enabled")) and bool(s.get("memory.auto_learn"))

    def _today_count(self) -> int:
        since = to_iso(utcnow().replace(hour=0, minute=0, second=0, microsecond=0))
        return int(self.gw.db.scalar("SELECT count(*) FROM memories WHERE source LIKE 'learned:%' AND created_at>=?", (since,)) or 0)

    def _forgotten(self, text: str) -> bool:
        return bool(self.gw.db.one("SELECT 1 FROM memory_forgotten WHERE content_hash=?", (chash(text),)))

    def _duplicate(self, text: str) -> bool:
        if self.gw.db.one("SELECT 1 FROM memories WHERE status IN ('ACTIVE','PROPOSED','DISABLED') AND lower(content)=lower(?)", (text,)):
            return True
        rows = self.gw.db.all("SELECT embedding FROM memories WHERE status IN ('ACTIVE','PROPOSED','DISABLED') AND embedding IS NOT NULL")
        if not rows:
            return False
        q = np.frombuffer(self.gw.memory._vec(text), dtype=np.float32)
        return any(float(np.frombuffer(r["embedding"], dtype=np.float32) @ q) > 0.88 for r in rows)

    # ------------------------------------------------------------------ storing one learned fact
    def store(self, content: str, type_: str, importance: str, source: str, source_ref: str | None, provenance: list[str],
              sensitivity: int = 1, force: bool = False) -> str | None:
        content = " ".join(content.split())[:400 if source == "learned:file" else 240]
        if source == "learned:file":        # file memories may carry dates and amounts, never ID-like values or DLP findings
            from ..pii import detect_values
            bad = bool(detect_values(content)) or bool(self.gw.dlp.check_outbound(content)["blocked"])
        else:
            bad = looks_sensitive(self.gw, content)
        if len(content) < 8 or bad or self._forgotten(content):
            return None
        if not force and (self._duplicate(content) or self._today_count() >= int(self.gw.settings.get("memory.auto_learn_per_day"))):
            return None
        mid = new_id("mem")
        self.gw.db.insert("memories", {
            "id": mid, "type": type_ if type_ in ("preference", "semantic", "project", "procedural", "episodic") else "semantic", "content": content, "source": source,
            "source_ref": source_ref, "provenance_json": provenance, "trust": Trust.INFERRED, "confidence": 0.6,
            "importance": importance if importance in ("low", "normal", "high") else "normal", "sensitivity": int(sensitivity), "status": "ACTIVE",
            "embedding": self.gw.memory._vec(content), "created_at": now_iso(), "last_verified_at": None})
        self.gw.audit.write("memory.learned", "memory", memory_id=mid, source=source.split(":")[0] + ":" + (source.split(":")[1] if ":" in source else ""))
        self.gw.emit("memory.changed", {"id": mid})
        return mid

    # ------------------------------------------------------------------ from chats
    def schedule_chat(self, chat_id: str) -> None:
        if not self.enabled():
            return
        with self._lock:
            if chat_id in self._busy:
                return
            self._busy.add(chat_id)
        threading.Thread(target=self._chat_job, args=(chat_id,), daemon=True, name=f"memlearn-{chat_id[-6:]}").start()

    def _chat_job(self, chat_id: str) -> None:
        try:
            time.sleep(1.0)
            self.learn_chat(chat_id)
        except Exception:  # noqa: BLE001 - learning must never disturb the chat
            self.gw.audit.write("memory.learn_error", "memory", chat_id=chat_id)
        finally:
            with self._lock:
                self._busy.discard(chat_id)

    def learn_chat(self, chat_id: str) -> int:
        gw = self.gw
        st = gw.db.one("SELECT last_msg_created_at FROM memory_learn_state WHERE chat_id=?", (chat_id,))
        rows = gw.db.all("SELECT created_at, content FROM chat_messages WHERE chat_id=? AND role='user' AND created_at>? ORDER BY created_at LIMIT 8",
                         (chat_id, st["last_msg_created_at"] if st else ""))
        texts = [r["content"].strip()[:600] for r in rows if r["content"].strip() and not r["content"].lstrip().startswith("/")]
        if rows:
            gw.db.execute("INSERT OR REPLACE INTO memory_learn_state(chat_id, last_msg_created_at) VALUES (?,?)", (chat_id, rows[-1]["created_at"]))
        if sum(len(t) for t in texts) < 25:
            return 0
        m = gw.llm.role_model("fast") or gw.llm.role_model("standard")
        if m is None or m["location"] == "remote":
            return 0                                   # only ever a model on this PC reads what you typed for learning
        body = "What the user typed:\n" + "\n---\n".join(texts)
        sid = f"memlearn:{chat_id}"
        gw.sessionlog.append(sid, "memory.learn.input", body, role="user", source="ui", trust=Trust.TRUSTED, sensitivity=1)
        try:
            out = gw.llm.complete(task=None, session_ids=[sid], messages=[{"role": "system", "content": SYSTEM}, {"role": "user", "content": body}],
                                  role="fast", json_mode=False, max_tokens=500, purpose="memory-learning")
        except PAError:
            return 0
        n = 0
        for item in self.parse(out["text"]):
            if self.store(item["content"], item.get("type", "semantic"), item.get("importance", "normal"), "learned:chat", None, [chat_id]):
                n += 1
        return n

    @staticmethod
    def parse(text: str) -> list[dict[str, Any]]:
        a, b = text.find("["), text.rfind("]")
        if a < 0 or b <= a:
            return []
        try:
            data = json.loads(text[a:b + 1])
        except ValueError:
            return []
        return [d for d in data[:3] if isinstance(d, dict) and isinstance(d.get("content"), str)]

    # ------------------------------------------------------------------ from routines / email skills (no model)
    def learn_routine(self, mission: dict[str, Any]) -> None:
        if not self.enabled():
            return
        from . import schedule as sch
        try:
            when = sch.describe(json.loads(mission["schedule_json"]))
        except Exception:  # noqa: BLE001
            when = "on a schedule"
        name = str(mission["name"]).removeprefix("Email: ")
        if mission.get("template_id"):
            text = f"Uses an automatic Outlook check: {name} ({when.lower() if when else 'scheduled'})"
        else:
            text = f"Has a {mission.get('kind', 'routine')} '{name}' that runs {when.lower() if when else 'on a schedule'}"
        self.gw.db.execute("DELETE FROM memories WHERE source_ref=? AND source='learned:routine'", (mission["id"],))
        self.store(text, "procedural", "normal", "learned:routine", mission["id"], [mission["id"]], force=True)

    def forget_routine(self, mission_id: str) -> None:
        self.gw.db.execute("DELETE FROM memories WHERE source_ref=? AND source='learned:routine'", (mission_id,))
