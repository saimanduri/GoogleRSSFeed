"""Session event log - "anything the model sees is logged" (spec 39.5, 25.5 Transcript Store).

Append-only, per session (= run), stored in the encrypted DB, hash-chained per session.
The gateway writes tool results itself; pa-core may append only its own user-visible messages and
summaries. Before any request reaches a model, llm.complete verifies that every non-system
message is present in this log and records the full request ("llm.request") first.
"""
from __future__ import annotations

import hashlib
import json
import threading
from typing import Any

from pa_common.errors import PAError
from pa_common.ids import new_id
from pa_common.sensitivity import Sensitivity, Trust
from pa_common.timeutil import now_iso

GENESIS = "0" * 64


def content_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


class SessionLog:
    def __init__(self, db):
        self.db = db
        self._lock = threading.RLock()

    def append(self, session_id: str, kind: str, content: str, *, role: str | None = None, source: str = "system",
               trust: str = Trust.TRUSTED, sensitivity: int = 0, meta: dict[str, Any] | None = None) -> dict[str, Any]:
        with self._lock, self.db.tx():
            last = self.db.one("SELECT seq, hash FROM session_events WHERE session_id=? ORDER BY seq DESC LIMIT 1", (session_id,))
            seq = (last["seq"] + 1) if last else 1
            prev = last["hash"] if last else GENESIS
            ts = now_iso()
            body = json.dumps({"s": session_id, "q": seq, "k": kind, "r": role, "c": content_hash(content), "src": source,
                               "t": trust, "sens": int(sensitivity), "p": prev, "ts": ts}, sort_keys=True)
            h = hashlib.sha256(body.encode()).hexdigest()
            row = {"id": new_id("sev"), "session_id": session_id, "seq": seq, "kind": kind, "role": role, "content": content,
                   "source": source, "trust": trust, "sensitivity": int(sensitivity), "meta_json": meta or {},
                   "prev_hash": prev, "hash": h, "ts": ts}
            self.db.insert("session_events", row)
            return row

    def events(self, session_id: str, after_seq: int = 0, limit: int = 5000) -> list[dict[str, Any]]:
        rows = self.db.all("SELECT * FROM session_events WHERE session_id=? AND seq>? ORDER BY seq LIMIT ?",
                           (session_id, after_seq, limit))
        for r in rows:
            r["meta"] = json.loads(r.pop("meta_json") or "{}")
        return rows

    def hwm(self, session_id: str) -> Sensitivity:
        v = self.db.scalar("SELECT max(sensitivity) FROM session_events WHERE session_id=?", (session_id,))
        return Sensitivity(int(v or 0))

    def sources(self, session_id: str) -> list[str]:
        return [r["source"] for r in self.db.all(
            "SELECT DISTINCT source FROM session_events WHERE session_id=? AND source NOT IN ('system','user','model')",
            (session_id,))]

    def has_untrusted_instructions_risk(self, session_id: str) -> bool:
        return bool(self.db.scalar("SELECT count(*) FROM session_events WHERE session_id=? AND kind='injection.flag'", (session_id,)))

    def verify_messages_logged(self, session_ids: list[str], messages: list[dict[str, Any]]) -> None:
        """Every non-system message must match a logged event of one of the sessions (parent sessions included)."""
        if not session_ids:
            raise PAError("no session", code="session_required")
        qs = ",".join("?" for _ in session_ids)
        known = {content_hash(r["content"]) for r in self.db.all(
            f"SELECT content FROM session_events WHERE session_id IN ({qs})", tuple(session_ids))}
        for m in messages:
            if m.get("role") == "system":
                continue
            if content_hash(str(m.get("content", ""))) not in known:
                raise PAError("a message was not recorded in the session log; refusing to send it to the model",
                              code="unlogged_context")

    def verify_chain(self, session_id: str) -> bool:
        prev = GENESIS
        for r in self.db.all("SELECT * FROM session_events WHERE session_id=? ORDER BY seq", (session_id,)):
            body = json.dumps({"s": r["session_id"], "q": r["seq"], "k": r["kind"], "r": r["role"],
                               "c": content_hash(r["content"]), "src": r["source"], "t": r["trust"],
                               "sens": int(r["sensitivity"]), "p": prev, "ts": r["ts"]}, sort_keys=True)
            if r["prev_hash"] != prev or hashlib.sha256(body.encode()).hexdigest() != r["hash"]:
                return False
            prev = r["hash"]
        return True

    def purge_older_than(self, cutoff_iso: str) -> int:
        return self.db.execute("DELETE FROM session_events WHERE ts < ?", (cutoff_iso,))
