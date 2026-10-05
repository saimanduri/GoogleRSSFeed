"""Runs and steps: the "show me every step" timeline (user requirement, like the DeepSeek harness view).

A RUN is one request (a chat turn, a mission run, a reminder firing, a sub-task). Each run has
ordered STEPS: plan, llm call, tool call, policy decision, approval, result, error, summary...
Steps stream live to the UI (event topic "run.step"). The last `retention.runs_visible` runs
(default 100) are shown in the timeline; older runs are archived but remain searchable (FTS).
"""
from __future__ import annotations

import json
import threading
import time
from typing import Any, Callable

from pa_common.ids import new_id
from pa_common.timeutil import now_iso


class RunService:
    def __init__(self, db, emit: Callable[[str, dict], None], settings, history):
        self.db = db
        self.emit = emit
        self.settings = settings
        self.history = history
        self._seq: dict[str, int] = {}
        self._lock = threading.RLock()

    def start(self, kind: str, title: str, *, chat_id: str | None = None, task_id: str | None = None,
              mission_id: str | None = None) -> str:
        run_id = new_id("run")
        self.db.insert("runs", {"id": run_id, "kind": kind, "title": title[:200], "chat_id": chat_id, "task_id": task_id,
                                "mission_id": mission_id, "status": "RUNNING", "started_at": now_iso()})
        self.emit("run.started", {"run_id": run_id, "kind": kind, "title": title[:200], "chat_id": chat_id})
        self._archive_old()
        return run_id

    def attach_task(self, run_id: str, task_id: str) -> None:
        self.db.update("runs", "id", run_id, {"task_id": task_id})

    def step(self, run_id: str | None, type_: str, title: str, status: str = "done", detail: dict[str, Any] | None = None,
             duration_ms: int | None = None, step_id: str | None = None) -> str | None:
        """Create (or update when step_id is given) a step. Returns the step id."""
        if not run_id:
            return None
        detail = detail or {}
        with self._lock:
            if step_id:
                changes: dict[str, Any] = {"status": status, "title": title[:300], "detail_json": detail}
                if status not in ("running", "waiting"):
                    changes["ended_at"] = now_iso()
                    changes["duration_ms"] = duration_ms
                self.db.update("run_steps", "id", step_id, changes)
                seq = self.db.scalar("SELECT seq FROM run_steps WHERE id=?", (step_id,))
            else:
                seq = self._seq.get(run_id)
                if seq is None:
                    seq = int(self.db.scalar("SELECT coalesce(max(seq),0) FROM run_steps WHERE run_id=?", (run_id,)) or 0)
                seq += 1
                self._seq[run_id] = seq
                step_id = new_id("stp")
                ended = None if status in ("running", "waiting") else now_iso()
                self.db.insert("run_steps", {"id": step_id, "run_id": run_id, "seq": seq, "type": type_, "title": title[:300],
                                             "status": status, "detail_json": detail, "started_at": now_iso(),
                                             "ended_at": ended, "duration_ms": duration_ms})
        self.emit("run.step", {"run_id": run_id, "step_id": step_id, "seq": seq, "type": type_, "title": title[:300],
                               "status": status, "detail": detail, "duration_ms": duration_ms, "ts": time.time()})
        return step_id

    def finish(self, run_id: str, status: str, summary: str | None = None, hwm: int | None = None,
               sources: list[str] | None = None) -> None:
        changes: dict[str, Any] = {"status": status, "ended_at": now_iso()}
        if summary is not None:
            changes["summary"] = summary[:4000]
        if hwm is not None:
            changes["hwm"] = hwm
        if sources is not None:
            changes["sources_json"] = sources
        self.db.update("runs", "id", run_id, changes)
        run = self.db.one("SELECT * FROM runs WHERE id=?", (run_id,))
        if run and summary and not self._chat_deleted(run.get("chat_id")):
            # the chat may have been deleted while this run was still finishing: never re-add it to search
            self.history.index("run", run_id, run["title"], summary, run["started_at"])
        self.emit("run.finished", {"run_id": run_id, "status": status})
        with self._lock:
            self._seq.pop(run_id, None)

    def _chat_deleted(self, chat_id: str | None) -> bool:
        if not chat_id:
            return False
        c = self.db.one("SELECT deleted FROM chats WHERE id=?", (chat_id,))
        return bool(c is None or c["deleted"])

    def add_usage(self, run_id: str | None, tokens_in: int = 0, tokens_out: int = 0, tool_calls: int = 0) -> None:
        if run_id:
            self.db.execute("UPDATE runs SET tokens_in=tokens_in+?, tokens_out=tokens_out+?, tool_calls=tool_calls+? WHERE id=?",
                            (tokens_in, tokens_out, tool_calls, run_id))

    def update_hwm(self, run_id: str | None, hwm: int, source: str | None = None) -> None:
        if not run_id:
            return
        r = self.db.one("SELECT hwm, sources_json FROM runs WHERE id=?", (run_id,))
        if not r:
            return
        sources = json.loads(r["sources_json"] or "[]")
        if source and source not in sources:
            sources.append(source)
        self.db.update("runs", "id", run_id, {"hwm": max(int(r["hwm"]), int(hwm)), "sources_json": sources})

    def list(self, limit: int = 100, include_archived: bool = False, chat_id: str | None = None) -> list[dict[str, Any]]:
        where = []
        params: list[Any] = []
        if not include_archived:
            where.append("archived=0")
        if chat_id:
            where.append("chat_id=?")
            params.append(chat_id)
        sql = "SELECT * FROM runs" + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY started_at DESC LIMIT ?"
        return self.db.all(sql, (*params, limit))

    def get(self, run_id: str) -> dict[str, Any] | None:
        run = self.db.one("SELECT * FROM runs WHERE id=?", (run_id,))
        if not run:
            return None
        steps = self.db.all("SELECT * FROM run_steps WHERE run_id=? ORDER BY seq", (run_id,))
        for s in steps:
            s["detail"] = json.loads(s.pop("detail_json") or "{}")
        run["steps"] = steps
        return run

    def _archive_old(self) -> None:
        keep = int(self.settings.get("retention.runs_visible")) if self.settings else 100
        self.db.execute("UPDATE runs SET archived=1 WHERE archived=0 AND id NOT IN "
                        "(SELECT id FROM runs ORDER BY started_at DESC LIMIT ?)", (keep,))
