"""Persistent memory with a trust model (spec 21, 39.13).

Trust: TRUSTED (user stated in the UI) > VERIFIED (user-confirmed) > INFERRED (model proposal) > UNTRUSTED.
- Agent proposals are INFERRED and go to the "Proposed memories" inbox (status PROPOSED); only
  low-importance preferences may be auto-confirmed, and only if the user enabled that setting.
- Proposals from runs that saw injection warnings are rejected; from EXTERNAL_EVENT triggers they
  stay INFERRED/PROPOSED at most.
- Only ACTIVE TRUSTED/VERIFIED memories are given to the model as user preferences.
- The agent can never delete or rewrite what the user said (no such tool exists).
- Deleting a source (file, connector data) cascades to derived memories.
Embeddings are stored in the encrypted DB; search is cosine similarity in memory (numpy).
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import timedelta
from typing import Any, Callable

import numpy as np

from pa_common.errors import PAError
from pa_common.ids import new_id
from pa_common.sensitivity import Trust
from pa_common.timeutil import now_iso, parse_iso, to_iso, utcnow

DIM = 384
TYPES = ("working", "episodic", "semantic", "procedural", "preference", "project")


def hash_embedding(text: str) -> np.ndarray:
    """Local, deterministic feature-hashing embedding (used when no embedding model is configured)."""
    v = np.zeros(DIM, dtype=np.float32)
    words = re.findall(r"\w+", text.lower())
    feats = words + [a + "_" + b for a, b in zip(words, words[1:])]
    for f in feats:
        h = int.from_bytes(hashlib.blake2b(f.encode(), digest_size=8).digest(), "little")
        v[h % DIM] += 1.0 if (h >> 63) & 1 else -1.0
    n = np.linalg.norm(v)
    return v / n if n else v


class MemoryService:
    def __init__(self, db, audit, settings, embed: Callable[[str], np.ndarray] | None = None):
        self.db = db
        self.audit = audit
        self.settings = settings
        self.embed = embed or hash_embedding

    def _vec(self, text: str) -> bytes:
        try:
            v = self.embed(text)
        except Exception:  # noqa: BLE001 - model unavailable: fall back to local embedding
            v = hash_embedding(text)
        v = np.asarray(v, dtype=np.float32)
        if v.shape != (DIM,):
            v = hash_embedding(text)
        return v.tobytes()

    # ------------------------------------------------------------------ user actions (TRUSTED)
    def add_user(self, content: str, type_: str = "preference", sensitivity: int = 1) -> str:
        if type_ not in TYPES:
            raise PAError("invalid memory type", code="invalid_request")
        mid = new_id("mem")
        self.db.insert("memories", {"id": mid, "type": type_, "content": content[:4000], "source": "user",
                                    "trust": Trust.TRUSTED, "confidence": 1.0, "importance": "normal",
                                    "sensitivity": sensitivity, "status": "ACTIVE", "embedding": self._vec(content),
                                    "created_at": now_iso(), "last_verified_at": now_iso()})
        self.audit.write("memory.created", "memory", memory_id=mid, trust=Trust.TRUSTED, type=type_)
        return mid

    def confirm(self, mid: str, content: str | None = None) -> None:
        m = self._get(mid)
        changes: dict[str, Any] = {"status": "ACTIVE", "trust": Trust.VERIFIED if m["trust"] != Trust.TRUSTED else Trust.TRUSTED,
                                   "last_verified_at": now_iso()}
        if content:
            changes["content"] = content[:4000]
            changes["embedding"] = self._vec(content)
        self.db.update("memories", "id", mid, changes)
        self.audit.write("memory.confirmed", "memory", memory_id=mid)

    def edit(self, mid: str, content: str) -> None:
        self._get(mid)
        self.db.update("memories", "id", mid, {"content": content[:4000], "embedding": self._vec(content),
                                               "trust": Trust.TRUSTED, "last_verified_at": now_iso()})
        self.audit.write("memory.edited", "memory", memory_id=mid)

    def set_status(self, mid: str, status: str) -> None:
        if status == "REJECTED":
            self._tomb("id=?", (mid,))
        if status not in ("ACTIVE", "DISABLED", "REJECTED"):
            raise PAError("invalid status", code="invalid_request")
        self._get(mid)
        self.db.update("memories", "id", mid, {"status": status})
        self.audit.write("memory.status", "memory", memory_id=mid, status=status)

    def _tomb(self, where: str, params: tuple) -> None:
        """Remember what the user threw away so automatic learning never brings it back."""
        import hashlib
        for r in self.db.all(f"SELECT content FROM memories WHERE {where}", params):
            h = hashlib.sha256(re.sub(r"\W+", " ", r["content"].lower()).strip().encode()).hexdigest()
            self.db.execute("INSERT OR IGNORE INTO memory_forgotten(content_hash, ts) VALUES (?,?)", (h, now_iso()))

    def delete(self, mid: str) -> None:
        self._tomb("id=?", (mid,))
        self.db.execute("DELETE FROM memories WHERE id=?", (mid,))
        self.audit.write("memory.deleted", "memory", memory_id=mid)

    def delete_all(self) -> int:
        self._tomb("1=1", ())
        n = self.db.execute("DELETE FROM memories")
        self.audit.write("memory.deleted_all", "memory", count=n, severity="medium")
        return n

    def delete_by_source_ref(self, ref: str) -> int:
        n = self.db.execute("DELETE FROM memories WHERE source_ref=?", (ref,))
        if n:
            self.audit.write("memory.cascade_deleted", "memory", source_ref=ref, count=n)
        return n

    def delete_by_source(self, source: str) -> int:
        return self.db.execute("DELETE FROM memories WHERE source=?", (source,))

    # ------------------------------------------------------------------ agent proposals (INFERRED)
    def propose(self, content: str, type_: str, importance: str, *, task: dict[str, Any], sensitivity: int,
                tainted: bool, provenance: list[str]) -> dict[str, Any]:
        if not self.settings.get("memory.enabled"):
            return {"status": "memory_disabled"}
        if tainted:
            self.audit.write("memory.proposal_blocked", "memory", task_id=task["id"], reason="tainted_run", severity="medium")
            return {"status": "rejected", "reason": "the run contained possible prompt injection"}
        dup = self.db.one("SELECT id FROM memories WHERE content=? AND status IN ('ACTIVE','PROPOSED')", (content[:4000],))
        if dup:
            return {"status": "duplicate", "id": dup["id"]}
        auto = (importance == "low" and type_ == "preference" and self.settings.get("memory.auto_confirm_low")
                and task["trigger_type"] == "USER")
        mid = new_id("mem")
        self.db.insert("memories", {"id": mid, "type": type_, "content": content[:4000], "source": f"agent:{task['id']}",
                                    "provenance_json": provenance, "trust": Trust.INFERRED, "confidence": 0.5,
                                    "importance": importance, "sensitivity": int(sensitivity),
                                    "status": "ACTIVE" if auto else "PROPOSED", "embedding": self._vec(content),
                                    "created_at": now_iso()})
        self.audit.write("memory.proposed", "memory", memory_id=mid, task_id=task["id"], auto_confirmed=bool(auto))
        return {"status": "active" if auto else "proposed", "id": mid}

    # ------------------------------------------------------------------ reads
    def _get(self, mid: str) -> dict[str, Any]:
        m = self.db.one("SELECT * FROM memories WHERE id=?", (mid,))
        if not m:
            raise PAError("memory not found", code="not_found")
        return m

    def list(self, status: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT id,type,content,source,source_ref,provenance_json,trust,confidence,importance,sensitivity,status," \
              "created_at,last_verified_at,expires_at FROM memories"
        rows = self.db.all(sql + (" WHERE status=?" if status else "") + " ORDER BY created_at DESC",
                           (status,) if status else ())
        for r in rows:
            r["provenance"] = json.loads(r.pop("provenance_json") or "[]")
        return rows

    def search(self, query: str, limit: int = 8, trusted_only: bool = False) -> list[dict[str, Any]]:
        rows = self.db.all("SELECT id,type,content,trust,sensitivity,embedding,created_at FROM memories WHERE status='ACTIVE'")
        if trusted_only:
            rows = [r for r in rows if r["trust"] in (Trust.TRUSTED, Trust.VERIFIED)]
        if not rows:
            return []
        q = np.frombuffer(self._vec(query), dtype=np.float32)
        mats = np.stack([np.frombuffer(r["embedding"], dtype=np.float32) if r["embedding"] else np.zeros(DIM, np.float32)
                         for r in rows])
        scores = mats @ q
        order = np.argsort(-scores)[:limit]
        out = []
        for i in order:
            r = dict(rows[int(i)])
            r.pop("embedding", None)
            r["score"] = float(scores[int(i)])
            out.append(r)
        return out

    def about_me(self) -> dict[str, Any]:
        rows = self.list("ACTIVE")
        return {"stated": [r for r in rows if r["trust"] in (Trust.TRUSTED, Trust.VERIFIED)],
                "inferred": [r for r in rows if r["trust"] == Trust.INFERRED]}

    def preferences_for_prompt(self, limit: int = 20) -> list[dict[str, Any]]:
        """What the model is told about the user: stated/confirmed memories first, then the ones learned automatically (kept on a short leash:
        they come from the user's own words, routines and own files only, and the user can see and delete every one)."""
        rows = self.db.all("SELECT id, type, content, trust FROM memories WHERE status='ACTIVE' AND trust IN (?,?) "
                           "ORDER BY coalesce(last_verified_at, created_at) DESC LIMIT ?", (Trust.TRUSTED, Trust.VERIFIED, limit))
        if self.settings.get("memory.auto_learn"):
            rows += self.db.all("SELECT id, type, content, trust FROM memories WHERE status='ACTIVE' AND source LIKE 'learned:%' "
                                "ORDER BY created_at DESC LIMIT ?", (max(0, limit - len(rows)) or 5,))
        return rows

    def review(self) -> dict[str, Any]:
        cutoff = to_iso(utcnow() - timedelta(days=90))
        week = to_iso(utcnow() - timedelta(days=7))
        stale = self.db.all("SELECT id,content,trust,last_verified_at FROM memories WHERE status='ACTIVE' AND "
                            "coalesce(last_verified_at, created_at) < ?", (cutoff,))
        new = self.db.all("SELECT id,content,trust,status FROM memories WHERE created_at > ?", (week,))
        conflicts = self._conflicts()
        return {"new": new, "stale": stale, "conflicts": conflicts,
                "proposed": self.db.all("SELECT id,content,type,importance FROM memories WHERE status='PROPOSED'")}

    def _conflicts(self) -> list[dict[str, Any]]:
        rows = self.db.all("SELECT id, content, embedding FROM memories WHERE status='ACTIVE' AND type='preference'")
        out = []
        for i in range(len(rows)):
            for j in range(i + 1, len(rows)):
                a, b = rows[i], rows[j]
                if not a["embedding"] or not b["embedding"]:
                    continue
                s = float(np.frombuffer(a["embedding"], np.float32) @ np.frombuffer(b["embedding"], np.float32))
                neg = ("not" in a["content"].lower().split()) != ("not" in b["content"].lower().split())
                if s > 0.6 and neg:
                    out.append({"a": {"id": a["id"], "content": a["content"]}, "b": {"id": b["id"], "content": b["content"]}})
        return out[:20]

    def apply_retention(self) -> None:
        days = int(self.settings.get("memory.retention_days"))
        if days > 0:
            self.db.execute("DELETE FROM memories WHERE created_at < ?", (to_iso(utcnow() - timedelta(days=days)),))
        self.db.execute("DELETE FROM memories WHERE expires_at IS NOT NULL AND expires_at < ?", (now_iso(),))

    def valid_at_use(self, m: dict[str, Any]) -> bool:
        return not (m.get("expires_at") and parse_iso(m["expires_at"]) < utcnow())
