"""Search your history (spec 39.12): SQLite FTS5 inside the encrypted database.

Indexed: chat messages, run summaries/results, file text. Deleting a source removes it from the index,
so deleted items never appear. Results are untrusted and raise the caller's sensitivity level.
"""
from __future__ import annotations

import re
from typing import Any


def fts_query(q: str) -> str:
    """Every word matches by its beginning (so 'expl' finds 'explain'), in any letter case; all words must be present."""
    tokens = re.findall(r"[\w@.\-]+", q, flags=re.UNICODE)[:20]
    return " ".join('"' + t.replace('"', "") + '"' + ("*" if len(t) >= 2 else "") for t in tokens) or '""'


class HistoryService:
    def __init__(self, db):
        self.db = db

    def index(self, kind: str, ref_id: str, title: str, body: str, created_at: str) -> None:
        self.remove(kind, ref_id)
        self.db.execute("INSERT INTO history_fts(kind, ref_id, title, body, created_at) VALUES (?,?,?,?,?)",
                        (kind, ref_id, (title or "")[:500], (body or "")[:200_000], created_at))

    def remove(self, kind: str, ref_id: str) -> None:
        self.db.execute("DELETE FROM history_fts WHERE kind=? AND ref_id=?", (kind, ref_id))

    def remove_prefix(self, kind: str, ref_prefix: str) -> None:
        self.db.execute("DELETE FROM history_fts WHERE kind=? AND ref_id LIKE ?", (kind, ref_prefix + "%"))

    def search(self, query: str, limit: int = 20, kinds: list[str] | None = None) -> list[dict[str, Any]]:
        sql = ("SELECT kind, ref_id, title, snippet(history_fts, 3, '[', ']', ' ... ', 16) AS snippet, created_at, "
               "bm25(history_fts) AS score FROM history_fts WHERE history_fts MATCH ?")
        params: list[Any] = [fts_query(query)]
        if kinds:
            sql += " AND kind IN (" + ",".join("?" for _ in kinds) + ")"
            params += kinds
        sql += " ORDER BY score LIMIT ?"
        params.append(limit)
        try:
            rows = self.db.all(sql, tuple(params))
        except Exception:  # noqa: BLE001 - a malformed query must never break the screen
            rows = []
        if len(rows) < limit and (not kinds or "chat" in kinds):
            rows += self._like_fallback(query, limit - len(rows), {r["ref_id"] for r in rows})
        return rows

    def _like_fallback(self, query: str, limit: int, seen: set[str]) -> list[dict[str, Any]]:
        """Safety net: plain substring search over chat messages, for text the index does not have (older chats, interrupted indexing)."""
        words = [w for w in re.findall(r"\w+", query, flags=re.UNICODE) if len(w) >= 2][:5]
        if not words:
            return []
        cond = " AND ".join("m.content LIKE ? ESCAPE '\\'" for _ in words)
        like = [f"%{w.replace(chr(92), '').replace('%', '').replace('_', chr(92) + '_')}%" for w in words]
        rows = self.db.all(f"SELECT m.id AS ref_id, c.title AS title, m.content AS content, m.created_at AS created_at FROM chat_messages m "
                           f"JOIN chats c ON c.id=m.chat_id WHERE c.deleted=0 AND {cond} ORDER BY m.created_at DESC LIMIT ?", tuple(like) + (limit * 2,))
        out = []
        for r in rows:
            if r["ref_id"] in seen:
                continue
            body = r["content"]
            i = body.lower().find(words[0].lower())
            out.append({"kind": "chat", "ref_id": r["ref_id"], "title": r["title"], "snippet": ("... " if i > 40 else "") + body[max(0, i - 40):i + 120],
                        "created_at": r["created_at"], "score": 0})
            if len(out) >= limit:
                break
        return out
