"""Search your history (spec 39.12): SQLite FTS5 inside the encrypted database.

Indexed: chat messages, run summaries/results, file text. Deleting a source removes it from the index,
so deleted items never appear. Results are untrusted and raise the caller's sensitivity level.
"""
from __future__ import annotations

import re
from typing import Any


def fts_query(q: str) -> str:
    tokens = re.findall(r"[\w@.\-]+", q, flags=re.UNICODE)[:20]
    return " ".join('"' + t.replace('"', "") + '"' for t in tokens) or '""'


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
        return self.db.all(sql, tuple(params))
