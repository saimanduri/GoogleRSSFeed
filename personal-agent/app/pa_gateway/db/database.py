"""Encrypted application database (spec 3, 17.2).

SQLCipher 4 (AES-256) keyed with K_db (raw 256-bit key, derived from the VMK). WAL mode.
One connection guarded by a re-entrant lock: the gateway is the only process that opens it.
"""
from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

try:
    import sqlcipher3 as sqlite  # type: ignore[import-not-found]
    HAVE_SQLCIPHER = True
except ImportError:  # pragma: no cover - the release build always ships sqlcipher3
    sqlite = None  # type: ignore[assignment]
    HAVE_SQLCIPHER = False

from .schema import MIGRATIONS


class DatabaseError(Exception):
    pass


class Database:
    def __init__(self, path: Path, key: bytes):
        if not HAVE_SQLCIPHER:
            raise DatabaseError("SQLCipher is not installed; refusing to store data unencrypted")
        self.path = path
        self._lock = threading.RLock()
        self._conn = sqlite.connect(str(path), check_same_thread=False, isolation_level=None, timeout=30)
        self._conn.row_factory = sqlite.Row
        self._conn.execute(f"PRAGMA key = \"x'{key.hex()}'\"")
        self._conn.execute("PRAGMA cipher_memory_security = ON")
        try:
            self._conn.execute("SELECT count(*) FROM sqlite_master").fetchone()
        except sqlite.DatabaseError as e:
            self._conn.close()
            raise DatabaseError("database key rejected or file corrupt") from e
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA secure_delete = ON")
        self.migrate()

    # ------------------------------------------------------------------ basics
    def migrate(self) -> None:
        with self._lock:
            self._conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
            row = self._conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
            current = int(row[0]) if row else 0
            for version, sql in MIGRATIONS:
                if version > current:
                    with self.tx():
                        for stmt in _split(sql):
                            self._conn.execute(stmt)
                        self._conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('schema_version',?)", (str(version),))

    @contextmanager
    def tx(self) -> Iterator[None]:
        with self._lock:
            in_tx = self._conn.in_transaction
            if not in_tx:
                self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                if not in_tx:
                    self._conn.execute("ROLLBACK")
                raise
            else:
                if not in_tx:
                    self._conn.execute("COMMIT")

    def execute(self, sql: str, params: tuple | dict = ()) -> int:
        with self._lock:
            cur = self._conn.execute(sql, params)
            return cur.rowcount

    def insert(self, table: str, row: dict[str, Any]) -> None:
        cols = ",".join(row)
        qs = ",".join("?" for _ in row)
        self.execute(f"INSERT INTO {table} ({cols}) VALUES ({qs})", tuple(_enc(v) for v in row.values()))

    def update(self, table: str, key_col: str, key: Any, changes: dict[str, Any]) -> int:
        sets = ",".join(f"{c}=?" for c in changes)
        return self.execute(f"UPDATE {table} SET {sets} WHERE {key_col}=?", (*(_enc(v) for v in changes.values()), key))

    def all(self, sql: str, params: tuple | dict = ()) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(r) for r in self._conn.execute(sql, params).fetchall()]

    def one(self, sql: str, params: tuple | dict = ()) -> dict[str, Any] | None:
        with self._lock:
            r = self._conn.execute(sql, params).fetchone()
            return dict(r) if r else None

    def scalar(self, sql: str, params: tuple | dict = ()) -> Any:
        with self._lock:
            r = self._conn.execute(sql, params).fetchone()
            return r[0] if r else None

    def rekey(self, new_key: bytes) -> None:
        with self._lock:
            self._conn.execute(f"PRAGMA rekey = \"x'{new_key.hex()}'\"")

    def checkpoint(self) -> None:
        with self._lock:
            self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def backup_to(self, dest: Path, key: bytes) -> None:
        """Consistent encrypted copy (same key) using SQLCipher's export."""
        with self._lock:
            if dest.exists():
                dest.unlink()
            self._conn.execute(f"ATTACH DATABASE ? AS bk KEY \"x'{key.hex()}'\"", (str(dest),))
            try:
                self._conn.execute("SELECT sqlcipher_export('bk')")
            finally:
                self._conn.execute("DETACH DATABASE bk")

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except Exception:  # noqa: BLE001
                pass
            self._conn.close()


def _enc(v: Any) -> Any:
    if isinstance(v, (dict, list)):
        return json.dumps(v, separators=(",", ":"))
    if isinstance(v, bool):
        return int(v)
    return v


def _split(sql: str) -> list[str]:
    return [s.strip() for s in sql.split(";\n") if s.strip()]


def jloads(v: Any, default: Any = None) -> Any:
    if v is None or v == "":
        return default
    if isinstance(v, (dict, list)):
        return v
    return json.loads(v)
