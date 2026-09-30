"""Unified security log (spec 25).

One active file: <logs>/agent-security.jsonl. JSON Lines, OCSF-aligned field names.

Integrity chain:
  chain_i = HMAC-SHA256(K_log, prev_chain || canonical(event_i))      (after unlock)
  chain_i = SHA-256(prev_chain || canonical(event_i))                 (before unlock, chain_mode="sha256")
The first HMAC'd event after an unlock covers the previous chain value, anchoring any pre-unlock
events (failed sign-ins etc.). Rotated segments end with a "log.seal" record.

Fail closed: `write()` raises AuditFailure if neither the log nor the bounded spool accepted the
event. Callers that are about to perform a consequential action MUST call write() first.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable

from pa_common.errors import AuditFailure
from pa_common.ids import new_id
from pa_common.timeutil import now_iso
from pa_common.version import APP_VERSION, LOG_SCHEMA_VERSION

from .redact import redact_event

ACTIVE_NAME = "agent-security.jsonl"
GENESIS = "0" * 64
SPOOL_MAX = 10 * 1024 * 1024


def canonical(event: dict[str, Any]) -> bytes:
    body = {k: v for k, v in event.items() if k != "chain_hmac"}
    return json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def chain_value(key: bytes | None, prev: str, event: dict[str, Any]) -> str:
    data = bytes.fromhex(prev) + canonical(event)
    if key is None:
        return hashlib.sha256(data).hexdigest()
    return hmac.new(key, data, hashlib.sha256).hexdigest()


class AuditWriter:
    def __init__(self, log_dir: Path, spool_path: Path, rotate_bytes: int = 100 * 1024 * 1024,
                 rotate_days: int = 7, retention_days: int = 365):
        self.log_dir = log_dir
        self.spool_path = spool_path
        self.rotate_bytes = rotate_bytes
        self.rotate_days = rotate_days
        self.retention_days = retention_days
        self._lock = threading.RLock()
        self._key: bytes | None = None
        self._seq = 0
        self._prev = GENESIS
        self._listeners: list[Callable[[dict[str, Any]], None]] = []
        self._secret_scanner: Callable[[str], bool] | None = None
        self.fail_next_for_test = False
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._recover_tail()

    @property
    def active_path(self) -> Path:
        return self.log_dir / ACTIVE_NAME

    # ---------------------------------------------------------------- key / hooks
    def set_key(self, key: bytes | None) -> None:
        with self._lock:
            self._key = key
            if key is not None:
                try:
                    self._drain_spool()
                except OSError:
                    pass

    def set_secret_scanner(self, scanner: Callable[[str], bool] | None) -> None:
        self._secret_scanner = scanner

    def add_listener(self, fn: Callable[[dict[str, Any]], None]) -> None:
        self._listeners.append(fn)

    # ---------------------------------------------------------------- write
    def write(self, event_type: str, category: str, /, **fields: Any) -> dict[str, Any]:
        with self._lock:
            ev: dict[str, Any] = {
                "schema_version": LOG_SCHEMA_VERSION,
                "event_id": new_id("evt"),
                "sequence": self._seq + 1,
                "prev_hash": self._prev,
                "timestamp": now_iso(),
                "category": category,
                "event_type": event_type,
                "severity": fields.pop("severity", "info"),
                "component": fields.pop("component", "pa-gateway"),
                "component_version": APP_VERSION,
                "user": "local-user",
            }
            # caller fields never overwrite envelope fields (e.g. a 'category' detail becomes 'x_category')
            safe = {(f"x_{k}" if k in ev else k): v for k, v in fields.items()}
            ev.update(redact_event(safe, self._secret_scanner))
            ev["chain_mode"] = "hmac" if self._key is not None else "sha256"
            ev["chain_hmac"] = chain_value(self._key, self._prev, ev)
            line = json.dumps(ev, ensure_ascii=False, separators=(",", ":")) + "\n"
            try:
                if self.fail_next_for_test:
                    raise OSError("simulated log failure")
                self._maybe_rotate()
                if self.spool_path.exists():
                    self._drain_spool(strict=True)
                self._append(self.active_path, line)
            except OSError:
                try:
                    if self.fail_next_for_test:
                        raise OSError("simulated spool failure")
                    if self.spool_path.exists() and self.spool_path.stat().st_size > SPOOL_MAX:
                        raise OSError("spool full")
                    self._append(self.spool_path, line)
                except OSError as e2:
                    raise AuditFailure("security log unavailable; action refused") from e2
            self._seq = ev["sequence"]
            self._prev = ev["chain_hmac"]
        for fn in list(self._listeners):
            try:
                fn(ev)
            except Exception:  # noqa: BLE001 - listeners (SIEM, UI) must never break the writer
                pass
        return ev

    @staticmethod
    def _append(path: Path, line: str) -> None:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())

    def _drain_spool(self, strict: bool = False) -> None:
        """Move spooled events (written while the log was unavailable) into the log, in order."""
        if not self.spool_path.exists():
            return
        try:
            data = self.spool_path.read_text("utf-8")
            if data:
                self._append(self.active_path, data)
            self.spool_path.unlink()
        except OSError:
            if strict:
                raise

    # ---------------------------------------------------------------- tail recovery / rotation
    def _recover_tail(self) -> None:
        last = None
        for path in (self.active_path, self.spool_path):
            if not path.exists():
                continue
            with open(path, "rb") as f:
                try:
                    f.seek(-65536, os.SEEK_END)
                except OSError:
                    f.seek(0)
                lines = f.read().splitlines()
            for raw in reversed(lines):
                try:
                    ev = json.loads(raw)
                except ValueError:
                    continue
                if last is None or ev.get("sequence", 0) > last.get("sequence", 0):
                    last = ev
                break
        if last is None:
            segs = self.segments()
            if segs:
                last = _last_event(segs[-1])
        if last:
            self._seq = int(last["sequence"])
            self._prev = last["chain_hmac"]

    def segments(self) -> list[Path]:
        return sorted(p for p in self.log_dir.glob("agent-security-*.jsonl"))

    def _maybe_rotate(self, force: bool = False) -> None:
        p = self.active_path
        if not p.exists():
            return
        st = p.stat()
        too_big = st.st_size >= self.rotate_bytes
        first_ts = _first_ts(p)
        too_old = first_ts is not None and (time.time() - first_ts) > self.rotate_days * 86400
        if not (force or too_big or too_old):
            return
        seal = {
            "schema_version": LOG_SCHEMA_VERSION, "event_id": new_id("evt"), "sequence": self._seq + 1,
            "prev_hash": self._prev, "timestamp": now_iso(), "category": "audit", "event_type": "log.seal",
            "severity": "info", "component": "pa-gateway", "component_version": APP_VERSION, "user": "local-user",
            "segment_bytes": st.st_size,
        }
        seal["chain_mode"] = "hmac" if self._key is not None else "sha256"
        seal["chain_hmac"] = chain_value(self._key, self._prev, seal)
        self._append(p, json.dumps(seal, separators=(",", ":")) + "\n")
        self._seq, self._prev = seal["sequence"], seal["chain_hmac"]
        stamp = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
        os.replace(p, self.log_dir / f"agent-security-{stamp}-{self._seq:012d}.jsonl")
        self._apply_retention()

    def rotate_now(self) -> None:
        with self._lock:
            self._maybe_rotate(force=True)

    def _apply_retention(self) -> None:
        cutoff = time.time() - self.retention_days * 86400
        for seg in self.segments():
            if seg.stat().st_mtime < cutoff:
                seg.unlink(missing_ok=True)

    # ---------------------------------------------------------------- verification
    def verify(self, key: bytes | None) -> dict[str, Any]:
        """Recompute the whole chain across retained segments + active file."""
        with self._lock:
            files = self.segments() + ([self.active_path] if self.active_path.exists() else [])
            prev: str | None = None
            count = 0
            unkeyed = 0
            for path in files:
                with open(path, encoding="utf-8") as f:
                    for lineno, raw in enumerate(f, 1):
                        raw = raw.strip()
                        if not raw:
                            continue
                        try:
                            ev = json.loads(raw)
                        except ValueError:
                            return _bad(count, path, lineno, "unparseable line")
                        if prev is not None and ev.get("prev_hash") != prev:
                            return _bad(count, path, lineno, "chain broken (deleted or reordered events)")
                        mode = ev.get("chain_mode", "hmac")
                        if mode == "hmac":
                            if key is None:
                                return _bad(count, path, lineno, "vault locked - cannot verify HMAC chain")
                            expect = chain_value(key, ev["prev_hash"], ev)
                        else:
                            unkeyed += 1
                            expect = chain_value(None, ev["prev_hash"], ev)
                        if not hmac.compare_digest(expect, ev.get("chain_hmac", "")):
                            return _bad(count, path, lineno, "event modified")
                        prev = ev["chain_hmac"]
                        count += 1
            return {"ok": True, "events": count, "unkeyed_events": unkeyed, "files": len(files), "checked_at": now_iso()}

    def read_events(self, limit: int = 500, since: str | None = None, until: str | None = None,
                    filters: dict[str, str] | None = None) -> list[dict[str, Any]]:
        files = self.segments() + ([self.active_path] if self.active_path.exists() else [])
        out: list[dict[str, Any]] = []
        for path in reversed(files):
            with open(path, encoding="utf-8") as f:
                lines = f.readlines()
            for raw in reversed(lines):
                try:
                    ev = json.loads(raw)
                except ValueError:
                    continue
                ts = ev.get("timestamp", "")
                if since and ts < since:
                    continue
                if until and ts > until:
                    continue
                if filters and any(v and str(ev.get(k, "")) != v for k, v in filters.items()):
                    continue
                out.append(ev)
                if len(out) >= limit:
                    return out
        return out


def _bad(count: int, path: Path, lineno: int, reason: str) -> dict[str, Any]:
    return {"ok": False, "events": count, "file": path.name, "line": lineno, "reason": reason, "checked_at": now_iso()}


def _first_ts(path: Path) -> float | None:
    try:
        with open(path, encoding="utf-8") as f:
            first = f.readline()
        if not first:
            return None
        from pa_common.timeutil import parse_iso
        return parse_iso(json.loads(first)["timestamp"]).timestamp()
    except (OSError, ValueError, KeyError):
        return None


def _last_event(path: Path) -> dict[str, Any] | None:
    last = None
    with open(path, encoding="utf-8") as f:
        for raw in f:
            try:
                last = json.loads(raw)
            except ValueError:
                continue
    return last
