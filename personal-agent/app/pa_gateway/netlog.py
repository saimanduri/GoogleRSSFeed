"""Network log: one row for every network request this application makes (web fetch / search, Microsoft sign-in and
Graph, language-model calls including the local runtime), with time, destination, purpose, outcome and size.

Observability only: it never contains request bodies, headers, tokens or query strings (the path is stored without
its query). Rows older than RETENTION_DAYS are deleted. Written by the gateway, the only process that has a network.
"""
from __future__ import annotations

import contextvars
import ipaddress
import time
from contextlib import contextmanager
from datetime import timedelta
from typing import Any
from urllib.parse import urlsplit

from pa_common.ids import new_id
from pa_common.timeutil import now_iso, to_iso, utcnow

RETENTION_DAYS = 14
_ctx: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar("netlog_ctx", default=None)


def is_loopback(host: str) -> bool:
    h = (host or "").strip("[]").lower()
    if h in ("localhost", ""):
        return True
    try:
        return ipaddress.ip_address(h).is_loopback
    except ValueError:
        return False


class NetLog:
    def __init__(self, gw):
        self.gw = gw
        self._last_purge = 0.0

    # ------------------------------------------------------------------ context (who asked for this request)
    @contextmanager
    def context(self, **info: Any):
        token = _ctx.set({**(_ctx.get() or {}), **{k: v for k, v in info.items() if v is not None}})
        try:
            yield
        finally:
            _ctx.reset(token)

    # ------------------------------------------------------------------ write
    def record(self, component: str, method: str, url: str, status: int | None = None, *, outcome: str | None = None, reason: str = "",
               bytes_out: int = 0, bytes_in: int = 0, duration_ms: int = 0, ip: str | None = None, purpose: str = "") -> None:
        """Never raises: logging must not break the request it describes."""
        try:
            if self.gw.db is None:
                return
            parts = urlsplit(url)
            host = (parts.hostname or "").lower()
            port = parts.port or (443 if parts.scheme == "https" else 80 if parts.scheme == "http" else None)
            if outcome is None:
                outcome = "ok" if status is not None and status < 400 else "error"
            c = _ctx.get() or {}
            self.gw.db.insert("net_log", {
                "id": new_id("net"), "ts": now_iso(), "component": component, "method": method.upper()[:10], "scheme": parts.scheme or "",
                "host": host[:255], "port": port, "path": (parts.path or "/")[:200], "status": status, "outcome": outcome, "reason": reason[:200],
                "bytes_out": int(bytes_out), "bytes_in": int(bytes_in), "duration_ms": int(duration_ms), "ip": (ip or "")[:64],
                "loopback": 1 if is_loopback(host) else 0, "purpose": (purpose or c.get("purpose") or "")[:120],
                "tool": str(c.get("tool") or "")[:80], "task_id": c.get("task_id"), "run_id": c.get("run_id")})
            if time.time() - self._last_purge > 3600:
                self._last_purge = time.time()
                self.purge()
        except Exception:  # noqa: BLE001
            pass

    def purge(self, days: int = RETENTION_DAYS) -> int:
        cutoff = to_iso(utcnow().replace(microsecond=0) - timedelta(days=days))
        return self.gw.db.execute("DELETE FROM net_log WHERE ts < ?", (cutoff,)) or 0

    # ------------------------------------------------------------------ read
    def query(self, days: int = 7, host: str = "", component: str = "", outcome: str = "", limit: int = 300, offset: int = 0,
              include_loopback: bool = True) -> dict[str, Any]:
        days = max(1, min(int(days), RETENTION_DAYS))
        since = to_iso(utcnow() - timedelta(days=days))
        where, args = ["ts >= ?"], [since]
        if host:
            where.append("host LIKE ?")
            args.append(f"%{host.lower()}%")
        if component:
            where.append("component = ?")
            args.append(component)
        if outcome:
            where.append("outcome = ?")
            args.append(outcome)
        if not include_loopback:
            where.append("loopback = 0")
        w = " AND ".join(where)
        db = self.gw.db
        rows = db.all(f"SELECT * FROM net_log WHERE {w} ORDER BY ts DESC LIMIT ? OFFSET ?", (*args, max(1, min(int(limit), 1000)), max(0, int(offset))))
        total = int(db.scalar(f"SELECT count(*) FROM net_log WHERE {w}", tuple(args)) or 0)
        tot = db.one(f"SELECT count(*) AS n, COALESCE(SUM(bytes_out),0) AS out, COALESCE(SUM(bytes_in),0) AS inn, "
                     f"COALESCE(SUM(outcome='blocked'),0) AS blocked, COALESCE(SUM(outcome='error'),0) AS errors, "
                     f"COALESCE(SUM(loopback),0) AS local FROM net_log WHERE {w}", tuple(args)) or {}
        hosts = db.all(f"SELECT host, count(*) AS requests, COALESCE(SUM(bytes_out),0) AS bytes_out, COALESCE(SUM(bytes_in),0) AS bytes_in, "
                       f"COALESCE(SUM(outcome='blocked'),0) AS blocked, MAX(ts) AS last_ts, MAX(component) AS component, MAX(loopback) AS loopback "
                       f"FROM net_log WHERE {w} GROUP BY host ORDER BY requests DESC LIMIT 25", tuple(args))
        by_day = db.all(f"SELECT substr(ts,1,10) AS day, count(*) AS requests, COALESCE(SUM(outcome='blocked'),0) AS blocked "
                        f"FROM net_log WHERE {w} GROUP BY day ORDER BY day", tuple(args))
        by_component = db.all(f"SELECT component, count(*) AS requests FROM net_log WHERE {w} GROUP BY component ORDER BY requests DESC", tuple(args))
        return {"rows": rows, "total": total, "retention_days": RETENTION_DAYS,
                "summary": {"requests": int(tot.get("n") or 0), "bytes_out": int(tot.get("out") or 0), "bytes_in": int(tot.get("inn") or 0),
                            "blocked": int(tot.get("blocked") or 0), "errors": int(tot.get("errors") or 0), "local": int(tot.get("local") or 0),
                            "hosts": hosts, "by_day": by_day, "by_component": by_component}}
