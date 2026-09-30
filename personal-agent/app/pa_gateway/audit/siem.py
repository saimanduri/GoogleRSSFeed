"""Optional SIEM forwarding (spec 25.6). Off by default.

Transports:
  - https : POST JSON array batches to a URL; server certificate pinned by SHA-256 fingerprint
  - syslog: RFC 5425 syslog over TLS (octet-counted framing), certificate pinned
Formats: "json" (the log line as-is, metadata only) or "cef".
Forwarding runs on a background thread from an in-memory queue; lag is exposed for the UI.
"""
from __future__ import annotations

import hashlib
import json
import queue
import socket
import ssl
import threading
import time
from typing import Any
from urllib.parse import urlparse

from pa_common.timeutil import now_iso


class PinnedTLSError(Exception):
    pass


def _pinned_context() -> ssl.SSLContext:
    # Trust is established by the pinned fingerprint, not by the CA chain (self-signed SIEMs are common).
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx


def _open_pinned(host: str, port: int, fingerprint: str, timeout: float = 10.0) -> ssl.SSLSocket:
    raw = socket.create_connection((host, port), timeout=timeout)
    tls = _pinned_context().wrap_socket(raw, server_hostname=host)
    der = tls.getpeercert(binary_form=True) or b""
    got = hashlib.sha256(der).hexdigest()
    want = fingerprint.lower().replace(":", "").strip()
    if not want or got != want:
        tls.close()
        raise PinnedTLSError(f"certificate fingerprint mismatch (got {got})")
    return tls


def to_cef(ev: dict[str, Any]) -> str:
    def esc(v: Any) -> str:
        return str(v).replace("\\", "\\\\").replace("=", "\\=").replace("\n", " ")
    sev = {"info": 3, "low": 3, "medium": 6, "high": 8, "critical": 10}.get(str(ev.get("severity", "info")), 3)
    ext = " ".join(f"{k}={esc(v)}" for k, v in ev.items() if k not in ("event_type", "severity") and not isinstance(v, (dict, list)))
    return f"CEF:0|PersonalAgent|DesktopAgent|{ev.get('component_version', '')}|{esc(ev.get('event_type'))}|{esc(ev.get('event_type'))}|{sev}|{ext}"


def _syslog_frame(msg: str) -> bytes:
    body = f"<134>1 {now_iso()} {socket.gethostname()} PersonalAgent - - - {msg}".encode()
    return f"{len(body)} ".encode() + body


class SiemForwarder:
    def __init__(self) -> None:
        self.config: dict[str, Any] = {"enabled": False}
        self._q: "queue.Queue[dict[str, Any]]" = queue.Queue(maxsize=50000)
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self.last_ok: str | None = None
        self.last_error: str | None = None
        self.forwarded = 0
        self.dropped = 0

    def configure(self, config: dict[str, Any]) -> None:
        self.config = dict(config)
        if self.config.get("enabled") and not self._thread:
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, name="siem-forwarder", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def on_event(self, ev: dict[str, Any]) -> None:
        if not self.config.get("enabled"):
            return
        try:
            self._q.put_nowait(ev)
        except queue.Full:
            self.dropped += 1

    def status(self) -> dict[str, Any]:
        return {"enabled": bool(self.config.get("enabled")), "queued": self._q.qsize(), "forwarded": self.forwarded,
                "dropped": self.dropped, "last_ok": self.last_ok, "last_error": self.last_error}

    def _format(self, ev: dict[str, Any]) -> str:
        return to_cef(ev) if self.config.get("format") == "cef" else json.dumps(ev, separators=(",", ":"))

    def send_batch(self, events: list[dict[str, Any]]) -> None:
        cfg = self.config
        if cfg.get("transport") == "syslog":
            with _open_pinned(cfg["host"], int(cfg.get("port", 6514)), cfg.get("fingerprint", "")) as s:
                for ev in events:
                    s.sendall(_syslog_frame(self._format(ev)))
        else:
            url = urlparse(cfg["url"])
            if url.scheme != "https":
                raise PinnedTLSError("SIEM HTTPS endpoint must use https://")
            port = url.port or 443
            body = json.dumps([json.loads(self._format(e)) if cfg.get("format") != "cef" else self._format(e) for e in events]).encode()
            path = url.path or "/"
            if url.query:
                path += "?" + url.query
            req = (f"POST {path} HTTP/1.1\r\nHost: {url.hostname}\r\nContent-Type: application/json\r\n"
                   f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode() + body
            with _open_pinned(url.hostname or "", port, cfg.get("fingerprint", "")) as s:
                s.sendall(req)
                status = s.recv(64).split(b" ", 2)
                if len(status) < 2 or not status[1].startswith(b"2"):
                    raise PinnedTLSError(f"SIEM endpoint returned {status[1:2]}")

    def test_connection(self) -> dict[str, Any]:
        try:
            self.send_batch([{"event_type": "siem.test", "timestamp": now_iso(), "severity": "info"}])
            return {"ok": True}
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "error": str(e)}

    def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            batch: list[dict[str, Any]] = []
            try:
                batch.append(self._q.get(timeout=1.0))
            except queue.Empty:
                continue
            while len(batch) < 200:
                try:
                    batch.append(self._q.get_nowait())
                except queue.Empty:
                    break
            while not self._stop.is_set():
                try:
                    self.send_batch(batch)
                    self.forwarded += len(batch)
                    self.last_ok = now_iso()
                    backoff = 1.0
                    break
                except Exception as e:  # noqa: BLE001
                    self.last_error = f"{now_iso()} {e}"
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 300)
