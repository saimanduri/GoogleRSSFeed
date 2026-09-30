"""Named-pipe client used by pa-core (and by tests). One connection per worker thread; synchronous
request/response. Opens the pipe with SECURITY_IDENTIFICATION (the server cannot impersonate us)
and checks that the server process is the gateway PID we were given (pipe-squatting defence)."""
from __future__ import annotations

import itertools
from typing import Any

from .errors import PAError
from .protocol import HEADER, FrameDecoder, encode_frame


class PipeClient:
    def __init__(self, pipe_name: str, role: str, token: str, worker: str, expected_server_pid: int | None = None):
        import win32file  # type: ignore[import-not-found]
        import win32pipe  # type: ignore[import-not-found]
        SECURITY_SQOS_PRESENT = 0x00100000
        SECURITY_IDENTIFICATION = 0x00010000
        win32pipe.WaitNamedPipe(pipe_name, 10_000)
        self.h = win32file.CreateFile(pipe_name, win32file.GENERIC_READ | win32file.GENERIC_WRITE, 0, None,
                                      win32file.OPEN_EXISTING, SECURITY_SQOS_PRESENT | SECURITY_IDENTIFICATION, None)
        if expected_server_pid is not None:
            server_pid = win32pipe.GetNamedPipeServerProcessId(self.h)
            if server_pid != expected_server_pid:
                self.h.Close()
                raise PAError("pipe server is not the gateway", code="pipe_squatting")
        self.worker = worker
        self.role = role
        self._ids = itertools.count(1)
        self._dec = FrameDecoder()
        self._pending: list[dict[str, Any]] = []
        self._send({"type": "hello", "role": role, "token": token, "protocol": 1})
        res = self._recv()
        if not res.get("ok"):
            raise PAError("gateway rejected the connection", code="handshake_failed")

    def _send(self, msg: dict[str, Any]) -> None:
        import win32file  # type: ignore[import-not-found]
        win32file.WriteFile(self.h, encode_frame(msg))

    def _recv(self) -> dict[str, Any]:
        import win32file  # type: ignore[import-not-found]
        while not self._pending:
            _, data = win32file.ReadFile(self.h, 65536)
            if not data:
                raise PAError("gateway closed the connection", code="disconnected")
            self._pending.extend(self._dec.feed(bytes(data)))
        return self._pending.pop(0)

    def call(self, method: str, params: dict[str, Any] | None = None) -> Any:
        params = dict(params or {})
        if self.role == "core":
            params.setdefault("worker", self.worker)
        rid = next(self._ids)
        self._send({"type": "req", "id": rid, "method": method, "params": params})
        while True:
            msg = self._recv()
            if msg.get("type") == "res" and msg.get("id") == rid:
                break
        if not msg.get("ok"):
            err = msg.get("error") or {}
            raise PAError(err.get("message", "error"), code=err.get("code", "error"), **(err.get("details") or {}))
        return msg.get("result")

    def close(self) -> None:
        try:
            self.h.Close()
        except Exception:  # noqa: BLE001
            pass


_ = HEADER
