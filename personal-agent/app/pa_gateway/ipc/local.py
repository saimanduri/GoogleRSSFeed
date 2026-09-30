"""In-process IPC client: same dispatcher, same role allowlists, no pipe. Used by tests and by the
developer-mode in-process core (the security checks are identical; only the transport differs)."""
from __future__ import annotations

import threading
from typing import Any

from pa_common.ids import new_id

from .dispatch import ClientInfo, Dispatcher


class LocalClient:
    def __init__(self, dispatcher: Dispatcher, role: str, worker: str = "w0", token: str | None = None):
        self.dispatcher = dispatcher
        self.worker = worker
        self.info = ClientInfo(role=role, client_id=f"local-{role}-{new_id('c')}")
        self.events: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        if role == "ui":
            self.info.send = self._on_event
            dispatcher.register_client(self.info)

    def _on_event(self, msg: dict[str, Any]) -> None:
        with self._lock:
            self.events.append(msg)
            if len(self.events) > 5000:
                del self.events[:1000]

    def call(self, method: str, params: dict[str, Any] | None = None) -> Any:
        params = dict(params or {})
        if self.info.role == "core":
            params.setdefault("worker", self.worker)
        return self.dispatcher.call(self.info, method, params)

    def raw(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        params = dict(params or {})
        if self.info.role == "core":
            params.setdefault("worker", self.worker)
        return self.dispatcher.handle(self.info, {"type": "req", "id": 1, "method": method, "params": params})

    def close(self) -> None:
        self.dispatcher.unregister_client(self.info.client_id)
