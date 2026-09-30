"""RPC dispatcher (spec 2.4, 39.3).

Every method is registered with:
  roles   which IPC clients may call it (ui / core). There is no "full access" token.
  state   "any" | "unlocked" (UI usable) | "keys" (vault open, UI may be locked - for pa-core)
  stepup  step-up category required (UI only), or None
Unknown/disallowed methods are denied and logged. Params are validated with `P` before use.
"""
from __future__ import annotations

import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable

from pa_common.errors import NotAllowed, PAError, StepUpRequired, ValidationError

UI = "ui"
CORE = "core"


@dataclass
class Method:
    name: str
    fn: Callable[..., Any]
    roles: tuple[str, ...]
    state: str
    stepup: str | None
    touch: bool


@dataclass
class ClientInfo:
    role: str
    client_id: str
    pid: int | None = None
    image: str | None = None
    send: Callable[[dict[str, Any]], None] | None = None
    subscriptions: set[str] = field(default_factory=lambda: {"*"})


REGISTRY: dict[str, Method] = {}


def rpc(name: str, roles: tuple[str, ...] = (UI,), state: str = "unlocked", stepup: str | None = None, touch: bool = True):
    def deco(fn):
        if name in REGISTRY:
            raise RuntimeError(f"duplicate rpc {name}")
        REGISTRY[name] = Method(name, fn, roles, state, stepup, touch)
        return fn
    return deco


class P:
    """Tiny schema validator for RPC params (fails closed with a readable message)."""

    def __init__(self, params: Any):
        if params is None:
            params = {}
        if not isinstance(params, dict):
            raise ValidationError("params must be an object")
        self.d = params

    def str(self, k: str, required: bool = True, max_len: int = 10_000, default: str = "") -> str:
        v = self.d.get(k, None)
        if v is None:
            if required:
                raise ValidationError(f"missing '{k}'")
            return default
        if not isinstance(v, str) or len(v) > max_len:
            raise ValidationError(f"'{k}' must be text (max {max_len})")
        return v

    def int(self, k: str, required: bool = True, lo: int | None = None, hi: int | None = None, default: int = 0) -> int:
        v = self.d.get(k, None)
        if v is None:
            if required:
                raise ValidationError(f"missing '{k}'")
            return default
        if isinstance(v, bool) or not isinstance(v, int):
            raise ValidationError(f"'{k}' must be an integer")
        if (lo is not None and v < lo) or (hi is not None and v > hi):
            raise ValidationError(f"'{k}' out of range")
        return v

    def bool(self, k: str, required: bool = False, default: bool = False) -> bool:
        v = self.d.get(k, None)
        if v is None:
            if required:
                raise ValidationError(f"missing '{k}'")
            return default
        if not isinstance(v, bool):
            raise ValidationError(f"'{k}' must be true/false")
        return v

    def list(self, k: str, required: bool = False, max_items: int = 1000) -> list:
        v = self.d.get(k, None)
        if v is None:
            if required:
                raise ValidationError(f"missing '{k}'")
            return []
        if not isinstance(v, list) or len(v) > max_items:
            raise ValidationError(f"'{k}' must be a list")
        return v

    def dict(self, k: str, required: bool = False) -> dict:
        v = self.d.get(k, None)
        if v is None:
            if required:
                raise ValidationError(f"missing '{k}'")
            return {}
        if not isinstance(v, dict):
            raise ValidationError(f"'{k}' must be an object")
        return v

    def opt(self, k: str) -> Any:
        return self.d.get(k)


class Dispatcher:
    def __init__(self, gw):
        self.gw = gw
        self.clients: dict[str, ClientInfo] = {}
        self._lock = threading.RLock()
        self.pool = ThreadPoolExecutor(max_workers=48, thread_name_prefix="rpc")
        gw.add_listener(self._broadcast)
        gw.rpc = self

    # ---------------------------------------------------------------- connections
    def register_client(self, info: ClientInfo) -> None:
        with self._lock:
            self.clients[info.client_id] = info
        self.gw.audit.write("ipc.client_connected", "ipc", role=info.role, pid=info.pid, image=info.image)

    def unregister_client(self, client_id: str) -> None:
        with self._lock:
            info = self.clients.pop(client_id, None)
        if info:
            self.gw.audit.write("ipc.client_disconnected", "ipc", role=info.role, pid=info.pid)

    def _broadcast(self, topic: str, data: dict) -> None:
        msg = {"type": "evt", "topic": topic, "data": data}
        with self._lock:
            targets = [c for c in self.clients.values() if c.role == UI and c.send]
        for c in targets:
            try:
                c.send(msg)  # type: ignore[misc]
            except Exception:  # noqa: BLE001
                pass

    # ---------------------------------------------------------------- calls
    def handle(self, client: ClientInfo, msg: dict[str, Any]) -> dict[str, Any]:
        rid = msg.get("id")
        method_name = msg.get("method")
        try:
            if msg.get("type") != "req" or not isinstance(method_name, str) or not isinstance(rid, (str, int)):
                raise ValidationError("malformed request")
            m = REGISTRY.get(method_name)
            if m is None or client.role not in m.roles:
                self.gw.audit.write("ipc.method_denied", "security", role=client.role, method=str(method_name)[:80],
                                    severity="high" if client.role == CORE else "medium")
                raise NotAllowed(f"method {method_name} is not allowed for {client.role}")
            gw = self.gw
            if m.state == "unlocked":
                gw.require_unlocked()
            elif m.state == "keys":
                gw.require_keys()
            if client.role == UI and m.touch and gw.session.state == "UNLOCKED":
                gw.session.touch()
            if m.stepup and not gw.session.has_stepup(m.stepup):
                raise StepUpRequired("please confirm it's you", category=m.stepup)
            result = m.fn(gw, P(msg.get("params")), client)
            return {"type": "res", "id": rid, "ok": True, "result": result}
        except PAError as e:
            return {"type": "res", "id": rid, "ok": False, "error": e.to_dict()}
        except Exception as e:  # noqa: BLE001 - never leak internals; log type only
            try:
                self.gw.audit.write("ipc.handler_error", "ipc", method=str(method_name)[:80], error=type(e).__name__,
                                    severity="medium")
            except Exception:  # noqa: BLE001
                pass
            if __debug__ and self.gw and getattr(self.gw, "debug", False):
                traceback.print_exc()
            return {"type": "res", "id": rid, "ok": False,
                    "error": {"code": "internal_error", "message": f"internal error ({type(e).__name__})", "details": {}}}

    def call(self, client: ClientInfo, method: str, params: dict[str, Any] | None = None) -> Any:
        """In-process convenience used by tests and the in-process core client."""
        res = self.handle(client, {"type": "req", "id": 1, "method": method, "params": params or {}})
        if not res["ok"]:
            err = res["error"]
            raise PAError(err["message"], code=err["code"], **err.get("details", {}))
        return res["result"]


def import_all_methods() -> None:
    from . import api_core, api_ui  # noqa: F401
