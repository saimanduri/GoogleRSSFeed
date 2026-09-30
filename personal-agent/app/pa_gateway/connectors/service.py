"""Connector framework (spec 8, 39.4).

Effective state, computed on EVERY call:
  USABLE = toggle ON AND NOT "Pause all connectors" AND NOT agent paused / emergency stop
           AND connection valid AND context flag ("Use in chat" / "Use in missions")

Switching OFF: saved + logged before the UI shows "Off"; new calls denied immediately; in-flight
results discarded by the tool gateway; mission tasks needing it -> WAITING_FOR_RESOURCE ("turned off
by you"); no alternate path (web.fetch to the connector's domains is denied).
"Disconnect" additionally deletes stored tokens and purges caches.
Future connectors (e.g. ChiRAG) plug in via ConnectorAdapter + a signed manifest - no framework change.
"""
from __future__ import annotations

import json
import threading
from typing import Any

from pa_common.errors import PAError
from pa_common.timeutil import now_iso

from ..policy.tools_registry import CONNECTOR_DOMAINS


class ConnectorAdapter:
    id = "abstract"
    label = "Connector"
    description = ""
    manifest: dict[str, Any] = {}

    def __init__(self, gw):
        self.gw = gw

    def connection_valid(self) -> tuple[bool, str]:
        return True, ""

    def status(self) -> dict[str, Any]:
        ok, reason = self.connection_valid()
        return {"connection_ok": ok, "connection_reason": reason}

    def disconnect(self) -> None:
        pass

    def register_tools(self, tg) -> None:
        pass


class ConnectorService:
    def __init__(self, gw):
        self.gw = gw
        self.adapters: dict[str, ConnectorAdapter] = {}
        self._lock = threading.RLock()

    def register(self, adapter: ConnectorAdapter) -> None:
        self.adapters[adapter.id] = adapter
        if not self.gw.db.one("SELECT id FROM connectors WHERE id=?", (adapter.id,)):
            self.gw.db.insert("connectors", {"id": adapter.id, "enabled": 0, "use_chat": 1, "use_missions": 1,
                                             "connected": 0, "updated_at": now_iso()})
        adapter.register_tools(self.gw.tools)

    @property
    def web(self):
        return self.adapters["web"]

    def row(self, cid: str) -> dict[str, Any]:
        r = self.gw.db.one("SELECT * FROM connectors WHERE id=?", (cid,))
        if not r:
            raise PAError("unknown connector", code="not_found")
        return r

    def pause_all(self) -> bool:
        r = self.gw.db.one("SELECT value FROM meta WHERE key='connectors.pause_all'")
        return bool(r and r["value"] == "1")

    def usable(self, cid: str, context: str) -> tuple[bool, str]:
        try:
            r = self.row(cid)
        except PAError:
            return False, f"unknown connector {cid}"
        if not r["enabled"]:
            return False, f"{self.adapters[cid].label} is turned off by you"
        if self.pause_all():
            return False, "all connectors are paused"
        ks = self.gw.killswitch
        if ks.agent_blocked() or ks.active("disable_connectors") or (cid == "web" and ks.active("disable_web")):
            return False, "emergency stop is active"
        if context == "chat" and not r["use_chat"]:
            return False, f"{self.adapters[cid].label} is not enabled for chat"
        if context == "mission" and not r["use_missions"]:
            return False, f"{self.adapters[cid].label} is not enabled for missions"
        ok, reason = self.adapters[cid].connection_valid()
        if not ok:
            return False, reason
        return True, ""

    def denied_domains(self) -> tuple[str, ...]:
        out: list[str] = []
        for cid, doms in CONNECTOR_DOMAINS.items():
            if cid in self.adapters and not self.usable(cid, "chat")[0] and not self.usable(cid, "mission")[0]:
                out += doms
        return tuple(out)

    def list(self) -> list[dict[str, Any]]:
        out = []
        for cid, a in self.adapters.items():
            r = self.row(cid)
            deps = self.gw.db.all("SELECT id, name, status FROM missions WHERE allowed_connectors_json LIKE ?", (f'%"{cid}"%',))
            out.append({"id": cid, "label": a.label, "description": a.description, "enabled": bool(r["enabled"]),
                        "use_chat": bool(r["use_chat"]), "use_missions": bool(r["use_missions"]),
                        "scopes": json.loads(r["scopes_json"] or "[]"), "last_used_at": r["last_used_at"],
                        "manifest": a.manifest, "dependent_missions": deps,
                        "usable_chat": self.usable(cid, "chat")[0], "usable_missions": self.usable(cid, "mission")[0],
                        **a.status()})
        return out

    def set_flags(self, cid: str, **flags: bool) -> None:
        r = self.row(cid)
        changes = {k: int(v) for k, v in flags.items() if k in ("enabled", "use_chat", "use_missions")}
        if not changes:
            return
        # save + log BEFORE the UI shows the new state (spec 8.3.1)
        self.gw.audit.write("connector.changed", "connector", connector=cid,
                            before={k: r[k] for k in changes}, after=changes)
        self.gw.db.update("connectors", "id", cid, {**changes, "updated_at": now_iso()})
        turned_off = any(v == 0 and r[k] for k, v in changes.items())
        if turned_off:
            self._on_off(cid)
        self.gw.emit("connectors.changed", {"connector": cid})

    def set_pause_all(self, paused: bool) -> None:
        self.gw.audit.write("connector.pause_all", "connector", paused=paused)
        self.gw.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('connectors.pause_all',?)", ("1" if paused else "0",))
        if paused:
            for cid in self.adapters:
                self._on_off(cid)
        self.gw.emit("connectors.changed", {"connector": "*"})

    def _on_off(self, cid: str) -> None:
        """Missions that depend on this connector wait for it; they resume only when you click Resume."""
        for m in self.gw.db.all("SELECT id FROM missions WHERE status='ACTIVE' AND allowed_connectors_json LIKE ?", (f'%"{cid}"%',)):
            for t in self.gw.db.all("SELECT id FROM tasks WHERE mission_id=? AND state IN ('QUEUED','RUNNING')", (m["id"],)):
                self.gw.tasks.cancel_event(t["id"]).set()
                self.gw.tasks.transition(t["id"], "WAITING_FOR_RESOURCE", f"{self.adapters[cid].label} turned off by you")
            self.gw.missions.set_status(m["id"], "SUSPENDED", f"{cid} turned off")

    def disconnect(self, cid: str, delete_data: bool = False) -> None:
        self.set_flags(cid, enabled=False)
        self.adapters[cid].disconnect()
        self.gw.db.update("connectors", "id", cid, {"connected": 0, "scopes_json": [], "updated_at": now_iso()})
        if delete_data:
            for f in self.gw.db.all("SELECT id FROM files WHERE source=? AND deleted=0", (cid,)):
                self.gw.files.delete(f["id"], "connector data deleted")
            self.gw.memory.delete_by_source(cid)
        self.gw.audit.write("connector.disconnected", "connector", connector=cid, delete_data=delete_data)
        self.gw.emit("connectors.changed", {"connector": cid})

    def touch(self, cid: str) -> None:
        self.gw.db.update("connectors", "id", cid, {"last_used_at": now_iso()})
