"""Secrets vault (spec 6).

Each item: random 256-bit item key; item key wrapped with K_secret (AES-256-GCM, AAD = item id);
the whole item (title, type, tags, username, url, notes, value) is one AEAD blob under the item key.
Items live in the SQLCipher DB (double encryption). History keeps previous versions (encrypted).

The agent NEVER receives secret values: there is no IPC method for pa-core that returns them.
A secret may be BOUND to a tool/connector ("web.search", "llm:<model id>", "stt:<model id>", ...);
the gateway injects it into the outbound request in memory only.
"""
from __future__ import annotations

import csv
import io
import json
import secrets as pysecrets
import string
import sys
import threading
from datetime import timedelta
from typing import Any

from pa_common.errors import PAError
from pa_common.ids import new_id
from pa_common.timeutil import now_iso, parse_iso, utcnow

from .auth.passwords import strength
from .vault.crypto import aead_decrypt, aead_encrypt, random_key

TYPES = ("password", "api_key", "token", "note", "card")


class SecretsService:
    def __init__(self, db, keys, audit, dlp):
        self.db = db
        self.keys = keys
        self.audit = audit
        self.dlp = dlp
        self._clip_timer: threading.Timer | None = None

    # ------------------------------------------------------------------ crypto helpers
    def _wrap(self, sid: str, item: dict[str, Any]) -> tuple[bytes, bytes]:
        ik = random_key()
        wrapped = aead_encrypt(self.keys.key("K_secret"), ik, f"pa/secret-key/{sid}".encode())
        blob = aead_encrypt(ik, json.dumps(item).encode("utf-8"), f"pa/secret/{sid}".encode())
        return wrapped, blob

    def _unwrap(self, sid: str, wrapped: bytes, blob: bytes) -> dict[str, Any]:
        ik = aead_decrypt(self.keys.key("K_secret"), bytes(wrapped), f"pa/secret-key/{sid}".encode())
        return json.loads(aead_decrypt(ik, bytes(blob), f"pa/secret/{sid}".encode()))

    def _row(self, sid: str) -> dict[str, Any]:
        r = self.db.one("SELECT * FROM secrets WHERE id=? AND deleted=0", (sid,))
        if not r:
            raise PAError("secret not found", code="not_found")
        return r

    def _fingerprint(self, sid: str, value: str) -> None:
        self.db.execute("DELETE FROM secret_fingerprints WHERE secret_id=?", (sid,))
        fp = self.dlp.add_secret_value(value)
        if fp:
            self.db.insert("secret_fingerprints", {"secret_id": sid, "length": fp[0], "fp": fp[1]})

    def load_dlp(self) -> None:
        self.dlp.load_fingerprints((r["length"], r["fp"]) for r in self.db.all(
            "SELECT f.length, f.fp FROM secret_fingerprints f JOIN secrets s ON s.id=f.secret_id WHERE s.deleted=0"))

    # ------------------------------------------------------------------ CRUD
    def create(self, item: dict[str, Any], bindings: list[str] | None = None) -> str:
        item = self._validate(item)
        sid = new_id("sec")
        wrapped, blob = self._wrap(sid, item)
        now = now_iso()
        with self.db.tx():
            self.db.insert("secrets", {"id": sid, "wrapped_key": wrapped, "blob": blob, "version": 1,
                                       "created_at": now, "updated_at": now})
            self._fingerprint(sid, item["value"])
            for b in bindings or []:
                self.db.insert("secret_bindings", {"secret_id": sid, "binding": b})
        self.audit.write("secret.created", "secrets", secret_id=sid, secret_type=item["type"], bindings=bindings or [])
        return sid

    def update(self, sid: str, item: dict[str, Any]) -> None:
        item = self._validate(item)
        r = self._row(sid)
        wrapped, blob = self._wrap(sid, item)
        with self.db.tx():
            self.db.insert("secret_versions", {"id": new_id("sv"), "secret_id": sid, "version": r["version"],
                                               "wrapped_key": r["wrapped_key"], "blob": r["blob"], "created_at": now_iso()})
            self.db.update("secrets", "id", sid, {"wrapped_key": wrapped, "blob": blob, "version": r["version"] + 1,
                                                  "updated_at": now_iso()})
            self._fingerprint(sid, item["value"])
        self.audit.write("secret.updated", "secrets", secret_id=sid, version=r["version"] + 1)

    def delete(self, sid: str) -> None:
        self._row(sid)
        with self.db.tx():
            self.db.execute("DELETE FROM secret_versions WHERE secret_id=?", (sid,))
            self.db.execute("DELETE FROM secret_bindings WHERE secret_id=?", (sid,))
            self.db.execute("DELETE FROM secret_fingerprints WHERE secret_id=?", (sid,))
            self.db.execute("DELETE FROM secrets WHERE id=?", (sid,))
        self.load_dlp()
        self.audit.write("secret.deleted", "secrets", secret_id=sid)

    @staticmethod
    def _validate(item: dict[str, Any]) -> dict[str, Any]:
        t = item.get("type", "password")
        if t not in TYPES:
            raise PAError("invalid secret type", code="invalid_request")
        value = item.get("value", "")
        if not isinstance(value, str) or not value or len(value) > 65536:
            raise PAError("secret value is required (max 64 KB)", code="invalid_request")
        title = str(item.get("title", "")).strip()
        if not title or len(title) > 200:
            raise PAError("title is required", code="invalid_request")
        return {"title": title, "type": t, "value": value, "username": str(item.get("username", ""))[:500],
                "url": str(item.get("url", ""))[:2000], "notes": str(item.get("notes", ""))[:20000],
                "tags": [str(x)[:50] for x in item.get("tags", [])][:30]}

    def list(self) -> list[dict[str, Any]]:
        out = []
        for r in self.db.all("SELECT * FROM secrets WHERE deleted=0 ORDER BY updated_at DESC"):
            item = self._unwrap(r["id"], r["wrapped_key"], r["blob"])
            out.append({"id": r["id"], "title": item["title"], "type": item["type"], "username": item["username"],
                        "url": item["url"], "tags": item["tags"], "has_notes": bool(item["notes"]),
                        "created_at": r["created_at"], "updated_at": r["updated_at"], "last_used_at": r["last_used_at"],
                        "version": r["version"], "bindings": self.bindings(r["id"])})
        return out

    def bindings(self, sid: str) -> list[str]:
        return [r["binding"] for r in self.db.all("SELECT binding FROM secret_bindings WHERE secret_id=?", (sid,))]

    def set_bindings(self, sid: str, bindings: list[str]) -> None:
        self._row(sid)
        with self.db.tx():
            self.db.execute("DELETE FROM secret_bindings WHERE secret_id=?", (sid,))
            for b in sorted(set(bindings)):
                self.db.insert("secret_bindings", {"secret_id": sid, "binding": b[:200]})
        self.audit.write("secret.bindings_changed", "secrets", secret_id=sid, bindings=bindings)

    def reveal(self, sid: str) -> dict[str, Any]:
        """UI only, after step-up. The value is auto-hidden by the UI after 20 s."""
        r = self._row(sid)
        item = self._unwrap(sid, r["wrapped_key"], r["blob"])
        self.audit.write("secret.revealed", "secrets", secret_id=sid, severity="medium")
        return item

    def versions(self, sid: str) -> list[dict[str, Any]]:
        return self.db.all("SELECT id, version, created_at FROM secret_versions WHERE secret_id=? ORDER BY version DESC", (sid,))

    def restore_version(self, sid: str, version_id: str) -> None:
        v = self.db.one("SELECT * FROM secret_versions WHERE id=? AND secret_id=?", (version_id, sid))
        if not v:
            raise PAError("version not found", code="not_found")
        item = self._unwrap(sid, v["wrapped_key"], v["blob"])
        self.update(sid, item)

    # ------------------------------------------------------------------ gateway-internal use
    def value_for_binding(self, binding: str) -> str | None:
        """Gateway-internal: returns the value bound to a tool/connector. NEVER exposed over IPC."""
        r = self.db.one("SELECT s.* FROM secrets s JOIN secret_bindings b ON b.secret_id=s.id "
                        "WHERE b.binding=? AND s.deleted=0 ORDER BY s.updated_at DESC LIMIT 1", (binding,))
        if not r:
            return None
        item = self._unwrap(r["id"], r["wrapped_key"], r["blob"])
        self.db.update("secrets", "id", r["id"], {"last_used_at": now_iso()})
        self.audit.write("secret.used_by_tool", "secrets", secret_id=r["id"], binding=binding)
        return item["value"]

    def has_binding(self, binding: str) -> bool:
        return bool(self.db.scalar("SELECT count(*) FROM secret_bindings b JOIN secrets s ON s.id=b.secret_id "
                                   "WHERE b.binding=? AND s.deleted=0", (binding,)))

    def upsert_bound(self, binding: str, title: str, value: str, type_: str = "token") -> str:
        """Store/replace a gateway-managed secret (e.g. an M365 refresh token) bound to `binding`."""
        existing = self.db.one("SELECT s.id FROM secrets s JOIN secret_bindings b ON b.secret_id=s.id WHERE b.binding=? "
                               "AND s.deleted=0 LIMIT 1", (binding,))
        item = {"title": title, "type": type_, "value": value, "tags": ["managed"]}
        if existing:
            self.update(existing["id"], item)
            return existing["id"]
        return self.create(item, [binding])

    def delete_bound(self, binding: str) -> None:
        for r in self.db.all("SELECT secret_id FROM secret_bindings WHERE binding=?", (binding,)):
            self.delete(r["secret_id"])

    # ------------------------------------------------------------------ clipboard (spec 6.3)
    def copy_to_clipboard(self, sid: str, clear_after: int = 30) -> dict[str, Any]:
        item = self.reveal(sid)
        self.audit.write("secret.copied", "secrets", secret_id=sid, clear_after=clear_after)
        if sys.platform != "win32":
            raise PAError("secure clipboard is only available on Windows", code="unavailable")
        _win_clipboard_set(item["value"])
        if self._clip_timer:
            self._clip_timer.cancel()
        self._clip_timer = threading.Timer(clear_after, _win_clipboard_clear_if, args=(item["value"],))
        self._clip_timer.daemon = True
        self._clip_timer.start()
        return {"cleared_in": clear_after}

    # ------------------------------------------------------------------ health / generator / import
    def health(self) -> dict[str, Any]:
        weak, reused, old = [], [], []
        seen: dict[str, str] = {}
        cutoff = utcnow() - timedelta(days=365)
        for r in self.db.all("SELECT * FROM secrets WHERE deleted=0"):
            item = self._unwrap(r["id"], r["wrapped_key"], r["blob"])
            if item["type"] == "password":
                if strength(item["value"])["score"] < 3:
                    weak.append(item["title"])
                h = self.dlp.fingerprint(item["value"]).hex()
                if h in seen:
                    reused.append(f"{item['title']} = {seen[h]}")
                seen[h] = item["title"]
            if parse_iso(r["updated_at"]) < cutoff:
                old.append(item["title"])
        return {"weak": weak, "reused": reused, "old": old}

    @staticmethod
    def generate(length: int = 20, lower: bool = True, upper: bool = True, digits: bool = True, symbols: bool = True) -> str:
        pools = [p for p, on in ((string.ascii_lowercase, lower), (string.ascii_uppercase, upper), (string.digits, digits),
                                 ("!@#$%^&*()-_=+[]{};:,.?/", symbols)) if on]
        if not pools or not (8 <= length <= 128):
            raise PAError("choose 8-128 characters and at least one character set", code="invalid_request")
        chars = [pysecrets.choice(p) for p in pools]
        alphabet = "".join(pools)
        chars += [pysecrets.choice(alphabet) for _ in range(length - len(chars))]
        pysecrets.SystemRandom().shuffle(chars)
        return "".join(chars)

    def import_csv(self, text: str) -> int:
        reader = csv.DictReader(io.StringIO(text))
        n = 0
        for row in reader:
            low = {k.lower().strip(): (v or "") for k, v in row.items() if k}
            value = low.get("password") or low.get("value") or low.get("secret")
            title = low.get("title") or low.get("name") or low.get("url") or "Imported"
            if not value:
                continue
            self.create({"title": title, "type": "password", "value": value, "username": low.get("username", ""),
                         "url": low.get("url", ""), "notes": low.get("notes", ""), "tags": ["imported"]})
            n += 1
        self.audit.write("secret.imported", "secrets", count=n)
        return n

    def export_items(self) -> list[dict[str, Any]]:
        """Plain items for an ENCRYPTED export archive only (caller encrypts with the password)."""
        out = []
        for r in self.db.all("SELECT * FROM secrets WHERE deleted=0"):
            item = self._unwrap(r["id"], r["wrapped_key"], r["blob"])
            item["bindings"] = self.bindings(r["id"])
            out.append(item)
        self.audit.write("secret.exported", "secrets", count=len(out), severity="medium")
        return out


# ---------------------------------------------------------------------- Windows clipboard helpers
def _win_clipboard_set(text: str) -> None:
    import win32clipboard  # type: ignore[import-not-found]
    import win32con  # type: ignore[import-not-found]

    exclude = win32clipboard.RegisterClipboardFormat("ExcludeClipboardContentFromMonitorProcessing")
    no_history = win32clipboard.RegisterClipboardFormat("CanIncludeInClipboardHistory")
    no_cloud = win32clipboard.RegisterClipboardFormat("CanUploadToCloudClipboard")
    win32clipboard.OpenClipboard()
    try:
        win32clipboard.EmptyClipboard()
        win32clipboard.SetClipboardData(win32con.CF_UNICODETEXT, text)
        zero = (0).to_bytes(4, "little")
        win32clipboard.SetClipboardData(no_history, zero)
        win32clipboard.SetClipboardData(no_cloud, zero)
        win32clipboard.SetClipboardData(exclude, zero)
    finally:
        win32clipboard.CloseClipboard()


def _win_clipboard_clear_if(expected: str) -> None:
    try:
        import win32clipboard  # type: ignore[import-not-found]
        import win32con  # type: ignore[import-not-found]
        win32clipboard.OpenClipboard()
        try:
            current = win32clipboard.GetClipboardData(win32con.CF_UNICODETEXT)
            if current == expected:
                win32clipboard.EmptyClipboard()
        finally:
            win32clipboard.CloseClipboard()
    except Exception:  # noqa: BLE001
        pass
