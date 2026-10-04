"""My Files + the file security pipeline (spec 15).

  ingest -> QUARANTINE (encrypted, not usable) -> validate -> scan (Defender + built-in) ->
  disarm (text only; macros/objects never executed or kept) -> classify -> parse in pa-parser ->
  READY (available to the agent as UNTRUSTED data)

Blobs: files/<id>.bin, AES-256-GCM in 1 MiB chunks (nonce per chunk, AAD = file id|index|final),
per-file random key wrapped with K_files. Plaintext only ever exists in memory, in pa-parser's
stdin pipe, and briefly in the ACL-protected tmp folder while Defender scans it.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import struct
import threading
from pathlib import Path
from typing import Any, Callable

from pa_common.errors import PAError
from pa_common.ids import new_id
from pa_common.sensitivity import Sensitivity
from pa_common.timeutil import now_iso

from ..vault.crypto import CryptoError, aead_decrypt, aead_encrypt, random_key
from ..workers import run_worker
from . import checks

CHUNK = 1024 * 1024
MAGIC = b"PAF1"

QUARANTINED = "QUARANTINED"
PROCESSING = "PROCESSING"
READY = "READY"
REJECTED = "REJECTED"


class FilesService:
    def __init__(self, gw):
        self.gw = gw
        self.db = gw.db
        self.dir: Path = gw.paths.files_dir
        self.tmp: Path = gw.paths.tmp_dir
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ blob crypto
    def _key(self, fid: str, wrapped: bytes) -> bytes:
        return aead_decrypt(self.gw.keys.key("K_files"), bytes(wrapped), f"pa/file-key/{fid}".encode())

    def _write_blob(self, fid: str, data: bytes) -> bytes:
        fk = random_key()
        wrapped = aead_encrypt(self.gw.keys.key("K_files"), fk, f"pa/file-key/{fid}".encode())
        path = self.dir / f"{fid}.bin"
        tmp = path.with_suffix(".part")
        n = max(1, (len(data) + CHUNK - 1) // CHUNK)
        with open(tmp, "wb") as f:
            f.write(MAGIC + struct.pack("<I", n))
            for i in range(n):
                chunk = data[i * CHUNK:(i + 1) * CHUNK]
                aad = f"{fid}|{i}|{int(i == n - 1)}".encode()
                ct = aead_encrypt(fk, chunk, aad)
                f.write(struct.pack("<I", len(ct)) + ct)
        os.replace(tmp, path)
        return wrapped

    def read_bytes(self, fid: str) -> bytes:
        row = self.get(fid)
        if not row:
            raise PAError("file not found", code="not_found")
        fk = self._key(fid, row["wrapped_key"])
        out = bytearray()
        with open(self.dir / f"{fid}.bin", "rb") as f:
            if f.read(4) != MAGIC:
                raise PAError("file blob corrupt", code="integrity")
            (n,) = struct.unpack("<I", f.read(4))
            for i in range(n):
                (ln,) = struct.unpack("<I", f.read(4))
                try:
                    out += aead_decrypt(fk, f.read(ln), f"{fid}|{i}|{int(i == n - 1)}".encode())
                except CryptoError as e:
                    raise PAError("file blob failed integrity check", code="integrity") from e
        return bytes(out)

    # ------------------------------------------------------------------ ingest
    def ingest(self, *, name: str, data: bytes, source: str, sensitivity: int | None = None, folder: str = "/",
               tags: list[str] | None = None, derived_from: str | None = None, run_async: bool = True,
               on_done: Callable[[dict], None] | None = None) -> dict[str, Any]:
        name = checks.safe_filename(name)
        max_mb = int(self.gw.settings.get("files.max_upload_mb"))
        if len(data) > max_mb * 1024 * 1024:
            raise PAError(f"file is larger than {max_mb} MB", code="too_large")
        used = int(self.db.scalar("SELECT coalesce(sum(size_bytes),0) FROM files WHERE deleted=0") or 0)
        if used + len(data) > int(self.gw.settings.get("files.quota_mb")) * 1024 * 1024:
            raise PAError("storage quota exceeded (Settings > Files & Storage)", code="quota_exceeded")
        default = Sensitivity[self.gw.settings.get("sensitivity.default.files")]
        sens = Sensitivity.max(default, Sensitivity(int(sensitivity))) if sensitivity is not None else default
        fid = new_id("file")
        with self._lock:
            wrapped = self._write_blob(fid, data)
            self.db.insert("files", {
                "id": fid, "name": name, "folder": folder[:500] or "/", "tags_json": tags or [], "size_bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(), "declared_type": Path(name).suffix.lower(), "source": source,
                "sensitivity": int(sens), "status": QUARANTINED, "wrapped_key": wrapped, "derived_from": derived_from,
                "created_at": now_iso()})
        self.gw.audit.write("file.ingested", "file", file_id=fid, source=source, size=len(data),
                            sha256=hashlib.sha256(data).hexdigest(), sensitivity=Sensitivity(sens).name)
        if run_async:
            threading.Thread(target=self._process, args=(fid, data, on_done), daemon=True, name=f"file-{fid}").start()
        else:
            self._process(fid, data, on_done)
        return self.get(fid)  # type: ignore[return-value]

    def _set(self, fid: str, **changes: Any) -> None:
        self.db.update("files", "id", fid, changes)
        self.gw.emit("files.changed", {"file_id": fid, "status": changes.get("status")})

    def _process(self, fid: str, data: bytes, on_done: Callable[[dict], None] | None, unscanned_ok: bool = False) -> None:
        row = self.get(fid)
        assert row
        self._set(fid, status=PROCESSING)
        scan_info: dict[str, Any] = {}
        try:
            info = checks.validate(row["name"], data, int(self.gw.settings.get("files.max_upload_mb")) * 1024 * 1024)
            scan_info["validation"] = info
            work = self.tmp / f"scan-{fid}"
            work.mkdir(parents=True, exist_ok=True)
            try:
                p = work / "item.bin"
                p.write_bytes(data)
                av = checks.scan_file(p, data)
            finally:
                shutil.rmtree(work, ignore_errors=True)
            scan_info["antivirus"] = av
            if not av.get("clean") and av.get("unavailable") and (unscanned_ok or self.gw.settings.get("files.accept_unscanned")):
                av = {"clean": True, "engine": "none", "unscanned": True, "released_by": "user" if unscanned_ok else "setting",
                      "note": "no antivirus could scan this file"}
                scan_info["antivirus"] = av
            if not av.get("clean"):
                if av.get("unavailable"):
                    raise checks.Rejected(f"not scanned: {av['error']}")
                raise checks.Rejected(f"malware detected: {av.get('threat') or av.get('error') or 'unknown'}")
            parsed = self._parse(row["name"], info["family"], data)
            hidden = parsed.get("hidden", [])
            if info["active_content"]:
                hidden.append({"kind": "active_content_removed", "text": ", ".join(info["active_content"])})
            text = parsed.get("text", "")
            text, vision_note = self._read_visually(info["family"], data, parsed, text, int(row["sensitivity"]), scan_info)
            reasons = (["active content removed: " + ", ".join(info["active_content"])] if info["active_content"] else []) + ([vision_note] if vision_note else []) \
                + (["NOT antivirus-scanned (no antivirus could scan it)"] if av.get("unscanned") else [])
            self._set(fid, status=READY, sniffed_type=info["family"], scan_json=scan_info, text_content=text,
                      hidden_json=hidden, status_reason="; ".join(reasons) or None)
            self.gw.history.index("file", fid, row["name"], text[:200_000], row["created_at"])
            self.gw.audit.write("file.ready", "file", file_id=fid, family=info["family"], engine=av.get("engine"),
                                active_content=info["active_content"])
            self.gw.insights.schedule(fid)          # background: summary, keywords, memory (setting files.auto_summary)
        except (checks.Rejected, PAError, OSError, ValueError) as e:
            self._set(fid, status=REJECTED, status_reason=str(e)[:500], scan_json=scan_info)
            self.gw.audit.write("file.quarantined", "file", file_id=fid, reason=str(e)[:200], severity="medium")
            self.gw.home_event("file_quarantined", "medium", f"File kept in quarantine: {row['name']}", str(e)[:500], fid)
        finally:
            if on_done:
                try:
                    on_done(self.get(fid) or {})
                except Exception:  # noqa: BLE001
                    pass

    def _read_visually(self, family: str, data: bytes, parsed: dict[str, Any], text: str, sens: int, scan_info: dict[str, Any]) -> tuple[str, str | None]:
        """Pictures and scanned PDFs are read by the vision model (if one is set up); everything else is returned unchanged."""
        from ..vision import is_scanned_pdf
        v = self.gw.vision
        kind = None
        if family in ("png", "jpeg", "gif") or (family == "riff" and data[8:12] == b"WEBP"):
            kind = "image"
        elif family == "pdf" and is_scanned_pdf(text, int(parsed.get("pages") or 0)):
            kind = "scan"
        if kind is None:
            return text, None
        try:
            if kind == "image":
                got, note = v.read_image(data, "webp" if family == "riff" else family, sens, "upload")
            else:
                got, note = v.read_scanned_pdf(sens, data=data)
        except PAError as e:
            got, note = "", f"could not be read visually ({e.code})"
        scan_info["vision"] = {"kind": kind, "state": "read" if got else "unavailable", "note": note}
        if got:
            head = "[Picture read by the vision model]" if kind == "image" else "[Scanned document read by the vision model]"
            return f"{head}\n{got}" if kind == "image" else got, note
        return text, note

    def release_unscanned(self, fid: str) -> dict[str, Any]:
        """The user explicitly accepts ONE quarantined file that no antivirus could scan (never a file with a malware detection)."""
        row = self.get(fid)
        if not row or row["deleted"]:
            raise PAError("file not found", code="not_found")
        scan = json.loads(row["scan_json"] or "{}") if isinstance(row["scan_json"], str) else (row["scan_json"] or {})
        av = scan.get("antivirus") or {}
        if row["status"] != REJECTED or not av.get("unavailable"):
            raise PAError("only files that were held because no antivirus could scan them can be released", code="invalid_state")
        self.gw.audit.write("file.released_unscanned", "file", file_id=fid, name=row["name"], severity="high")
        self._process(fid, self.read_bytes(fid), None, unscanned_ok=True)
        return self.public(self.get(fid))

    def reread(self, fid: str) -> dict[str, Any]:
        """'Read again' after a vision model was added (or after the setting was switched on)."""
        row = self.get(fid)
        if not row or row["deleted"]:
            raise PAError("file not found", code="not_found")
        if row["status"] != READY:
            raise PAError("only files that are ready can be read again", code="file_not_ready")
        data = self.read_bytes(fid)
        fam = row["sniffed_type"] or ""
        info = json.loads(row["scan_json"] or "{}") if isinstance(row["scan_json"], str) else (row["scan_json"] or {})
        parsed = self._parse(row["name"], fam, data) if fam == "pdf" else {}
        base = parsed.get("text", row["text_content"] or "")
        text, note = self._read_visually(fam, data, parsed, base, int(row["sensitivity"]), info)
        if info.get("vision", {}).get("state") != "read":
            raise PAError(note or "nothing to read visually in this file", code="no_vision_model")
        self._set(fid, text_content=text, scan_json=info, status_reason=note)
        self.gw.history.index("file", fid, row["name"], text[:200_000], row["created_at"])
        self.gw.audit.write("file.reread_visually", "file", file_id=fid)
        self.gw.insights.schedule(fid, force=False)
        return self.public(self.get(fid))

    def _parse(self, name: str, family: str, data: bytes) -> dict[str, Any]:
        header = json.dumps({"name": name, "family": family}).encode() + b"\n"
        work = self.tmp / f"parse-{new_id('p')}"
        work.mkdir(parents=True, exist_ok=True)
        try:
            out = run_worker("pa_workers.parser", header + data, timeout=120, cwd=work, memory_mb=1024)
        except Exception as e:  # noqa: BLE001
            raise PAError(f"parser failed: {type(e).__name__}", code="parse_failed") from e
        finally:
            shutil.rmtree(work, ignore_errors=True)
        try:
            res = json.loads(out.decode("utf-8"))
        except ValueError as e:
            raise PAError("parser returned invalid output", code="parse_failed") from e
        if not res.get("ok"):
            raise PAError(f"could not parse file: {res.get('error')}", code="parse_failed")
        return res

    # ------------------------------------------------------------------ queries
    def get(self, fid: str) -> dict[str, Any] | None:
        return self.db.one("SELECT * FROM files WHERE id=?", (fid,))

    def public(self, row: dict[str, Any]) -> dict[str, Any]:
        return {k: (json.loads(v) if k.endswith("_json") and isinstance(v, str) else v)
                for k, v in row.items() if k not in ("wrapped_key", "text_content")}

    def list(self, folder: str | None = None, query: str = "", include_rejected: bool = True) -> list[dict[str, Any]]:
        sql = "SELECT * FROM files WHERE deleted=0"
        params: list[Any] = []
        if folder:
            sql += " AND folder=?"
            params.append(folder)
        if query:
            sql += " AND (name LIKE ? OR tags_json LIKE ?)"
            params += [f"%{query}%", f"%{query}%"]
        if not include_rejected:
            sql += " AND status!='REJECTED'"
        sql += " ORDER BY created_at DESC LIMIT 1000"
        return [self.public(r) for r in self.db.all(sql, tuple(params))]

    def text(self, fid: str, max_chars: int = 20000) -> tuple[str, dict[str, Any]]:
        row = self.get(fid)
        if not row or row["deleted"]:
            raise PAError("file not found", code="not_found")
        if row["status"] != READY:
            raise PAError(f"file is {row['status'].lower()}", code="file_not_ready")
        return (row["text_content"] or "")[:max_chars], row

    def update_meta(self, fid: str, *, folder: str | None = None, tags: list[str] | None = None,
                    in_knowledge: bool | None = None, name: str | None = None) -> None:
        changes: dict[str, Any] = {}
        if folder is not None:
            changes["folder"] = folder[:500] or "/"
        if tags is not None:
            changes["tags_json"] = [t[:50] for t in tags][:30]
        if in_knowledge is not None:
            changes["in_knowledge"] = int(in_knowledge)
        if name:
            changes["name"] = checks.safe_filename(name)
        if changes:
            self._set(fid, **changes)

    def set_label(self, fid: str, level: int) -> dict[str, Any]:
        row = self.get(fid)
        if not row:
            raise PAError("file not found", code="not_found")
        lowering = int(level) < int(row["sensitivity"])
        self.db.update("files", "id", fid, {"sensitivity": int(level)})
        self.gw.audit.write("file.label_changed", "file", file_id=fid, before=Sensitivity(row["sensitivity"]).name,
                            after=Sensitivity(level).name, lowered=lowering, severity="medium" if lowering else "info")
        return {"lowered": lowering}

    def delete(self, fid: str, reason: str = "user") -> int:
        """Delete + cascade to derived files, memories and search index (spec 28)."""
        row = self.get(fid)
        if not row:
            return 0
        n = 1
        for child in self.db.all("SELECT id FROM files WHERE derived_from=? AND deleted=0", (fid,)):
            n += self.delete(child["id"], "cascade")
        path = self.dir / f"{fid}.bin"
        if path.exists():
            try:
                size = path.stat().st_size
                with open(path, "r+b") as f:
                    f.write(os.urandom(min(size, 4 * 1024 * 1024)))
            except OSError:
                pass
            path.unlink(missing_ok=True)
        self.db.update("files", "id", fid, {"deleted": 1, "text_content": None, "wrapped_key": b""})
        self.gw.history.remove("file", fid)
        self.gw.memory.delete_by_source_ref(fid)
        self.gw.insights.remove(fid)
        self.gw.audit.write("file.deleted", "file", file_id=fid, reason=reason)
        self.gw.emit("files.changed", {"file_id": fid, "status": "DELETED"})
        return n

    def storage(self) -> dict[str, Any]:
        used = int(self.db.scalar("SELECT coalesce(sum(size_bytes),0) FROM files WHERE deleted=0") or 0)
        return {"used_bytes": used, "quota_bytes": int(self.gw.settings.get("files.quota_mb")) * 1024 * 1024,
                "count": int(self.db.scalar("SELECT count(*) FROM files WHERE deleted=0") or 0),
                "quarantined": int(self.db.scalar("SELECT count(*) FROM files WHERE deleted=0 AND status='REJECTED'") or 0)}

    def save_copy(self, fid: str, dest_path: str) -> None:
        """'Save a copy' (explicit user action): writes the ORIGINAL bytes where the user chose."""
        row = self.get(fid)
        if not row or row["status"] != READY:
            raise PAError("only files that passed the checks can be saved", code="file_not_ready")
        Path(dest_path).write_bytes(self.read_bytes(fid))
        self.gw.audit.write("file.saved_copy", "file", file_id=fid)
