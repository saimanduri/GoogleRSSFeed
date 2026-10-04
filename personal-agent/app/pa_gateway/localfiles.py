"""Local files and folders: let the assistant read what the user explicitly allows, where it lives on this PC - without uploading it.

Like attaching a file or granting a directory in Claude Code / Codex, the USER picks it in the window; that creates a read-only *grant*
tied to ONE chat and ONE app session:

  * a FILE grant lets the assistant read that file;
  * a FOLDER grant lets it list and read the files directly inside that folder - NOT its subfolders;
  * subfolders need a separate, explicit approval (a checkbox when granting, or an approval card when the assistant asks for a file
    in a subfolder). Each approval is for that folder in that chat only;
  * grants are tied to the chat (a new chat has none) and to the app session (signing out or restarting expires them; the window then
    offers "Allow again"). The assistant can never create, widen or renew a grant - only the UI role can.

The model sees an id and a relative name - never an absolute path - and can only call the localfile.* tools, which run inside the
isolated pa-parser process (no network, memory/CPU limits). Every path is resolved (links followed) and must stay inside the granted
root; Windows, Program Files, this app's data, AppData, credential folders and network paths are always refused. Nothing is copied
into My Files and nothing is ever written back. Pictures and scanned PDFs are read by the vision model (vision.py).
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from pathlib import Path
from typing import Any

from pa_common.errors import PAError
from pa_common.ids import new_id
from pa_common.sensitivity import Sensitivity, Trust
from pa_common.timeutil import now_iso

from .files.checks import BLOCKED_EXT
from .tools.base import ExecContext, ToolFailed, ToolResult
from .vision import IMAGE_EXT, is_scanned_pdf
from .workers import run_worker

TABLE_EXT = {".xlsx", ".xlsm", ".csv", ".tsv"}
TEXT_EXT = {".txt", ".md", ".json", ".log", ".xml", ".html", ".htm", ".yaml", ".yml", ".ini", ".docx", ".pdf"}
READABLE_EXT = TABLE_EXT | TEXT_EXT | set(IMAGE_EXT)
MAX_BYTES = 4 * 1024 ** 3
CACHE_DAYS = 7
MAX_FILE_GRANTS = 10
MAX_FOLDER_GRANTS = 5
SCAN_LIMIT = 20000            # entries looked at when counting a folder for the approval dialog
SCAN_DEPTH = 8
BROWSE_LIMIT = 300
# path pieces that are never readable, even inside an approved folder
NEVER_PARTS = (".ssh", ".aws", ".gnupg", ".azure", ".kube", ".docker", "$recycle.bin", "system volume information", "windows.old")


def _blocked_roots(gw) -> list[Path]:
    roots = [gw.paths.root]
    for env in ("WINDIR", "PROGRAMDATA", "PROGRAMFILES", "PROGRAMFILES(X86)", "PROGRAMW6432"):
        if os.environ.get(env):
            roots.append(Path(os.environ[env]))
    for env in ("APPDATA", "LOCALAPPDATA"):        # the whole AppData tree: browser profiles, tokens, other apps' data
        if os.environ.get(env):
            p = Path(os.environ[env])
            roots.append(p.parent if p.parent.name.lower() == "appdata" else p)
    return roots


def _inside(child: Path, root: Path) -> bool:
    try:
        child.relative_to(root)
        return True
    except ValueError:
        return False


class SubfolderApprovalNeeded(ToolFailed):
    pass


class LocalFiles:
    def __init__(self, gw):
        self.gw = gw
        self.db = gw.db
        self.requests: dict[str, dict[str, Any]] = {}       # pending "may I read this subfolder?" questions (memory only)

    # ------------------------------------------------------------------ session
    @property
    def nonce(self) -> str:
        return self.gw.session.s.nonce

    def _active(self, g: dict[str, Any]) -> bool:
        return bool(g.get("session_nonce")) and g["session_nonce"] == self.nonce

    @property
    def cache_dir(self) -> Path:
        d = self.gw.paths.tmp_dir / "localfiles"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _cache(self, key: str) -> Path:
        return self.cache_dir / f"{key}.sqlite"

    @staticmethod
    def kind(ext: str) -> str:
        return "table" if ext in TABLE_EXT else ("image" if ext in IMAGE_EXT else "text")

    # ------------------------------------------------------------------ validation
    def _check_not_sensitive(self, real: Path) -> None:
        for root in _blocked_roots(self.gw):
            try:
                if _inside(real, root.resolve()):
                    raise PAError("files inside Windows, Program Files or this app's own data cannot be used", code="policy_denied")
            except OSError:
                continue
        low = [p.lower() for p in real.parts]
        if any(n in low for n in NEVER_PARTS):
            raise PAError("credential and system folders cannot be used", code="policy_denied")

    @staticmethod
    def _no_network(raw: str, real: Path | None = None) -> None:
        if raw.startswith("\\\\") or raw.startswith("//") or (real is not None and str(real).startswith("\\\\")):
            raise PAError("network paths are not supported - copy it to this PC first", code="invalid_request")

    def validate(self, raw: str) -> Path:
        """A single readable file (checked again every time it is used)."""
        if not raw or len(raw) > 1000 or "\x00" in raw:
            raise PAError("choose a file", code="invalid_request")
        self._no_network(raw)
        try:
            real = Path(raw).resolve(strict=True)      # follows links: what is checked is what would be read
        except (OSError, RuntimeError) as e:
            raise PAError("that file cannot be found", code="not_found") from e
        self._no_network(raw, real)
        if not real.is_file():
            raise PAError("choose a file, not a folder", code="invalid_request")
        ext = real.suffix.lower()
        if ext == ".xls":
            raise PAError("old .xls files are not supported - save the workbook as .xlsx or .csv and use that", code="invalid_request")
        if ext in BLOCKED_EXT:
            raise PAError("programs and scripts cannot be read", code="policy_denied")
        if ext not in READABLE_EXT:
            raise PAError(f"{ext or 'this'} files are not supported here (spreadsheets: .xlsx .csv .tsv; documents: .docx .pdf .txt .md .json; "
                          "pictures: .png .jpg .gif .webp)", code="invalid_request")
        self._check_not_sensitive(real)
        if real.stat().st_size > MAX_BYTES:
            raise PAError("that file is larger than 4 GB", code="invalid_request")
        return real

    def validate_folder(self, raw: str) -> Path:
        if not raw or len(raw) > 1000 or "\x00" in raw:
            raise PAError("choose a folder", code="invalid_request")
        self._no_network(raw)
        try:
            real = Path(raw).resolve(strict=True)
        except (OSError, RuntimeError) as e:
            raise PAError("that folder cannot be found", code="not_found") from e
        self._no_network(raw, real)
        if not real.is_dir():
            raise PAError("choose a folder, not a file", code="invalid_request")
        if real.parent == real:
            raise PAError("a whole drive cannot be shared - choose a folder", code="policy_denied")
        home = Path(os.path.expanduser("~")).resolve()
        users = home.parent
        if real in (home, users):
            raise PAError("your whole user folder cannot be shared - choose a folder inside it (Documents, Desktop ...)", code="policy_denied")
        self._check_not_sensitive(real)
        return real

    def _resolve_in(self, g: dict[str, Any], rel: str | None) -> Path:
        """The real file a request means: the grant's file, or a path inside the granted folder (never outside it)."""
        try:
            if g["scope"] == "file":
                return self.validate(g["path"])
            root = self.validate_folder(g["path"])
        except PAError as e:
            raise ToolFailed(str(e)) from e
        rel = (rel or "").strip().replace("/", "\\")
        if not rel or "\x00" in rel or len(rel) > 400 or rel.startswith("\\") or ":" in rel or any(seg in ("", ".", "..") for seg in rel.split("\\")):
            raise ToolFailed("give the path of the file inside the folder, like 'reports\\2026.xlsx' (use localfile.browse to see what is there)")
        try:
            real = (root / rel).resolve(strict=True)
        except (OSError, RuntimeError) as e:
            raise ToolFailed("that file is not in the folder") from e
        if not _inside(real, root):
            self.gw.audit.write("localfile.escape_blocked", "security", grant_id=g["id"], severity="high")
            raise ToolFailed("that path leads outside the approved folder (a shortcut or link?) and is not allowed")
        if real.is_dir():
            raise ToolFailed("that is a folder: use localfile.browse to list it")
        if real.parent != root and not g["recursive"]:
            self._ask_subfolders(g, real.parent.relative_to(root))
        try:
            self.validate(str(real))
        except PAError as e:
            raise ToolFailed(str(e)) from e
        return real

    # ------------------------------------------------------------------ subfolder approval (explicit, per chat)
    def _ask_subfolders(self, g: dict[str, Any], sub: Path) -> None:
        top = sub.parts[0] if sub.parts else ""
        for r in self.requests.values():
            if r["grant_id"] == g["id"] and r["subfolder"] == top:
                break
        else:
            rid = new_id("lfr")
            self.requests[rid] = {"id": rid, "grant_id": g["id"], "chat_id": g["chat_id"], "folder": g["name"], "subfolder": top, "created_at": now_iso()}
            self.gw.audit.write("localfile.subfolder_requested", "file", grant_id=g["id"], chat_id=g["chat_id"])
            self.gw.emit("localfiles.request", {"chat_id": g["chat_id"]})
        raise SubfolderApprovalNeeded(
            f"'{top}' is a subfolder of '{g['name']}'. Only the files directly inside the folder are allowed. The user was asked to allow "
            "subfolders; tell them to approve it in the chat window, then try again. Do not try other paths.")

    def pending_requests(self, chat_id: str) -> list[dict[str, Any]]:
        return [dict(r) for r in self.requests.values() if r["chat_id"] == chat_id]

    def deny_request(self, rid: str) -> None:
        r = self.requests.pop(rid, None)
        if r:
            self.gw.audit.write("localfile.subfolder_denied", "file", grant_id=r["grant_id"], chat_id=r["chat_id"])
            self.gw.emit("localfiles.request", {"chat_id": r["chat_id"]})

    def allow_subfolders(self, gid: str, confirm: bool) -> dict[str, Any]:
        """Explicit approval to include subfolders of a folder grant, for this chat and session only."""
        g = self.db.one("SELECT * FROM local_grants WHERE id=? AND scope='folder'", (gid,))
        if not g:
            raise PAError("folder not found", code="not_found")
        if not confirm:
            raise PAError("allowing subfolders needs an explicit confirmation", code="subfolders_need_approval")
        if not self._active(g):
            raise PAError("this approval has expired - allow the folder again first", code="grant_expired")
        self.db.update("local_grants", "id", gid, {"recursive": 1})
        for rid in [k for k, r in self.requests.items() if r["grant_id"] == gid]:
            self.requests.pop(rid, None)
        self.gw.audit.write("localfile.subfolders_allowed", "file", grant_id=gid, name=g["name"], chat_id=g["chat_id"], severity="medium")
        self.gw.emit("localfiles.changed", {"chat_id": g["chat_id"]})
        return self.public(self.db.one("SELECT * FROM local_grants WHERE id=?", (gid,)))

    # ------------------------------------------------------------------ public views
    def public(self, r: dict[str, Any]) -> dict[str, Any]:
        folder = r["scope"] == "folder"
        return {"id": r["id"], "chat_id": r["chat_id"], "name": r["name"], "ext": r["ext"], "scope": r["scope"], "recursive": bool(r["recursive"]),
                "expired": not self._active(r), "kind": "folder" if folder else self.kind(r["ext"]), "size": r["size"],
                "sensitivity": Sensitivity(int(r["sensitivity"])).name, "created_at": r["created_at"],
                "folder": r["path"] if folder else str(Path(r["path"]).parent),
                "indexed": bool(self._cache(r["id"]).exists()) if not folder else False}

    def folder_info(self, raw: str) -> dict[str, Any]:
        """What the approval dialog shows before the user decides about a folder and its subfolders (bounded scan, nothing is read)."""
        root = self.validate_folder(raw)
        top_files = sub_files = subs = 0
        names: list[str] = []
        total = 0
        truncated = False
        scanned = 0
        for dirpath, dirnames, filenames in os.walk(root):
            depth = len(Path(dirpath).relative_to(root).parts)
            dirnames[:] = [d for d in dirnames if d.lower() not in NEVER_PARTS and not (Path(dirpath) / d).is_symlink() and not (Path(dirpath) / d).is_junction()]
            if depth == 0:
                names = list(dirnames[:8])
            subs += len(dirnames)
            readable = sum(1 for f in filenames if Path(f).suffix.lower() in READABLE_EXT)
            if depth == 0:
                top_files = readable
            else:
                sub_files += readable
            for f in filenames:
                try:
                    total += (Path(dirpath) / f).stat().st_size
                except OSError:
                    pass
            scanned += len(filenames) + len(dirnames)
            if depth >= SCAN_DEPTH - 1:
                dirnames[:] = []
            if scanned > SCAN_LIMIT:
                truncated = True
                break
        return {"path": str(root), "name": root.name or str(root), "files": top_files, "subfolders": subs, "subfolder_files": sub_files,
                "subfolder_names": names, "total_mb": round(total / 1e6, 1), "truncated": truncated}

    # ------------------------------------------------------------------ grants (UI only)
    def _check_chat(self, chat_id: str) -> None:
        if not self.db.one("SELECT id FROM chats WHERE id=? AND deleted=0", (chat_id,)):
            raise PAError("chat not found", code="not_found")

    def _label(self, sensitivity: str | None) -> str:
        label = sensitivity or self.gw.settings.get("sensitivity.default.files")
        if label not in Sensitivity.__members__:
            raise PAError("unknown label", code="invalid_request")
        return label

    def grant(self, chat_id: str, path: str, sensitivity: str | None = None) -> dict[str, Any]:
        self._check_chat(chat_id)
        if int(self.db.scalar("SELECT count(*) FROM local_grants WHERE chat_id=? AND scope='file'", (chat_id,)) or 0) >= MAX_FILE_GRANTS:
            raise PAError(f"at most {MAX_FILE_GRANTS} local files per chat - remove one first", code="invalid_request")
        real = self.validate(path)
        label = self._label(sensitivity)
        st = real.stat()
        gid = new_id("lf")
        row = {"id": gid, "chat_id": chat_id, "path": str(real), "name": real.name, "ext": real.suffix.lower(), "size": st.st_size,
               "mtime": int(st.st_mtime), "sensitivity": int(Sensitivity[label]), "created_at": now_iso(), "last_used_at": None,
               "scope": "file", "recursive": 0, "session_nonce": self.nonce}
        self.db.insert("local_grants", row)
        self.gw.audit.write("localfile.granted", "file", grant_id=gid, name=real.name, size=st.st_size, label=label, chat_id=chat_id)
        self.gw.emit("localfiles.changed", {"chat_id": chat_id})
        return self.public(row)

    def grant_folder(self, chat_id: str, path: str, include_subfolders: bool, confirm_subfolders: bool, sensitivity: str | None = None) -> dict[str, Any]:
        self._check_chat(chat_id)
        if include_subfolders and not confirm_subfolders:
            raise PAError("including subfolders needs your explicit approval", code="subfolders_need_approval")
        if int(self.db.scalar("SELECT count(*) FROM local_grants WHERE chat_id=? AND scope='folder'", (chat_id,)) or 0) >= MAX_FOLDER_GRANTS:
            raise PAError(f"at most {MAX_FOLDER_GRANTS} folders per chat - remove one first", code="invalid_request")
        real = self.validate_folder(path)
        label = self._label(sensitivity)
        gid = new_id("lf")
        row = {"id": gid, "chat_id": chat_id, "path": str(real), "name": real.name or str(real), "ext": "", "size": 0, "mtime": int(real.stat().st_mtime),
               "sensitivity": int(Sensitivity[label]), "created_at": now_iso(), "last_used_at": None,
               "scope": "folder", "recursive": int(include_subfolders), "session_nonce": self.nonce}
        self.db.insert("local_grants", row)
        self.gw.audit.write("localfile.folder_granted", "file", grant_id=gid, name=row["name"], label=label, chat_id=chat_id,
                            subfolders=include_subfolders, severity="medium" if include_subfolders else "info")
        self.gw.emit("localfiles.changed", {"chat_id": chat_id})
        return self.public(row)

    def reapprove(self, gid: str, include_subfolders: bool = False, confirm_subfolders: bool = False) -> dict[str, Any]:
        """'Allow again' after a new app session: re-checks the path and renews the approval. Subfolders must be approved again too."""
        g = self.db.one("SELECT * FROM local_grants WHERE id=?", (gid,))
        if not g:
            raise PAError("not found", code="not_found")
        if include_subfolders and not confirm_subfolders:
            raise PAError("including subfolders needs your explicit approval", code="subfolders_need_approval")
        if g["scope"] == "folder":
            self.validate_folder(g["path"])
        else:
            self.validate(g["path"])
        self.db.update("local_grants", "id", gid, {"session_nonce": self.nonce, "recursive": int(include_subfolders and g["scope"] == "folder")})
        self.gw.audit.write("localfile.reapproved", "file", grant_id=gid, name=g["name"], chat_id=g["chat_id"], subfolders=include_subfolders)
        self.gw.emit("localfiles.changed", {"chat_id": g["chat_id"]})
        return self.public(self.db.one("SELECT * FROM local_grants WHERE id=?", (gid,)))

    def list(self, chat_id: str) -> list[dict[str, Any]]:
        return [self.public(r) for r in self.db.all("SELECT * FROM local_grants WHERE chat_id=? ORDER BY created_at", (chat_id,))]

    def revoke(self, gid: str) -> None:
        r = self.db.one("SELECT * FROM local_grants WHERE id=?", (gid,))
        if not r:
            raise PAError("not found", code="not_found")
        self.db.execute("DELETE FROM local_grants WHERE id=?", (gid,))
        for rid in [k for k, q in self.requests.items() if q["grant_id"] == gid]:
            self.requests.pop(rid, None)
        for f in self.cache_dir.glob(f"{gid}*"):
            f.unlink(missing_ok=True)
        self.gw.audit.write("localfile.revoked", "file", grant_id=gid, name=r["name"], chat_id=r["chat_id"])
        self.gw.emit("localfiles.changed", {"chat_id": r["chat_id"]})

    def revoke_chat(self, chat_id: str) -> None:
        for r in self.db.all("SELECT id FROM local_grants WHERE chat_id=?", (chat_id,)):
            self.revoke(r["id"])

    def active_grants(self, chat_id: str | None) -> list[dict[str, Any]]:
        if not chat_id:
            return []
        return [r for r in self.db.all("SELECT * FROM local_grants WHERE chat_id=? ORDER BY created_at", (chat_id,)) if self._active(r)]

    def has_grants(self, chat_id: str | None) -> bool:
        return bool(self.active_grants(chat_id))

    def for_prompt(self, chat_id: str | None) -> list[dict[str, Any]]:
        out = []
        for r in self.active_grants(chat_id):
            if r["scope"] == "folder":
                out.append({"id": r["id"], "name": r["name"], "kind": "folder", "size_mb": 0, "label": Sensitivity(int(r["sensitivity"])).name,
                            "subfolders": bool(r["recursive"])})
            else:
                out.append({"id": r["id"], "name": r["name"], "kind": self.kind(r["ext"]), "size_mb": round(r["size"] / 1e6, 1),
                            "label": Sensitivity(int(r["sensitivity"])).name})
        return out

    def purge_stale(self) -> None:
        """Index files of removed grants, and indexes untouched for CACHE_DAYS, are deleted (they hold a copy of the data)."""
        live = {r["id"] for r in self.db.all("SELECT id FROM local_grants")}
        limit = time.time() - CACHE_DAYS * 86400
        for f in self.cache_dir.glob("*"):
            gid = f.name.split(".")[0].split("-")[0]
            try:
                if gid not in live or f.stat().st_mtime < limit or f.suffix == ".building":
                    f.unlink()
            except OSError:
                pass

    # ------------------------------------------------------------------ running the isolated worker
    def _grant_for(self, c: ExecContext, grant_id: str) -> dict[str, Any]:
        g = self.db.one("SELECT * FROM local_grants WHERE id=?", (grant_id,))
        chat = c.task.get("chat_id")
        if not g or not chat or g["chat_id"] != chat:
            raise ToolFailed("that file or folder is not shared with this chat. Ask the user to attach it with the paperclip / folder button.")
        if not self._active(g):
            raise ToolFailed("the user's approval for this file or folder has expired (new app session). Ask them to press 'Allow again' above the message box.")
        return g

    def _run(self, c: ExecContext, g: dict[str, Any], real: Path, op: str, params: dict[str, Any], timeout: int) -> str:
        key = g["id"] if g["scope"] == "file" else f"{g['id']}-{hashlib.sha1(str(real).encode()).hexdigest()[:12]}"
        header = {"mode": "table", "op": op, "path": str(real), "cache": str(self._cache(key)), "params": params}
        work = self.gw.paths.tmp_dir / f"lf-{new_id('w')}"
        work.mkdir(parents=True, exist_ok=True)
        try:
            out = run_worker("pa_workers.parser", json.dumps(header).encode() + b"\n", timeout=timeout, cwd=work, memory_mb=3072)
        except Exception as e:  # noqa: BLE001
            raise ToolFailed(f"reading the file took too long or failed ({type(e).__name__})") from e
        finally:
            shutil.rmtree(work, ignore_errors=True)
        try:
            res = json.loads(out.decode("utf-8"))
        except ValueError as e:
            raise ToolFailed("the file reader returned invalid output") from e
        if not res.get("ok"):
            raise ToolFailed(str(res.get("error", "could not read the file"))[:500])
        self.db.update("local_grants", "id", g["id"], {"last_used_at": now_iso()})
        self.gw.audit.write("localfile.read", "file", grant_id=g["id"], name=real.name, op=op, scope=g["scope"])
        self.gw.budgets.add(c.task["id"], "files")
        return res["text"]

    def _result(self, text: str, g: dict[str, Any], real: Path) -> ToolResult:
        return ToolResult(text, int(g["sensitivity"]), "files", {"file_id": g["id"], "name": real.name}, trust=Trust.UNTRUSTED)

    # ------------------------------------------------------------------ tools
    def register_tools(self, tg) -> None:
        tg.register("localfile.list", self.t_list)
        tg.register("localfile.browse", self.t_browse)
        tg.register("localfile.digest", self.t_digest)
        tg.register("localfile.inspect", lambda a, c: self._tool(c, a, "inspect", 900))
        tg.register("localfile.rows", lambda a, c: self._tool(c, a, "rows", 300))
        tg.register("localfile.query", lambda a, c: self._tool(c, a, "query", 300))
        tg.register("localfile.text", lambda a, c: self._tool(c, a, "text", 300))

    def t_list(self, a: dict[str, Any], c: ExecContext) -> ToolResult:
        rows = self.active_grants(c.task.get("chat_id"))
        text = "\n".join(
            (f"- {r['id']} | FOLDER {r['name']} | subfolders {'allowed' if r['recursive'] else 'NOT allowed'} | {Sensitivity(int(r['sensitivity'])).name}"
             if r["scope"] == "folder" else
             f"- {r['id']} | {r['name']} | {self.kind(r['ext'])} | {r['size'] / 1e6:.1f} MB | {Sensitivity(int(r['sensitivity'])).name}") for r in rows) \
            or "Nothing is shared with this chat."
        return ToolResult(text, max([int(r["sensitivity"]) for r in rows] or [0]), "files", {"count": len(rows)})

    def t_browse(self, a: dict[str, Any], c: ExecContext) -> ToolResult:
        g = self._grant_for(c, a["grant_id"])
        if g["scope"] != "folder":
            raise ToolFailed("that is a single file, not a folder")
        try:
            root = self.validate_folder(g["path"])
        except PAError as e:
            raise ToolFailed(str(e)) from e
        sub = (a.get("subpath") or "").strip().replace("/", "\\")
        target = root
        if sub:
            if ":" in sub or sub.startswith("\\") or any(seg in ("", ".", "..") for seg in sub.split("\\")):
                raise ToolFailed("give a folder path inside the approved folder, like 'reports\\2026'")
            try:
                target = (root / sub).resolve(strict=True)
            except (OSError, RuntimeError) as e:
                raise ToolFailed("that subfolder does not exist") from e
            if not _inside(target, root) or not target.is_dir():
                self.gw.audit.write("localfile.escape_blocked", "security", grant_id=g["id"], severity="high")
                raise ToolFailed("that path is outside the approved folder")
            if not g["recursive"]:
                self._ask_subfolders(g, target.relative_to(root))
        entries, more = [], 0
        try:
            for e in sorted(os.scandir(target), key=lambda x: (not x.is_dir(follow_symlinks=False), x.name.lower())):
                if e.name.lower() in NEVER_PARTS or e.is_symlink() or e.is_junction():
                    continue
                if len(entries) >= BROWSE_LIMIT:
                    more += 1
                    continue
                rel = str((Path(e.path).relative_to(root)))
                if e.is_dir(follow_symlinks=False):
                    entries.append(f"[folder] {rel}" + ("" if g["recursive"] or target != root else "   (subfolder: needs the user's approval)"))
                elif Path(e.name).suffix.lower() in READABLE_EXT:
                    entries.append(f"{rel} | {e.stat().st_size / 1e6:.2f} MB")
        except OSError as ex:
            raise ToolFailed(f"the folder could not be listed ({type(ex).__name__})") from ex
        self.gw.audit.write("localfile.browsed", "file", grant_id=g["id"], count=len(entries))
        text = "\n".join(entries) + (f"\n[{more} more entries not shown]" if more else "") if entries else "No readable files here."
        return ToolResult(text, int(g["sensitivity"]), "files", {"count": len(entries)}, trust=Trust.UNTRUSTED)

    def t_digest(self, a: dict[str, Any], c: ExecContext) -> ToolResult:
        """An overview of a folder in ONE call (what a small local model needs: it cannot loop over dozens of files reliably)."""
        g = self._grant_for(c, a["grant_id"])
        if g["scope"] != "folder":
            raise ToolFailed("that is a single file, not a folder")
        try:
            root = self.validate_folder(g["path"])
        except PAError as e:
            raise ToolFailed(str(e)) from e
        sub = (a.get("subpath") or "").strip().replace("/", "\\")
        target = root
        if sub:
            if ":" in sub or sub.startswith("\\") or any(seg in ("", ".", "..") for seg in sub.split("\\")):
                raise ToolFailed("give a folder path inside the approved folder")
            try:
                target = (root / sub).resolve(strict=True)
            except (OSError, RuntimeError) as e:
                raise ToolFailed("that subfolder does not exist") from e
            if not _inside(target, root) or not target.is_dir():
                raise ToolFailed("that path is outside the approved folder")
            if not g["recursive"]:
                self._ask_subfolders(g, target.relative_to(root))
        names: list[Path] = []
        subs = 0
        try:
            for e in sorted(os.scandir(target), key=lambda x: x.name.lower()):
                if e.name.lower() in NEVER_PARTS or e.is_symlink() or e.is_junction():
                    continue
                if e.is_dir(follow_symlinks=False):
                    subs += 1
                elif Path(e.name).suffix.lower() in READABLE_EXT:
                    names.append(Path(e.path))
        except OSError as ex:
            raise ToolFailed(f"the folder could not be listed ({type(ex).__name__})") from ex
        limit, chars = int(a.get("max_files", 25)), int(a.get("chars", 500))
        lines = [f"Folder '{g['name']}'{(' / ' + sub) if sub else ''}: {len(names)} readable files here, {subs} subfolders"
                 f"{' (allowed)' if g['recursive'] else ' (not allowed: ask the user)'}. Showing {min(limit, len(names))}."]
        budget = 14000
        for f in names[:limit]:
            rel = str(f.relative_to(root))
            ext = f.suffix.lower()
            size = ""
            try:
                real = self._resolve_in(g, rel)
                size = f"{real.stat().st_size / 1024:.0f} KB"
                if ext in IMAGE_EXT:
                    snippet = "(picture: read it with localfile.text to get its text/description from the vision model)"
                elif ext in TABLE_EXT:
                    snippet = self._run(c, g, real, "inspect", {"sample_rows": 2}, 120)
                else:
                    snippet = self._run(c, g, real, "text", {"offset": 0, "max_chars": max(500, chars)}, 60)
                snippet = " ".join(snippet.split())[:chars]
            except ToolFailed as ex:
                snippet = f"(could not be read: {str(ex)[:80]})"
            entry = f"- {rel} | {size} | {snippet}"
            budget -= len(entry)
            if budget < 0:
                lines.append("[overview truncated - ask for a subset or read single files]")
                break
            lines.append(entry)
        if len(names) > limit:
            lines.append(f"[{len(names) - limit} more files not shown; call again with a higher max_files or read single files]")
        self.gw.audit.write("localfile.digest", "file", grant_id=g["id"], files=min(limit, len(names)))
        return ToolResult("\n".join(lines), int(g["sensitivity"]), "files", {"count": len(names)}, trust=Trust.UNTRUSTED)

    def _tool(self, c: ExecContext, a: dict[str, Any], op: str, timeout: int) -> ToolResult:
        a = dict(a)
        g = self._grant_for(c, a.pop("file_id"))
        rel = a.pop("path", None)
        real = self._resolve_in(g, rel)
        ext = real.suffix.lower()
        if ext in IMAGE_EXT:
            return self._result(self._read_image(g, real, c), g, real)
        if op in ("query",):
            a["where"] = [w if isinstance(w, dict) else w.model_dump() for w in a.get("where") or []]
        text = self._run(c, g, real, op, a, timeout)
        if ext == ".pdf" and op == "text" and not a.get("offset") and is_scanned_pdf(text, text.count("[page ") or 1):
            scanned = self._read_scan(g, real, c)
            if scanned:
                text = scanned
        return self._result(text, g, real)

    # ------------------------------------------------------------------ pictures and scans (vision model, cached per file version)
    def _vision_cache(self, g: dict[str, Any], real: Path) -> Path:
        st = real.stat()
        key = hashlib.sha1(f"{real}|{st.st_size}|{int(st.st_mtime)}".encode()).hexdigest()[:20]
        return self.cache_dir / f"{g['id']}-v{key}.vision.txt"

    def _read_image(self, g: dict[str, Any], real: Path, c: ExecContext) -> str:
        cache = self._vision_cache(g, real)
        if cache.exists():
            return cache.read_text(encoding="utf-8")
        fam = IMAGE_EXT[real.suffix.lower()]
        text, note = self.gw.vision.read_image(real.read_bytes(), fam, int(g["sensitivity"]), "local-file")
        if not text:
            raise ToolFailed(note or "the picture could not be read")
        out = f"[Picture read by the vision model]\n{text}"
        cache.write_text(out, encoding="utf-8")
        self.db.update("local_grants", "id", g["id"], {"last_used_at": now_iso()})
        self.gw.audit.write("localfile.read", "file", grant_id=g["id"], name=real.name, op="vision", scope=g["scope"])
        self.gw.budgets.add(c.task["id"], "files")
        return out

    def _read_scan(self, g: dict[str, Any], real: Path, c: ExecContext) -> str | None:
        cache = self._vision_cache(g, real)
        if cache.exists():
            return cache.read_text(encoding="utf-8")
        text, note = self.gw.vision.read_scanned_pdf(int(g["sensitivity"]), path=real)
        if not text:
            return None
        out = text + (f"\n\n[note: {note}]" if note else "")
        cache.write_text(out, encoding="utf-8")
        self.gw.audit.write("localfile.read", "file", grant_id=g["id"], name=real.name, op="vision-scan", scope=g["scope"])
        return out
