"""Encrypted backups (.pabk) and restore (spec 27, 39.11).

File layout:
  "PABK1\n" + header JSON line + chunked AES-256-GCM ciphertext of a tar stream
  header: {version, created, app_version, kdf:{salt, params}, wrapped_key, contents, chunk}
  backup key: random 256-bit; wrapped with Argon2id(password at backup time)
The tar contains: vault.header, db/agent.db (SQLCipher export, same K_db), files/*.bin (already
encrypted), optionally sealed logs. Secrets are never exported in plaintext. Restoring needs the
backup-time password; on a new PC a new PIN and recovery key are created (TPM parts are not portable).
"""
from __future__ import annotations

import io
import json
import os
import shutil
import struct
import tarfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from pa_common.errors import PAError
from pa_common.timeutil import now_iso
from pa_common.version import APP_VERSION

from .vault.crypto import Argon2Params, CryptoError, aead_decrypt, aead_encrypt, argon2id, b64d, b64e, random_key
from .vault.vault import calibrate_password_params

MAGIC = b"PABK1\n"
CHUNK = 4 * 1024 * 1024


def _write_encrypted(out, key: bytes, data_iter) -> None:
    idx = 0
    for chunk in data_iter:
        ct = aead_encrypt(key, chunk, f"pabk|{idx}".encode())
        out.write(struct.pack("<I", len(ct)) + ct)
        idx += 1
    out.write(struct.pack("<I", 0) + aead_encrypt(key, b"END", f"pabk|end|{idx}".encode()))


def _read_encrypted(f, key: bytes) -> bytes:
    buf = bytearray()
    idx = 0
    while True:
        hdr = f.read(4)
        if len(hdr) < 4:
            raise PAError("backup is truncated", code="backup_corrupt")
        (n,) = struct.unpack("<I", hdr)
        if n == 0:
            trailer = f.read()
            aead_decrypt(key, trailer, f"pabk|end|{idx}".encode())  # detects truncation/reordering
            return bytes(buf)
        buf += aead_decrypt(key, f.read(n), f"pabk|{idx}".encode())
        idx += 1


def derive_kek(password: str) -> tuple[bytes, Argon2Params, bytes]:
    salt = os.urandom(16)
    params = calibrate_password_params(0.5)
    return salt, params, argon2id(password.encode(), salt, params)


def create_backup(gw, password: str | None, dest_dir: Path, include_logs: bool = False, label: str = "",
                  kek: tuple[bytes, Argon2Params, bytes] | None = None) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    stage = gw.paths.tmp_dir / f"backup-{int(time.time())}"
    stage.mkdir(parents=True)
    try:
        db_copy = stage / "agent.db"
        gw.db.backup_to(db_copy, gw.keys.key("K_db"))
        tar_buf = io.BytesIO()
        with tarfile.open(fileobj=tar_buf, mode="w") as tar:
            tar.add(gw.paths.vault_header, arcname="vault.header")
            tar.add(db_copy, arcname="db/agent.db")
            for p in sorted(gw.paths.files_dir.glob("*.bin")):
                tar.add(p, arcname=f"files/{p.name}")
            if include_logs:
                for p in gw.audit.segments():
                    tar.add(p, arcname=f"logs/{p.name}")
        data = tar_buf.getvalue()
    finally:
        shutil.rmtree(stage, ignore_errors=True)
    bkey = random_key()
    if kek is None:
        if not password:
            raise PAError("password required", code="password_required")
        kek = derive_kek(password)
    salt, params, kek_bytes = kek
    header = {"version": 1, "created": now_iso(), "app_version": APP_VERSION, "label": label,
              "kdf": {"salt": b64e(salt), "params": params.to_dict()},
              "wrapped_key": b64e(aead_encrypt(kek_bytes, bkey, b"pabk/key")),
              "size": len(data), "include_logs": include_logs}
    name = f"PersonalAgent-{datetime.now().strftime('%Y%m%d-%H%M%S')}{('-' + label) if label else ''}.pabk"
    out_path = dest_dir / name
    tmp = out_path.with_suffix(".part")
    with open(tmp, "wb") as out:
        out.write(MAGIC + json.dumps(header).encode() + b"\n")
        _write_encrypted(out, bkey, (data[i:i + CHUNK] for i in range(0, len(data), CHUNK)))
    os.replace(tmp, out_path)
    gw.audit.write("backup.created", "backup", file=name, size=out_path.stat().st_size, include_logs=include_logs)
    gw.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('backup.last',?)", (now_iso(),))
    return out_path


def open_backup(path: Path, password: str) -> tuple[dict[str, Any], bytes]:
    with open(path, "rb") as f:
        if f.read(len(MAGIC)) != MAGIC:
            raise PAError("not a Personal Agent backup", code="backup_corrupt")
        header = json.loads(f.readline())
        kdf = header["kdf"]
        try:
            bkey = aead_decrypt(argon2id(password.encode(), b64d(kdf["salt"]), Argon2Params.from_dict(kdf["params"])),
                                b64d(header["wrapped_key"]), b"pabk/key")
        except CryptoError as e:
            raise PAError("wrong password for this backup", code="auth_failed") from e
        try:
            data = _read_encrypted(f, bkey)
        except CryptoError as e:
            raise PAError("backup failed its integrity check", code="backup_corrupt") from e
    return header, data


def verify_backup(path: Path, password: str) -> dict[str, Any]:
    header, data = open_backup(path, password)
    with tarfile.open(fileobj=io.BytesIO(data)) as tar:
        names = tar.getnames()
    ok = "vault.header" in names and "db/agent.db" in names
    return {"ok": ok, "created": header["created"], "files": len([n for n in names if n.startswith("files/")]), "entries": len(names)}


def stage_restore(path: Path, password: str, data_root: Path) -> Path:
    """Extract into <data>/restore-staging. The gateway swaps it in on the next start (while locked)."""
    header, data = open_backup(path, password)
    staging = data_root / "restore-staging"
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)
    with tarfile.open(fileobj=io.BytesIO(data)) as tar:
        for m in tar.getmembers():
            target = (staging / m.name).resolve()
            if not str(target).startswith(str(staging.resolve())) or not (m.isfile() or m.isdir()):
                raise PAError("backup contains an unsafe path", code="backup_corrupt")
        tar.extractall(staging, filter="data")
    (staging / "RESTORE_READY").write_text(json.dumps({"from": path.name, "created": header["created"]}))
    return staging


def apply_staged_restore(data_root: Path) -> bool:
    """Called at gateway start BEFORE anything opens the DB."""
    staging = data_root / "restore-staging"
    if not (staging / "RESTORE_READY").exists():
        return False
    old = data_root / f"pre-restore-{int(time.time())}"
    old.mkdir()
    for name in ("vault.header", "db", "files"):
        src = data_root / name
        if src.exists():
            shutil.move(str(src), str(old / name))
    for name in ("vault.header", "db", "files"):
        s = staging / name
        if s.exists():
            shutil.move(str(s), str(data_root / name))
    (data_root / "files").mkdir(exist_ok=True)
    marker = data_root / "state" / "restored_needs_pin"
    marker.parent.mkdir(exist_ok=True)
    marker.write_text(now_iso())
    shutil.rmtree(staging, ignore_errors=True)
    return True


def prune(dest_dir: Path, keep: int) -> int:
    files = sorted(dest_dir.glob("PersonalAgent-*.pabk"), key=lambda p: p.stat().st_mtime, reverse=True)
    for p in files[keep:]:
        p.unlink(missing_ok=True)
    return max(0, len(files) - keep)


def snapshot_for_update(gw) -> Path:
    """39.11: encrypted snapshot before an update (DB, settings, skills, policies live in the DB)."""
    snap_dir = gw.paths.snapshots_dir
    stage = snap_dir / f"snap-{int(time.time())}"
    stage.mkdir(parents=True)
    gw.db.backup_to(stage / "agent.db", gw.keys.key("K_db"))
    shutil.copy2(gw.paths.vault_header, stage / "vault.header")
    gw.audit.write("update.snapshot", "update", snapshot=stage.name)
    return stage
