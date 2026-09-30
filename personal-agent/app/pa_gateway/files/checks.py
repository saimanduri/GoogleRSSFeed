"""File validation and malware scanning (spec 15)."""
from __future__ import annotations

import glob
import io
import os
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import Any

# Assembled at runtime so that this source file itself is never flagged by antivirus engines.
EICAR = b"".join([b"X5O!P%@AP[4\\PZX54(P^)7CC)7}$", b"EICAR-STANDARD-", b"ANTIVIRUS-TEST-FILE!$H+H*"])

MAGIC: list[tuple[bytes, int, str]] = [
    (b"%PDF-", 0, "pdf"),
    (b"PK\x03\x04", 0, "zip"),
    (b"PK\x05\x06", 0, "zip"),
    (b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1", 0, "ole"),
    (b"\x89PNG\r\n\x1a\n", 0, "png"),
    (b"\xff\xd8\xff", 0, "jpeg"),
    (b"GIF87a", 0, "gif"),
    (b"GIF89a", 0, "gif"),
    (b"RIFF", 0, "riff"),
    (b"MZ", 0, "exe"),
    (b"\x7fELF", 0, "elf"),
    (b"#!", 0, "script"),
    (b"{\\rtf", 0, "rtf"),
    (b"\x1f\x8b", 0, "gzip"),
    (b"7z\xbc\xaf\x27\x1c", 0, "7z"),
    (b"Rar!\x1a\x07", 0, "rar"),
    (b"\xca\xfe\xba\xbe", 0, "macho"),
]
EXT_FAMILY = {
    ".pdf": {"pdf"}, ".docx": {"zip"}, ".xlsx": {"zip"}, ".pptx": {"zip"}, ".docm": {"zip"}, ".xlsm": {"zip"},
    ".zip": {"zip"}, ".doc": {"ole"}, ".xls": {"ole"}, ".ppt": {"ole"}, ".msg": {"ole"}, ".png": {"png"},
    ".jpg": {"jpeg"}, ".jpeg": {"jpeg"}, ".gif": {"gif"}, ".rtf": {"rtf"}, ".gz": {"gzip"},
    ".txt": {"text"}, ".md": {"text"}, ".csv": {"text"}, ".json": {"text"}, ".xml": {"text"}, ".html": {"text", "html"},
    ".htm": {"text", "html"}, ".eml": {"text"}, ".log": {"text"}, ".svg": {"text", "svg"}, ".py": {"text"},
    ".yaml": {"text"}, ".yml": {"text"}, ".ics": {"text"},
}
BLOCKED_FAMILIES = {"exe", "elf", "macho", "script"}
BLOCKED_EXT = {".exe", ".dll", ".scr", ".com", ".bat", ".cmd", ".ps1", ".vbs", ".js", ".jse", ".wsf", ".msi", ".lnk",
               ".hta", ".cpl", ".jar", ".reg", ".iso", ".img", ".vhd", ".vhdx", ".appx", ".msix", ".sys", ".chm"}

MAX_ARCHIVE_MEMBERS = 1000
MAX_ARCHIVE_RATIO = 100
MAX_ARCHIVE_TOTAL = 512 * 1024 * 1024
MAX_ARCHIVE_DEPTH = 2


class Rejected(Exception):
    pass


def sniff(data: bytes) -> str:
    for sig, off, fam in MAGIC:
        if data[off:off + len(sig)] == sig:
            return fam
    head = data[:4096]
    try:
        text = head.decode("utf-8")
    except UnicodeDecodeError:
        try:
            text = head.decode("utf-16")
        except UnicodeDecodeError:
            return "binary"
    low = text.lower()
    if "<svg" in low:
        return "svg"
    if "<html" in low or "<!doctype html" in low:
        return "html"
    return "text"


def validate(name: str, data: bytes, max_bytes: int) -> dict[str, Any]:
    ext = Path(name).suffix.lower()
    fam = sniff(data)
    info: dict[str, Any] = {"family": fam, "ext": ext, "size": len(data), "active_content": [], "archive": None}
    if len(data) > max_bytes:
        raise Rejected("file is larger than the allowed maximum")
    if len(data) == 0:
        raise Rejected("file is empty")
    if ext in BLOCKED_EXT or fam in BLOCKED_FAMILIES:
        raise Rejected("executable or script files are not accepted")
    expected = EXT_FAMILY.get(ext)
    if expected is not None and fam not in expected and not (fam in ("html", "svg") and "text" in expected):
        raise Rejected(f"file content ({fam}) does not match its extension ({ext})")
    if fam == "zip":
        info["archive"] = check_zip(data, depth=0)
        info["active_content"] += info["archive"].get("active", [])
    if fam == "ole":
        if b"VBA" in data or b"_VBA_PROJECT" in data or b"Macros" in data:
            info["active_content"].append("vba_macros")
    if fam in ("pdf",):
        for marker, label in ((b"/JavaScript", "pdf_javascript"), (b"/JS", "pdf_javascript"), (b"/Launch", "pdf_launch"),
                              (b"/EmbeddedFile", "pdf_embedded_file"), (b"/OpenAction", "pdf_open_action")):
            if marker in data and label not in info["active_content"]:
                info["active_content"].append(label)
    if fam in ("svg", "html"):
        low = data[:2_000_000].lower()
        if b"<script" in low or b"javascript:" in low or b"onload=" in low or b"onerror=" in low:
            info["active_content"].append("html_script")
    if fam == "rtf" and (b"\\object" in data or b"\\objdata" in data):
        info["active_content"].append("rtf_embedded_object")
    return info


def check_zip(data: bytes, depth: int) -> dict[str, Any]:
    if depth > MAX_ARCHIVE_DEPTH:
        raise Rejected("archive nesting too deep")
    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile as e:
        raise Rejected("corrupt archive") from e
    infos = zf.infolist()
    if len(infos) > MAX_ARCHIVE_MEMBERS:
        raise Rejected("archive has too many files")
    total = sum(i.file_size for i in infos)
    comp = max(1, sum(i.compress_size for i in infos))
    if total > MAX_ARCHIVE_TOTAL or total / comp > MAX_ARCHIVE_RATIO:
        raise Rejected("archive looks like a decompression bomb")
    active: list[str] = []
    for i in infos:
        n = i.filename.lower()
        if n.endswith("vbaproject.bin") or n.endswith(".bin") and "activex" in n:
            active.append("office_macros")
        if "oleobject" in n or "/embeddings/" in n:
            active.append("office_embedded_object")
        if Path(n).suffix in BLOCKED_EXT:
            raise Rejected(f"archive contains a blocked file type ({Path(n).suffix})")
        if n.endswith(".zip") and i.file_size < 50 * 1024 * 1024:
            check_zip(zf.read(i), depth + 1)
    return {"members": len(infos), "uncompressed": total, "ratio": round(total / comp, 1), "active": sorted(set(active))}


# ------------------------------------------------------------------------------ antivirus
def find_defender() -> str | None:
    if sys.platform != "win32":
        return None
    cands = sorted(glob.glob(r"C:\ProgramData\Microsoft\Windows Defender\Platform\*\MpCmdRun.exe"), reverse=True)
    cands.append(r"C:\Program Files\Windows Defender\MpCmdRun.exe")
    for c in cands:
        if os.path.exists(c):
            return c
    return None


def scan_file(path: Path, data: bytes) -> dict[str, Any]:
    """Built-in signature check + Microsoft Defender custom scan (spec 15)."""
    if EICAR in data[:4096] or EICAR in data:
        return {"clean": False, "engine": "builtin", "threat": "EICAR-Test-File"}
    exe = find_defender()
    if not exe:
        return {"clean": True, "engine": "builtin-only", "note": "Microsoft Defender not found; basic checks only"}
    try:
        proc = subprocess.run([exe, "-Scan", "-ScanType", "3", "-File", str(path), "-DisableRemediation"],
                              capture_output=True, timeout=300, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    except (OSError, subprocess.TimeoutExpired) as e:
        return {"clean": False, "engine": "defender", "threat": None, "error": f"scan failed: {e}"}
    if not path.exists():
        return {"clean": False, "engine": "defender", "threat": "removed by real-time protection"}
    if proc.returncode == 0:
        return {"clean": True, "engine": "defender"}
    if proc.returncode == 2:
        out = proc.stdout.decode(errors="ignore")
        threat = next((line.split(":", 1)[1].strip() for line in out.splitlines() if "Threat" in line and ":" in line), "threat")
        return {"clean": False, "engine": "defender", "threat": threat}
    return {"clean": False, "engine": "defender", "threat": None, "error": f"scanner exit code {proc.returncode}"}
