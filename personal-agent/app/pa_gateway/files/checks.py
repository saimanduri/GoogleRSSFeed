"""File validation and malware scanning (spec 15)."""
from __future__ import annotations

import glob
import io
import os
import re
import struct
import subprocess
import sys
import unicodedata
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
    (b"\x4c\x00\x00\x00\x01\x14\x02\x00", 0, "lnk"),
    (b"MSCF", 0, "cab"),
]
EXT_FAMILY = {
    ".pdf": {"pdf"}, ".docx": {"zip"}, ".xlsx": {"zip"}, ".pptx": {"zip"}, ".docm": {"zip"}, ".xlsm": {"zip"},
    ".zip": {"zip"}, ".doc": {"ole"}, ".xls": {"ole"}, ".ppt": {"ole"}, ".msg": {"ole"}, ".png": {"png"},
    ".jpg": {"jpeg"}, ".jpeg": {"jpeg"}, ".gif": {"gif"}, ".rtf": {"rtf"}, ".gz": {"gzip"},
    ".txt": {"text"}, ".md": {"text"}, ".csv": {"text"}, ".json": {"text"}, ".xml": {"text"}, ".html": {"text", "html"},
    ".htm": {"text", "html"}, ".eml": {"text"}, ".log": {"text"}, ".svg": {"text", "svg"}, ".py": {"text"},
    ".yaml": {"text"}, ".yml": {"text"}, ".ics": {"text"},
}
BLOCKED_FAMILIES = {"exe", "elf", "macho", "script", "lnk", "cab"}
BLOCKED_EXT = {".exe", ".dll", ".scr", ".com", ".bat", ".cmd", ".ps1", ".vbs", ".js", ".jse", ".wsf", ".msi", ".lnk",
               ".hta", ".cpl", ".jar", ".reg", ".iso", ".img", ".vhd", ".vhdx", ".appx", ".msix", ".sys", ".chm",
               ".url", ".iqy", ".slk", ".scf", ".settingcontent-ms", ".library-ms", ".pif", ".gadget", ".application", ".wsc", ".wsh", ".vbe",
               ".msc", ".jnlp", ".xll", ".wll", ".ocx", ".appref-ms", ".diagcab", ".rdp", ".psm1", ".ps1xml", ".psd1", ".sct", ".inf"}
OOXML_CORE = {".docx": "word/document.xml", ".docm": "word/document.xml", ".xlsx": "xl/workbook.xml", ".xlsm": "xl/workbook.xml",
              ".pptx": "ppt/presentation.xml"}
MAX_MEMBER_RATIO = 1000
MAX_IMAGE_PIXELS = 400_000_000
MAX_IMAGE_SIDE = 30_000
MAX_IMAGE_FRAMES = 1000
MAX_ICC_BYTES = 1024 * 1024
_RESERVED = {"con", "prn", "aux", "nul", *(f"com{i}" for i in range(1, 10)), *(f"lpt{i}" for i in range(1, 10))}


def clean_label(text: str) -> str:
    """A user-typed display name (chat title ...): no control / invisible direction characters, collapsed spaces."""
    text = "".join(c for c in str(text) if unicodedata.category(c) not in ("Cc", "Cf", "Cs", "Co", "Cn"))
    return re.sub(r"\s+", " ", text).strip()


def safe_filename(name: str) -> str:
    """A name that is safe to show and to use later as a Windows file name: no path parts, no control or invisible
    direction/format characters (NUL, RTLO, zero-width), no alternate-data-stream colon, no reserved device names."""
    name = str(name).replace(chr(92), "/").split("/")[-1]
    name = "".join(c for c in name if unicodedata.category(c) not in ("Cc", "Cf", "Cs", "Co", "Cn"))
    name = re.sub(r'[<>:"|?*]', "_", name).strip(" .")
    if name.split(".")[0].lower() in _RESERVED:
        name = "_" + name
    return name[:200] or "file"

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
            if head[:2] not in (b"\xff\xfe", b"\xfe\xff"):   # UTF-16 only with a byte-order mark: arbitrary binary decodes as UTF-16 too
                raise UnicodeDecodeError("utf-16", b"", 0, 1, "no BOM")
            text = head.decode("utf-16")
        except UnicodeDecodeError:
            return "text" if _looks_like_legacy_text(head) else "binary"
    low = text.lower()
    if "<svg" in low:
        return "svg"
    if "<html" in low or "<!doctype html" in low:
        return "html"
    return "text"


def _looks_like_legacy_text(head: bytes) -> bool:
    """Windows-1252 / Latin-1 text is not valid UTF-8 but is common: accept it when it has no NUL bytes and almost no control codes."""
    if not head or b"\x00" in head:
        return False
    ctrl = sum(1 for b in head if b < 9 or 14 <= b < 32 or b == 127)
    return ctrl / len(head) < 0.02


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
    if fam in ("png", "jpeg", "gif", "riff"):
        info["active_content"] += check_image(fam, data)
    if fam == "zip":
        info["archive"] = check_zip(data, depth=0)
        info["active_content"] += info["archive"].get("active", [])
        need = OOXML_CORE.get(ext)
        if need and need not in info["archive"]["names"]:
            raise Rejected(f"file content (zip) is not a valid {ext} document")
    if fam == "ole":
        if b"VBA" in data or b"_VBA_PROJECT" in data or b"Macros" in data:
            info["active_content"].append("vba_macros")
    if fam in ("pdf",):
        for marker, label in ((b"/JavaScript", "pdf_javascript"), (b"/JS", "pdf_javascript"), (b"/Launch", "pdf_launch"),
                              (b"/EmbeddedFile", "pdf_embedded_file"), (b"/OpenAction", "pdf_open_action"), (b"/GoToR", "pdf_remote_goto"),
                              (b"/SubmitForm", "pdf_submit_form"), (b"/ImportData", "pdf_import_data"), (b"/XFA", "pdf_xfa"),
                              (b"/Encrypt", "pdf_encrypted")):
            if marker in data and label not in info["active_content"]:
                info["active_content"].append(label)
        eof = data.rfind(b"%%EOF")
        if eof >= 0 and len(data) - (eof + 5) > 1024:
            info["active_content"].append("pdf_trailing_data")
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
    seen: set[str] = set()
    ordered = sorted(infos, key=lambda x: x.header_offset)
    for a, b in zip(ordered, ordered[1:]):
        if a.header_offset + a.compress_size > b.header_offset:
            raise Rejected("archive entries overlap (a compression trick)")
    for i in infos:
        raw_name = i.filename.replace("\\", "/")
        if raw_name.startswith("/") or re.match(r"^[A-Za-z]:", raw_name) or "\x00" in raw_name or ".." in raw_name.split("/"):
            raise Rejected("archive contains an unsafe path (it would write outside its folder)")
        key = raw_name.lower()
        if key in seen:
            raise Rejected("archive has duplicate entries (programs may read different ones)")
        seen.add(key)
        if i.flag_bits & 0x1:
            raise Rejected("archive is password protected and cannot be scanned")
        if i.file_size > 1_000_000 and i.file_size / max(1, i.compress_size) > MAX_MEMBER_RATIO:
            raise Rejected("archive looks like a decompression bomb")
        n = i.filename.lower()
        if n.endswith("vbaproject.bin") or n.endswith(".bin") and "activex" in n:
            active.append("office_macros")
        if "oleobject" in n or "/embeddings/" in n:
            active.append("office_embedded_object")
        if n.endswith(".rels") and i.file_size < 2_000_000 and b'TargetMode="External"' in zf.read(i):
            active.append("office_external_reference")
        if n in ("word/document.xml", "word/footnotes.xml", "word/header1.xml") and i.file_size < 50_000_000:
            body = zf.read(i).upper()
            if b"DDEAUTO" in body or re.search(rb"<W:INSTRTEXT[^>]*>\s*DDE\b", body):
                active.append("office_dde_field")
        if Path(n).suffix in BLOCKED_EXT:
            raise Rejected(f"archive contains a blocked file type ({Path(n).suffix})")
        if n.endswith(".zip") and i.file_size < 50 * 1024 * 1024:
            check_zip(zf.read(i), depth + 1)
    return {"members": len(infos), "uncompressed": total, "ratio": round(total / comp, 1), "active": sorted(set(active)),
            "names": sorted(x.lower() for x in seen)}


def check_image(fam: str, data: bytes) -> list[str]:
    """Header-level checks only (nothing is decoded): reject pixel bombs, frame bombs, oversized colour profiles and
    truncated or overflowing structures before any viewer or vision model could touch the file."""
    def dims(w: int, h: int) -> None:
        if w <= 0 or h <= 0 or w > MAX_IMAGE_SIDE or h > MAX_IMAGE_SIDE or w * h > MAX_IMAGE_PIXELS:
            raise Rejected(f"image dimensions are too large ({w}x{h})")
    extra: list[str] = []
    try:
        if fam == "png":
            pos, seen_iend, w = 8, False, 0
            while pos + 8 <= len(data):
                ln, typ = struct.unpack(">I4s", data[pos:pos + 8])
                if ln > 0x7FFFFFFF or pos + 12 + ln > len(data):
                    raise Rejected("PNG chunk length is invalid")
                if typ == b"IHDR":
                    w, h = struct.unpack(">II", data[pos + 8:pos + 16])
                    dims(w, h)
                if typ == b"IEND":
                    seen_iend = True
                    if len(data) - (pos + 12) > 16:
                        extra.append("image_trailing_data")
                    break
                pos += 12 + ln
            if not w or not seen_iend:
                raise Rejected("PNG is truncated or has no header")
        elif fam == "jpeg":
            pos, icc, got = 2, 0, False
            while pos + 4 <= len(data):
                if data[pos] != 0xFF:
                    raise Rejected("JPEG structure is invalid")
                marker = data[pos + 1]
                if marker == 0xFF:
                    pos += 1
                    continue
                if marker in (0xD8, 0x01) or 0xD0 <= marker <= 0xD7:
                    pos += 2
                    continue
                ln = struct.unpack(">H", data[pos + 2:pos + 4])[0]
                if marker in range(0xC0, 0xD0) and marker not in (0xC4, 0xC8, 0xCC):
                    h, w = struct.unpack(">HH", data[pos + 5:pos + 9])
                    dims(w, h)
                    got = True
                if marker == 0xE2 and data[pos + 4:pos + 15] == b"ICC_PROFILE":
                    icc += ln
                    if icc > MAX_ICC_BYTES:
                        raise Rejected("JPEG colour profile is oversized")
                if marker == 0xDA:
                    break
                pos += 2 + ln
            if not got:
                raise Rejected("JPEG has no frame header")
            if b"\xff\xd9" not in data[-4096:]:
                raise Rejected("JPEG is truncated")
            if len(data) - (data.rfind(b"\xff\xd9") + 2) > 16:
                extra.append("image_trailing_data")
        elif fam == "gif":
            w, h = struct.unpack("<HH", data[6:10])
            dims(w, h)
            flags, pos, frames = data[10], 13, 0
            if flags & 0x80:
                pos += 3 * (2 << (flags & 7))
            while pos < len(data):
                b = data[pos]
                if b == 0x3B:
                    break
                if b == 0x21:
                    pos += 2
                elif b == 0x2C:
                    frames += 1
                    if frames > MAX_IMAGE_FRAMES:
                        raise Rejected("GIF has too many frames")
                    lf = data[pos + 9]
                    pos += 10 + (3 * (2 << (lf & 7)) if lf & 0x80 else 0) + 1
                else:
                    raise Rejected("GIF structure is invalid")
                while pos < len(data) and data[pos]:
                    pos += data[pos] + 1
                pos += 1
        elif fam == "riff" and data[8:12] == b"WEBP":
            pos, frames = 12, 0
            while pos + 8 <= len(data):
                typ, ln = data[pos:pos + 4], struct.unpack("<I", data[pos + 4:pos + 8])[0]
                if typ == b"VP8X":
                    dims(int.from_bytes(data[pos + 12:pos + 15], "little") + 1, int.from_bytes(data[pos + 15:pos + 18], "little") + 1)
                if typ == b"ANMF":
                    frames += 1
                    if frames > MAX_IMAGE_FRAMES:
                        raise Rejected("animated image has too many frames")
                pos += 8 + ln + (ln & 1)
    except (struct.error, IndexError) as e:
        raise Rejected("image structure is invalid") from e
    return extra


# ------------------------------------------------------------------------------ antivirus
def find_defender() -> str | None:
    if sys.platform != "win32":
        return None
    from pa_common.devmode import dev_mode
    if dev_mode() and os.environ.get("PA_SKIP_DEFENDER") == "1":
        return None  # developer mode only (tests / e2e on PCs where another antivirus is active); never in release builds
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
        if "disabled" in out.lower() or "0x80004005" in out:
            # Defender is switched off (typically because another antivirus is the active one): this is "cannot scan",
            # not "malware". First ask the antivirus that IS active, through Windows' AMSI (only trusted when it proves itself on the EICAR
            # test string); otherwise fail closed - the file stays in quarantine until the user explicitly releases it - and say why.
            from . import amsi
            if amsi.available():
                return amsi.scan(data, path.name)
            return {"clean": False, "engine": "defender", "threat": None, "unavailable": True,
                    "error": "Microsoft Defender is turned off (another antivirus may be active), so the file cannot be scanned"}
        threat = next((line.split(":", 1)[1].strip() for line in out.splitlines() if "Threat" in line and ":" in line), "threat")
        return {"clean": False, "engine": "defender", "threat": threat}
    return {"clean": False, "engine": "defender", "threat": None, "error": f"scanner exit code {proc.returncode}"}
