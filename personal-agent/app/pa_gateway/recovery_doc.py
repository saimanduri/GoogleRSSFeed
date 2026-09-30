"""Minimal, dependency-free PDF with the recovery key (spec 4.2 step 4 "Save as PDF").
The UI recommends printing it and deleting the file."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path


def _esc(s: str) -> str:
    return s.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def write_recovery_pdf(dest: Path, username: str, recovery_key: str) -> None:
    lines = [
        ("F2", 20, "Personal Agent - Recovery Key"),
        ("F1", 11, f"User: {username}        Created: {datetime.now().strftime('%Y-%m-%d %H:%M')}"),
        ("F1", 11, ""),
        ("F3", 18, recovery_key),
        ("F1", 11, ""),
        ("F1", 11, "To reset a forgotten password you need BOTH your PIN and this recovery key, on THIS computer."),
        ("F1", 11, "If you forget your password AND lose either your PIN or this key, your data cannot be recovered."),
        ("F1", 11, "Keep an encrypted backup (Settings > Backup) - backups are restored with your password."),
        ("F1", 11, ""),
        ("F1", 11, "Print this page, store it somewhere safe and offline, then delete this file."),
    ]
    y = 780
    ops = []
    for font, size, text in lines:
        ops.append(f"BT /{font} {size} Tf 56 {y} Td ({_esc(text)}) Tj ET")
        y -= size + 14
    stream = "\n".join(ops).encode("latin-1", "replace")
    objs = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Contents 4 0 R /Resources << /Font << "
        b"/F1 5 0 R /F2 6 0 R /F3 7 0 R >> >> >>",
        b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Courier-Bold >>",
    ]
    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for i, o in enumerate(objs, 1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n".encode() + o + b"\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(objs) + 1}\n0000000000 65535 f \n".encode()
    for off in offsets:
        out += f"{off:010d} 00000 n \n".encode()
    out += f"trailer\n<< /Size {len(objs) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    dest.write_bytes(bytes(out))
