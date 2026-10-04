"""Tiny hand-made PDFs and JPEGs for tests (standard library only): a scanned PDF = pages that are one JPEG picture each,
and a normal PDF = pages with a real text layer."""
from __future__ import annotations

import struct


def tiny_jpeg(width: int = 64, height: int = 48) -> bytes:
    """A structurally valid JPEG header + scan + EOI (not decodable pixels, but passes header-level checks and is stored as-is by PDFs)."""
    sof = b"\xff\xc0" + struct.pack(">HBHHB", 17, 8, height, width, 3) + b"\x01\x11\x00\x02\x11\x00\x03\x11\x00"
    sos = b"\xff\xda" + struct.pack(">HB", 12, 3) + b"\x01\x00\x02\x00\x03\x00" + b"\x00\x3f\x00"
    return b"\xff\xd8" + sof + sos + b"\x12\x34\x56" * 40 + b"\xff\xd9"


def _pdf(objects: list[bytes]) -> bytes:
    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for i, body in enumerate(objects, 1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n".encode() + body + b"\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode()
    for off in offsets:
        out += f"{off:010d} 00000 n \n".encode()
    out += f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    return bytes(out)


def scanned_pdf(pages: int = 2, jpeg: bytes | None = None) -> bytes:
    """Every page is a full-page JPEG image and has NO text (what a scanner or phone app produces)."""
    jpeg = jpeg or tiny_jpeg()
    objs: list[bytes] = [b"<< /Type /Catalog /Pages 2 0 R >>", b""]
    kids = []
    for n in range(pages):
        page_id, content_id, img_id = 3 + n * 3, 4 + n * 3, 5 + n * 3
        kids.append(f"{page_id} 0 R")
        objs.append(f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents {content_id} 0 R /Resources << /XObject << /Im0 {img_id} 0 R >> >> >>".encode())
        stream = b"q 612 0 0 792 0 0 cm /Im0 Do Q"
        objs.append(f"<< /Length {len(stream)} >>\nstream\n".encode() + stream + b"\nendstream")
        objs.append(f"<< /Type /XObject /Subtype /Image /Width 64 /Height 48 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /DCTDecode /Length {len(jpeg)} >>\nstream\n".encode()
                    + jpeg + b"\nendstream")
    objs[1] = f"<< /Type /Pages /Kids [{' '.join(kids)}] /Count {pages} >>".encode()
    return _pdf(objs)


def text_pdf(pages: int = 2, line: str = "This is a normal document with a real text layer, long enough to be clearly not a scan.") -> bytes:
    objs: list[bytes] = [b"<< /Type /Catalog /Pages 2 0 R >>", b"", b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"]
    kids = []
    for n in range(pages):
        page_id, content_id = 4 + n * 2, 5 + n * 2
        kids.append(f"{page_id} 0 R")
        objs.append(f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents {content_id} 0 R /Resources << /Font << /F1 3 0 R >> >> >>".encode())
        stream = f"BT /F1 12 Tf 72 700 Td ({line} Page {n + 1}.) Tj ET".encode()
        objs.append(f"<< /Length {len(stream)} >>\nstream\n".encode() + stream + b"\nendstream")
    objs[1] = f"<< /Type /Pages /Kids [{' '.join(kids)}] /Count {pages} >>".encode()
    return _pdf(objs)
