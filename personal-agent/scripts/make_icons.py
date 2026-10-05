"""Builds every icon of the app from ONE source .ico (default C:\\Users\\<you>\\Downloads\\ChiRAG_favicon.ico) with the standard library + numpy only.

The largest frame of the source is decoded (PNG or 32-bit BMP), enlarged smoothly (bilinear, premultiplied alpha) where needed and written as PNG
files for the window/tray/installer and as a multi-size .ico (PNG frames 16..256). If you later have a bigger source (256 px or an SVG), run
`npx tauri icon <file>` instead for sharper large icons.
    python scripts\\make_icons.py [source.ico]
"""
import struct
import sys
import zlib
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
SRC = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.home() / "Downloads" / "ChiRAG_favicon.ico"
ICONS = ROOT / "app" / "ui" / "src-tauri" / "icons"
PUBLIC = ROOT / "app" / "ui" / "public"


def png_encode(rgba: np.ndarray) -> bytes:
    h, w, _ = rgba.shape
    raw = b"".join(b"\x00" + rgba[y].tobytes() for y in range(h))

    def chunk(t: bytes, d: bytes) -> bytes:
        return struct.pack(">I", len(d)) + t + d + struct.pack(">I", zlib.crc32(t + d) & 0xFFFFFFFF)
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 6, 0, 0, 0)) + chunk(b"IDAT", zlib.compress(raw, 9)) + chunk(b"IEND", b"")


def png_decode(b: bytes) -> np.ndarray:
    pos, idat, w = 8, b"", 0
    while pos < len(b):
        ln, typ = struct.unpack(">I4s", b[pos:pos + 8])
        body = b[pos + 8:pos + 8 + ln]
        if typ == b"IHDR":
            w, h, depth, ctype = struct.unpack(">IIBB", body[:10])
            assert depth == 8 and ctype in (2, 6), "only 8-bit RGB/RGBA PNG frames are supported"
        elif typ == b"IDAT":
            idat += body
        pos += 12 + ln
    ch = 4 if ctype == 6 else 3
    raw = np.frombuffer(zlib.decompress(idat), dtype=np.uint8).reshape(h, 1 + w * ch)
    out = np.zeros((h, w * ch), dtype=np.int32)
    for y in range(h):
        f, line = raw[y, 0], raw[y, 1:].astype(np.int32)
        prev = out[y - 1] if y else np.zeros(w * ch, dtype=np.int32)
        for x in range(w * ch):
            a = out[y, x - ch] if x >= ch else 0
            c = prev[x - ch] if x >= ch else 0
            v = line[x]
            if f == 1:
                v += a
            elif f == 2:
                v += prev[x]
            elif f == 3:
                v += (a + prev[x]) // 2
            elif f == 4:
                p = a + prev[x] - c
                pa, pb, pc = abs(p - a), abs(p - prev[x]), abs(p - c)
                v += a if pa <= pb and pa <= pc else (prev[x] if pb <= pc else c)
            out[y, x] = v & 255
    img = out.reshape(h, w, ch).astype(np.uint8)
    return img if ch == 4 else np.dstack([img, np.full((h, w, 1), 255, np.uint8)])


def largest_frame(path: Path) -> np.ndarray:
    b = path.read_bytes()
    _, _, n = struct.unpack("<HHH", b[:6])
    best = None
    for i in range(n):
        w, h, _c, _r, _p, bits, size, off = struct.unpack("<BBBBHHII", b[6 + 16 * i:22 + 16 * i])
        w, h = w or 256, h or 256
        if best is None or w > best[0]:
            best = (w, h, bits, size, off)
    w, h, bits, size, off = best
    data = b[off:off + size]
    if data[:4] == b"\x89PNG":
        return png_decode(data)
    hdr = struct.unpack("<IiiHHIIiiII", data[:40])
    assert hdr[4] == 32, "only 32-bit icon frames are supported"
    px = np.frombuffer(data[hdr[0]:hdr[0] + w * h * 4], dtype=np.uint8).reshape(h, w, 4)[::-1]       # bottom-up BGRA
    return np.dstack([px[..., 2], px[..., 1], px[..., 0], px[..., 3]]).copy()


def resize(img: np.ndarray, size: int) -> np.ndarray:
    """Bilinear resize with premultiplied alpha (no dark fringes), square output."""
    h, w, _ = img.shape
    f = img.astype(np.float32)
    a = f[..., 3:4] / 255.0
    pm = np.concatenate([f[..., :3] * a, f[..., 3:4]], axis=2)
    ys, xs = np.linspace(0, h - 1, size), np.linspace(0, w - 1, size)
    y0, x0 = np.floor(ys).astype(int), np.floor(xs).astype(int)
    y1, x1 = np.minimum(y0 + 1, h - 1), np.minimum(x0 + 1, w - 1)
    wy, wx = (ys - y0)[:, None, None], (xs - x0)[None, :, None]
    top = pm[y0][:, x0] * (1 - wx) + pm[y0][:, x1] * wx
    bot = pm[y1][:, x0] * (1 - wx) + pm[y1][:, x1] * wx
    out = top * (1 - wy) + bot * wy
    alpha = out[..., 3:4]
    rgb = np.where(alpha > 0, out[..., :3] / np.maximum(alpha / 255.0, 1e-6), 0)
    return np.clip(np.concatenate([rgb, alpha], axis=2), 0, 255).astype(np.uint8)


def write_ico(frames: dict[int, bytes], path: Path) -> None:
    head = struct.pack("<HHH", 0, 1, len(frames))
    off = 6 + 16 * len(frames)
    entries, blobs = b"", b""
    for size, png in sorted(frames.items()):
        entries += struct.pack("<BBBBHHII", size % 256, size % 256, 0, 0, 1, 32, len(png), off + len(blobs))
        blobs += png
    path.write_bytes(head + entries + blobs)


def main() -> None:
    src = largest_frame(SRC)
    print(f"source frame {src.shape[1]}x{src.shape[0]} from {SRC.name}")
    sizes = {16: 16, 32: 32, 48: 48, 64: 64, 128: 128, 256: 256}
    pngs = {s: png_encode(src if s == src.shape[0] else resize(src, s)) for s in sizes}
    ICONS.mkdir(parents=True, exist_ok=True)
    write_ico(pngs, ICONS / "icon.ico")
    named = {"32x32.png": 32, "64x64.png": 64, "128x128.png": 128, "128x128@2x.png": 256, "icon.png": 256, "Square30x30Logo.png": 30, "Square44x44Logo.png": 44,
             "Square71x71Logo.png": 71, "Square89x89Logo.png": 89, "Square107x107Logo.png": 107, "Square142x142Logo.png": 142, "Square150x150Logo.png": 150,
             "Square284x284Logo.png": 284, "Square310x310Logo.png": 310, "StoreLogo.png": 50}
    for name, s in named.items():
        (ICONS / name).write_bytes(png_encode(resize(src, s)))
    PUBLIC.mkdir(parents=True, exist_ok=True)
    (PUBLIC / "chirag-logo.png").write_bytes(png_encode(resize(src, 128)))
    # the logo in the window (top bar, sign-in, wizard) is imported from src/assets so its name changes with its content
    (ROOT / "app" / "ui" / "src" / "assets").mkdir(parents=True, exist_ok=True)
    (ROOT / "app" / "ui" / "src" / "assets" / "app-logo.png").write_bytes((PUBLIC / "chirag-logo.png").read_bytes())
    (PUBLIC / "favicon.ico").write_bytes((ICONS / "icon.ico").read_bytes())
    (ROOT / "installer" / "windows" / "ChiRAG.ico").write_bytes((ICONS / "icon.ico").read_bytes())
    print("wrote icons to", ICONS, "and", PUBLIC)


main()
