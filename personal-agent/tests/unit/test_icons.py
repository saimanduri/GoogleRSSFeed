"""The ChiRAG icon is used everywhere: a valid multi-size .ico for the exe/window/tray/shortcut, matching PNGs, and the logo/favicon in the web UI."""
import struct
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ICONS = ROOT / "app" / "ui" / "src-tauri" / "icons"


def _png_size(b: bytes) -> tuple[int, int]:
    assert b[:8] == b"\x89PNG\r\n\x1a\n"
    return struct.unpack(">II", b[16:24])


def test_ico_has_all_sizes_as_png_frames():
    for ico in (ICONS / "icon.ico", ROOT / "installer" / "windows" / "ChiRAG.ico", ROOT / "app" / "ui" / "public" / "favicon.ico"):
        b = ico.read_bytes()
        _, kind, n = struct.unpack("<HHH", b[:6])
        assert kind == 1 and n == 6
        sizes = []
        for i in range(n):
            w, h, _c, _r, _p, bits, size, off = struct.unpack("<BBBBHHII", b[6 + 16 * i:22 + 16 * i])
            sizes.append(w or 256)
            assert _png_size(b[off:off + size]) == (w or 256, h or 256) and bits == 32
        assert sizes == [16, 32, 48, 64, 128, 256]


def test_png_set_matches_names():
    for name, s in {"32x32.png": 32, "128x128.png": 128, "128x128@2x.png": 256, "icon.png": 256, "Square150x150Logo.png": 150}.items():
        assert _png_size((ICONS / name).read_bytes()) == (s, s)
    assert _png_size((ROOT / "app" / "ui" / "public" / "chirag-logo.png").read_bytes()) == (128, 128)


def test_ui_uses_the_logo_and_the_installer_makes_shortcuts():
    assert "chirag-logo.png" in (ROOT / "app" / "ui" / "src" / "Shell.tsx").read_text(encoding="utf-8")
    assert "favicon.png" in (ROOT / "app" / "ui" / "index.html").read_text(encoding="utf-8")
    inst = (ROOT / "installer" / "windows" / "install-dev.ps1").read_text(encoding="utf-8")
    assert "ChiRAG Agent.lnk" in inst and "ChiRAG.ico" in inst
