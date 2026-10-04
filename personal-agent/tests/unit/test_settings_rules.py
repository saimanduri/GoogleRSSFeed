"""Settings: tighten immediately, loosen needs password + 10 s delay, floor cannot be breached (spec 5.4/5.5)."""
import time

import pytest

from pa_common.errors import PAError
from tests.conftest import PASSWORD


def test_tighten_immediately(env_nocore):
    env_nocore.setup(model=False)
    env_nocore.ui.call("settings.apply", {"changes": {"security.auto_lock_minutes": 5}})
    assert env_nocore.gw.settings.get("security.auto_lock_minutes") == 5


def test_loosen_requires_password_and_delay(env_nocore, monkeypatch):
    env_nocore.setup(model=False)
    ui = env_nocore.ui
    with pytest.raises(PAError) as e:
        ui.call("settings.apply", {"changes": {"web.fetch_any_site": True}})
    assert e.value.code == "password_required"
    tok = ui.call("settings.begin_loosen", {"changes": {"web.fetch_any_site": True}})["token"]
    with pytest.raises(PAError) as e:
        ui.call("settings.apply", {"changes": {"web.fetch_any_site": True}, "password": PASSWORD, "loosen_token": tok})
    assert e.value.code == "loosen_too_fast"
    import pa_gateway.settings as st
    monkeypatch.setattr(st, "LOOSEN_DELAY_SECONDS", 0)
    tok = ui.call("settings.begin_loosen", {"changes": {"web.fetch_any_site": True}})["token"]
    time.sleep(0.01)
    r = ui.call("settings.apply", {"changes": {"web.fetch_any_site": True}, "password": PASSWORD, "loosen_token": tok})
    assert "web.fetch_any_site" in r["loosened"]


def test_floor_cannot_be_breached(env_nocore):
    env_nocore.setup(model=False)
    with pytest.raises(PAError) as e:
        env_nocore.ui.call("settings.apply", {"changes": {"sensitivity.default.mail": "INTERNAL"}})
    assert e.value.code in ("invalid_request", "floor_violation")
    with pytest.raises(PAError):
        env_nocore.ui.call("settings.apply", {"changes": {"security.auto_lock_minutes": 0}})


def test_unknown_setting_rejected(env_nocore):
    env_nocore.setup(model=False)
    with pytest.raises(PAError):
        env_nocore.ui.call("settings.apply", {"changes": {"security.disable_audit": True}})


def test_picture_settings_accept_only_small_real_pictures():
    import base64
    import struct
    import zlib

    from pa_gateway.settings_schema import BY_KEY, coerce

    def png(n=1):
        raw = b"".join(b"\x00" + b"\xff\x00\x00" * n for _ in range(n))
        def chunk(t, d):
            c = struct.pack(">I", len(d)) + t + d
            return c + struct.pack(">I", zlib.crc32(t + d) & 0xFFFFFFFF)
        return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", n, n, 8, 2, 0, 0, 0)) + chunk(b"IDAT", zlib.compress(raw)) + chunk(b"IEND", b"")

    spec = BY_KEY["ui.assistant_icon"]
    ok = "data:image/png;base64," + base64.b64encode(png()).decode()
    assert coerce(spec, ok) == ok and coerce(spec, "") == ""
    for bad in ("data:image/svg+xml;base64," + base64.b64encode(b"<svg onload=alert(1)/>").decode(),
                "data:image/png;base64," + base64.b64encode(b"not a png").decode(),
                "data:image/png;base64," + base64.b64encode(png() + b"0" * 70_000).decode(),
                "data:image/png;base64,@@@", "http://example.com/x.png", 5):
        with pytest.raises(ValueError):
            coerce(spec, bad)
