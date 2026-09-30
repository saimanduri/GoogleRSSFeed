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
