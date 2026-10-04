"""UI organisation features: pinned chats, folders (migration 2) and the theme/background settings."""
import pytest

from pa_common.errors import PAError


def test_pin_and_folder_roundtrip(env_nocore):
    env_nocore.setup(model=False)
    cid = env_nocore.ui.call("chat.create", {})["id"]
    env_nocore.ui.call("chat.update", {"chat_id": cid, "pinned": True, "folder": "Work"})
    row = next(c for c in env_nocore.ui.call("chat.list", {}) if c["id"] == cid)
    assert row["pinned"] == 1 and row["folder"] == "Work"
    env_nocore.ui.call("chat.update", {"chat_id": cid, "pinned": False, "folder": ""})
    row = next(c for c in env_nocore.ui.call("chat.list", {}) if c["id"] == cid)
    assert row["pinned"] == 0 and row["folder"] == ""


def test_folder_name_is_validated(env_nocore):
    env_nocore.setup(model=False)
    cid = env_nocore.ui.call("chat.create", {})["id"]
    with pytest.raises(PAError):
        env_nocore.ui.call("chat.update", {"chat_id": cid, "folder": "<script>"})
    with pytest.raises(PAError):
        env_nocore.ui.call("chat.update", {"chat_id": cid, "folder": "x" * 41})


def test_themes_and_background_are_settings(env_nocore):
    env_nocore.setup(model=False)
    desc = env_nocore.ui.call("settings.describe")
    byk = {s["key"]: s for s in desc["settings"]}
    assert {"aurora", "ocean", "forest", "sunset", "time_of_day"} <= set(byk["ui.theme"]["options"])
    assert set(byk["ui.background"]["options"]) == {"off", "aurora", "bubbles", "waves", "stars"}
    env_nocore.ui.call("settings.apply", {"changes": {"ui.theme": "ocean", "ui.background": "waves"}})
    ui = env_nocore.ui.call("session.status")["ui"]
    assert ui["ui.theme"] == "ocean" and ui["ui.background"] == "waves"
    with pytest.raises(PAError):
        env_nocore.ui.call("settings.apply", {"changes": {"ui.theme": "neon-pink"}})
