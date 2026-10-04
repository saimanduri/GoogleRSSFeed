"""Rename and pin any chat (R1): names are cleaned, never empty, persisted; pin/unpin independent of rename."""
import pytest

from pa_common.errors import PAError


def _chat(env, **kw):
    return env.ui.call("chat.create", kw)["id"]


def _row(env, cid):
    return next(c for c in env.ui.call("chat.list", {}) if c["id"] == cid)


def test_rename_persists_and_cleans_the_name(env_nocore):
    env_nocore.setup(model=False)
    cid = _chat(env_nocore)
    env_nocore.ui.call("chat.update", {"chat_id": cid, "title": "  Budget ‮ review\x00   2026  "})
    assert _row(env_nocore, cid)["title"] == "Budget review 2026"
    env_nocore.ui.call("chat.update", {"chat_id": cid, "title": "x" * 200})
    assert len(_row(env_nocore, cid)["title"]) == 120
    env_nocore.ui.call("chat.update", {"chat_id": cid, "title": "नमस्ते चैट"})
    assert _row(env_nocore, cid)["title"] == "नमस्ते चैट"


@pytest.mark.parametrize("bad", ["", "   ", "​‮", "\x00\x01"])
def test_empty_names_are_refused(env_nocore, bad):
    env_nocore.setup(model=False)
    cid = _chat(env_nocore)
    before = _row(env_nocore, cid)["title"]
    with pytest.raises(PAError) as e:
        env_nocore.ui.call("chat.update", {"chat_id": cid, "title": bad})
    assert e.value.code == "invalid_request" and _row(env_nocore, cid)["title"] == before


def test_pin_and_rename_are_independent_and_work_on_archived_chats(env_nocore):
    env_nocore.setup(model=False)
    cid = _chat(env_nocore)
    env_nocore.ui.call("chat.update", {"chat_id": cid, "pinned": True})
    env_nocore.ui.call("chat.update", {"chat_id": cid, "title": "Renamed"})
    assert _row(env_nocore, cid)["pinned"] == 1 and _row(env_nocore, cid)["title"] == "Renamed"
    env_nocore.ui.call("chat.update", {"chat_id": cid, "archived": True})
    arch = next(c for c in env_nocore.ui.call("chat.list", {"archived": True}) if c["id"] == cid)
    env_nocore.ui.call("chat.update", {"chat_id": cid, "title": "Renamed archived", "pinned": False})
    arch = next(c for c in env_nocore.ui.call("chat.list", {"archived": True}) if c["id"] == cid)
    assert arch["title"] == "Renamed archived" and arch["pinned"] == 0


def test_unknown_chat_is_not_silently_accepted(env_nocore):
    env_nocore.setup(model=False)
    try:
        env_nocore.ui.call("chat.update", {"chat_id": "chat_does_not_exist", "title": "x"})
    except PAError as e:
        assert e.code != "internal_error"
