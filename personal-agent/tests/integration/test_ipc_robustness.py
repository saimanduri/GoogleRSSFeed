"""Findings of the RPC/pipe DAST (scripts/pentest_rpc.py), pinned as regression tests."""
import json
import struct

import pytest

from pa_common.errors import PAError
from pa_common.protocol import FrameDecoder, FrameError, encode_frame

LONE = "bad\ud800text"


def test_lone_surrogate_never_breaks_framing():
    frame = encode_frame({"type": "res", "id": 1, "ok": True, "result": {"t": LONE}})
    assert json.loads(frame[4:].decode("utf-8"))["result"]["t"].startswith("bad")
    d = FrameDecoder()
    assert d.feed(frame)[0]["id"] == 1


def test_lone_surrogate_in_params_is_scrubbed_not_stored(env_nocore):
    env_nocore.setup(model=False)
    cid = env_nocore.ui.call("chat.create", {"title": LONE})["id"]
    chats = env_nocore.ui.call("chat.list", {})
    title = next(c for c in chats if c["id"] == cid)["title"]
    assert "\ud800" not in title and "\ufffd" in title
    encode_frame({"result": chats})                      # the reply can be encoded
    env_nocore.ui.call("account.set_profile", {"display_name": LONE, "assistant_name": "A"})
    assert env_nocore.ui.call("session.status")                          # still answering


def test_deeply_nested_params_are_refused(env_nocore):
    env_nocore.setup(model=False)
    deep: list = []
    cur = deep
    for _ in range(200):
        nxt: list = []
        cur.append(nxt)
        cur = nxt
    with pytest.raises(PAError) as e:
        env_nocore.ui.call("memory.add", {"content": "x", "extra": deep})
    assert e.value.code == "invalid_request"


def test_deeply_nested_frame_is_a_frame_error_not_a_crash():
    body = b"[" * 100000 + b"]" * 100000
    with pytest.raises(FrameError):
        FrameDecoder().feed(struct.pack("<I", len(body)) + body)


def test_backup_verify_with_hostile_path_is_a_clean_error(env_nocore):
    env_nocore.setup(model=False)
    for path in ("C:\\definitely\\missing\\file.bak", "..\\..\\..\\Windows\\win.ini", "x\x00y", "C:\\Windows\\System32\\config\\SAM", "NUL", "A" * 5000):
        with pytest.raises(PAError) as e:
            env_nocore.ui.call("backup.verify", {"path": path, "password": "x"})
        assert e.value.code != "internal_error", path


def test_bad_labels_and_tags_are_clean_errors(env_nocore):
    env_nocore.setup(model=False)
    import base64
    up = env_nocore.ui.call("files.upload", {"name": "a.txt", "data_b64": base64.b64encode(b"hello").decode()})
    fid = up["id"]
    for bad in ("TOP_SECRET", "", "x" * 19, "public"):
        with pytest.raises(PAError) as e:
            env_nocore.ui.call("files.set_label", {"file_id": fid, "level": bad})
        assert e.value.code != "internal_error"
    with pytest.raises(PAError) as e:
        env_nocore.ui.call("files.upload", {"name": "b.txt", "data_b64": base64.b64encode(b"x").decode(), "sensitivity": "SECRET"})
    assert e.value.code == "invalid_request"
    for tags in ([{"a": 1}], [None, 5], ["ok"] * 31):
        try:
            env_nocore.ui.call("files.update", {"file_id": fid, "tags": tags})
        except PAError as e:
            assert e.code != "internal_error"
