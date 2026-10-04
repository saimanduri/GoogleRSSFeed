"""Local files: the user attaches a file from disk to a chat; the model reads it in place through localfile.* tools (isolated
pa-parser worker, per-chat grant, read-only, no path ever shown to the model)."""
import json

import pytest

from pa_common.errors import PAError
from tests.helpers_xlsx import write_xlsx

HEAD = ["Region", "Product", "Units", "Price"]
ROWS = [["North", "Pen", 10, 1.5], ["North", "Book", 3, 12.0], ["South", "Pen", 25, 1.5], ["South", "Lamp", 2, 30.25], ["East", "Pen", 7, 1.75]]


@pytest.fixture(autouse=True)
def _appdata_elsewhere(monkeypatch, tmp_path_factory):
    """pytest's temp folders live under the real AppData (which the app refuses to share): point APPDATA at a fake place for these tests."""
    fake = tmp_path_factory.mktemp("fake_home") / "AppData"
    monkeypatch.setenv("APPDATA", str(fake / "Roaming"))
    monkeypatch.setenv("LOCALAPPDATA", str(fake / "Local"))


@pytest.fixture
def sales(tmp_path):
    p = tmp_path / "Q3 sales.xlsx"
    write_xlsx(p, {"Sales": [HEAD] + ROWS})
    return p


def _chat(env):
    return env.ui.call("chat.create", {})["id"]


def test_grant_validation(env_nocore, tmp_path, sales):
    env_nocore.setup(model=False)
    ui, cid = env_nocore.ui, _chat(env_nocore)
    bad = {
        str(tmp_path / "missing.xlsx"): "not_found", str(tmp_path): "invalid_request", chr(92) * 2 + "server" + chr(92) + "share" + chr(92) + "x.xlsx": "invalid_request",
        str(env_nocore.gw.paths.root / "vault.header"): None,
    }
    exe = tmp_path / "x.exe"
    exe.write_bytes(b"MZ")
    old = tmp_path / "old.xls"
    old.write_bytes(b"\xd0\xcf\x11\xe0")
    for path in list(bad) + [str(exe), str(old)]:
        with pytest.raises(PAError):
            ui.call("localfiles.grant", {"chat_id": cid, "path": path})
    with pytest.raises(PAError) as e:
        ui.call("localfiles.grant", {"chat_id": "chat_nope", "path": str(sales)})
    assert e.value.code == "not_found"
    g = ui.call("localfiles.grant", {"chat_id": cid, "path": str(sales)})
    assert g["name"] == "Q3 sales.xlsx" and g["kind"] == "table" and g["sensitivity"] == "INTERNAL"
    assert [x["id"] for x in ui.call("localfiles.list", {"chat_id": cid})] == [g["id"]]
    log = (env_nocore.gw.paths.logs_dir / "agent-security.jsonl").read_text()
    assert "localfile.granted" in log and str(tmp_path) not in log          # file name yes, folder path never


def test_model_reads_the_file_in_place_with_exact_numbers(env, sales, monkeypatch):
    import pa_gateway.llm.mock as mock
    env.setup()
    cid = _chat(env)
    g = env.ui.call("localfiles.grant", {"chat_id": cid, "path": str(sales)})
    seen = {"tools": None, "system": ""}
    steps = iter([
        {"thought": "look", "action": "tool", "tool": "localfile.inspect", "args": {"file_id": g["id"]}},
        {"thought": "sum", "action": "tool", "tool": "localfile.query",
         "args": {"file_id": g["id"], "group_by": ["Region"], "aggregates": [{"fn": "sum", "col": "Units", "name": "units"}], "order_by": [{"col": "units", "dir": "desc"}]}},
        {"thought": "done", "action": "final", "answer": "South has the most units."},
    ])

    def model(messages):
        seen["system"] = messages[0]["content"]
        return json.dumps(next(steps))

    monkeypatch.setattr(mock, "mock_complete", model)
    env.ui.call("chat.send", {"chat_id": cid, "text": "Which region sold the most units in my attached file?"})
    msgs = env.wait(lambda: [m for m in env.ui.call("chat.get", {"chat_id": cid})["messages"] if m["role"] == "assistant"], timeout=60)
    assert msgs and "South" in msgs[-1]["content"]
    assert "LOCAL FILES" in seen["system"] and g["id"] in seen["system"] and "Q3 sales.xlsx" in seen["system"]
    assert str(sales.parent) not in seen["system"]                           # the model never sees a path
    events = " ".join(str(e["content"]) for e in env.gw.sessionlog.events(cid))
    assert "| South | 27 |" in events and "| North | 13 |" in events and "4 columns" in events
    assert env.gw.db.scalar("SELECT count(*) FROM files") == 0               # nothing was copied into My Files
    assert env.gw.paths.tmp_dir.joinpath("localfiles", f"{g['id']}.sqlite").exists()


def test_other_chat_cannot_use_the_grant_and_revoke_cleans_up(env, sales, monkeypatch):
    import pa_gateway.llm.mock as mock
    env.setup()
    a, b = _chat(env), _chat(env)
    g = env.ui.call("localfiles.grant", {"chat_id": a, "path": str(sales)})
    out = {}
    state = {"n": 0}

    def model(messages):
        state["n"] += 1
        if state["n"] == 1:
            return json.dumps({"thought": "x", "action": "tool", "tool": "localfile.inspect", "args": {"file_id": g["id"]}})
        out["note"] = " ".join(m["content"] for m in messages if "not shared" in m["content"])
        return json.dumps({"thought": "x", "action": "final", "answer": "cannot"})

    monkeypatch.setattr(mock, "mock_complete", model)
    env.ui.call("chat.send", {"chat_id": b, "text": "read the file"})
    assert env.wait(lambda: [m for m in env.ui.call("chat.get", {"chat_id": b})["messages"] if m["role"] == "assistant"], timeout=60)
    assert "not shared with this chat" in out["note"]
    # the tools are not even offered in a chat without local files
    tools_b = env.gw.tools.available_tools({"chat_id": b, "mission_id": None, "id": "t", "allowed_tools_json": "[]"})
    assert not [t for t in tools_b if t["name"].startswith("localfile.")]
    env.ui.call("localfiles.revoke", {"grant_id": g["id"]})
    assert env.ui.call("localfiles.list", {"chat_id": a}) == []
    assert not env.gw.paths.tmp_dir.joinpath("localfiles", f"{g['id']}.sqlite").exists()
    g2 = env.ui.call("localfiles.grant", {"chat_id": a, "path": str(sales)})
    env.ui.call("chat.delete", {"chat_id": a})
    assert env.gw.db.scalar("SELECT count(*) FROM local_grants WHERE id=?", (g2["id"],)) == 0
