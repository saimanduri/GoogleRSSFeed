"""Local files and folders are shared only with an explicit, per-chat, per-session approval; subfolders need a separate approval;
nothing outside the approved root can ever be reached (traversal, links, drive/home/system/AppData/credential folders)."""
import json
import os
import subprocess

import pytest

from pa_common.errors import PAError
from pa_gateway.auth.session import SIGNED_OUT
from pa_gateway.tools.base import ExecContext, ToolFailed


@pytest.fixture(autouse=True)
def _appdata_elsewhere(monkeypatch, tmp_path_factory):
    fake = tmp_path_factory.mktemp("fake_home") / "AppData"
    monkeypatch.setenv("APPDATA", str(fake / "Roaming"))
    monkeypatch.setenv("LOCALAPPDATA", str(fake / "Local"))


@pytest.fixture
def tree(tmp_path):
    root = tmp_path / "Reports"
    (root / "2026" / "q1").mkdir(parents=True)
    (root / ".ssh").mkdir()
    (root / "notes.txt").write_text("top level note", encoding="utf-8")
    (root / "data.csv").write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
    (root / "run.exe").write_bytes(b"MZ" + b"\0" * 100)
    (root / "2026" / "plan.txt").write_text("plan inside a subfolder", encoding="utf-8")
    (root / "2026" / "q1" / "deep.txt").write_text("deep file", encoding="utf-8")
    (root / ".ssh" / "id_rsa.txt").write_text("PRIVATE KEY MATERIAL", encoding="utf-8")
    (tmp_path / "outside.txt").write_text("outside the approved folder", encoding="utf-8")
    return root


def _setup(env):
    env.setup(model=False)
    cid = env.ui.call("chat.create", {})["id"]
    return cid


def _ctx(env, cid):
    return ExecContext(task={"id": "task_x", "chat_id": cid}, run_id=None, session_id=None, approved=False, cancelled=lambda: False)


def _tool(env, cid, name, args):
    lf = env.gw.localfiles
    env.gw.budgets.add = lambda *a, **k: None          # these direct calls have no real task to charge
    fn = {"digest": lf.t_digest, "browse": lf.t_browse, "text": lambda a, c: lf._tool(c, a, "text", 60), "list": lf.t_list}[name]
    return fn(args, _ctx(env, cid))


def test_folder_grant_lists_and_reads_direct_files_only(env_nocore, tree):
    cid = _setup(env_nocore)
    g = env_nocore.ui.call("localfiles.grant_folder", {"chat_id": cid, "path": str(tree), "include_subfolders": False, "confirm_subfolders": False})
    assert g["scope"] == "folder" and g["recursive"] is False and g["expired"] is False
    listing = _tool(env_nocore, cid, "browse", {"grant_id": g["id"]}).content
    assert "notes.txt" in listing and "data.csv" in listing and "[folder] 2026" in listing and "needs the user's approval" in listing
    assert "run.exe" not in listing and ".ssh" not in listing                    # programs and credential folders are not even listed
    assert _tool(env_nocore, cid, "text", {"file_id": g["id"], "path": "notes.txt"}).content.startswith("top level note")


@pytest.mark.windows  # uses Windows paths (backslash separators, junctions)
def test_subfolders_need_a_separate_explicit_approval(env_nocore, tree):
    cid = _setup(env_nocore)
    ui = env_nocore.ui
    with pytest.raises(PAError) as e:
        ui.call("localfiles.grant_folder", {"chat_id": cid, "path": str(tree), "include_subfolders": True, "confirm_subfolders": False})
    assert e.value.code == "subfolders_need_approval"
    g = ui.call("localfiles.grant_folder", {"chat_id": cid, "path": str(tree), "include_subfolders": False, "confirm_subfolders": False})
    with pytest.raises(ToolFailed) as t:
        _tool(env_nocore, cid, "text", {"file_id": g["id"], "path": "2026\\plan.txt"})
    assert "subfolder" in str(t.value)
    with pytest.raises(ToolFailed):
        _tool(env_nocore, cid, "browse", {"grant_id": g["id"], "subpath": "2026"})
    pend = ui.call("localfiles.requests", {"chat_id": cid})
    assert len(pend) == 1 and pend[0]["subfolder"] == "2026" and pend[0]["grant_id"] == g["id"]
    assert len(ui.call("localfiles.requests", {"chat_id": cid})) == 1            # asking twice does not pile up requests
    with pytest.raises(PAError) as e2:
        ui.call("localfiles.allow_subfolders", {"grant_id": g["id"], "confirm": False})
    assert e2.value.code == "subfolders_need_approval"
    ui.call("localfiles.allow_subfolders", {"grant_id": g["id"], "confirm": True})
    assert ui.call("localfiles.requests", {"chat_id": cid}) == []
    assert "plan inside a subfolder" in _tool(env_nocore, cid, "text", {"file_id": g["id"], "path": "2026\\plan.txt"}).content
    assert "deep file" in _tool(env_nocore, cid, "text", {"file_id": g["id"], "path": "2026/q1/deep.txt"}).content
    assert "plan.txt" in _tool(env_nocore, cid, "browse", {"grant_id": g["id"], "subpath": "2026"}).content


@pytest.mark.windows  # uses Windows paths (backslash separators, junctions)
def test_denying_a_request_removes_it(env_nocore, tree):
    cid = _setup(env_nocore)
    g = env_nocore.ui.call("localfiles.grant_folder", {"chat_id": cid, "path": str(tree), "include_subfolders": False, "confirm_subfolders": False})
    with pytest.raises(ToolFailed):
        _tool(env_nocore, cid, "text", {"file_id": g["id"], "path": "2026\\plan.txt"})
    rid = env_nocore.ui.call("localfiles.requests", {"chat_id": cid})[0]["id"]
    env_nocore.ui.call("localfiles.deny_request", {"request_id": rid})
    assert env_nocore.ui.call("localfiles.requests", {"chat_id": cid}) == []
    with pytest.raises(ToolFailed):
        _tool(env_nocore, cid, "text", {"file_id": g["id"], "path": "2026\\plan.txt"})


@pytest.mark.parametrize("rel", ["..\\outside.txt", "../outside.txt", "2026\\..\\..\\outside.txt", "C:\\Windows\\win.ini", "\\Windows\\win.ini", "notes.txt:stream",
                                 "", ".", "..", "x\x00.txt", "\\\\server\\share\\a.txt", ".ssh\\id_rsa.txt", "run.exe", "missing.txt"])
def test_paths_cannot_leave_the_folder_or_reach_forbidden_things(env_nocore, tree, rel):
    cid = _setup(env_nocore)
    g = env_nocore.ui.call("localfiles.grant_folder", {"chat_id": cid, "path": str(tree), "include_subfolders": True, "confirm_subfolders": True})
    with pytest.raises(ToolFailed):
        _tool(env_nocore, cid, "text", {"file_id": g["id"], "path": rel})


@pytest.mark.windows  # uses Windows paths (backslash separators, junctions)
def test_junction_pointing_outside_is_refused(env_nocore, tree, tmp_path):
    cid = _setup(env_nocore)
    out_dir = tmp_path / "elsewhere"
    out_dir.mkdir()
    (out_dir / "secret.txt").write_text("secret from another place", encoding="utf-8")
    link = tree / "shortcut"
    r = subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(out_dir)], capture_output=True)
    if r.returncode != 0:
        pytest.skip("cannot create a junction here")
    g = env_nocore.ui.call("localfiles.grant_folder", {"chat_id": cid, "path": str(tree), "include_subfolders": True, "confirm_subfolders": True})
    with pytest.raises(ToolFailed) as t:
        _tool(env_nocore, cid, "text", {"file_id": g["id"], "path": "shortcut\\secret.txt"})
    assert "outside" in str(t.value)
    assert "shortcut" not in _tool(env_nocore, cid, "browse", {"grant_id": g["id"]}).content       # links are not even listed


@pytest.mark.windows  # uses Windows paths (backslash separators, junctions)
def test_dangerous_folders_cannot_be_granted(env_nocore, tmp_path):
    cid = _setup(env_nocore)
    home = os.path.expanduser("~")
    bad = [os.path.splitdrive(home)[0] + "\\", home, os.path.dirname(home), os.environ["WINDIR"], os.environ.get("PROGRAMFILES", "C:\\Program Files"),
           str(env_nocore.gw.paths.root), "\\\\server\\share", str(tmp_path / "missing")]
    fake_appdata = os.environ["APPDATA"]
    os.makedirs(fake_appdata, exist_ok=True)
    bad.append(fake_appdata)
    cred = tmp_path / ".aws"
    cred.mkdir()
    bad.append(str(cred))
    for p in bad:
        with pytest.raises(PAError):
            env_nocore.ui.call("localfiles.grant_folder", {"chat_id": cid, "path": p, "include_subfolders": False, "confirm_subfolders": False})
    assert env_nocore.ui.call("localfiles.list", {"chat_id": cid}) == []


def test_approval_is_bound_to_the_app_session(env_nocore, tree):
    cid = _setup(env_nocore)
    ui, gw = env_nocore.ui, env_nocore.gw
    g = ui.call("localfiles.grant_folder", {"chat_id": cid, "path": str(tree), "include_subfolders": True, "confirm_subfolders": True})
    f = ui.call("localfiles.grant", {"chat_id": cid, "path": str(tree / "notes.txt")})
    assert gw.localfiles.has_grants(cid) and len(gw.localfiles.for_prompt(cid)) == 2
    gw.session.set_state(SIGNED_OUT)                         # sign out / restart = a new app session
    gw.session.signed_in()
    rows = {r["id"]: r for r in ui.call("localfiles.list", {"chat_id": cid})}
    assert rows[g["id"]]["expired"] and rows[f["id"]]["expired"]
    assert not gw.localfiles.has_grants(cid) and gw.localfiles.for_prompt(cid) == []
    for args in ({"file_id": g["id"], "path": "notes.txt"}, {"file_id": f["id"]}):
        with pytest.raises(ToolFailed) as t:
            _tool(env_nocore, cid, "text", args)
        assert "expired" in str(t.value)
    with pytest.raises(PAError) as e:                        # renewing never brings the subfolder approval back silently
        ui.call("localfiles.reapprove", {"grant_id": g["id"], "include_subfolders": True, "confirm_subfolders": False})
    assert e.value.code == "subfolders_need_approval"
    again = ui.call("localfiles.reapprove", {"grant_id": g["id"]})
    assert again["expired"] is False and again["recursive"] is False
    with pytest.raises(ToolFailed):
        _tool(env_nocore, cid, "text", {"file_id": g["id"], "path": "2026\\plan.txt"})
    ui.call("localfiles.reapprove", {"grant_id": f["id"]})
    assert "top level note" in _tool(env_nocore, cid, "text", {"file_id": f["id"]}).content


def test_a_new_chat_has_no_access_to_what_another_chat_approved(env_nocore, tree):
    cid = _setup(env_nocore)
    other = env_nocore.ui.call("chat.create", {})["id"]
    g = env_nocore.ui.call("localfiles.grant_folder", {"chat_id": cid, "path": str(tree), "include_subfolders": False, "confirm_subfolders": False})
    with pytest.raises(ToolFailed) as t:
        _tool(env_nocore, other, "text", {"file_id": g["id"], "path": "notes.txt"})
    assert "not shared with this chat" in str(t.value)
    assert env_nocore.ui.call("localfiles.list", {"chat_id": other}) == [] and env_nocore.gw.localfiles.for_prompt(other) == []


def test_folder_info_counts_for_the_approval_dialog(env_nocore, tree):
    _setup(env_nocore)
    info = env_nocore.ui.call("localfiles.folder_info", {"path": str(tree)})
    assert info["files"] == 2 and info["subfolders"] == 2 and info["subfolder_files"] == 2 and info["subfolder_names"] == ["2026"]
    assert info["truncated"] is False and info["name"] == "Reports"
    with pytest.raises(PAError):
        env_nocore.ui.call("localfiles.folder_info", {"path": str(tree / "notes.txt")})


def test_limits_and_cleanup(env_nocore, tree, tmp_path):
    cid = _setup(env_nocore)
    ids = []
    for i in range(5):
        d = tmp_path / f"d{i}"
        d.mkdir()
        ids.append(env_nocore.ui.call("localfiles.grant_folder", {"chat_id": cid, "path": str(d), "include_subfolders": False, "confirm_subfolders": False})["id"])
    with pytest.raises(PAError):
        env_nocore.ui.call("localfiles.grant_folder", {"chat_id": cid, "path": str(tree), "include_subfolders": False, "confirm_subfolders": False})
    env_nocore.ui.call("localfiles.revoke", {"grant_id": ids[0]})
    assert len(env_nocore.ui.call("localfiles.list", {"chat_id": cid})) == 4
    env_nocore.ui.call("chat.delete", {"chat_id": cid})
    assert env_nocore.gw.db.scalar("SELECT count(*) FROM local_grants WHERE chat_id=?", (cid,)) == 0


def test_model_cannot_create_or_widen_grants(env_nocore):
    from pa_gateway.ipc.dispatch import REGISTRY
    for name in ("localfiles.grant", "localfiles.grant_folder", "localfiles.allow_subfolders", "localfiles.reapprove", "localfiles.folder_info",
                 "localfiles.requests", "localfiles.deny_request"):
        assert REGISTRY[name].roles == ("ui",) or list(REGISTRY[name].roles) == ["ui"], name


@pytest.mark.windows  # uses Windows paths (backslash separators, junctions)
def test_chat_flow_browse_read_and_subfolder_question(env, tree, monkeypatch):
    import pa_gateway.llm.mock as mock
    env.setup()
    cid = env.ui.call("chat.create", {})["id"]
    g = env.ui.call("localfiles.grant_folder", {"chat_id": cid, "path": str(tree), "include_subfolders": False, "confirm_subfolders": False})
    steps = iter([
        {"thought": "look", "action": "tool", "tool": "localfile.browse", "args": {"grant_id": g["id"]}},
        {"thought": "read", "action": "tool", "tool": "localfile.text", "args": {"file_id": g["id"], "path": "notes.txt"}},
        {"thought": "deeper", "action": "tool", "tool": "localfile.text", "args": {"file_id": g["id"], "path": "2026\\plan.txt"}},
        {"thought": "done", "action": "final", "answer": "Read the top-level note; the subfolder needs approval."},
    ])
    seen = {}

    def model(messages):
        seen["system"] = messages[0]["content"]
        return json.dumps(next(steps))

    monkeypatch.setattr(mock, "mock_complete", model)
    env.ui.call("chat.send", {"chat_id": cid, "text": "what is in my Reports folder?"})
    msgs = env.wait(lambda: [m for m in env.ui.call("chat.get", {"chat_id": cid})["messages"] if m["role"] == "assistant"], timeout=60)
    assert msgs and "subfolder needs approval" in msgs[-1]["content"]
    assert "FOLDER Reports" in seen["system"] and "NOT allowed" in seen["system"] and str(tree) not in seen["system"]
    assert len(env.ui.call("localfiles.requests", {"chat_id": cid})) == 1
    events = " ".join(str(e["content"]) for e in env.gw.sessionlog.events(cid))
    assert "top level note" in events and "plan inside a subfolder" not in events and "PRIVATE KEY" not in events


@pytest.mark.windows  # uses Windows paths (backslash separators, junctions)
def test_digest_gives_an_overview_of_the_folder_in_one_call(env_nocore, tree):
    cid = _setup(env_nocore)
    g = env_nocore.ui.call("localfiles.grant_folder", {"chat_id": cid, "path": str(tree), "include_subfolders": False, "confirm_subfolders": False})
    out = _tool(env_nocore, cid, "digest", {"grant_id": g["id"]}).content
    assert "2 readable files here, 1 subfolders (not allowed" in out
    assert "notes.txt" in out and "top level note" in out and "data.csv" in out
    assert "run.exe" not in out and "PRIVATE KEY" not in out and "plan inside a subfolder" not in out
    with pytest.raises(ToolFailed):                                  # a subfolder still needs its own approval
        _tool(env_nocore, cid, "digest", {"grant_id": g["id"], "subpath": "2026"})
    env_nocore.ui.call("localfiles.allow_subfolders", {"grant_id": g["id"], "confirm": True})
    sub = _tool(env_nocore, cid, "digest", {"grant_id": g["id"], "subpath": "2026"}).content
    assert "plan inside a subfolder" in sub
    short = _tool(env_nocore, cid, "digest", {"grant_id": g["id"], "max_files": 1}).content
    assert "1 more files not shown" in short
