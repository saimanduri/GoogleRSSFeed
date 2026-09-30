"""Windows-only checks. Run on GitHub windows-latest runners and on the laptop (see TESTS.md)."""
import os
import subprocess
import sys
import time

import pytest

from tests.conftest import PASSWORD

pytestmark = pytest.mark.windows


@pytest.fixture
def piped(tmp_path):
    from tests.conftest import Env
    e = Env(tmp_path / "d", start_core=False)
    from pa_gateway.ipc.pipe_server import PipeServer
    srv = PipeServer(e.d, e.paths.rendezvous)
    e.gw.pipe_server = srv
    srv.start()
    time.sleep(0.3)
    yield e, srv
    srv.stop()
    e.close()


def test_pipe_handshake_ok_and_bad_token(piped):
    e, srv = piped
    from pa_common.errors import PAError
    from pa_common.pipeclient import PipeClient
    c = PipeClient(srv.pipe_name, "ui", srv.ui_token, "w", expected_server_pid=os.getpid())
    assert c.call("session.status")["state"] == "SETUP_REQUIRED"
    c.close()
    with pytest.raises(PAError):
        PipeClient(srv.pipe_name, "ui", "wrong-token", "w")
    with pytest.raises(PAError):
        PipeClient(srv.pipe_name, "core", "anything", "w")  # no core expected / wrong pid


def test_pipe_squatting_detected(piped):
    e, srv = piped
    from pa_common.errors import PAError
    from pa_common.pipeclient import PipeClient
    with pytest.raises(PAError) as ex:
        PipeClient(srv.pipe_name, "ui", srv.ui_token, "w", expected_server_pid=os.getpid() + 1)
    assert ex.value.code == "pipe_squatting"


def test_pipe_dacl_denies_network_and_grants_only_user(piped):
    e, srv = piped
    import win32security
    import win32file
    h = win32file.CreateFile(srv.pipe_name, win32file.GENERIC_READ | win32file.GENERIC_WRITE, 0, None, win32file.OPEN_EXISTING, 0, None)
    sd = win32security.GetKernelObjectSecurity(h, win32security.DACL_SECURITY_INFORMATION)
    dacl = sd.GetSecurityDescriptorDacl()
    sids = [win32security.ConvertSidToStringSid(dacl.GetAce(i)[2]) for i in range(dacl.GetAceCount())]
    h.Close()
    assert "S-1-5-2" in sids            # NETWORK denied
    assert "S-1-1-0" not in sids        # no Everyone
    assert "S-1-5-32-545" not in sids   # no BUILTIN\Users


def test_job_object_memory_limit(tmp_path):
    from pa_gateway.workers import JobLimits, popen_limited, scrubbed_env
    p, job = popen_limited([sys.executable, "-c", "x = bytearray(600*1024*1024); print('allocated')"], cwd=tmp_path,
                           env=scrubbed_env(), limits=JobLimits(memory_mb=200, max_processes=4))
    out, _ = p.communicate(timeout=60)
    assert b"allocated" not in out and p.returncode != 0


def test_parser_worker_runs_under_job(tmp_path):
    from pa_gateway.workers import run_worker
    out = run_worker("pa_workers.parser", b'{"name":"a.txt","family":"text"}\nhello', timeout=60, cwd=tmp_path)
    assert b"hello" in out


def test_appcontainer_has_no_network(tmp_path):
    from pa_workers.sandbox import appcontainer
    if not appcontainer.available():
        pytest.skip("AppContainer API unavailable")
    base = os.path.dirname(getattr(sys, "_base_executable", sys.executable))
    _, sid = appcontainer.container_sid()
    appcontainer.grant(__import__("pathlib").Path(base), sid, write=False)
    (tmp_path / "out").mkdir()
    appcontainer.grant(tmp_path / "out", sid, write=True)
    code = ("import socket\ntry:\n socket.create_connection(('1.1.1.1',443),timeout=5); print('NET-OPEN')\n"
            "except OSError as e:\n print('NET-BLOCKED', e)\n")
    script = tmp_path / "out" / "t.py"
    script.write_text(code)
    appcontainer.grant(tmp_path / "out", sid, write=True)
    py = getattr(sys, "_base_executable", sys.executable)
    rc = appcontainer.run_in_appcontainer(f'"{py}" -I "{script}"', tmp_path / "out", tmp_path / "out" / "o.txt",
                                          tmp_path / "out" / "e.txt", 60)
    out = (tmp_path / "out" / "o.txt").read_text(errors="ignore") + (tmp_path / "out" / "e.txt").read_text(errors="ignore")
    assert "NET-OPEN" not in out, out
    assert "NET-BLOCKED" in out or rc != 0, out


def test_secure_clipboard_excluded_from_history():
    import win32clipboard
    from pa_gateway.secrets_store import _win_clipboard_clear_if, _win_clipboard_set
    _win_clipboard_set("clip-secret-value-1")
    win32clipboard.OpenClipboard()
    try:
        fmt = win32clipboard.RegisterClipboardFormat("CanIncludeInClipboardHistory")
        assert win32clipboard.IsClipboardFormatAvailable(fmt)
        assert win32clipboard.GetClipboardData(fmt)[:4] == b"\x00\x00\x00\x00"
    finally:
        win32clipboard.CloseClipboard()
    _win_clipboard_clear_if("clip-secret-value-1")


def test_data_folder_acl(tmp_path):
    from pa_gateway.app import secure_data_folder
    from pa_gateway.posture import data_acl_ok
    d = tmp_path / "secured"
    d.mkdir()
    secure_data_folder(d)
    r = data_acl_ok(str(d))
    assert r["ok"], r


def test_posture_runs(env_nocore):
    env_nocore.setup(model=False)
    checks = env_nocore.ui.call("posture.run", {})
    ids = {c["id"] for c in checks}
    assert {"tpm", "bitlocker", "defender", "firewall", "data_acl", "sandbox", "log_chain"} <= ids


def test_tpm_probe_does_not_crash():
    from pa_gateway.vault.protector import tpm_status
    assert "usable" in tpm_status()


def test_defender_detects_eicar_if_present(tmp_path):
    from pa_gateway.files.checks import EICAR, find_defender, scan_file
    if not find_defender():
        pytest.skip("Defender not present")
    p = tmp_path / "e.txt"
    try:
        p.write_bytes(EICAR)
    except OSError:
        return  # real-time protection removed it immediately: also a pass
    r = scan_file(p, b"clean-prefix")
    assert r["clean"] is False


def test_firewall_rule_script_syntax():
    root = __import__("pathlib").Path(__file__).resolve().parents[2]
    r = subprocess.run(["powershell", "-NoProfile", "-Command",
                        f"$t = $null; $e = $null; $null = [System.Management.Automation.Language.Parser]::ParseFile('{root / 'installer' / 'windows' / 'firewall-rules.ps1'}', [ref]$t, [ref]$e); if ($e) {{ exit 1 }}"],
                       capture_output=True)
    assert r.returncode == 0, r.stderr


def test_selftest_over_named_pipes_with_core_process(tmp_path):
    """Real pa-gateway process + pa-core process connected over named pipes."""
    env = dict(os.environ, PA_DEV_MODE="1", PA_TEST_FAST_KDF="1", PA_CORE_INPROCESS="0",
               PYTHONPATH=str(__import__("pathlib").Path(__file__).resolve().parents[2] / "app"))
    data = tmp_path / "gwdata"
    gw = subprocess.Popen([sys.executable, "-m", "pa_gateway", "--data-dir", str(data)], env=env, stdout=subprocess.PIPE)
    try:
        rv = data / "run" / "gateway.json"
        for _ in range(100):
            if rv.exists():
                break
            time.sleep(0.2)
        import json
        info = json.loads(rv.read_text())
        from pa_common.pipeclient import PipeClient
        ui = PipeClient(info["pipe"], "ui", info["ui_token"], "ui", expected_server_pid=info["pid"])
        ui.call("setup.create", {"username": "tester", "password": PASSWORD, "pin": "480713"})
        ui.call("llm.add", {"model": {"provider": "dev_mock", "name": "mock"}})
        cid = ui.call("chat.create", {})["id"]
        ui.call("chat.send", {"chat_id": cid, "text": "hello over pipes"})
        reply = None
        for _ in range(150):
            msgs = [m for m in ui.call("chat.get", {"chat_id": cid})["messages"] if m["role"] == "assistant"]
            if msgs:
                reply = msgs[0]["content"]
                break
            time.sleep(0.2)
        assert reply and "hello over pipes" in reply
    finally:
        gw.terminate()
        gw.wait(20)
