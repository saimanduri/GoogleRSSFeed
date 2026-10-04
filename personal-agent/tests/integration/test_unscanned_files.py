"""No antivirus can scan (Defender off, another AV active): files stay in quarantine; the user can release one file (re-authentication) or
switch the acceptance on; malware detections are never releasable; AMSI is used only when it proves itself."""
import base64
import subprocess
import types

import pytest

from pa_common.errors import PAError
from pa_gateway.files import amsi, checks

UNAVAILABLE = {"clean": False, "engine": "defender", "threat": None, "unavailable": True, "error": "Microsoft Defender is turned off, so the file cannot be scanned"}


def _upload(env, name=b"report.txt", body=b"quarterly numbers"):
    up = env.ui.call("files.upload", {"name": name.decode(), "data_b64": base64.b64encode(body).decode()})
    assert env.wait(lambda: env.ui.call("files.preview", {"file_id": up["id"]})["status"] in ("READY", "REJECTED"), 30)
    return env.ui.call("files.preview", {"file_id": up["id"]})


@pytest.fixture
def no_av(monkeypatch):
    monkeypatch.setattr(checks, "scan_file", lambda path, data: dict(UNAVAILABLE))


def test_unscannable_files_are_held(env_nocore, no_av):
    env_nocore.setup(model=False)
    row = _upload(env_nocore)
    assert row["status"] == "REJECTED" and "not scanned" in row["status_reason"] and row["scan_json"]["antivirus"]["unavailable"] is True


def test_release_needs_reauthentication_and_marks_the_file(env_nocore, no_av):
    env_nocore.setup(model=False)
    row = _upload(env_nocore)
    with pytest.raises(PAError) as e:
        env_nocore.ui.call("files.release_unscanned", {"file_id": row["id"]})
    assert e.value.code == "step_up_required"
    env_nocore.gw.session.grant_stepup("security_settings", 5, "password")
    out = env_nocore.ui.call("files.release_unscanned", {"file_id": row["id"]})
    assert out["status"] == "READY" and "NOT antivirus-scanned" in out["status_reason"] and out["scan_json"]["antivirus"]["unscanned"] is True
    assert "quarterly numbers" in env_nocore.ui.call("files.preview", {"file_id": row["id"]})["text"]


def test_malware_detections_can_never_be_released(env_nocore, monkeypatch):
    env_nocore.setup(model=False)
    monkeypatch.setattr(checks, "scan_file", lambda path, data: {"clean": False, "engine": "defender", "threat": "Trojan:Test"})
    row = _upload(env_nocore)
    assert row["status"] == "REJECTED" and "malware detected" in row["status_reason"]
    env_nocore.gw.session.grant_stepup("security_settings", 5, "password")
    with pytest.raises(PAError) as e:
        env_nocore.ui.call("files.release_unscanned", {"file_id": row["id"]})
    assert e.value.code == "invalid_state"


def test_ready_files_cannot_be_released_again(env_nocore):
    env_nocore.setup(model=False)
    row = _upload(env_nocore)
    assert row["status"] == "READY"
    env_nocore.gw.session.grant_stepup("security_settings", 5, "password")
    with pytest.raises(PAError):
        env_nocore.ui.call("files.release_unscanned", {"file_id": row["id"]})


def test_setting_accepts_unscanned_files_with_a_warning(env_nocore, no_av, monkeypatch):
    import pa_gateway.settings as settings_mod
    monkeypatch.setattr(settings_mod, "LOOSEN_DELAY_SECONDS", 0)
    env_nocore.setup(model=False)
    ch = {"files.accept_unscanned": True}
    tok = env_nocore.gw.settings.begin_loosen(ch)["token"]
    env_nocore.gw.settings.apply(ch, password_ok=True, loosen_token=tok, stepup_ok=True)
    row = _upload(env_nocore)
    assert row["status"] == "READY" and "NOT antivirus-scanned" in row["status_reason"]
    # the built-in checks still apply: programs are refused regardless
    exe = _upload(env_nocore, b"tool.exe", b"MZ" + b"\0" * 100)
    assert exe["status"] == "REJECTED" and "executable" in exe["status_reason"]


def test_amsi_is_used_when_defender_is_off_and_amsi_proves_itself(monkeypatch, tmp_path):
    f = tmp_path / "x.bin"
    f.write_bytes(b"hello")
    monkeypatch.setattr(checks, "find_defender", lambda: "defender.exe")
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: types.SimpleNamespace(returncode=2, stdout=b"Scan is disabled"))
    monkeypatch.setattr(amsi, "available", lambda force=False: True)
    monkeypatch.setattr(amsi, "scan", lambda data, name="": {"clean": True, "engine": "amsi"})
    assert checks.scan_file(f, b"hello") == {"clean": True, "engine": "amsi"}
    monkeypatch.setattr(amsi, "available", lambda force=False: False)
    assert checks.scan_file(f, b"hello")["unavailable"] is True


def test_amsi_module_is_safe_to_call_anywhere():
    assert isinstance(amsi.available(True), bool)
    assert amsi.scan(b"harmless text")["engine"] == "amsi"
    assert amsi.scan(b"")["engine"] == "amsi"
