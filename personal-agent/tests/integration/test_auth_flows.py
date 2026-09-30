"""Sign-in, lock, quick unlock, reset, step-up, brute-force delays (spec 4, 34.1-34.4)."""
import pytest

from pa_common.errors import PAError
from tests.conftest import PASSWORD, PIN, Env


def test_setup_and_cold_start(tmp_path):
    e = Env(tmp_path / "d", start_core=False)
    e.setup(model=False)
    e.gw.sign_out()
    assert e.ui.call("session.status")["state"] == "SIGNED_OUT"
    with pytest.raises(PAError):
        e.ui.call("chat.list")
    e.ui.call("auth.sign_in", {"username": "tester", "password": PASSWORD})
    assert e.ui.call("session.status")["state"] == "UNLOCKED"
    e.close()


def test_profile_names_setup_signin_and_change(tmp_path):
    e = Env(tmp_path / "d", start_core=False)
    e.ui.call("setup.create", {"username": "tester", "password": PASSWORD, "pin": PIN,
                               "display_name": "Sai", "assistant_name": "Jarvis"})
    st = e.ui.call("session.status")
    assert (st["display_name"], st["assistant_name"]) == ("Sai", "Jarvis")
    e.gw.sign_out()
    st = e.ui.call("session.status")  # shown on the sign-in screen before unlock
    assert st["state"] == "SIGNED_OUT" and (st["display_name"], st["assistant_name"]) == ("Sai", "Jarvis")
    with pytest.raises(PAError):
        e.ui.call("account.set_profile", {"display_name": "X", "assistant_name": "Y"})
    e.ui.call("auth.sign_in", {"username": "tester", "password": PASSWORD})
    e.ui.call("account.set_profile", {"display_name": "Sai M", "assistant_name": "Friday"})
    for bad in ("", "a" * 41, "<script>", "line\nbreak"):
        with pytest.raises(PAError):
            e.ui.call("account.set_profile", {"display_name": bad, "assistant_name": "Friday"})
    e.close()
    e2 = Env(tmp_path / "d", start_core=False)  # header MAC still valid after the change
    e2.ui.call("auth.sign_in", {"username": "tester", "password": PASSWORD})
    st = e2.ui.call("session.status")
    assert (st["display_name"], st["assistant_name"]) == ("Sai M", "Friday")
    assert e2.ui.call("account.signin_history") is not None
    e2.close()


def test_profile_defaults_and_prompt():
    from pa_core.prompts import system_prompt
    ctx = {"task": {"hwm": "PUBLIC", "trigger": "USER"}, "tools": [], "events": [], "now": "2026-01-01T00:00:00Z",
           "profile": {"display_name": "Sai", "assistant_name": "Jarvis"}}
    p = system_prompt(ctx)
    assert "You are Jarvis" in p and "their name is Sai" in p
    ctx.pop("profile")
    assert "You are Personal Agent" in system_prompt(ctx)


def test_profile_defaults_when_not_given(env_nocore):
    env_nocore.setup(model=False)
    st = env_nocore.ui.call("session.status")
    assert (st["display_name"], st["assistant_name"]) == ("tester", "Personal Agent")


def test_weak_password_and_trivial_pin_rejected(env_nocore):
    for pw, pin in (("password1234", "480713"), ("tester-Correct-Horse-9!", "480713"), (PASSWORD, "123456"), (PASSWORD, "111111")):
        with pytest.raises(PAError):
            env_nocore.ui.call("setup.create", {"username": "tester", "password": pw, "pin": pin})


def test_wrong_password_delay(env_nocore):
    env_nocore.setup(model=False)
    env_nocore.gw.sign_out()
    for _ in range(3):
        with pytest.raises(PAError):
            env_nocore.ui.call("auth.sign_in", {"username": "tester", "password": "wrong-password-123"})
    with pytest.raises(PAError) as e:
        env_nocore.ui.call("auth.sign_in", {"username": "tester", "password": PASSWORD})
    assert e.value.code == "rate_limited"


def test_quick_unlock_and_pin_lockout(env_nocore):
    env_nocore.setup(model=False)
    ui = env_nocore.ui
    ui.call("auth.lock")
    assert ui.call("session.status")["state"] == "UI_LOCKED"
    ui.call("auth.quick_unlock", {"pin": PIN})
    assert ui.call("session.status")["state"] == "UNLOCKED"
    ui.call("auth.lock")
    for _ in range(5):
        with pytest.raises(PAError):
            ui.call("auth.quick_unlock", {"pin": "000001"})
    with pytest.raises(PAError) as e:
        ui.call("auth.quick_unlock", {"pin": PIN})
    assert e.value.code == "pin_unavailable"
    ui.call("auth.sign_in", {"username": "tester", "password": PASSWORD})
    assert ui.call("session.status")["state"] == "UNLOCKED"


def test_pause_missions_when_locked_wipes_keys(env_nocore):
    env_nocore.setup(model=False)
    env_nocore.ui.call("settings.apply", {"changes": {"security.pause_missions_when_locked": True}})
    env_nocore.ui.call("auth.lock")
    assert env_nocore.gw.keys is None and env_nocore.ui.call("session.status")["state"] == "SIGNED_OUT"


def test_forgot_password_rotates_recovery_key(env_nocore):
    r = env_nocore.setup(model=False)
    env_nocore.gw.sign_out()
    out = env_nocore.ui.call("auth.forgot_password", {"pin": PIN, "recovery_key": r["recovery_key"], "new_password": "Brand-New-Password-42!"})
    assert out["recovery_key"] != r["recovery_key"]
    env_nocore.gw.sign_out()
    with pytest.raises(PAError):
        env_nocore.ui.call("auth.forgot_password", {"pin": PIN, "recovery_key": r["recovery_key"], "new_password": "Another-One-Pass-43!"})
    env_nocore.ui.call("auth.sign_in", {"username": "tester", "password": "Brand-New-Password-42!"})


def test_recovery_attempt_lockout(env_nocore):
    env_nocore.setup(model=False)
    env_nocore.gw.sign_out()
    from pa_gateway.vault import recovery
    for _ in range(5):
        with pytest.raises(PAError):
            env_nocore.ui.call("auth.forgot_password", {"pin": PIN, "recovery_key": recovery.generate(), "new_password": "Brand-New-Password-42!"})
    with pytest.raises(PAError) as e:
        env_nocore.ui.call("auth.forgot_password", {"pin": PIN, "recovery_key": recovery.generate(), "new_password": "Brand-New-Password-42!"})
    assert e.value.code == "rate_limited"


def test_step_up_required_for_secret_reveal(env_nocore):
    env_nocore.setup(model=False)
    ui = env_nocore.ui
    sid = ui.call("secrets.create", {"item": {"title": "bank", "type": "password", "value": "S3cr3t-canary-VALUE"}})["id"]
    with pytest.raises(PAError) as e:
        ui.call("secrets.reveal", {"id": sid})
    assert e.value.code == "step_up_required"
    ui.call("auth.step_up", {"category": "secrets", "method": "pin", "secret": PIN})
    assert ui.call("secrets.reveal", {"id": sid})["value"] == "S3cr3t-canary-VALUE"


def test_copied_folder_cannot_be_opened_without_password(tmp_path):
    e = Env(tmp_path / "d", start_core=False)
    e.setup(model=False)
    e.close()
    import shutil
    shutil.copytree(tmp_path / "d", tmp_path / "copy")
    from pa_gateway.db.database import Database, DatabaseError
    with pytest.raises(DatabaseError):
        Database(tmp_path / "copy" / "db" / "agent.db", b"\x00" * 32)
    e2 = Env(tmp_path / "copy", start_core=False)
    with pytest.raises(PAError):
        e2.ui.call("auth.sign_in", {"username": "tester", "password": "guess-guess-guess1"})
    e2.ui.call("auth.sign_in", {"username": "tester", "password": PASSWORD})
    e2.close()


def test_delete_everything(env_nocore):
    env_nocore.setup(model=False)
    with pytest.raises(PAError):
        env_nocore.ui.call("privacy.delete_everything", {"password": PASSWORD, "confirmation": "yes"})
    env_nocore.ui.call("privacy.delete_everything", {"password": PASSWORD, "confirmation": "DELETE EVERYTHING"})
    assert env_nocore.ui.call("session.status")["state"] == "SETUP_REQUIRED"
    assert not env_nocore.paths.vault_header.exists()
