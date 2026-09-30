"""Vault / key hierarchy (spec 4.3-4.6, 31 'Authentication and vault')."""
import json

import pytest

from pa_gateway.vault import recovery
from pa_gateway.vault.protector import PinRejected, SoftwareProtector
from pa_gateway.vault.vault import HeaderTampered, Vault, WrongPassword, WrongRecovery

PW, PIN = "Correct-Horse-Battery-9!x", "480713"


@pytest.fixture
def vault(tmp_path):
    v = Vault(tmp_path / "vault.header")
    keys, rk = v.create("tester", PW, PIN, SoftwareProtector())
    return v, keys, rk


def test_unlock_with_password(vault):
    v, keys, _ = vault
    v2 = Vault(v.header.path)
    v2.load()
    k2 = v2.unlock_with_password(PW)
    assert k2.key("K_db") == keys.key("K_db")


def test_wrong_password(vault):
    v, _, _ = vault
    with pytest.raises(WrongPassword):
        v.unlock_with_password("nope-nope-nope-1")


def test_subkeys_distinct(vault):
    _, keys, _ = vault
    vals = {keys.key(n) for n in ("K_db", "K_files", "K_log", "K_secret", "K_ipc", "K_hdr")}
    assert len(vals) == 6


def test_reset_needs_pin_and_recovery_key(vault):
    v, keys, rk = vault
    k = v.unlock_with_pin_and_recovery(PIN, rk)
    assert k.key("K_db") == keys.key("K_db")
    with pytest.raises(PinRejected):
        v.unlock_with_pin_and_recovery("999999", rk)
    with pytest.raises(WrongRecovery):
        v.unlock_with_pin_and_recovery(PIN, recovery.generate())


def test_recovery_rotates_and_old_key_stops_working(vault):
    v, keys, rk = vault
    new_rk = v.rotate_recovery_key(keys, PIN)
    assert new_rk != rk
    with pytest.raises(WrongRecovery):
        v.unlock_with_pin_and_recovery(PIN, rk)
    assert v.unlock_with_pin_and_recovery(PIN, new_rk)


def test_change_password(vault):
    v, keys, _ = vault
    v.change_password(keys, "Another-Strong-Pass-77?")
    with pytest.raises(WrongPassword):
        v.unlock_with_password(PW)
    assert v.unlock_with_password("Another-Strong-Pass-77?")


def test_header_tamper_detected(vault):
    v, _, _ = vault
    data = json.loads(v.header.path.read_text())
    data["pw"]["params"]["t"] = 99
    v.header.path.write_text(json.dumps(data))
    v2 = Vault(v.header.path)
    v2.load()
    with pytest.raises((WrongPassword, HeaderTampered)):
        v2.unlock_with_password(PW)


def test_header_mac_tamper_on_non_crypto_field(vault):
    v, _, _ = vault
    data = json.loads(v.header.path.read_text())
    data["username"] = "attacker"
    v.header.path.write_text(json.dumps(data))
    v2 = Vault(v.header.path)
    v2.load()
    with pytest.raises(HeaderTampered):
        v2.unlock_with_password(PW)


def test_recovery_key_format():
    rk = recovery.generate()
    assert len(rk.split("-")) == 8 and all(len(g) == 5 for g in rk.split("-"))
    assert recovery.normalize(rk.lower().replace("0", "o")) == rk
    with pytest.raises(ValueError):
        recovery.normalize("ABC")


def test_set_pin_requires_current_recovery_key(vault):
    v, keys, rk = vault
    with pytest.raises(WrongRecovery):
        v.set_pin(keys, "730291", recovery.generate(), SoftwareProtector())
    v.set_pin(keys, "730291", rk, SoftwareProtector())
    assert v.verify_pin("730291") and not v.verify_pin(PIN)


def test_no_plaintext_vmk_in_header(vault):
    v, keys, _ = vault
    assert keys.vmk.get().hex() not in v.header.path.read_text()
