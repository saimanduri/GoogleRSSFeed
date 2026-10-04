"""Packaged-build regression: the password/PIN lists must be shipped by the PyInstaller spec (v0.1.1 bundles
lacked them, so setup.check_password crashed and the wizard's Continue button never enabled)."""
from pathlib import Path

from pa_gateway.auth.passwords import common_passwords, common_pins, validate_password

ROOT = Path(__file__).resolve().parents[2]


def test_data_files_exist_and_load():
    assert "password" in common_passwords()
    assert "123456" in common_pins()
    assert validate_password("short", "bob")


def test_spec_ships_the_data_files_explicitly():
    spec = (ROOT / "scripts" / "pyinstaller" / "personal-agent.spec").read_text(encoding="utf-8")
    assert "common_passwords.txt" in spec and "common_pins.txt" in spec
    assert 'os.path.join("pa_gateway", "data")' in spec
