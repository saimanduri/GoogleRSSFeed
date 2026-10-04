"""Spec 14.2 layer 1 / 35.8: release builds refuse to start pa-core / workers when a firewall block rule is missing."""
import pytest

from pa_common.errors import PAError
from pa_gateway import posture


@pytest.fixture(autouse=True)
def _clear_cache():
    posture._FW_CACHE.update(at=0.0, missing=[])
    yield
    posture._FW_CACHE.update(at=0.0, missing=[])


def test_not_enforced_in_source_runs():
    assert posture.firewall_enforced() is False
    posture.require_firewall_rules()  # no error, no lookup


def test_missing_rule_blocks_start(monkeypatch):
    monkeypatch.setattr(posture, "firewall_enforced", lambda: True)
    monkeypatch.setattr(posture, "firewall_rules_present",
                        lambda: {p: p != "pa-core.exe" for p in posture.BLOCKED_PROGRAMS})
    with pytest.raises(PAError) as e:
        posture.require_firewall_rules()
    assert e.value.code == "firewall_missing" and "pa-core.exe" in str(e.value)


def test_all_rules_present_allows_start(monkeypatch):
    monkeypatch.setattr(posture, "firewall_enforced", lambda: True)
    monkeypatch.setattr(posture, "firewall_rules_present", lambda: {p: True for p in posture.BLOCKED_PROGRAMS})
    posture.require_firewall_rules()


def test_failed_lookup_fails_closed(monkeypatch):
    monkeypatch.setattr(posture, "firewall_enforced", lambda: True)
    monkeypatch.setattr(posture, "firewall_rules_present", lambda: {})
    with pytest.raises(PAError):
        posture.require_firewall_rules()


def test_worker_launch_is_gated(monkeypatch, tmp_path):
    from pa_gateway import workers
    monkeypatch.setattr(posture, "firewall_enforced", lambda: True)
    monkeypatch.setattr(posture, "firewall_rules_present", lambda: {})
    with pytest.raises(PAError):
        workers.popen_limited(["cmd", "/c", "exit"], cwd=tmp_path, env={}, limits=None)
