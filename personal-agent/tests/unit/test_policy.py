"""Deterministic policy engine and data-flow table 13.3."""
import pytest

from pa_common.sensitivity import Sensitivity as S
from pa_gateway.policy.engine import ALLOW, DENY, REQUIRE_APPROVAL, SANDBOX, PolicyContext, PolicyEngine
from pa_gateway.policy.tools_registry import BY_NAME
from pa_gateway.settings_schema import BY_KEY


def engine(**over):
    vals = {k: s.default for k, s in BY_KEY.items()}
    vals.update(over)
    return PolicyEngine(vals.get)


def ctx(tool, hwm=S.PUBLIC, trigger="USER", **kw):
    return PolicyContext(tool=BY_NAME[tool], args={}, context=kw.pop("context", "chat"), trigger=trigger, hwm=hwm,
                         allowed_tools=kw.pop("allowed_tools", None), **kw)


@pytest.mark.parametrize("hwm,expected", [(S.PUBLIC, ALLOW), (S.INTERNAL, ALLOW), (S.CONFIDENTIAL, REQUIRE_APPROVAL), (S.RESTRICTED, DENY)])
def test_web_table(hwm, expected):
    assert engine().evaluate(ctx("web.search", hwm)).decision == expected


@pytest.mark.parametrize("hwm,expected,pw", [(S.PUBLIC, REQUIRE_APPROVAL, False), (S.INTERNAL, REQUIRE_APPROVAL, False),
                                             (S.CONFIDENTIAL, REQUIRE_APPROVAL, True), (S.RESTRICTED, DENY, False)])
def test_external_write_table(hwm, expected, pw):
    d = engine().evaluate(ctx("m365.send_mail", hwm))
    assert d.decision == expected
    assert d.requires_password == pw


def test_external_event_trigger_is_read_only():
    e = engine()
    assert e.evaluate(ctx("web.fetch", trigger="EXTERNAL_EVENT")).decision == DENY
    assert e.evaluate(ctx("m365.create_draft", trigger="EXTERNAL_EVENT")).decision == DENY
    assert e.evaluate(ctx("m365.search_mail", trigger="EXTERNAL_EVENT")).decision == ALLOW


def test_mission_allowlist_and_disabled_tools():
    e = engine()
    assert e.evaluate(ctx("web.search", allowed_tools=["files.list"], context="mission")).decision == DENY
    assert e.evaluate(ctx("web.search", tool_enabled=False)).decision == DENY


def test_connector_unusable_denies():
    assert engine().evaluate(ctx("m365.search_mail", connector_usable=False, connector_reason="off")).decision == DENY


def test_python_needs_sandbox():
    e = engine()
    assert e.evaluate(ctx("python.run")).decision == SANDBOX
    assert e.evaluate(ctx("python.run", sandbox_available=False)).decision == DENY


def test_confidential_egress_can_be_tightened_to_deny():
    assert engine(**{"flow.confidential_egress": "deny"}).evaluate(ctx("web.fetch", S.CONFIDENTIAL)).decision == DENY


def test_injection_flag_forces_approval():
    assert engine().evaluate(ctx("web.fetch", injection_suspected=True)).decision == REQUIRE_APPROVAL


def test_engine_failure_denies():
    def boom(_):
        raise RuntimeError("x")
    e = PolicyEngine(boom)
    assert e.evaluate(ctx("web.search")).decision == DENY


def test_reminder_always_needs_confirmation():
    assert engine().evaluate(ctx("reminders.propose")).decision == REQUIRE_APPROVAL


def test_tool_definition_hash_is_stable():
    assert BY_NAME["web.search"].definition_hash == BY_NAME["web.search"].definition_hash
    assert BY_NAME["web.search"].definition_hash != BY_NAME["web.fetch"].definition_hash
