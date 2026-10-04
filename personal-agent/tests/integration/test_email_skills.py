"""Email monitoring skills (Outlook routines): switching on/off, VIP names, readiness checks, and a full scripted run
(real agent loop + gateway + tool policy; only Outlook and the model are replaced)."""
import json

import pytest

from pa_common.errors import PAError
from pa_gateway.agentdata.email_skills import BY_ID, NOTHING, TEMPLATES


@pytest.fixture
def ol(env, monkeypatch):
    """Environment where 'classic Outlook' exists and the connector can be switched on."""
    import pa_gateway.connectors.outlook_local as mod
    monkeypatch.setattr(mod, "outlook_classic_installed", lambda: True)
    env.setup()
    return env


def _enable_connector(env):
    env.ui.call("connectors.set", {"connector": "outlook_local", "enabled": True, "use_missions": True, "use_chat": True})


def test_templates_are_read_only_and_valid():
    ids = [t["id"] for t in TEMPLATES]
    assert len(ids) == len(set(ids)) >= 10
    from pa_gateway.agentdata import schedule as sch
    from pa_gateway.policy.tools_registry import BY_NAME, EXTERNAL_WRITE, INTERNAL_WRITE
    for t in TEMPLATES:
        sch.validate({"type": "cron", "cron": t["schedule"]}, "Asia/Calcutta")
        for tool in t["tools"]:
            assert tool in BY_NAME, tool
            assert BY_NAME[tool].side_effect not in (EXTERNAL_WRITE, INTERNAL_WRITE), f"{t['id']} may only read: {tool}"
        assert NOTHING in t["objective"]


def test_list_enable_disable_roundtrip(ol):
    ui = ol.ui
    r = ui.call("emailskills.list")
    assert len(r["skills"]) == len(TEMPLATES) and not any(s["enabled"] for s in r["skills"])
    with pytest.raises(PAError) as e:                       # connector still off
        ui.call("emailskills.set", {"skill": "approvals", "enabled": True})
    assert e.value.code == "needs_setup" and "Local Outlook" in str(e.value)
    _enable_connector(ol)
    out = ui.call("emailskills.set", {"skill": "approvals", "enabled": True})
    m = ol.gw.missions.get(out["mission_id"])
    assert m["status"] == "ACTIVE" and m["template_id"] == "approvals" and m["name"] == "Email: Emails waiting for my approval"
    assert json.loads(m["allowed_tools_json"]) == BY_ID["approvals"]["tools"]
    s = next(x for x in ui.call("emailskills.list")["skills"] if x["id"] == "approvals")
    assert s["enabled"] and s["schedule_text"] == "Every 2 hours between 08:00 and 18:59 on Mon, Tue, Wed, Thu, Fri, Sat" or s["schedule_text"]
    ui.call("emailskills.set", {"skill": "approvals", "enabled": False})
    assert ol.gw.missions.get(m["id"])["status"] == "PAUSED"
    again = ui.call("emailskills.set", {"skill": "approvals", "enabled": True})
    assert again["mission_id"] == m["id"]                     # re-uses the same routine, no duplicates
    assert len([x for x in ui.call("missions.list") if x["template_id"] == "approvals"]) == 1


def test_vip_skill_needs_names_and_renders_them(ol):
    _enable_connector(ol)
    with pytest.raises(PAError) as e:
        ol.ui.call("emailskills.set", {"skill": "vip_alert", "enabled": True})
    assert e.value.code == "needs_setup" and "VIP" in str(e.value)
    ol.ui.call("settings.apply", {"changes": {"emailskills.vips": ["Anita Rao", "Governor"]}})
    out = ol.ui.call("emailskills.set", {"skill": "vip_alert", "enabled": True})
    text = ol.gw.emailskills.render(ol.gw.missions.get(out["mission_id"]))
    assert '"Anita Rao", "Governor"' in text and "{VIPS}" not in text


def test_scripted_run_shows_result_and_unseen_counter(ol, monkeypatch):
    import pa_gateway.connectors.outlook_local as mod
    import pa_gateway.llm.mock as mock
    _enable_connector(ol)
    calls = []

    def fake_call(self, op, **params):
        calls.append((op, params))
        return {"text": "1 listed\n- id=X\n  subject: Budget for approval\n  flags: APPROVAL", "count": 1, "max_sensitivity": 0}

    monkeypatch.setattr(mod.OutlookLocalConnector, "_call", fake_call)
    state = {"n": 0}

    def model(messages):
        state["n"] += 1
        if state["n"] == 1:
            return json.dumps({"thought": "scan", "action": "tool", "tool": "outlook_local.digest",
                               "args": {"only_flag": "approval", "unanswered_only": True, "since_minutes": 7200, "max_results": 20}})
        return json.dumps({"thought": "done", "action": "final", "answer": "1 approval waiting: Budget (sender X) - needs sign-off."})

    monkeypatch.setattr(mock, "mock_complete", model)
    ol.ui.call("emailskills.set", {"skill": "approvals", "enabled": True})
    ol.ui.call("emailskills.run", {"skill": "approvals"})
    ok = ol.wait(lambda: next(x for x in ol.ui.call("emailskills.list")["skills"] if x["id"] == "approvals")["last_state"] == "COMPLETED", timeout=40)
    assert ok
    assert calls and calls[0][0] == "digest" and calls[0][1]["only_flag"] == "approval"
    s = next(x for x in ol.ui.call("emailskills.list")["skills"] if x["id"] == "approvals")
    assert "Budget" in s["last_result"] and not s["last_nothing"]
    assert ol.ui.call("session.status")["outlook_unseen"] == 1
    ol.ui.call("emailskills.list", {"mark_seen": True})
    assert ol.ui.call("session.status")["outlook_unseen"] == 0
    # template runs do not pile up files in My Files
    assert not [f for f in ol.ui.call("files.list", {"query": ""})["files"] if f["folder"] == "/Mission outputs"]


def test_nothing_new_is_silent(ol, monkeypatch):
    import pa_gateway.connectors.outlook_local as mod
    import pa_gateway.llm.mock as mock
    _enable_connector(ol)
    # never read the real mailbox from a test (the prefetch calls Outlook before the model starts)
    monkeypatch.setattr(mod.OutlookLocalConnector, "_call", lambda self, op, **p: {"text": "0 listed", "count": 0, "max_sensitivity": 0})
    monkeypatch.setattr(mock, "mock_complete", lambda m: json.dumps({"thought": "t", "action": "final", "answer": NOTHING}))
    ol.ui.call("emailskills.set", {"skill": "inbox_hourly", "enabled": True})
    ol.ui.call("emailskills.run", {"skill": "inbox_hourly"})
    assert ol.wait(lambda: next(x for x in ol.ui.call("emailskills.list")["skills"] if x["id"] == "inbox_hourly")["last_state"] == "COMPLETED", timeout=40)
    s = next(x for x in ol.ui.call("emailskills.list")["skills"] if x["id"] == "inbox_hourly")
    assert s["last_nothing"] and s["last_result"] == ""
    assert ol.ui.call("session.status")["outlook_unseen"] == 0


def test_data_is_gathered_by_the_application_before_the_model_starts(ol, monkeypatch):
    """The model that never calls a tool still sees real data: the first tool calls are made by the app itself."""
    import pa_gateway.connectors.outlook_local as mod
    import pa_gateway.llm.mock as mock
    _enable_connector(ol)
    calls, seen = [], []
    monkeypatch.setattr(mod.OutlookLocalConnector, "_call", lambda self, op, **params: (calls.append((op, params)) or
                        {"text": f"{op} DATA-123", "count": 1, "max_sensitivity": 0}))

    def model(messages):
        seen.append(" ".join(m["content"] for m in messages))
        return json.dumps({"thought": "t", "action": "final", "answer": "Summary of DATA-123."})

    monkeypatch.setattr(mock, "mock_complete", model)
    ol.ui.call("emailskills.set", {"skill": "morning_brief", "enabled": True})
    ol.ui.call("emailskills.run", {"skill": "morning_brief"})
    assert ol.wait(lambda: next(x for x in ol.ui.call("emailskills.list")["skills"] if x["id"] == "morning_brief")["last_state"] == "COMPLETED", timeout=40)
    assert [c[0] for c in calls] == ["mail_stats", "digest", "calendar_read"]          # three calls, made by code, in order
    assert calls[2][1]["start"].endswith("T00:00:00") and calls[2][1]["end"].endswith("T23:59:59") and calls[2][1]["start"][:10] == calls[2][1]["end"][:10]
    assert "mail_stats DATA-123" in seen[0] and "calendar_read DATA-123" in seen[0]     # the model's very first prompt already has the data
