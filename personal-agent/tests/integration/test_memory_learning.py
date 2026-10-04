"""Automatic memory: learned from the user's own words, routines; never secrets; forgetting is permanent; everything is visible and deletable."""
import json

import pytest

import pa_gateway.llm.mock as mock
from pa_gateway.agentdata.memory_learn import SYSTEM, MemoryLearner, looks_sensitive

EXTRACT = {"out": "[]", "seen": []}


@pytest.fixture(autouse=True)
def scripted(monkeypatch):
    EXTRACT.update(out="[]", seen=[])
    real = mock.mock_complete

    def fake(messages):
        if messages and messages[0].get("content") == SYSTEM:
            EXTRACT["seen"].append(messages[-1]["content"])
            return EXTRACT["out"]
        return real(messages)
    monkeypatch.setattr(mock, "mock_complete", fake)


def _chat(env, text):
    cid, _ = env.chat(text)
    assert env.wait(lambda: [m for m in env.ui.call("chat.get", {"chat_id": cid})["messages"] if m["role"] == "assistant"], 30)
    return cid


def _learned(env):
    return [m for m in env.ui.call("memory.list", {"status": ""}) if m["source"].startswith("learned:")]


def test_facts_from_what_you_type_are_learned_automatically(env):
    env.setup()
    EXTRACT["out"] = json.dumps([{"type": "preference", "content": "Prefers short bullet-point reports", "importance": "normal"},
                                 {"type": "semantic", "content": "Works in the finance team", "importance": "normal"}])
    _chat(env, "I prefer short bullet-point reports and I work in the finance team, please remember how I like things")
    assert env.wait(lambda: len(_learned(env)) == 2, 20)
    mems = {m["content"]: m for m in _learned(env)}
    assert mems["Prefers short bullet-point reports"]["status"] == "ACTIVE" and mems["Works in the finance team"]["source"] == "learned:chat"
    assert "bullet-point" in " ".join(m["content"] for m in env.gw.memory.preferences_for_prompt())          # the assistant is told
    # only the user's own words were shown to the extraction model
    assert "finance team" in EXTRACT["seen"][0] and "What the user typed" in EXTRACT["seen"][0]


def test_secrets_and_ids_are_never_stored(env):
    env.setup()
    EXTRACT["out"] = json.dumps([{"content": "PAN number is ABCDE1234F"}, {"content": "Phone number is 98765 43210"}, {"content": "Likes dark mode in every app"}])
    for bad in ("Email is sai@example.com", "Bank account 123456789012 at HDFC", "The password is hunter2"):         # the model may return more than 3: only 3 are read
        assert looks_sensitive(env.gw, bad)
    _chat(env, "my details are all in my message, please remember them and my love of dark mode")
    assert env.wait(lambda: _learned(env), 20)
    assert [m["content"] for m in _learned(env)] == ["Likes dark mode in every app"]


def test_forgotten_memories_are_never_learned_again(env):
    env.setup()
    gw = env.gw
    mid = gw.memory_learner.store("Prefers tea over coffee", "preference", "normal", "learned:chat", None, [], force=True)
    assert mid
    env.ui.call("memory.action", {"id": mid, "action": "delete"})
    assert gw.memory_learner.store("Prefers tea over coffee", "preference", "normal", "learned:chat", None, []) is None
    assert gw.memory_learner.store("prefers TEA over coffee!", "preference", "normal", "learned:chat", None, []) is None      # same fact, other wording of case/punctuation
    other = gw.memory_learner.store("Prefers a quiet office", "preference", "normal", "learned:chat", None, [])
    env.ui.call("memory.action", {"id": other, "action": "reject"})
    assert gw.memory_learner.store("Prefers a quiet office", "preference", "normal", "learned:chat", None, []) is None


def test_delete_all_forgets_everything_for_good(env):
    env.setup()
    gw = env.gw
    gw.memory_learner.store("Uses Excel every day", "semantic", "normal", "learned:chat", None, [], force=True)
    gw.memory.delete_all()
    assert _learned(env) == [] and gw.memory_learner.store("Uses Excel every day", "semantic", "normal", "learned:chat", None, []) is None


def test_switching_it_off_stops_learning(env, monkeypatch):
    env.setup()
    env.ui.call("settings.apply", {"changes": {"memory.auto_learn": False}})
    EXTRACT["out"] = json.dumps([{"content": "Prefers long detailed answers"}])
    _chat(env, "I prefer long detailed answers about everything, remember that please")
    import time
    time.sleep(3)
    assert _learned(env) == [] and EXTRACT["seen"] == []
    assert env.gw.memory.preferences_for_prompt() == []


def test_duplicates_and_daily_cap(env):
    env.setup()
    L = env.gw.memory_learner
    assert L.store("Works on the Pune project", "project", "normal", "learned:chat", None, [])
    assert L.store("Works on the Pune project", "project", "normal", "learned:chat", None, []) is None        # duplicate
    env.ui.call("settings.apply", {"changes": {"memory.auto_learn_per_day": 1}})
    assert L.store("Reviews invoices on Fridays", "procedural", "normal", "learned:chat", None, []) is None   # cap reached


def test_routines_and_email_skills_are_remembered_and_forgotten_with_them(env):
    env.setup()
    mid = env.ui.call("missions.create", {"mission": {"name": "Morning digest", "objective": "Summarise the news", "schedule": "30 7 * * 1-5",
                                                      "allowed_tools": ["notify.user"], "output_format": "digest"}})["id"]
    env.ui.call("missions.activate", {"mission_id": mid})
    mem = [m for m in _learned(env) if m["source"] == "learned:routine"]
    assert len(mem) == 1 and "Morning digest" in mem[0]["content"] and mem[0]["source_ref"] == mid
    env.ui.call("missions.set_status", {"mission_id": mid, "status": "CANCELLED"})
    assert [m for m in _learned(env) if m["source"] == "learned:routine"] == []


def test_sensitive_text_detector():
    class G:
        class dlp:
            @staticmethod
            def check_outbound(t):
                return {"blocked": False}
    for bad in ("PAN ABCDE1234F", "call 98765 43210", "mail me at a@b.co", "api key = abc", "card 4111 1111 1111 1111"):
        assert looks_sensitive(G, bad), bad
    for ok in ("Prefers short reports", "Has a routine 'Morning digest' that runs at 07:30"):
        assert not looks_sensitive(G, ok), ok
    assert MemoryLearner.parse('noise [{"content":"x y z a b c d e"}] tail') == [{"content": "x y z a b c d e"}]
    assert MemoryLearner.parse("no json") == []
