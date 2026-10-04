"""History search finds what you typed and what the assistant answered, also by word start (prefix) and in any letter case."""
import pytest


@pytest.mark.parametrize("q", ["explain", "Explain", "expl", "explai", "contents", "EXPLAIN CONTENTS", "explain cont"])
def test_search_finds_a_question_you_asked(env, q):
    env.setup()
    cid, _ = env.chat("Please explain contents of my quarterly report")
    assert env.wait(lambda: [m for m in env.ui.call("chat.get", {"chat_id": cid})["messages"] if m["role"] == "assistant"], 30)
    hits = env.ui.call("history.search", {"query": q})
    assert hits, (q, hits)
    assert any(h["kind"] == "run" for h in hits) or any("explain" in str(h["snippet"]).lower() or "expl" in str(h["snippet"]).lower() for h in hits)


def test_deleted_chats_never_appear(env):
    env.setup()
    cid, _ = env.chat("remember the zebra crossing question")
    assert env.wait(lambda: [m for m in env.ui.call("chat.get", {"chat_id": cid})["messages"] if m["role"] == "assistant"], 30)
    assert env.ui.call("history.search", {"query": "zebra"})
    env.ui.call("chat.delete", {"chat_id": cid})
    assert env.ui.call("history.search", {"query": "zebra"}) == []
