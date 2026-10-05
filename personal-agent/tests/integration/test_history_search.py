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


def test_run_finishing_after_chat_delete_is_not_searchable(env_nocore):
    """Regression (CI on Windows): the answer is stored before the run finishes; deleting the chat in that gap must not
    let the run's summary reach the search index afterwards."""
    env_nocore.setup(model=False)
    gw = env_nocore.gw
    cid = env_nocore.ui.call("chat.create", {})["id"]
    run_id = gw.runs.start("chat", "the walrus question", chat_id=cid)
    env_nocore.ui.call("chat.delete", {"chat_id": cid})
    gw.runs.finish(run_id, "COMPLETED", summary="walrus answer text")
    assert env_nocore.ui.call("history.search", {"query": "walrus"}) == []


def test_stale_index_rows_of_deleted_chats_are_dropped(env_nocore):
    env_nocore.setup(model=False)
    gw = env_nocore.gw
    cid = env_nocore.ui.call("chat.create", {})["id"]
    run_id = gw.runs.start("chat", "the narwhal question", chat_id=cid)
    gw.runs.finish(run_id, "COMPLETED", summary="narwhal answer text")
    assert env_nocore.ui.call("history.search", {"query": "narwhal"})
    gw.db.update("chats", "id", cid, {"deleted": 1})                     # deleted without cleaning the index (old data)
    assert env_nocore.ui.call("history.search", {"query": "narwhal"}) == []
    assert not gw.db.all("SELECT 1 FROM history_fts WHERE ref_id=?", (run_id,))   # the stale row was cleaned up
