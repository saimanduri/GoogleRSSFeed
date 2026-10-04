"""Web access is asked per chat and per app session: the first web tool call in a chat needs the user's OK, which covers that chat until
sign-out/restart; another chat asks again; the setting can be switched off; routines (no chat) are unaffected."""
import threading

from pa_gateway.auth.session import SIGNED_OUT


def _task(env, text="search please"):
    env.ui.call("connectors.set", {"connector": "web", "enabled": True, "use_chat": True, "use_missions": True})
    cid = env.ui.call("chat.create", {})["id"]
    env.ui.call("chat.send", {"chat_id": cid, "text": text})
    core = env.core()
    job = core.call("work.next", {"wait": 2})
    return core, job["task_id"], cid


def _invoke_async(core, tid, query):
    out = {}
    th = threading.Thread(target=lambda: out.update(r=core.call("tools.invoke", {"task_id": tid, "tool": "web.search", "args": {"query": query}})))
    th.start()
    return th, out


def _approve_first(env):
    a = env.wait(lambda: env.ui.call("approvals.list", {}))[0]
    env.ui.call("approvals.decide", {"approval_id": a["id"], "approve": True, "payload_hash": a["payload_hash"]})
    return a


def test_first_web_use_in_a_chat_needs_approval_then_the_chat_is_covered(env_nocore):
    env_nocore.setup()
    core, tid, cid = _task(env_nocore)
    th, out = _invoke_async(core, tid, "local llm news")
    a = env_nocore.wait(lambda: env_nocore.ui.call("approvals.list", {}))[0]
    assert a["tool"] == "web.search" and "THIS chat" in a["reason"] and "local llm news" in a["reason"]
    env_nocore.ui.call("approvals.decide", {"approval_id": a["id"], "approve": True, "payload_hash": a["payload_hash"]})
    th.join(15)
    assert out["r"]["status"] == "error" and "provider" in out["r"]["reason"]          # it ran (no search provider is set up in tests)
    assert env_nocore.gw.web_grants[cid] == env_nocore.gw.session.s.nonce
    res = core.call("tools.invoke", {"task_id": tid, "tool": "web.search", "args": {"query": "second search"}})
    assert res["status"] == "error" and env_nocore.ui.call("approvals.list", {}) == []     # no second question in the same chat


def test_denying_blocks_the_search_and_asks_again_next_time(env_nocore):
    env_nocore.setup()
    core, tid, cid = _task(env_nocore)
    th, out = _invoke_async(core, tid, "x")
    a = env_nocore.wait(lambda: env_nocore.ui.call("approvals.list", {}))[0]
    env_nocore.ui.call("approvals.decide", {"approval_id": a["id"], "approve": False, "payload_hash": a["payload_hash"]})
    th.join(15)
    assert out["r"]["status"] == "denied" and cid not in env_nocore.gw.web_grants
    th2, _ = _invoke_async(core, tid, "again")
    assert env_nocore.wait(lambda: env_nocore.ui.call("approvals.list", {}))
    a2 = env_nocore.ui.call("approvals.list", {})[0]
    env_nocore.ui.call("approvals.decide", {"approval_id": a2["id"], "approve": False, "payload_hash": a2["payload_hash"]})
    th2.join(15)


def test_another_chat_and_a_new_session_ask_again(env_nocore):
    env_nocore.setup()
    core, tid, cid = _task(env_nocore)
    th, _ = _invoke_async(core, tid, "one")
    _approve_first(env_nocore)
    th.join(15)
    env_nocore.gw.session.set_state(SIGNED_OUT)               # new app session
    env_nocore.gw.session.signed_in()
    th2, out2 = _invoke_async(core, tid, "two")
    a = env_nocore.wait(lambda: env_nocore.ui.call("approvals.list", {}))
    assert a, "after a new session the same chat is asked again"
    env_nocore.ui.call("approvals.decide", {"approval_id": a[0]["id"], "approve": True, "payload_hash": a[0]["payload_hash"]})
    th2.join(15)
    env_nocore.ui.call("chat.delete", {"chat_id": cid})
    assert cid not in env_nocore.gw.web_grants


def test_switching_the_question_off_restores_the_old_behaviour(env_nocore, monkeypatch):
    import pa_gateway.settings as settings_mod
    monkeypatch.setattr(settings_mod, "LOOSEN_DELAY_SECONDS", 0)
    env_nocore.setup()
    ch = {"web.ask_per_chat": False}
    tok = env_nocore.gw.settings.begin_loosen(ch)["token"]
    env_nocore.gw.settings.apply(ch, password_ok=True, loosen_token=tok, stepup_ok=True)
    core, tid, _ = _task(env_nocore)
    res = core.call("tools.invoke", {"task_id": tid, "tool": "web.search", "args": {"query": "no question"}})
    assert res["status"] == "error" and env_nocore.ui.call("approvals.list", {}) == []
