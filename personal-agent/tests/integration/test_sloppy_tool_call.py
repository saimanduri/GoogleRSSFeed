"""A model that writes {"action": "<tool name>", "tool": "<tool name>", ...} must still get its tool executed
(real agent loop + gateway; only the model is replaced) and never leave raw JSON in the chat."""
import json


def test_sloppy_tool_call_is_executed_and_answer_is_readable(env, monkeypatch):
    env.setup()
    import pa_gateway.llm.mock as svc
    calls = {"n": 0}

    def fake(messages):
        calls["n"] += 1
        if calls["n"] == 1:
            return json.dumps({"thought": "look up the time", "action": "time.now", "tool": "time.now", "args": {}})
        return json.dumps({"thought": "done", "action": "final", "answer": "It is now known."})

    monkeypatch.setattr(svc, "mock_complete", fake)
    cid, _ = env.chat("what time is it?")
    msgs = env.wait(lambda: [m for m in env.ui.call("chat.get", {"chat_id": cid})["messages"] if m["role"] == "assistant"], timeout=30)
    assert msgs, "no reply"
    text = msgs[-1]["content"]
    assert "It is now known." in text and '"action"' not in text
    assert calls["n"] == 2          # tool call executed, then the final answer
