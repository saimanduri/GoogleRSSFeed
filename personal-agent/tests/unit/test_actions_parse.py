import pytest

from pa_core.actions import ActionError, parse_action


def test_standard_tool_and_final():
    assert parse_action('{"action":"tool","tool":"time.now","args":{}}')["tool"] == "time.now"
    assert parse_action('{"action":"final","answer":"hi"}')["answer"] == "hi"


@pytest.mark.parametrize("text", [
    '{"thought":"x","action":"reminders.propose","tool":"reminders.propose","args":{"text":"a","due_at":"2026-10-01T16:03:43.999+05:30"}}',
    '{"action":"reminders.propose","args":{"text":"a","due_at":"2026-10-01T16:04"}}',
    '{"tool":"reminders.propose","arguments":{"text":"a","due_at":"2026-10-01T16:04"}}',
    '```json\n{"action":"tool_call","tool":"reminders.propose","args":"{\\"text\\": \\"a\\", \\"due_at\\": \\"2026-10-01T16:04\\"}"}\n```',
])
def test_sloppy_tool_calls_are_accepted(text):
    r = parse_action(text)
    assert r["action"] == "tool" and r["tool"] == "reminders.propose" and r["args"]["text"] == "a"


def test_plain_text_is_final_and_bad_args_rejected():
    assert parse_action("just text")["action"] == "final"
    with pytest.raises(ActionError):
        parse_action('{"action":"tool","tool":"x.y","args":[1]}')


def test_never_shows_raw_json_when_the_model_keeps_failing():
    from pa_core.agent import _readable
    out = _readable('{"thought": "I should remind the user", "extra": 1}')
    assert out.startswith("I should remind the user") and "did not follow" in out and "{" not in out
    assert _readable("plain words") == "plain words"


def test_placeholder_answers_are_rejected():
    with pytest.raises(ActionError):
        parse_action('{"thought": "x", "action": "final", "answer": "..."}')
    with pytest.raises(ActionError):
        parse_action('{"thought": "x", "action": "final", "answer": "   "}')
    assert parse_action('{"thought": "x", "action": "final", "answer": "NOTHING_NEW"}')["answer"] == "NOTHING_NEW"
