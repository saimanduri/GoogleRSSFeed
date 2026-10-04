"""R2: Windows toasts are titled with the NAME of the reminder / routine / chat (never with mail or document content)."""
import time

import pytest

from pa_common.timeutil import now_iso, to_iso, utcnow
from datetime import timedelta


@pytest.fixture(autouse=True)
def fast_loosen(monkeypatch):
    import pa_gateway.settings as settings_mod
    monkeypatch.setattr(settings_mod, "LOOSEN_DELAY_SECONDS", 0)


def _set(gw, changes):
    """Change settings through the real path (loosening ones need the password and the read delay)."""
    info = gw.settings.classify(changes)
    token = gw.settings.begin_loosen(changes)["token"] if info["loosening"] else None
    gw.settings.apply(changes, password_ok=True, loosen_token=token, stepup_ok=True)


def _capture(gw):
    events = []
    gw.add_listener(lambda topic, data: events.append(data) if topic == "notify" else None)
    return events


def _due_reminder(gw, text):
    rid = "rem_" + str(int(time.time() * 1000))
    gw.db.insert("reminders", {"id": rid, "text": text, "due_at": to_iso(utcnow() - timedelta(seconds=5)), "status": "SCHEDULED", "timezone": "Asia/Calcutta", "source": "user", "created_at": now_iso()})
    return rid


def test_reminder_toast_is_named_after_the_reminder(env_nocore):
    env_nocore.setup(model=False)
    gw = env_nocore.gw
    ev = _capture(gw)
    _due_reminder(gw, "Call the dentist about the crown")
    gw.reminders.tick()
    n = ev[-1]
    assert n["toast"] and n["toast_title"] == "Call the dentist about the crown" and n["toast_body"] == "Reminder"


def test_names_can_be_switched_off(env_nocore):
    env_nocore.setup(model=False)
    gw = env_nocore.gw
    _set(gw, {"notifications.show_names": False})
    ev = _capture(gw)
    _due_reminder(gw, "Secret plan")
    gw.reminders.tick()
    assert ev[-1]["toast_title"] == "Reminder" and "Secret plan" not in str(ev[-1]["toast_title"]) + str(ev[-1]["toast_body"])


def test_name_is_cleaned_and_content_stays_out(env_nocore):
    env_nocore.setup(model=False)
    gw = env_nocore.gw
    ev = _capture(gw)
    gw.notify("mission", "Hourly inbox check finished", "From: boss@corp.example - salary table attached", 2, "outlook", "m1",
              subject="‮Inbox\x00 check   " + "x" * 100, status="New results")
    n = ev[-1]
    assert "‮" not in n["toast_title"] and "\x00" not in n["toast_title"] and len(n["toast_title"]) <= 60 and n["toast_title"].startswith("Inbox check")
    assert n["toast_body"] == "New results" and "salary" not in str(n) .replace(n["body"] or "@@", "")


def test_summary_level_adds_text_but_never_confidential(env_nocore):
    env_nocore.setup(model=False)
    gw = env_nocore.gw
    _set(gw, {"notifications.content_level": "summary"})
    ev = _capture(gw)
    gw.notify("mission", "Done", "public text", 0, "missions", "m1", subject="Routine A", status="Finished")
    gw.notify("mission", "Done", "confidential text", 2, "missions", "m1", subject="Routine A", status="Finished")
    assert ev[-2]["toast_body"] == "Finished - public text"
    assert ev[-1]["toast_body"] == "Finished" and "confidential" not in str(ev[-1]["toast_body"])


def test_chat_toast_is_not_recorded_in_the_notification_list(env_nocore):
    env_nocore.setup(model=False)
    gw = env_nocore.gw
    ev = _capture(gw)
    before = len(env_nocore.ui.call("notifications.list", {}) if hasattr(env_nocore.ui, "call") else [])
    gw.notify("chat", "Answer ready", None, 0, "chat", "chat_1", subject="Budget chat", status="Answer ready", record=False)
    n = ev[-1]
    assert n["kind"] == "chat" and n["toast_title"] == "Budget chat" and n["toast_body"] == "Answer ready" and n["id"] is None
    assert len(env_nocore.ui.call("notifications.list", {})) == before


def test_quiet_hours_and_master_switch_still_win(env_nocore):
    env_nocore.setup(model=False)
    gw = env_nocore.gw
    _set(gw, {"notifications.toasts": False})
    ev = _capture(gw)
    gw.notify("mission", "Done", None, 0, "missions", "m1", subject="Routine A", status="Finished")
    assert ev[-1]["toast"] is False


def test_chat_answer_sends_a_named_toast(env):
    env.setup()
    ev = _capture(env.gw)
    cid, _ = env.chat("hello agent")
    env.ui.call("chat.update", {"chat_id": cid, "title": "Quarterly plan"})
    assert env.wait(lambda: any(e["kind"] == "chat" for e in ev), 30)
    assert any(e["kind"] == "chat" and e["toast_body"] == "Answer ready" for e in ev)
