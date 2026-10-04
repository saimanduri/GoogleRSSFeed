import pytest

from pa_gateway.agentdata import schedule as sch


@pytest.mark.parametrize("cron,text", [
    ("20 15 * * *", "Every day at 15:20"),
    ("30 7 * * 1-5", "At 07:30 on weekdays"),
    ("0 9 * * 0,6", "At 09:00 on weekends"),
    ("0 9 * * 1,3", "At 09:00 on Mon, Wed"),
    ("*/15 * * * *", "Every 15 minutes"),
    ("*/30 9-17 * * 1-5", "Every 30 minutes between 09:00 and 17:59 on weekdays"),
    ("0 9-17/2 * * *", "Every 2 hours between 09:00 and 17:59"),
    ("0 */3 * * *", "Every 3 hours"),
    ("0 9 1 * *", "cron 0 9 1 * *"),
])
def test_cron_is_described_in_plain_words(cron, text):
    assert sch.describe({"type": "cron", "cron": cron}) == text
    sch.validate({"type": "cron", "cron": cron}, "UTC")


def test_interval_text():
    assert sch.describe({"type": "interval", "minutes": 120}) == "Every 2 hours"
    assert sch.describe({"type": "interval", "minutes": 45}) == "Every 45 minutes"


def test_ui_schedule_picker_texts_are_understood_by_the_backend():
    """What SchedulePicker.tsx generates must parse and validate server side."""
    for text in ["every 2 hours", "every 30 minutes", "20 15 * * *", "30 7 * * 1-5", "*/15 9-17 * * 1-5", "0 9-17/2 * * 0,6", "0 */3 * * *", "0 8 * * 1,3,5"]:
        parsed = sch.parse_plain(text) or {"type": "cron", "cron": text}
        sch.validate(parsed, "Asia/Calcutta")
        assert sch.describe(parsed)
