from datetime import date

import pytest

from pa_workers.outlook.mailscan import find_deadline, flag_text, scan

TODAY = date(2026, 10, 1)  # a Thursday


@pytest.mark.parametrize("subject,body,approval", [
    ("Proposal for approval", "Please find attached.", True),
    ("Leave policy", "The note is submitted for kind approval of the Competent Authority.", True),
    ("Re: budget", "Kindly approve the revised budget at the earliest.", True),
    ("Vendor", "Your approval is requested before we proceed.", True),
    ("FYI", "The proposal was approved yesterday. For your information.", False),
    ("Newsletter", "Approval ratings rose this quarter.", False),
    ("Meeting", "Lunch at 1 pm?", False),
])
def test_approval_detection(subject, body, approval):
    assert scan(subject, body, today=TODAY)["approval"] is approval


@pytest.mark.parametrize("text,expected", [
    ("Please reply by 2026-10-03.", date(2026, 10, 3)),
    ("Deadline: 05/10/2026", date(2026, 10, 5)),
    ("submit before 7 Oct", date(2026, 10, 7)),
    ("due on or before October 9, 2026", date(2026, 10, 9)),
    ("Need this by tomorrow", date(2026, 10, 2)),
    ("please send by EOD", date(2026, 10, 1)),
    ("finish by Monday", date(2026, 10, 5)),
    ("by next Thursday", date(2026, 10, 8)),
    ("We met on 2026-09-01 and agreed", None),          # no deadline cue
    ("deadline was 2026-08-01", None),                   # in the past
    ("nothing here", None),
])
def test_deadline_detection(text, expected):
    assert find_deadline(text, TODAY) == expected


def test_urgent_question_and_flag_text():
    f = scan("URGENT: need your view", "Can you confirm by tomorrow?", importance=1, today=TODAY)
    assert f["urgent"] and f["question"] and f["deadline"] == "2026-10-02"
    assert flag_text(f) == "DEADLINE:2026-10-02,URGENT,QUESTION"
    assert scan("x", "y", importance=2, today=TODAY)["urgent"]
