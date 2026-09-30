"""Unified security log: HMAC chain, tamper detection, fail-closed, redaction (spec 25, 31 'Audit')."""
import json

import pytest

from pa_common.errors import AuditFailure
from pa_gateway.audit.writer import AuditWriter

KEY = b"k" * 32


def mk(tmp_path):
    w = AuditWriter(tmp_path / "logs", tmp_path / "spool.jsonl")
    w.set_key(KEY)
    return w


def test_chain_verifies(tmp_path):
    w = mk(tmp_path)
    for i in range(20):
        w.write("test.event", "test", n=i)
    assert w.verify(KEY)["ok"]


@pytest.mark.parametrize("attack", ["edit", "delete", "reorder"])
def test_tampering_detected(tmp_path, attack):
    w = mk(tmp_path)
    for i in range(10):
        w.write("test.event", "test", n=i)
    lines = w.active_path.read_text().splitlines()
    if attack == "edit":
        ev = json.loads(lines[4])
        ev["n"] = 999
        lines[4] = json.dumps(ev)
    elif attack == "delete":
        del lines[4]
    else:
        lines[3], lines[4] = lines[4], lines[3]
    w.active_path.write_text("\n".join(lines) + "\n")
    assert not w.verify(KEY)["ok"]


def test_chain_cannot_be_recomputed_without_key(tmp_path):
    w = mk(tmp_path)
    w.write("a", "t")
    assert not w.verify(b"x" * 32)["ok"]


def test_fail_closed(tmp_path):
    w = mk(tmp_path)
    w.fail_next_for_test = True
    with pytest.raises(AuditFailure):
        w.write("tool.execute", "execution")


def test_redaction(tmp_path):
    w = mk(tmp_path)
    w.set_secret_scanner(lambda s: "hunter2hunter2" in s)
    w.write("x", "t", password="p", token="t", body="email body", note="contains hunter2hunter2 value", ok="fine")
    ev = json.loads(w.active_path.read_text().splitlines()[-1])
    assert ev["password"] == ev["token"] == ev["body"] == "[REDACTED]"
    assert ev["note"] == "[REDACTED:SECRET]" and ev["ok"] == "fine"


def test_pre_unlock_events_anchored(tmp_path):
    w = AuditWriter(tmp_path / "logs", tmp_path / "spool.jsonl")
    w.write("auth.signin_failed", "authentication")
    w.set_key(KEY)
    w.write("auth.signin", "authentication")
    assert w.verify(KEY)["ok"]


def test_rotation_seals_and_verifies(tmp_path):
    w = mk(tmp_path)
    for i in range(5):
        w.write("e", "t", n=i)
    w.rotate_now()
    for i in range(5):
        w.write("e", "t", n=i)
    assert len(w.segments()) == 1
    r = w.verify(KEY)
    assert r["ok"] and r["files"] == 2
    assert json.loads(w.segments()[0].read_text().splitlines()[-1])["event_type"] == "log.seal"


def test_restart_continues_chain(tmp_path):
    w = mk(tmp_path)
    w.write("a", "t")
    w2 = AuditWriter(tmp_path / "logs", tmp_path / "spool.jsonl")
    w2.set_key(KEY)
    w2.write("b", "t")
    assert w2.verify(KEY)["ok"]
