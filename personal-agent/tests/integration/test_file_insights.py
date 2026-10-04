"""Automatic file summaries: kind/summary/keywords/personal-data kinds, label raised, a memory about the file (never the secret values), editable,
findable by what it is, removable. A scripted model stands in for the local one."""
import base64
import json

import pytest

import pa_gateway.llm.mock as mock
from pa_gateway.files.insights import SYSTEM, detect_pii, guess_type, luhn, redact

PAN_TEXT = ("INCOME TAX DEPARTMENT\nGOVT. OF INDIA\nPermanent Account Number Card\nABCDE1234F\nName: SAI TEST\nDate of Birth: 01/01/1990\n"
            "Signature\n" + "This card is issued by the Income Tax Department. " * 3)
MODEL = {"out": "", "seen": []}


@pytest.fixture(autouse=True)
def scripted(monkeypatch):
    MODEL.update(out="", seen=[])
    real = mock.mock_complete

    def fake(messages):
        if messages and messages[0].get("content") == SYSTEM:
            MODEL["seen"].append(messages[-1]["content"])
            return MODEL["out"]
        return real(messages)
    monkeypatch.setattr(mock, "mock_complete", fake)


def _upload(env, name, text):
    up = env.ui.call("files.upload", {"name": name, "data_b64": base64.b64encode(text.encode()).decode(), "folder": "/Identity"})
    assert env.wait(lambda: (env.ui.call("files.preview", {"file_id": up["id"]})["meta"] or {}).get("status") in ("READY", "FAILED"), 40)
    return env.ui.call("files.preview", {"file_id": up["id"]})


def _mem(env, fid, wait=True):
    """The memory is written just after the summary: give the background job a moment."""
    get = lambda: [m for m in env.ui.call("memory.list", {"status": ""}) if m["source"] == "learned:file" and m["source_ref"] == fid]  # noqa: E731
    return (env.wait(get, 5) or []) if wait else get()


def test_pan_card_is_described_and_remembered_without_the_number(env_nocore):
    env_nocore.setup()
    MODEL["out"] = json.dumps({"title": "PAN card of Sai", "doc_type": "PAN card", "summary": "Income tax identity card, number ABCDE1234F, issued to Sai Test.",
                               "keywords": ["pan", "income tax", "ABCDE1234F"], "personal_data": ["PAN number", "name", "date of birth"]})
    row = _upload(env_nocore, "PAN card.txt", PAN_TEXT)
    meta = row["meta"]
    assert meta["doc_type"] == "PAN card" and "ABCDE1234F" not in json.dumps(meta) and "[hidden]" in meta["summary"] or "ABCDE1234F" not in meta["summary"]
    assert "PAN number" in meta["personal_data"] and "date of birth" in meta["personal_data"] and meta["model"]
    assert "ABCDE1234F" in row["text"]                                      # the value stays in the file itself
    assert row["sensitivity"] >= 2                                          # personal data -> CONFIDENTIAL
    mem = _mem(env_nocore, row["id"])
    assert len(mem) == 1 and "PAN card" in mem[0]["content"] and "/Identity" in mem[0]["content"] and "ABCDE1234F" not in mem[0]["content"]
    assert "values are in the file" in mem[0]["content"]


def test_without_a_model_a_basic_description_is_still_made(env_nocore):
    env_nocore.setup(model=False)
    row = _upload(env_nocore, "pan.txt", PAN_TEXT)
    meta = row["meta"]
    assert meta["doc_type"] == "PAN card" and "PAN number" in meta["personal_data"] and "no chat model" in meta["note"] and meta["model"] is None
    assert "ABCDE1234F" not in meta["summary"]


def test_user_can_edit_and_edits_survive(env_nocore):
    env_nocore.setup()
    MODEL["out"] = json.dumps({"title": "PAN", "doc_type": "PAN card", "summary": "A card.", "keywords": ["pan"], "personal_data": ["PAN number"]})
    row = _upload(env_nocore, "pan.txt", PAN_TEXT)
    out = env_nocore.ui.call("files.meta_update", {"file_id": row["id"], "title": "My PAN card", "doc_type": "PAN card", "summary": "Original card, kept in the blue folder.", "keywords": ["pan", "tax"]})
    assert out["title"] == "My PAN card" and out["edited"] is True and out["summary"].startswith("Original card")
    assert "blue folder" in _mem(env_nocore, row["id"])[0]["content"]       # the memory follows the edit
    env_nocore.gw.insights.analyse(row["id"])                               # an automatic run never overwrites an edit
    assert env_nocore.ui.call("files.preview", {"file_id": row["id"]})["meta"]["title"] == "My PAN card"
    from pa_common.errors import PAError
    for bad in ("number ABCDE1234F here", "call 98765 43210", "me@example.com"):
        with pytest.raises(PAError):
            env_nocore.ui.call("files.meta_update", {"file_id": row["id"], "title": "x", "summary": bad, "keywords": []})
    env_nocore.ui.call("files.analyse", {"file_id": row["id"]})
    assert env_nocore.wait(lambda: not env_nocore.ui.call("files.preview", {"file_id": row["id"]})["meta"]["edited"], 20)       # 'Summarise again' replaces the edit on purpose


def test_switches_auto_summary_and_auto_learn(env_nocore):
    env_nocore.setup()
    env_nocore.ui.call("settings.apply", {"changes": {"files.auto_summary": False}})
    up = env_nocore.ui.call("files.upload", {"name": "n.txt", "data_b64": base64.b64encode(PAN_TEXT.encode()).decode()})
    assert env_nocore.wait(lambda: env_nocore.ui.call("files.preview", {"file_id": up["id"]})["status"] == "READY", 30)
    import time
    time.sleep(1.5)
    assert env_nocore.ui.call("files.preview", {"file_id": up["id"]})["meta"] is None
    env_nocore.ui.call("settings.apply", {"changes": {"memory.auto_learn": False}})
    env_nocore.ui.call("files.analyse", {"file_id": up["id"]})
    assert env_nocore.wait(lambda: (env_nocore.ui.call("files.preview", {"file_id": up["id"]})["meta"] or {}).get("status") == "READY", 30)
    assert _mem(env_nocore, up["id"], wait=False) == []                      # summary yes, memory no


def test_find_returns_where_it_is_and_the_assistant_can_then_read_it(env_nocore):
    env_nocore.setup(model=False)
    row = _upload(env_nocore, "pan.txt", PAN_TEXT)
    _upload(env_nocore, "lunch menu.txt", "Monday: dal rice. Tuesday: roti sabzi. " * 5)
    hits = env_nocore.gw.insights.find("my pan card")
    assert hits and hits[0]["id"] == row["id"] and hits[0]["type"] == "PAN card" and "PAN number" in hits[0]["personal_data"]
    assert "ABCDE1234F" not in json.dumps(hits)
    cid = env_nocore.ui.call("chat.create", {})["id"]
    env_nocore.ui.call("chat.send", {"chat_id": cid, "text": "show my pan card"})
    core = env_nocore.core()
    tid = core.call("work.next", {"wait": 2})["task_id"]
    found = core.call("tools.invoke", {"task_id": tid, "tool": "files.find", "args": {"query": "PAN card"}})
    assert found["status"] == "ok" and row["id"] in json.dumps(found) and "ABCDE1234F" not in json.dumps(found)
    shown = core.call("tools.invoke", {"task_id": tid, "tool": "files.read", "args": {"file_id": row["id"]}})
    assert shown["status"] == "ok" and "ABCDE1234F" in json.dumps(shown)      # the user's own document: they asked to see it


def test_deleting_the_file_removes_summary_and_memory(env_nocore):
    env_nocore.setup(model=False)
    row = _upload(env_nocore, "pan.txt", PAN_TEXT)
    assert _mem(env_nocore, row["id"])
    env_nocore.ui.call("files.delete", {"file_id": row["id"]})
    assert _mem(env_nocore, row["id"], wait=False) == [] and env_nocore.gw.db.one("SELECT 1 FROM file_meta WHERE file_id=?", (row["id"],)) is None
    assert env_nocore.gw.insights.find("pan card") == []


def test_pii_detectors():
    assert {"PAN number", "date of birth"} <= set(detect_pii(PAN_TEXT))
    assert "Aadhaar number" in detect_pii("Aadhaar 2345 6789 0123")
    assert "card number" in detect_pii("Card 4111 1111 1111 1111 exp 12/29") and luhn("4111111111111111") and not luhn("4111111111111112")
    assert "phone number" in detect_pii("Call +91 98765 43210") and "email address" in detect_pii("write to a.b@corp.in")
    assert detect_pii("Invoice 2026-10-02 total 1,234.50 for 12 items") == []
    assert "ABCDE1234F" not in redact("pan ABCDE1234F and 4111 1111 1111 1111")
    assert guess_type("x.pdf", "TAX INVOICE bill to ACME") == "Invoice" and guess_type("report.docx", "hello") == "Word document"


def test_deleting_while_the_summary_is_being_written_leaves_nothing_behind(env_nocore, monkeypatch):
    import time

    from pa_gateway.files import insights as ins
    env_nocore.setup()
    gate = {"go": False}

    def slow(self, m, name, text, sens):
        while not gate["go"]:
            time.sleep(0.05)
        return {"title": "t", "doc_type": "PAN card", "summary": "late summary", "keywords": ["x"], "personal_data": []}
    monkeypatch.setattr(ins.FileInsights, "_ask", slow)
    up = env_nocore.ui.call("files.upload", {"name": "zebra-secret.txt", "data_b64": base64.b64encode(("zebra-unique-term " * 10).encode()).decode()})
    assert env_nocore.wait(lambda: env_nocore.ui.call("files.preview", {"file_id": up["id"]})["status"] == "READY", 30)
    env_nocore.ui.call("files.delete", {"file_id": up["id"]})
    gate["go"] = True
    time.sleep(1.5)
    assert env_nocore.gw.db.one("SELECT 1 FROM file_meta WHERE file_id=?", (up["id"],)) is None
    assert env_nocore.ui.call("history.search", {"query": "zebra-unique-term"}) == [] and _mem(env_nocore, up["id"], wait=False) == []
