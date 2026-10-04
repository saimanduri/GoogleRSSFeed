"""Home widgets: catalogue, on/off + order, only enabled widgets are computed, data from the database, mail widgets from (faked) Outlook counts."""
import base64


from pa_gateway.agentdata import home_widgets as hw


def test_catalogue_and_defaults(env_nocore):
    env_nocore.setup(model=False)
    r = env_nocore.ui.call("home.widgets", {})
    ids = {c["id"] for c in r["catalog"]}
    assert ids == set(hw.CATALOG) and len(ids) >= 14 and set(r["enabled"]) <= ids and r["enabled"] == hw.DEFAULT
    assert {"mail_unread", "mail_to_me", "mail_approvals", "reminders", "attention", "models_health"} <= ids
    assert set(r["data"]) == set(r["enabled"])
    assert all(c["title"] and c["about"] and c["group"] for c in r["catalog"])


def test_switching_widgets_on_and_off_and_ordering(env_nocore):
    env_nocore.setup(model=False)
    out = env_nocore.ui.call("home.widgets_set", {"enabled": ["storage", "reminders", "nonsense", "reminders", "memory_recent"]})
    assert out["enabled"] == ["storage", "reminders", "memory_recent"] and set(out["data"]) == {"storage", "reminders", "memory_recent"}
    assert env_nocore.ui.call("home.widgets", {})["enabled"] == ["storage", "reminders", "memory_recent"]
    assert env_nocore.ui.call("home.widgets_set", {"enabled": []})["enabled"] == []
    assert env_nocore.ui.call("home.widgets", {})["data"] == {}                      # nothing enabled -> nothing computed
    assert env_nocore.ui.call("session.status")["ui"]["home.widgets"] == []


def test_every_widget_computes_without_outlook(env_nocore):
    env_nocore.setup()
    out = env_nocore.ui.call("home.widgets_set", {"enabled": list(hw.CATALOG)})["data"]
    assert set(out) == set(hw.CATALOG)
    for k in hw.CATALOG:
        assert out[k]["state"] in ("ok", "off", "loading"), (k, out[k])
    for k in ("mail_unread", "mail_to_me", "mail_approvals", "mail_deadlines", "mail_awaiting_reply"):
        assert out[k]["state"] == "off" and "Local Outlook" in out[k]["note"]
    assert out["storage"]["quota"] > 0 and set(out["models_health"]["kinds"]) == {"chat", "voice", "vision", "embedding"}


def test_widgets_show_real_data(env_nocore):
    env_nocore.setup(model=False)
    gw = env_nocore.gw
    up = env_nocore.ui.call("files.upload", {"name": "invoice.txt", "data_b64": base64.b64encode(b"TAX INVOICE bill to ACME total due 1,234.50 for services").decode()})
    assert env_nocore.wait(lambda: (env_nocore.ui.call("files.preview", {"file_id": up["id"]})["meta"] or {}).get("status") == "READY", 30)
    mid = gw.memory_learner.store("Prefers short bullet-point reports", "preference", "normal", "learned:chat", None, [], force=True)
    env_nocore.ui.call("home.widgets_set", {"enabled": ["recent_files", "memory_recent", "attention", "approvals", "storage"]})
    d = env_nocore.ui.call("home.widgets", {})["data"]
    assert d["recent_files"]["items"][0]["name"] == "invoice.txt" and d["recent_files"]["items"][0]["doc_type"] == "Invoice"
    assert any(m["id"] == mid for m in d["memory_recent"]["items"]) and d["storage"]["value"] == 1 and d["approvals"]["value"] == 0
    # a file held in quarantine shows up under "needs attention"
    gw.db.execute("UPDATE files SET status='REJECTED' WHERE id=?", (up["id"],))
    assert any("quarantine" in i["text"] for i in env_nocore.ui.call("home.widgets", {})["data"]["attention"]["items"])


def test_mail_widgets_use_cached_outlook_counts(env_nocore, monkeypatch):
    import pa_gateway.connectors.outlook_local as mod
    monkeypatch.setattr(mod, "outlook_classic_installed", lambda: True)
    env_nocore.setup(model=False)
    calls = []

    def fake(self, op, **params):
        calls.append(op)
        if op == "mail_stats":
            return {"text": "Period: x\nTotal received: 120 | unread: 45 | you are in To: 30 | in CC: 12 | other (lists/BCC): 78\nHigh importance: 3", "count": 120}
        return {"text": "", "count": {"digest": 4, "awaiting_reply": 2}[op]}
    monkeypatch.setattr(mod.OutlookLocalConnector, "_call", fake)
    env_nocore.ui.call("connectors.set", {"connector": "outlook_local", "enabled": True, "use_missions": True, "use_chat": True})
    env_nocore.ui.call("home.widgets_set", {"enabled": ["mail_unread", "mail_to_me", "mail_approvals", "mail_deadlines", "mail_awaiting_reply"]})
    d = env_nocore.wait(lambda: (lambda x: x if x["mail_unread"]["state"] == "ok" else None)(env_nocore.ui.call("home.widgets", {})["data"]), 15)
    assert d["mail_unread"]["value"] == 45 and "120 received" in d["mail_unread"]["sub"]
    assert d["mail_to_me"]["value"] == 30 and "12 in CC" in d["mail_to_me"]["sub"]
    assert d["mail_approvals"]["value"] == 4 and d["mail_approvals"]["tone"] == "warn" and d["mail_deadlines"]["value"] == 4 and d["mail_awaiting_reply"]["value"] == 2
    n = len(calls)
    env_nocore.ui.call("home.widgets", {})
    assert len(calls) == n                                                              # cached: opening Home again does not re-read Outlook
    env_nocore.ui.call("home.widgets", {"refresh": True})
    assert env_nocore.wait(lambda: len(calls) > n, 10)


def test_outlook_errors_are_shown_gently(env_nocore, monkeypatch):
    import pa_gateway.connectors.outlook_local as mod
    monkeypatch.setattr(mod, "outlook_classic_installed", lambda: True)
    env_nocore.setup(model=False)

    def boom(self, op, **params):
        raise RuntimeError("Outlook did not respond")
    monkeypatch.setattr(mod.OutlookLocalConnector, "_call", boom)
    env_nocore.ui.call("connectors.set", {"connector": "outlook_local", "enabled": True, "use_missions": True, "use_chat": True})
    env_nocore.ui.call("home.widgets_set", {"enabled": ["mail_unread"]})
    env_nocore.ui.call("home.widgets", {})
    d = env_nocore.wait(lambda: (lambda x: x if x["mail_unread"]["state"] == "error" else None)(env_nocore.ui.call("home.widgets", {})["data"]), 10)
    assert "did not respond" in d["mail_unread"]["note"]
