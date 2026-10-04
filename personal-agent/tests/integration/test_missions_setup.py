"""Routines that need web search must say so up front instead of failing silently; schedules and output formats."""
import pytest

from pa_common.errors import PAError


def _mission(ui, tools, fmt="digest", schedule="*/30 9-17 * * 1-5"):
    return ui.call("missions.create", {"mission": {"name": "AI news", "objective": "Study AI news and give me the gist", "schedule": schedule,
                                                   "allowed_tools": tools, "output_format": fmt}})["id"]


def test_web_search_mission_warns_and_does_not_run_without_provider(env_nocore):
    env_nocore.setup()
    ui = env_nocore.ui
    ui.call("connectors.set", {"connector": "web", "enabled": True})
    mid = _mission(ui, ["web.search", "notify.user"])
    row = next(m for m in ui.call("missions.list") if m["id"] == mid)
    assert any("search provider" in w for w in row["warnings"])
    assert row["schedule_text"] == "Every 30 minutes between 09:00 and 17:59 on weekdays"
    ui.call("missions.activate", {"mission_id": mid})
    with pytest.raises(PAError) as e:
        ui.call("missions.run_now", {"mission_id": mid})
    assert e.value.code == "needs_setup"
    assert env_nocore.gw.missions.run_now(mid, "SCHEDULE") == ""          # scheduled run: no task, a Home notice instead
    ev = env_nocore.gw.db.all("SELECT title, detail FROM home_events WHERE kind='mission_blocked'")
    assert ev and "search provider" in ev[0]["detail"]


def test_output_format_is_validated_and_reaches_the_task(env_nocore):
    env_nocore.setup()
    ui = env_nocore.ui
    mid = _mission(ui, ["notify.user"], fmt="table", schedule="every 2 hours")
    assert env_nocore.gw.missions.get(mid)["output_format"] == "table"
    bad = _mission(ui, ["notify.user"], fmt="something odd")
    assert env_nocore.gw.missions.get(bad)["output_format"] == "markdown"
    tid = env_nocore.gw.missions.run_now(mid, "USER")
    assert "Markdown table" in env_nocore.gw.db.one("SELECT objective FROM tasks WHERE id=?", (tid,))["objective"]


def test_parse_schedule_accepts_cron_and_reports_text(env_nocore):
    env_nocore.setup(model=False)
    r = env_nocore.ui.call("missions.parse_schedule", {"text": "0 9-17/2 * * *"})
    assert r["text"] == "Every 2 hours between 09:00 and 17:59" and r["next_run"]
    assert env_nocore.ui.call("missions.parse_schedule", {"text": "99 99 * * *"})["schedule"] is None


def test_agent_proposed_routine_is_visible_and_activatable(env):
    env.setup()
    cid, _ = env.chat("Create a routine: every morning plan my day")
    env.wait(lambda: env.ui.call("session.status")["proposals_pending"] >= 1, timeout=20)
    st = env.ui.call("session.status")
    assert st["proposals_pending"] == 1
    m = [x for x in env.ui.call("missions.list") if x["proposed_by"] == "agent"][0]
    assert m["status"] == "DRAFT" and m["schedule_text"]
    assert any(e["kind"] == "mission_proposed" for e in env.ui.call("home.summary")["events"])
    env.ui.call("missions.activate", {"mission_id": m["id"]})
    assert env.ui.call("session.status")["proposals_pending"] == 0
