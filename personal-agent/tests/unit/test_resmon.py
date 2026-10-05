"""Resource meters (Settings > Diagnostics & About): thresholds, which processes count as 'this app', and a real reading."""
import os

import pytest

from pa_gateway import resmon


@pytest.mark.parametrize("pct, lvl", [(None, "unknown"), (0, "ok"), (74.9, "ok"), (75, "warn"), (89.9, "warn"), (90, "critical"), (100, "critical")])
def test_levels(pct, lvl):
    assert resmon.level(pct) == lvl


def test_meter_uses_the_worse_of_pc_and_app_and_clamps():
    m = resmon.meter("Memory", 40.0, "x", app_pct=93.0)
    assert m["level"] == "critical" and m["pct"] == 40.0 and m["app_pct"] == 93.0
    assert resmon.meter("CPU", 140.0, "x")["pct"] == 100.0


def test_app_processes_are_the_gateway_tree_plus_app_exes():
    procs = [(1, 0, "System"), (10, 1, "explorer.exe"), (100, 10, "pa-gateway.exe"), (101, 100, "pa-core.exe"),
             (102, 101, "python.exe"), (200, 10, "pa-ui.exe"), (300, 10, "chrome.exe"), (400, 10, "PA-PARSER.EXE")]
    assert resmon.app_pids(procs, 100) == {100, 101, 102, 200, 400}


class FakeProbe(resmon._Probe):
    def __init__(self):
        self.t = 0

    def processes(self):
        return [(1, 0, "init"), (os.getpid(), 1, "pa-gateway.exe"), (50, os.getpid(), "pa-core.exe"), (60, 1, "other")]

    def proc_stats(self, pid):
        self.t += 1
        return {os.getpid(): (self.t * 0.01, 300 * 2**20), 50: (self.t * 0.01, 200 * 2**20)}.get(pid)

    def system_cpu(self):
        self.t += 1
        return self.t * 9.5, self.t * 10.0          # 95 % busy

    def system_memory(self):
        return 15 * 2**30, 16 * 2**30                # 93.75 % used


def test_snapshot_flags_red_at_90_percent(tmp_path):
    (tmp_path / "db").mkdir()
    (tmp_path / "db" / "agent.db").write_bytes(b"x" * 4096)
    mon = resmon.ResourceMonitor(tmp_path, probe=FakeProbe())
    snap = mon.snapshot()
    by = {m["label"]: m for m in snap["meters"]}
    assert by["CPU"]["level"] == "critical" and by["Memory"]["level"] == "critical"
    assert "500 MB" in by["Memory"]["app_detail"]                       # gateway + its child, not the unrelated process
    assert {p["pid"] for p in snap["processes"]} == {os.getpid(), 50}
    assert by["Storage"]["pct"] is not None and snap["red_at"] == 90.0


def test_real_reading_on_this_machine(tmp_path):
    """No fake: the platform probe works here (Linux /proc in CI containers, Windows API on Windows)."""
    mon = resmon.ResourceMonitor(tmp_path)
    snap = mon.snapshot()
    by = {m["label"]: m for m in snap["meters"]}
    assert by["CPU"]["pct"] is not None and 0 <= by["CPU"]["pct"] <= 100
    assert by["Memory"]["pct"] is not None and by["Memory"]["app_pct"] is not None
    assert any(p["pid"] == os.getpid() for p in snap["processes"])
    again = mon.snapshot()                                              # second call: no priming sleep, still valid
    assert all(m["level"] in ("ok", "warn", "critical", "unknown") for m in again["meters"])


def test_rpc_returns_meters(env_nocore):
    env_nocore.setup(model=False)
    snap = env_nocore.ui.call("diagnostics.resources")
    assert {m["label"] for m in snap["meters"]} >= {"CPU", "Memory", "GPU", "Storage"}
