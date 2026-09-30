"""Shared fixtures. Tests run in developer mode with fast KDF parameters and the mock model.
Windows-only tests are marked @pytest.mark.windows; hardware/app-dependent ones @pytest.mark.laptop."""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pytest

os.environ["PA_DEV_MODE"] = "1"
os.environ["PA_TEST_FAST_KDF"] = "1"
os.environ["PA_CORE_INPROCESS"] = "1"
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

PASSWORD = "Correct-Horse-Battery-9!x"
PIN = "480713"


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "windows" in item.keywords and sys.platform != "win32":
            item.add_marker(pytest.mark.skip(reason="Windows-only test"))
        if "laptop" in item.keywords and os.environ.get("PA_LAPTOP_TESTS") != "1":
            item.add_marker(pytest.mark.skip(reason="laptop test: set PA_LAPTOP_TESTS=1 (see TESTS.md)"))


class Env:
    def __init__(self, tmp: Path, start_core: bool = True):
        from pa_common.paths import DataPaths
        from pa_gateway.app import Gateway
        from pa_gateway.ipc.dispatch import Dispatcher, import_all_methods
        from pa_gateway.ipc.local import LocalClient
        self.paths = DataPaths(tmp)
        self.gw = Gateway(self.paths, start_core=start_core)
        import_all_methods()
        self.d = Dispatcher(self.gw)
        self.ui = LocalClient(self.d, "ui")
        self.LocalClient = LocalClient

    def setup(self, model: bool = True) -> dict:
        r = self.ui.call("setup.create", {"username": "tester", "password": PASSWORD, "pin": PIN})
        groups = r["recovery_key"].split("-")
        self.ui.call("setup.confirm_recovery", {"answers": {str(i): groups[i] for i in r["confirm_groups"]}})
        if model:
            self.ui.call("llm.add", {"model": {"provider": "dev_mock", "name": "mock"}})
        self.rk = r["recovery_key"]
        return r

    def core(self, worker: str = "w0"):
        return self.LocalClient(self.d, "core", worker)

    def wait(self, fn, timeout: float = 15.0, interval: float = 0.05):
        end = time.time() + timeout
        last = None
        while time.time() < end:
            last = fn()
            if last:
                return last
            time.sleep(interval)
        return last

    def chat(self, text: str, chat_id: str | None = None) -> tuple[str, dict]:
        cid = chat_id or self.ui.call("chat.create", {})["id"]
        sent = self.ui.call("chat.send", {"chat_id": cid, "text": text})
        return cid, sent

    def reply(self, chat_id: str, n: int = 1, timeout: float = 15.0) -> str | None:
        def got():
            msgs = [m for m in self.ui.call("chat.get", {"chat_id": chat_id})["messages"] if m["role"] == "assistant"]
            return msgs[n - 1]["content"] if len(msgs) >= n else None
        return self.wait(got, timeout)

    def close(self):
        self.gw.shutdown()


@pytest.fixture
def env(tmp_path):
    e = Env(tmp_path / "data")
    yield e
    e.close()


@pytest.fixture
def env_nocore(tmp_path):
    e = Env(tmp_path / "data", start_core=False)
    yield e
    e.close()
