"""GPU activity for the live meter in the left menu (Windows performance counters, read-only, local).

Sampling runs only while the window asks for it (the UI polls `system.usage`), in one background thread of the gateway.
No process names or window titles leave the counters: only per-adapter utilisation and dedicated-memory totals are kept.
"""
from __future__ import annotations

import re
import sys
import threading
import time
from collections import deque
from typing import Any

_ENGINE = re.compile(r"luid_(0x[0-9a-fA-F]+_0x[0-9a-fA-F]+)_phys_\d+_eng_\d+_engtype_(\w*)")
_ADAPTER = re.compile(r"luid_(0x[0-9a-fA-F]+_0x[0-9a-fA-F]+)_phys_\d+")
HISTORY = 90          # samples kept (about 2-3 minutes)
IDLE_AFTER = 30.0     # stop sampling when nobody asked for this long


def aggregate(engines: dict[str, float], memory: dict[str, float]) -> dict[str, Any]:
    """Counters -> {util: busiest adapter's utilisation %, vram_mb: dedicated memory in use on the busiest-memory adapter}.

    engines: counter instance name -> utilisation %. Windows shows an adapter's load as the busiest engine TYPE (3D, Compute,
    Copy, Video...), where each type is the sum over all processes."""
    per: dict[tuple[str, str], float] = {}
    for name, val in engines.items():
        m = _ENGINE.search(name)
        if m:
            key = (m.group(1), (m.group(2) or "other").lower())
            per[key] = per.get(key, 0.0) + float(val)
    adapters: dict[str, float] = {}
    for (luid, _typ), v in per.items():
        adapters[luid] = max(adapters.get(luid, 0.0), v)
    util = min(100.0, max(adapters.values(), default=0.0))
    vram = 0.0
    for name, val in memory.items():
        if _ADAPTER.search(name):
            vram = max(vram, float(val))
    return {"util": round(util, 1), "vram_mb": round(vram / (1024 * 1024)), "adapters": len(adapters)}


class _Pdh:
    """Thin wrapper over win32pdh; counters are re-expanded now and then because processes come and go."""

    def __init__(self) -> None:
        import win32pdh  # type: ignore[import-not-found]
        self.p = win32pdh
        self.q = win32pdh.OpenQuery()
        self.eng: list[tuple[str, Any]] = []
        self.mem: list[tuple[str, Any]] = []
        self.expanded = 0.0

    def _expand(self) -> None:
        p = self.p
        for h in [h for _, h in self.eng + self.mem]:
            try:
                p.RemoveCounter(h)
            except Exception:  # noqa: BLE001
                pass
        self.eng, self.mem = [], []
        for path_pat, store in ((r"\GPU Engine(*)\Utilization Percentage", self.eng), (r"\GPU Adapter Memory(*)\Dedicated Usage", self.mem)):
            try:
                paths = p.ExpandCounterPath(path_pat)
            except Exception:  # noqa: BLE001
                paths = []
            for path in paths[:1500]:
                try:
                    store.append((path, p.AddCounter(self.q, path)))
                except Exception:  # noqa: BLE001
                    pass
        self.expanded = time.time()

    def read(self) -> tuple[dict[str, float], dict[str, float]]:
        p = self.p
        if time.time() - self.expanded > 15 or not self.eng:
            self._expand()
        p.CollectQueryData(self.q)
        out: list[dict[str, float]] = []
        for store in (self.eng, self.mem):
            vals: dict[str, float] = {}
            for path, h in store:
                try:
                    vals[path] = float(p.GetFormattedCounterValue(h, p.PDH_FMT_DOUBLE)[1])
                except Exception:  # noqa: BLE001
                    pass
            out.append(vals)
        return out[0], out[1]


class GpuMonitor:
    def __init__(self) -> None:
        self.history: deque[dict[str, Any]] = deque(maxlen=HISTORY)
        self._wanted = 0.0
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self.available: bool | None = None

    def usage(self) -> dict[str, Any]:
        """Latest samples; starts the sampler on first use."""
        if sys.platform != "win32":
            return {"available": False, "history": []}
        self._wanted = time.time()
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(target=self._run, daemon=True, name="gpu-monitor")
                self._thread.start()
        h = list(self.history)
        last = h[-1] if h else {"util": 0.0, "vram_mb": 0}
        return {"available": self.available is not False, "util": last["util"], "vram_mb": last["vram_mb"],
                "history": [x["util"] for x in h]}

    def _run(self) -> None:
        try:
            pdh = _Pdh()
            pdh.read()          # the first collection only primes the rate counters
            time.sleep(1.0)
        except Exception:  # noqa: BLE001
            self.available = False
            return
        while time.time() - self._wanted < IDLE_AFTER:
            try:
                eng, mem = pdh.read()
                s = aggregate(eng, mem)
                self.available = bool(eng or mem)
                self.history.append({"util": s["util"], "vram_mb": s["vram_mb"], "t": time.time()})
            except Exception:  # noqa: BLE001
                self.available = False
                return
            time.sleep(1.5)
