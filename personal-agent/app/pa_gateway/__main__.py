"""pa-gateway entry point.

  pa-gateway                 run the gateway (started at Windows sign-in by the per-user scheduled task)
  pa-gateway --selftest      end-to-end smoke test in a temporary data folder (developer mode, mock model)
"""
from __future__ import annotations

import argparse
import os
import signal
import sys
import threading
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="pa-gateway")
    ap.add_argument("--selftest", action="store_true", help="run an end-to-end smoke test (developer mode)")
    ap.add_argument("--data-dir", help="data folder (developer use only)")
    args = ap.parse_args(argv)
    if args.selftest:
        from .selftest import run_selftest
        return run_selftest()

    from pa_common.devmode import dev_mode
    from pa_common.paths import DataPaths, default_data_dir

    from .app import Gateway
    from .ipc.dispatch import Dispatcher, import_all_methods

    if args.data_dir and not dev_mode():
        print("--data-dir is only allowed in developer mode", file=sys.stderr)
        return 2
    paths = DataPaths(Path(args.data_dir) if args.data_dir else default_data_dir())
    mutex = None
    if sys.platform == "win32":
        from . import winsession
        winsession.harden_process()
        mutex = winsession.single_instance()
        if mutex is None:
            print("pa-gateway is already running", file=sys.stderr)
            return 0
    elif not dev_mode():
        print("Personal Agent runs on Windows 11. Set PA_DEV_MODE=1 for developer/test use on other systems.", file=sys.stderr)
        return 2
    gw = Gateway(paths, start_core=True)
    import_all_methods()
    dispatcher = Dispatcher(gw)
    gw.pipe_server = None
    stop = threading.Event()
    if sys.platform == "win32":
        from . import winsession
        from .ipc.pipe_server import PipeServer
        server = PipeServer(dispatcher, paths.rendezvous)
        gw.pipe_server = server
        server.start()
        winsession.start(gw)
        print(f"pa-gateway listening on {server.pipe_name}", flush=True)
    else:
        print("pa-gateway (developer mode, no named pipes on this OS): use --selftest or the test suite", flush=True)

    def _term(*_a):
        stop.set()
    signal.signal(signal.SIGINT, _term)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _term)
    try:
        while not stop.wait(1):
            pass
    finally:
        if gw.pipe_server:
            gw.pipe_server.stop()
        gw.shutdown()
        _ = mutex
        os._exit(0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
