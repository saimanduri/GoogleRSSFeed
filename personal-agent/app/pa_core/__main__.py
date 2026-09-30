"""pa-core entry point. Started ONLY by pa-gateway, which passes the pipe name and a per-launch token
on stdin (never on the command line or in a file)."""
from __future__ import annotations

import json
import socket
import sys


def selftest_network() -> int:
    """Used by the Security Posture live test: outbound traffic from pa-core MUST be blocked.
    Exit 0 = blocked (good), 1 = connected (firewall rule missing)."""
    for host in ("1.1.1.1", "8.8.8.8"):
        try:
            with socket.create_connection((host, 443), timeout=5):
                print(f"CONNECTED to {host}:443 - outbound NOT blocked")
                return 1
        except OSError:
            continue
    print("outbound blocked")
    return 0


def main() -> int:
    if "--selftest-network" in sys.argv:
        return selftest_network()
    if sys.platform != "win32":
        print("pa-core runs in-process on non-Windows developer setups", file=sys.stderr)
        return 2
    cfg = json.loads(sys.stdin.readline())
    from pa_common.pipeclient import PipeClient

    from .agent import CoreRuntime

    def factory(wid: str):
        return PipeClient(cfg["pipe"], "core", cfg["token"], wid, expected_server_pid=cfg.get("gateway_pid"))

    CoreRuntime(factory).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
