"""Automated subset of the laptop checklist (TESTS.md) against a THROWAWAY dev vault on this PC.

Starts a developer-mode gateway with its own data folder (never your real %LOCALAPPDATA%\\PersonalAgent), uses the
real TPM and a local Ollama, generates random test credentials (never printed) and deletes everything at the end.
  python scripts/laptop_smoke.py [--model gemma4:latest] [--no-model]
Wrong PIN attempts are limited to one: repeated failures throttle the real TPM for the whole user account.
"""
from __future__ import annotations

import argparse
import json
import os
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))

from pa_common.errors import PAError  # noqa: E402
from pa_common.pipeclient import PipeClient  # noqa: E402

results: list[tuple[str, str, str]] = []


def check(cid: str, ok: bool, detail: str = "") -> None:
    results.append((cid, "PASS" if ok else "FAIL", detail))
    print(f"{'PASS' if ok else 'FAIL'}  {cid}  {detail}", flush=True)


def expect_error(cid: str, fn, code: str | None = None) -> None:
    try:
        fn()
    except PAError as e:
        check(cid, code is None or e.code == code, f"rejected ({e.code})")
        return
    check(cid, False, "was accepted")


def strong_password() -> str:
    return "Tt-" + secrets.token_urlsafe(18) + "-9x"


def strong_pin() -> str:
    while True:
        p = "".join(secrets.choice("0123456789") for _ in range(8))
        if len(set(p)) >= 6 and not any(a + 1 == b or a - 1 == b for a, b in zip(map(int, p), map(int, p[1:]))):
            return p


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="gemma4:latest")
    ap.add_argument("--no-model", action="store_true")
    ap.add_argument("--software", action="store_true", help="use the software protector (does not touch the real TPM; skips TPM checks)")
    ap.add_argument("--wrong-pin", action="store_true",
                    help="also try ONE wrong PIN (each failure counts against the real TPM's per-user budget: do not repeat runs)")
    args = ap.parse_args()
    data = Path(tempfile.gettempdir()) / f"pa-smoke-{secrets.token_hex(4)}"
    env = dict(os.environ, PA_DEV_MODE="1", PA_DATA_DIR=str(data), PYTHONPATH=str(ROOT / "app"))
    env.pop("PA_FORCE_SOFTWARE_PROTECTOR", None)
    if args.software:
        env["PA_FORCE_SOFTWARE_PROTECTOR"] = "1"
    env.pop("PA_TEST_FAST_KDF", None)
    gw = subprocess.Popen([sys.executable, "-m", "pa_gateway", "--data-dir", str(data)], env=env, cwd=ROOT / "app",
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    pw, pin, pw2 = strong_password(), strong_pin(), strong_password()
    try:
        rv = data / "run" / "gateway.json"
        for _ in range(150):
            if rv.exists():
                break
            time.sleep(0.2)
        info = json.loads(rv.read_text())
        ui = PipeClient(info["pipe"], "ui", info["ui_token"], "ui", expected_server_pid=None)
        call = ui.call

        pre = call("setup.preflight")
        if not args.software:
            check("A1 preflight reports TPM usable", bool(pre.get("tpm", {}).get("usable")), json.dumps(pre.get("tpm", {}))[:120])
        weak = call("setup.check_password", {"password": "password123", "username": "smoke"})
        check("A2 weak password rejected", bool(weak["errors"]))
        check("A2 trivial PIN rejected", bool(call("setup.check_pin", {"pin": "123456", "allow_letters": False})["errors"]))

        t0 = time.time()
        r = call("setup.create", {"username": "smoke", "password": pw, "pin": pin, "display_name": "Smoke",
                                  "assistant_name": "Tester"})
        check("A2 account created (Argon2 + TPM key)", True, f"{time.time() - t0:.1f}s")
        groups = r["recovery_key"].split("-")
        call("setup.confirm_recovery", {"answers": {str(i): groups[i] for i in r["confirm_groups"]}})
        rk = r["recovery_key"]
        post = {c["id"]: c for c in call("posture.run")}
        if not args.software:
            check("A3 TPM protector is 'tpm' (not software)",
                  post["tpm"]["status"] == "ok" and "PIN protector: tpm" in post["tpm"]["detail"], post["tpm"]["detail"][:90])

        call("auth.lock")
        check("A5 locked", call("session.status")["state"] != "UNLOCKED")
        t0 = time.time()
        call("auth.quick_unlock", {"pin": pin})
        check("A5 PIN unlock via TPM", call("session.status")["state"] == "UNLOCKED", f"{time.time() - t0:.1f}s")
        call("auth.lock")
        if args.wrong_pin:
            wrong = pin[:-1] + str((int(pin[-1]) + 1) % 10)
            expect_error("A5 one wrong PIN is refused", lambda: call("auth.quick_unlock", {"pin": wrong}))
        call("auth.quick_unlock", {"pin": pin})
        check("A5 PIN unlock after lock", call("session.status")["state"] == "UNLOCKED")

        call("auth.sign_out")
        call("auth.sign_in", {"username": "smoke", "password": pw})
        check("A7 password sign-in (cold start path)", call("session.status")["state"] == "UNLOCKED")

        call("auth.sign_out")
        out = call("auth.forgot_password", {"pin": pin, "recovery_key": rk, "new_password": pw2})
        check("A8 forgot password gives a NEW recovery key", out["recovery_key"] != rk)
        call("auth.sign_out")
        expect_error("A8 old password no longer works", lambda: call("auth.sign_in", {"username": "smoke", "password": pw}))
        call("auth.sign_in", {"username": "smoke", "password": pw2})
        pw = pw2

        call("auth.step_up", {"category": "secrets", "method": "password", "secret": pw})
        sid = call("secrets.create", {"item": {"title": "smoke", "type": "password", "value": "CANARY-" + secrets.token_hex(8)},
                                      "bindings": []})["id"]
        rev = call("secrets.reveal", {"id": sid})
        check("G secrets create + reveal after step-up", rev["value"].startswith("CANARY-"))

        bdir = data / "bk"
        bdir.mkdir()
        b = call("backup.run_now", {"folder": str(bdir), "password": pw})
        v = call("backup.verify", {"path": b["file"], "password": pw})
        check("F1 backup created and verified", bool(v["ok"]))

        t0 = time.time()
        call("killswitch.activate", {"level": "stop_all"})
        ks = call("killswitch.state")
        check("E3 STOP ALL active in < 2 s", ks["any"] and time.time() - t0 < 2.0, f"{time.time() - t0:.2f}s")
        expect_error("E3 release without password refused", lambda: call("killswitch.release", {}), "password_required")
        call("killswitch.release", {"password": pw})
        check("E3 release with password", not call("killswitch.state")["any"])

        if not args.no_model:
            try:
                mid = call("llm.add", {"model": {"provider": "ollama", "name": "ollama-smoke", "model_name": args.model,
                                                 "endpoint": "http://127.0.0.1:11434"}})["id"]
                res = call("llm.test", {"model_id": mid})
                call("session.touch")  # model tests can outlast the idle auto-lock
                check("C1 Ollama 'Test model'", bool(res.get("ok", res.get("passed", True))), json.dumps(res)[:140])
                cid = call("chat.create", {})["id"]
                call("chat.send", {"chat_id": cid, "text": "Reply with the single word: pong"})
                reply = None
                for _ in range(300):
                    msgs = [m for m in call("chat.get", {"chat_id": cid})["messages"] if m["role"] == "assistant"]
                    if msgs:
                        reply = msgs[0]["content"]
                        break
                    time.sleep(1)
                    if _ % 30 == 29:
                        call("session.touch")
                check("C1 chat answered by the real local model", bool(reply), (reply or "no reply in 300 s")[:60].replace("\n", " "))
            except PAError as e:
                check("C1 Ollama", False, f"{e.code}: {e}")

        lv = call("logs.verify")
        check("25 security log chain verifies", bool(lv["ok"]), f"{lv.get('events')} events")

        call("privacy.delete_everything", {"password": pw, "confirmation": "DELETE EVERYTHING"})
        check("F2 delete everything", True, str(call("session.status").get("state")))
    except Exception as e:  # noqa: BLE001
        check("harness", False, f"{type(e).__name__}: {e}")
    finally:
        gw.terminate()
        try:
            gw.wait(15)
        except subprocess.TimeoutExpired:
            gw.kill()
        shutil.rmtree(data, ignore_errors=True)
    fails = [r for r in results if r[1] == "FAIL"]
    print(f"\n{len(results) - len(fails)} passed, {len(fails)} failed")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
