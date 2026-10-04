"""Run tests/e2e/scenarios.json against a THROWAWAY developer-mode gateway and check, for every UI action, the backend
response AND the backend side effects (security-log events, follow-up reads).

  python scripts/e2e_runner.py                    # software protector (does not touch the real TPM)
  python scripts/e2e_runner.py --tpm              # use the real TPM (never repeat runs with wrong PINs: TPM budget)
  python scripts/e2e_runner.py --only chat.       # steps whose id starts with a prefix
  python scripts/e2e_runner.py --learn            # print the audit events each step produced (to author "audit")
  python scripts/e2e_runner.py --json out.json    # machine-readable report

Step schema (tests/e2e/scenarios.json): id, ui (where the user triggers it), call, params ("${VAR}" substitution; a
whole-string "${VAR}" keeps its type), expect {error_code | has | eq | truthy | falsy | contains | len_ge | absent |
file_exists}, save {var: path}, set {VAR: value}, poll {until: {path, op, value}, timeout, every, call, params},
audit [event types that must be in the security log after the step], hook, requires ("ollama").
Responses never get printed, so secrets cannot leak into the report; "absent" asserts a canary is NOT in the response.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))

from pa_common.errors import PAError  # noqa: E402
from pa_common.pipeclient import PipeClient  # noqa: E402


def getp(obj, path: str):
    cur = obj
    for part in re.split(r"\.(?![^\[]*\])", path):
        if part == "$len":
            return len(cur)
        m = re.fullmatch(r"\[(\w+)=([^\]]+)\]", part)  # first list item whose field equals a value, e.g. [kind=llm.request]
        if m:
            cur = next(x for x in cur if isinstance(x, dict) and str(x.get(m.group(1))) == m.group(2))
            continue
        if isinstance(cur, list):
            cur = cur[int(part)]
        elif isinstance(cur, dict):
            cur = cur[part]
        else:
            raise KeyError(path)
    return cur


def subst(v, vars_):
    if isinstance(v, str):
        m = re.fullmatch(r"\$\{(\w+)\}", v)
        if m:
            return vars_[m.group(1)]
        return re.sub(r"\$\{(\w+)\}", lambda mm: str(vars_[mm.group(1)]), v)
    if isinstance(v, list):
        return [subst(x, vars_) for x in v]
    if isinstance(v, dict):
        return {k: subst(x, vars_) for k, x in v.items()}
    return v


def strong_password() -> str:
    return "Zq-" + secrets.token_urlsafe(18) + "-9x"


def strong_pin() -> str:
    while True:
        p = "".join(secrets.choice("0123456789") for _ in range(8))
        d = list(map(int, p))
        if len(set(p)) >= 6 and not any(abs(a - b) == 1 for a, b in zip(d, d[1:])):
            return p


def ollama_up() -> bool:
    try:
        urllib.request.urlopen("http://127.0.0.1:11434/api/tags", timeout=2).read()
        return True
    except Exception:  # noqa: BLE001
        return False


class Runner:
    def __init__(self, args):
        self.args = args
        self.vars: dict = {}
        self.results: list[dict] = []
        self.pending_audit: list[tuple[str, str]] = []  # (step id, event type) waiting for an unlocked session

    # ------------------------------------------------------------------ gateway
    def start(self):
        self.data = Path(tempfile.gettempdir()) / f"pa-e2e-{secrets.token_hex(4)}"
        env = dict(os.environ, PA_DEV_MODE="1", PA_DATA_DIR=str(self.data), PYTHONPATH=str(ROOT / "app"))
        env.pop("PA_TEST_FAST_KDF", None)
        env["PA_FORCE_SOFTWARE_PROTECTOR"] = "0" if self.args.tpm else "1"
        if os.environ.get("PA_TEST_REAL_DEFENDER") != "1":
            env["PA_SKIP_DEFENDER"] = "1"  # developer gateway: do not depend on which antivirus this PC runs
        if not self.args.tpm:
            env["PA_FORCE_SOFTWARE_PROTECTOR"] = "1"
        else:
            env.pop("PA_FORCE_SOFTWARE_PROTECTOR")
        log = open(os.environ["E2E_GATEWAY_LOG"], "w") if os.environ.get("E2E_GATEWAY_LOG") else subprocess.DEVNULL
        if log is not subprocess.DEVNULL:
            env["PA_DEBUG"] = "1"
            env["PA_DEBUG_DUMP"] = os.environ.get("E2E_DUMP_S", "90")
        self.gw = subprocess.Popen([sys.executable, "-m", "pa_gateway", "--data-dir", str(self.data)], env=env, cwd=ROOT / "app",
                                   stdout=log, stderr=subprocess.STDOUT if log is not subprocess.DEVNULL else subprocess.DEVNULL)
        rv = self.data / "run" / "gateway.json"
        for _ in range(200):
            if rv.exists():
                break
            time.sleep(0.2)
        info = json.loads(rv.read_text())
        self.ui = PipeClient(info["pipe"], "ui", info["ui_token"], "ui", expected_server_pid=None)
        base = Path(__file__).resolve().parents[1] / "build"      # not under AppData: the app refuses to share that
        base.mkdir(exist_ok=True)
        tmp = base / f"pa-e2e-files-{secrets.token_hex(3)}"
        tmp.mkdir()
        (tmp / "sub").mkdir()
        (tmp / "sub" / "inner.txt").write_text("inner file\n", encoding="utf-8")
        self.tmp = tmp
        (tmp / "hello.txt").write_text("hello e2e file\n", encoding="utf-8")
        (tmp / "sales.csv").write_text("Region,Units\nNorth,10\nSouth,25\nNorth,3\n", encoding="utf-8")
        (tmp / "skill.json").write_text(json.dumps({"name": "e2e-skill", "description": "list files", "tools": ["files.list"],
                                                    "steps": [{"instruction": "list my files", "tool": "files.list"}]}), encoding="utf-8")
        (tmp / "import.csv").write_text("name,url,username,password\nsite,https://example.com,bob,Pa55-w0rd-csv!\n", encoding="utf-8")
        (tmp / "bk").mkdir()
        self.vars.update(PASSWORD=strong_password(), PW2=strong_password(), PW3=strong_password(), PIN=strong_pin(), PIN2=strong_pin(),
                         TMP=str(tmp).replace("\\", "/"), CANARY="CANARY-" + secrets.token_hex(8))

    def stop(self):
        try:
            self.gw.terminate()
            self.gw.wait(15)
        except Exception:  # noqa: BLE001
            self.gw.kill()
        shutil.rmtree(self.data, ignore_errors=True)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def call(self, method, params):
        return self.ui.call(method, params)

    def unlocked(self) -> bool:
        try:
            return self.call("session.status", {}).get("state") == "UNLOCKED"
        except PAError:
            return False

    # ------------------------------------------------------------------ audit
    def max_seq(self) -> int:
        evs = self.call("activity.events", {"filters": {}, "limit": 5000})["events"]
        return max((e.get("sequence", 0) for e in evs), default=0)

    def events_since(self, ts: str) -> list[str]:
        r = self.call("activity.events", {"filters": {}, "limit": 5000, "since": ts})
        return [e.get("event_type", "") for e in r["events"]]

    # ------------------------------------------------------------------ one step
    def run_step(self, s: dict) -> dict:
        rec = {"id": s["id"], "ui": s.get("ui", ""), "call": s.get("call"), "status": "PASS", "detail": ""}
        if s.get("requires") == "ollama" and not self.args.ollama:
            rec.update(status="SKIP", detail="needs Ollama (use --ollama)")
            return rec
        if s.get("tpm_only") and not self.args.tpm:
            rec.update(status="SKIP", detail="needs --tpm")
            return rec
        t0 = time.time()
        marker = None
        if self.unlocked():
            marker = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(time.time() - 1)) + "Z"
        try:
            if s.get("pre") == "sign_out":
                self.call("auth.sign_out", {})
            if s.get("stepup"):
                self.call("auth.step_up", {"category": s["stepup"], "method": "password", "secret": self.vars["PASSWORD"]})
            before_seq = self.max_seq() if (self.args.learn and self.unlocked()) else None
            params = subst(s.get("params", {}), self.vars)
            result, err = None, None
            try:
                result = self.call(s["call"], params)
            except PAError as e:
                err = e
            ex = s.get("expect", {})
            if "error_code" in ex:
                if err is None:
                    raise AssertionError("expected error %s but call succeeded" % ex["error_code"])
                if err.code != ex["error_code"]:
                    raise AssertionError(f"expected error {ex['error_code']} got {err.code}: {err}")
            elif err is not None:
                raise AssertionError(f"unexpected error {err.code}: {err}")
            if s.get("poll") and err is None:
                pl = s["poll"]
                end = time.time() + pl.get("timeout", 30)
                while time.time() < end:
                    r2 = self.call(pl.get("call", s["call"]), subst(pl.get("params", params), self.vars))
                    if self.check_cond(r2, pl["until"]):
                        result = r2
                        break
                    time.sleep(pl.get("every", 0.5))
                else:
                    raise AssertionError(f"poll timed out waiting for {pl['until']}")
            if err is None:
                self.check_expect(result, ex)
                for var, path in s.get("save", {}).items():
                    self.vars[var] = getp(result, path)
                if s.get("hook") == "recovery_answers":
                    groups = result["recovery_key"].split("-")
                    self.vars["RK"] = result["recovery_key"]
                    self.vars["RK_ANSWERS"] = {str(i): groups[i] for i in result["confirm_groups"]}
                for var, val in s.get("set", {}).items():
                    self.vars[var] = subst(val, self.vars)
            if s.get("wait_s"):
                time.sleep(s["wait_s"])
            if self.args.learn and before_seq is not None and self.unlocked():
                new = sorted({e["event_type"] for e in self.call("activity.events", {"filters": {}, "limit": 5000})["events"]
                              if e.get("sequence", 0) > before_seq})
                keys = ",".join(result.keys()) if isinstance(result, dict) else f"{type(result).__name__}[{len(result) if hasattr(result, '__len__') else ''}]"
                rec["detail"] = f"keys: {keys} | events: " + ", ".join(new)
            # backend side effects: audit events
            wanted = s.get("audit", [])
            if wanted:
                self.pending_audit.extend((s["id"], w) for w in wanted)
            if self.pending_audit and self.unlocked():
                since = marker or time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(time.time() - 600)) + "Z"
                have = set(self.events_since(since if marker else self.run_started))
                missing = [(i, w) for (i, w) in self.pending_audit if w not in have]
                self.pending_audit = []
                if missing:
                    raise AssertionError("audit events missing: " + ", ".join(f"{w} (from {i})" for i, w in missing))
        except AssertionError as e:
            rec.update(status="FAIL", detail=str(e)[:300])
        except Exception as e:  # noqa: BLE001
            rec.update(status="FAIL", detail=f"{type(e).__name__}: {str(e)[:250]}")
        rec["ms"] = int((time.time() - t0) * 1000)
        return rec

    def check_cond(self, obj, c) -> bool:
        try:
            v = getp(obj, c["path"])
        except (KeyError, IndexError, ValueError, TypeError):
            return False
        op = c.get("op", "truthy")
        size = lambda: v if isinstance(v, int) else len(v)  # noqa: E731
        return {"truthy": lambda: bool(v), "falsy": lambda: not v, "eq": lambda: v == c.get("value"), "ne": lambda: v != c.get("value"),
                "len_ge": lambda: size() >= c.get("value", 0), "contains": lambda: c.get("value") in v}[op]()

    def check_expect(self, result, ex):
        for p in ex.get("has", []):
            try:
                getp(result, p)
            except (KeyError, IndexError, TypeError):
                raise AssertionError(f"response lacks '{p}'") from None
        for p, val in ex.get("eq", {}).items():
            got = getp(result, p)
            val = subst(val, self.vars)
            if got != val:
                raise AssertionError(f"'{p}' = {got!r}, expected {val!r}")
        for p in ex.get("truthy", []):
            if not getp(result, p):
                raise AssertionError(f"'{p}' is empty/false")
        for p in ex.get("falsy", []):
            if getp(result, p):
                raise AssertionError(f"'{p}' should be empty/false")
        for p, sub in ex.get("contains", {}).items():
            if subst(sub, self.vars) not in str(getp(result, p)):
                raise AssertionError(f"'{p}' lacks {sub!r}")
        for p, n in ex.get("len_ge", {}).items():
            v = getp(result, p)
            size = v if isinstance(v, int) else len(v)
            if size < n:
                raise AssertionError(f"'{p}' has fewer than {n} items")
        blob = json.dumps(result, default=str)
        for a in ex.get("absent", []):
            if str(subst(a, self.vars)) in blob:
                raise AssertionError("response contains a value that must never be returned")
        for f in ex.get("file_exists", []):
            if not Path(subst(f, self.vars)).exists():
                raise AssertionError(f"file not created: {f}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tpm", action="store_true")
    ap.add_argument("--ollama", action="store_true", help="run steps that need a local Ollama (auto when reachable)")
    ap.add_argument("--only", default="")
    ap.add_argument("--learn", action="store_true")
    ap.add_argument("--json")
    ap.add_argument("--keep-going", action="store_true", default=True)
    args = ap.parse_args()
    args.ollama = args.ollama or ollama_up()
    scen = json.loads((ROOT / "tests" / "e2e" / "scenarios.json").read_text(encoding="utf-8"))
    r = Runner(args)
    r.start()
    r.run_started = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(time.time() - 2)) + "Z"
    try:
        for s in scen["steps"]:
            if args.only and not s["id"].startswith(args.only):
                continue
            rec = r.run_step(s)
            r.results.append(rec)
            mark = {"PASS": "PASS", "FAIL": "FAIL", "SKIP": "skip"}[rec["status"]]
            print(f"{mark}  {rec['id']:<34} {rec.get('ui', '')[:46]:<46} {rec['detail'][:150]}", flush=True)
            if rec["status"] == "FAIL" and s.get("critical"):
                print("critical step failed - stopping (later steps would only cascade)")
                break
    finally:
        r.stop()
    n = {k: sum(1 for x in r.results if x["status"] == k) for k in ("PASS", "FAIL", "SKIP")}
    print(f"\n{n['PASS']} passed, {n['FAIL']} failed, {n['SKIP']} skipped of {len(r.results)} steps")
    if args.json:
        Path(args.json).write_text(json.dumps({"summary": n, "steps": r.results}, indent=1), encoding="utf-8")
    return 1 if n["FAIL"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
