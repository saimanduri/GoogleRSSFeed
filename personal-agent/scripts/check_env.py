"""Environment check - run on the laptop (or CI) to verify the Python side works on this machine.

    python scripts/check_env.py

Prints versions and runs isolated SQLCipher/crypto smoke tests, each in its own subprocess so a crash in a
native library is reported instead of killing the check.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

PROBES = {
    "versions": "import sys, platform, sqlcipher3, cryptography, argon2; c=sqlcipher3.connect(':memory:'); "
                "print(sys.version.split()[0], platform.platform(), 'sqlcipher', c.execute('pragma cipher_version').fetchone(), "
                "'sqlite', sqlcipher3.sqlite_version, 'cryptography', cryptography.__version__)",
    "sqlcipher_basic": "import sqlcipher3; c=sqlcipher3.connect(':memory:'); c.execute(\"pragma key=\\\"x'\" + '00'*32 + \"'\\\"\"); "
                       "c.execute('create table t(a)'); c.execute('insert into t values (1)'); print(c.execute('select * from t').fetchall())",
    "sqlcipher_memsec": "import sqlcipher3; c=sqlcipher3.connect(':memory:'); c.execute(\"pragma key=\\\"x'\" + '00'*32 + \"'\\\"\"); "
                        "c.execute('pragma cipher_memory_security = ON'); c.execute('create table t(a)'); print('ok')",
    "sqlcipher_fts5": "import sqlcipher3; c=sqlcipher3.connect(':memory:'); c.execute(\"pragma key=\\\"x'\" + '00'*32 + \"'\\\"\"); "
                      "c.execute(\"create virtual table f using fts5(a, b, tokenize='porter unicode61')\"); print('ok')",
    "sqlcipher_file_wal": "import sqlcipher3, tempfile, os; p=os.path.join(tempfile.mkdtemp(),'x.db'); c=sqlcipher3.connect(p); "
                          "c.execute(\"pragma key=\\\"x'\" + '00'*32 + \"'\\\"\"); c.execute('pragma journal_mode=WAL'); "
                          "c.execute('create table t(a)'); print('ok')",
    "sqlcipher_memsec_file_wal (expected to FAIL on Windows; not used)": "import sqlcipher3, tempfile, os; p=os.path.join(tempfile.mkdtemp(),'x.db'); c=sqlcipher3.connect(p); "
                          "c.execute(\"pragma key=\\\"x'\" + '00'*32 + \"'\\\"\"); c.execute('pragma cipher_memory_security = ON'); c.execute('pragma journal_mode=WAL'); "
                          "[c.execute(f'create table t{i}(a)') for i in range(30)]; print('ok')",
    "schema_in_thread": "import sys, threading; sys.path.insert(0, r'" + str(ROOT / "app") + "'); "
                        "import tempfile, os; from pathlib import Path; from pa_gateway.db.database import Database; "
                        "r={}; t=threading.Thread(target=lambda: r.update(db=Database(Path(tempfile.mkdtemp())/'a.db', b'\\x01'*32))); "
                        "t.start(); t.join(); print('ok' if 'db' in r else 'failed')",
    "schema_main_thread": "import sys; sys.path.insert(0, r'" + str(ROOT / "app") + "'); import tempfile; from pathlib import Path; "
                          "from pa_gateway.db.database import Database; Database(Path(tempfile.mkdtemp())/'a.db', b'\\x01'*32); print('ok')",
}


def main() -> int:
    bad = 0
    for name, code in PROBES.items():
        r = subprocess.run([sys.executable, "-X", "faulthandler", "-c", code], capture_output=True, text=True, timeout=120)
        ok = r.returncode == 0
        bad += not ok
        out = (r.stdout.strip() or r.stderr.strip().splitlines()[-1] if (r.stdout or r.stderr) else "")
        print(f"[{'OK' if ok else 'FAIL'}] {name}: {out[:300]}")
        if not ok:
            print("   stderr:", r.stderr.strip()[-1500:])
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
