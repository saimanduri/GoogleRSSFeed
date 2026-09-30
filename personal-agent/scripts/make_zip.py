"""Create personal-agent.zip (source only: no node_modules, build outputs, caches, venvs, local data).

    python scripts/make_zip.py [output.zip]
"""
from __future__ import annotations

import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP_DIRS = {"node_modules", "dist", "build", "target", "gen", "__pycache__", ".pytest_cache", ".ruff_cache", ".venv", "venv",
             ".mypy_cache", "llm-runtime/bin", "sandbox-python"}
SKIP_SUFFIX = {".pyc", ".pabk", ".gguf", ".zip"}


def keep(p: Path) -> bool:
    rel = p.relative_to(ROOT).as_posix()
    parts = rel.split("/")
    if any(part in SKIP_DIRS for part in parts) or any(rel.startswith(d + "/") for d in SKIP_DIRS if "/" in d):
        return False
    return p.suffix not in SKIP_SUFFIX


def main() -> int:
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT.parent / "personal-agent.zip"
    n = 0
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as z:
        for p in sorted(ROOT.rglob("*")):
            if p.is_file() and keep(p):
                z.write(p, Path("personal-agent") / p.relative_to(ROOT))
                n += 1
    print(f"{out} ({n} files, {out.stat().st_size / 1024:.0f} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
