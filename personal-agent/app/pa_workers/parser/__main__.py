"""pa-parser: parses one document per process. No network (firewall rule), no keys, no pipe to anyone
but its parent (stdin/stdout). Protocol: first stdin line = JSON {"name","family"}, rest = raw bytes.
Output: one JSON object on stdout. Resource limits are applied by the gateway (Windows Job Object)."""
from __future__ import annotations

import json
import sys


def main() -> int:
    try:
        import resource  # POSIX only (tests); on Windows the Job Object enforces limits
        resource.setrlimit(resource.RLIMIT_AS, (1024 * 1024 * 1024, 1024 * 1024 * 1024))
        resource.setrlimit(resource.RLIMIT_CPU, (60, 60))
    except (ImportError, ValueError, OSError):
        pass
    raw = sys.stdin.buffer.read()
    nl = raw.find(b"\n")
    try:
        header = json.loads(raw[:nl if nl >= 0 else len(raw)])
        if header.get("mode") == "table":      # large spreadsheet / CSV read straight from disk (see tables.py)
            from pa_workers.parser.tables import main_table
            out = main_table(header)
        elif header.get("mode") == "pdf_images":   # scanned PDF: hand the embedded page pictures to the gateway (for the vision model)
            from pathlib import Path

            from pa_workers.parser.parsers import pdf_page_images
            data = Path(header["path"]).read_bytes() if header.get("path") else raw[nl + 1:]
            out = {"ok": True, **pdf_page_images(data, int(header.get("max_pages", 12)))}
        else:
            from pa_workers.parser.parsers import parse
            result = parse(header.get("family", "binary"), header.get("name", "file"), raw[nl + 1:])
            out = {"ok": True, **result}
    except Exception as e:  # noqa: BLE001
        out = {"ok": False, "error": f"{type(e).__name__}: {str(e)[:300]}"}
    # ASCII-only JSON: the console code page of a Windows pipe (cp1252) must never decide whether non-English text survives
    sys.stdout.write(json.dumps(out, ensure_ascii=True))
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
