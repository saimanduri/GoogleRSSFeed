"""Reading pictures and scanned pages with the local vision model (Settings > AI Model > Vision models).

Triggered automatically: an image in My Files, an attached image, or a PDF whose pages carry (almost) no text layer is a scan.
For scans the isolated pa-parser hands over the embedded page pictures; the gateway asks the vision model to transcribe each one.
The result is plain text that goes through the same path as any other file text (untrusted data, injection heuristics, labels).
Nothing leaves this PC unless the vision model itself is remote, and CONFIDENTIAL/RESTRICTED files are never sent to a remote one.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from pa_common.errors import PAError
from pa_common.ids import new_id
from pa_common.sensitivity import Sensitivity

from .workers import run_worker

IMAGE_MIME = {"png": "image/png", "jpeg": "image/jpeg", "gif": "image/gif", "webp": "image/webp"}
IMAGE_EXT = {".png": "png", ".jpg": "jpeg", ".jpeg": "jpeg", ".gif": "gif", ".webp": "webp"}
MIN_CHARS_PER_PAGE = 40          # below this a PDF page counts as "no text layer" (a scan)
PROMPT = ("Read this image. First write every piece of visible text exactly as written, keeping the line breaks; show tables as Markdown "
          "tables. Then on a new line starting with 'Picture:' describe in one to three sentences what the picture shows. "
          "If there is no text write 'No text.' first. Add nothing else.")


def is_scanned_pdf(text: str, pages: int) -> bool:
    """A PDF is treated as a scan when its text layer is (almost) empty compared with its page count."""
    if pages <= 0:
        return False
    body = "".join(ch for ch in text if not ch.isspace() and ch != "-")
    body = body.replace("page", "")          # the '--- page N ---' markers do not count as text
    return len(body) < MIN_CHARS_PER_PAGE * min(pages, 5)


class VisionReader:
    def __init__(self, gw):
        self.gw = gw

    # ------------------------------------------------------------------ availability
    def enabled(self) -> bool:
        return bool(self.gw.settings.get("vision.auto"))

    def model(self) -> dict[str, Any] | None:
        return self.gw.llm.role_model("vision")

    def _may_send(self, m: dict[str, Any], sensitivity: int) -> str | None:
        if m["location"] == "remote" and sensitivity >= Sensitivity.CONFIDENTIAL:
            return "this file is labelled CONFIDENTIAL or higher and the vision model is not on this PC, so it was not sent"
        return None

    # ------------------------------------------------------------------ single picture
    def read_image(self, data: bytes, family: str, sensitivity: int, purpose: str = "image") -> tuple[str, str | None]:
        """Returns (text, note). `note` explains why nothing could be read (None on success)."""
        if not self.enabled():
            return "", "automatic reading of images is switched off (Settings > AI Model > Vision)"
        m = self.model()
        if m is None:
            return "", "no vision model is set up: add one under Settings > AI Model > Vision models, then press Read again"
        why = self._may_send(m, sensitivity)
        if why:
            return "", why
        try:
            out = self.gw.llm.describe_image(data, IMAGE_MIME.get(family, "image/png"), PROMPT, sensitivity, purpose=purpose)
        except PAError as e:
            return "", f"the vision model could not read it ({e.code})"
        return out["text"], None

    # ------------------------------------------------------------------ scanned PDF
    def page_images(self, *, path: Path | None = None, data: bytes | None = None) -> dict[str, Any]:
        header = {"mode": "pdf_images", "max_pages": int(self.gw.settings.get("vision.max_pages")), "path": str(path) if path else None}
        work = self.gw.paths.tmp_dir / f"vis-{new_id('w')}"
        work.mkdir(parents=True, exist_ok=True)
        try:
            out = run_worker("pa_workers.parser", json.dumps(header).encode() + b"\n" + (data or b""), timeout=180, cwd=work, memory_mb=1536)
        except Exception as e:  # noqa: BLE001
            raise PAError(f"could not open the PDF pages ({type(e).__name__})", code="parse_failed") from e
        finally:
            shutil.rmtree(work, ignore_errors=True)
        res = json.loads(out.decode("utf-8"))
        if not res.get("ok"):
            raise PAError(str(res.get("error", "could not open the PDF"))[:300], code="parse_failed")
        return res

    def read_scanned_pdf(self, sensitivity: int, *, path: Path | None = None, data: bytes | None = None) -> tuple[str, str | None]:
        import base64
        if not self.enabled():
            return "", "this PDF looks scanned but automatic reading of images is switched off (Settings > AI Model > Vision)"
        m = self.model()
        if m is None:
            return "", "this PDF looks scanned (no text layer): add a vision model under Settings > AI Model > Vision models, then press Read again"
        why = self._may_send(m, sensitivity)
        if why:
            return "", why
        res = self.page_images(path=path, data=data)
        pages, skipped, total = res["pages"], int(res["skipped"]), int(res["total_pages"])
        if not pages:
            return "", ("this PDF looks scanned but its pages use a picture format that cannot be extracted without a PDF renderer "
                        "(only JPEG scans are supported for now)")
        parts, failed = [], 0
        for p in pages:
            try:
                out = self.gw.llm.describe_image(base64.b64decode(p["b64"]), p["mime"], PROMPT, sensitivity, purpose="scanned-pdf")
                parts.append(f"\n--- page {p['page']} (read by the vision model) ---\n{out['text']}")
            except PAError as e:
                failed += 1
                parts.append(f"\n--- page {p['page']} --- [could not be read: {e.code}]")
        notes = []
        if total > len(pages) + skipped:
            notes.append(f"only the first {len(pages) + skipped} of {total} pages were read (Settings > AI Model > Vision: pages per scan)")
        if skipped:
            notes.append(f"{skipped} page(s) use an unsupported picture format and were skipped")
        if failed:
            notes.append(f"{failed} page(s) could not be read")
        return "".join(parts).strip(), ("; ".join(notes) or None)
