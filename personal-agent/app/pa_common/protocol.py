"""IPC wire protocol shared by pa-gateway, pa-core, workers and the Tauri shell.

Framing: 4-byte little-endian unsigned length, then UTF-8 JSON. Max frame 16 MiB.

Messages:
  hello    {"type":"hello","role":"ui|core|parser|outlook|sandbox","token":"...","protocol":1}
  request  {"type":"req","id":"...","method":"ns.name","params":{...}}
  response {"type":"res","id":"...","ok":true,"result":...} | {"type":"res","id":"...","ok":false,"error":{code,message,details}}
  event    {"type":"evt","topic":"...","data":{...}}          (gateway -> ui only)
"""
from __future__ import annotations

import json
import struct
from typing import Any

MAX_FRAME = 16 * 1024 * 1024
HEADER = struct.Struct("<I")


class FrameError(Exception):
    pass


def encode_frame(msg: dict[str, Any]) -> bytes:
    body = json.dumps(msg, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(body) > MAX_FRAME:
        raise FrameError("frame too large")
    return HEADER.pack(len(body)) + body


class FrameDecoder:
    """Incremental decoder; feed bytes, iterate complete messages."""

    def __init__(self) -> None:
        self._buf = bytearray()

    def feed(self, data: bytes) -> list[dict[str, Any]]:
        self._buf.extend(data)
        out: list[dict[str, Any]] = []
        while len(self._buf) >= HEADER.size:
            (n,) = HEADER.unpack_from(self._buf, 0)
            if n > MAX_FRAME:
                raise FrameError("frame too large")
            if len(self._buf) < HEADER.size + n:
                break
            body = bytes(self._buf[HEADER.size:HEADER.size + n])
            del self._buf[:HEADER.size + n]
            try:
                msg = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                raise FrameError(f"bad json: {e}") from e
            if not isinstance(msg, dict) or "type" not in msg:
                raise FrameError("bad message")
            out.append(msg)
        return out
