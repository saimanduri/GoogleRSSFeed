"""Extension security (spec 39.4).

V1 connectors ship inside the signed app. Future connector packages (e.g. ChiRAG) must carry a
manifest signed with the publisher's Ed25519 key that is PINNED in the app:

  {"id","version","publisher","destinations":[...],"data_types":[...],"side_effects":[...],
   "secrets":[...],"tools":[...],"sha256": "<package hash>", "signature": "<base64 ed25519>"}

Unsigned or changed packages fail to load; revoked versions are blocked by hash. Text in manifests
is untrusted and never rendered as instructions. There is no marketplace.
"""
from __future__ import annotations

import base64
import json
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

# Publisher key pinned at build time (replace with the real release key in scripts/build.ps1).
PINNED_PUBLISHER_KEYS: dict[str, str] = {}
REVOKED_PACKAGE_HASHES: set[str] = set()
REQUIRED = ("id", "version", "publisher", "destinations", "data_types", "side_effects", "secrets", "tools", "sha256")


class ExtensionRejected(Exception):
    pass


def verify_manifest(manifest: dict[str, Any], package_sha256: str) -> dict[str, Any]:
    missing = [k for k in REQUIRED if k not in manifest]
    if missing:
        raise ExtensionRejected(f"manifest missing fields: {', '.join(missing)}")
    if manifest["sha256"] != package_sha256:
        raise ExtensionRejected("package contents do not match the signed manifest")
    if package_sha256 in REVOKED_PACKAGE_HASHES:
        raise ExtensionRejected("this package version has been revoked")
    key_b64 = PINNED_PUBLISHER_KEYS.get(manifest["publisher"])
    if not key_b64:
        raise ExtensionRejected("unknown publisher (not pinned in this app)")
    body = json.dumps({k: v for k, v in manifest.items() if k != "signature"}, sort_keys=True, separators=(",", ":")).encode()
    try:
        Ed25519PublicKey.from_public_bytes(base64.b64decode(key_b64)).verify(base64.b64decode(manifest.get("signature", "")), body)
    except (InvalidSignature, ValueError) as e:
        raise ExtensionRejected("manifest signature is invalid") from e
    return manifest


def enforce(manifest: dict[str, Any], tool: str, destination: str | None) -> None:
    """Runtime enforcement: a connector may only use the tools and destinations it declared."""
    if tool not in manifest.get("tools", []):
        raise ExtensionRejected(f"{manifest['id']} did not declare tool {tool}")
    if destination and destination not in manifest.get("destinations", []):
        raise ExtensionRejected(f"{manifest['id']} did not declare destination {destination}")
