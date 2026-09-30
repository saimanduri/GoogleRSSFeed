"""Developer mode switch.

Developer mode exists so the app can be built, run and tested on machines (and CI runners)
that lack a TPM, code signing, Windows Sandbox or Outlook. It is never available in a
release build (see buildinfo.RELEASE_BUILD) and the Security Posture page shows it as a
High finding whenever it is on.
"""
import os
import sys

from .buildinfo import RELEASE_BUILD


def dev_mode() -> bool:
    if RELEASE_BUILD:
        return False
    return os.environ.get("PA_DEV_MODE", "0") == "1"


def is_windows() -> bool:
    return sys.platform == "win32"


def fast_kdf_for_tests() -> bool:
    """Lower Argon2id cost for automated tests only. Never honoured in release builds."""
    return (not RELEASE_BUILD) and os.environ.get("PA_TEST_FAST_KDF", "0") == "1"
