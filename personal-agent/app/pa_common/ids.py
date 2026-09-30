import secrets
import time


def new_id(prefix: str) -> str:
    """Time-sortable random id: <prefix>_<ms-hex><random>."""
    return f"{prefix}_{int(time.time() * 1000):011x}{secrets.token_hex(6)}"
