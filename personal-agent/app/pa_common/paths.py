"""Filesystem layout (spec 2.5).

Data:  %LOCALAPPDATA%\\PersonalAgent\\   (ACL: current user + SYSTEM only, inheritance removed)
  vault.header           wrapped Vault Master Key (versioned, AEAD-protected)
  state\\auth_state.json  brute-force counters (readable before unlock; not secret)
  db\\agent.db            SQLCipher database (all app data)
  files\\                 encrypted file blobs (My Files, quarantine, artifacts)
  logs\\agent-security.jsonl  unified security log (HMAC chain)
  run\\gateway.json       per-launch rendezvous (pipe name, UI token) - deleted on exit
  tmp\\                   per-run scratch (sandbox in/out) - wiped after each run
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .version import APP_NAME

CLOUD_SYNC_MARKERS = ("onedrive", "dropbox", "google drive", "googledrive", "icloud", "box sync", "my drive")


def default_data_dir() -> Path:
    override = os.environ.get("PA_DATA_DIR")
    if override:
        return Path(override)
    base = os.environ.get("LOCALAPPDATA")
    if base:
        return Path(base) / APP_NAME
    return Path.home() / ".personal-agent"


def looks_cloud_synced(path: Path) -> bool:
    p = str(path).lower().replace("/", "\\")
    if any(m in p for m in CLOUD_SYNC_MARKERS):
        return True
    for env in ("OneDrive", "OneDriveConsumer", "OneDriveCommercial"):
        root = os.environ.get(env)
        if root and p.startswith(root.lower().replace("/", "\\")):
            return True
    return False


@dataclass(frozen=True)
class DataPaths:
    root: Path

    @property
    def vault_header(self) -> Path:
        return self.root / "vault.header"

    @property
    def state_dir(self) -> Path:
        return self.root / "state"

    @property
    def auth_state(self) -> Path:
        return self.state_dir / "auth_state.json"

    @property
    def db_dir(self) -> Path:
        return self.root / "db"

    @property
    def db_file(self) -> Path:
        return self.db_dir / "agent.db"

    @property
    def files_dir(self) -> Path:
        return self.root / "files"

    @property
    def logs_dir(self) -> Path:
        return self.root / "logs"

    @property
    def run_dir(self) -> Path:
        return self.root / "run"

    @property
    def rendezvous(self) -> Path:
        return self.run_dir / "gateway.json"

    @property
    def tmp_dir(self) -> Path:
        return self.root / "tmp"

    @property
    def snapshots_dir(self) -> Path:
        return self.root / "snapshots"

    def ensure(self) -> None:
        for d in (self.root, self.state_dir, self.db_dir, self.files_dir, self.logs_dir,
                  self.run_dir, self.tmp_dir, self.snapshots_dir):
            d.mkdir(parents=True, exist_ok=True)
