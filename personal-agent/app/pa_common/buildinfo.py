"""Build flags. The release build pipeline (scripts/build.ps1) overwrites this file.

RELEASE_BUILD = True disables every developer shortcut:
  - the software key protector (TPM stand-in) cannot be used
  - mock/dev LLM providers and mock connectors cannot be loaded (spec 35.12)
  - IPC client image-path/signature verification cannot be skipped
"""
RELEASE_BUILD = False
BUILD_HASH = "dev"
