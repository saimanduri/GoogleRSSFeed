"""Build flags. The build pipeline (scripts/build.ps1) overwrites this file.

RELEASE_BUILD = True disables every developer shortcut:
  - the software key protector (TPM stand-in) cannot be used
  - mock/dev LLM providers cannot be loaded (spec 35.12)
  - developer mode (PA_DEV_MODE) is ignored
SIGNED_BUILD = True additionally requires IPC clients to carry a valid Authenticode signature
(set only when the executables are code-signed with the publisher certificate).
"""
RELEASE_BUILD = False
SIGNED_BUILD = False
BUILD_HASH = "dev"
