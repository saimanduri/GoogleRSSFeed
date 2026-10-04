# PyInstaller spec: one install folder with several executables sharing one _internal/ runtime.
# All executables must live in the SAME folder: firewall rules and IPC client checks use exact paths.
# Build:  pyinstaller scripts/pyinstaller/personal-agent.spec --distpath dist --workpath build
import os
from PyInstaller.utils.hooks import collect_data_files, collect_submodules

ROOT = os.path.abspath(os.path.join(SPECPATH, "..", ".."))
APP = os.path.join(ROOT, "app")
HERE = SPECPATH
hidden = (collect_submodules("pa_gateway") + collect_submodules("pa_core") + collect_submodules("pa_workers")
          + collect_submodules("pa_common") + ["win32timezone", "win32pdh", "pypdf", "sqlcipher3", "argon2", "tzdata", "tzlocal"])
# Explicit data files: collect_data_files("pa_gateway") finds nothing because app/ is not on sys.path while the
# spec runs (the password/PIN lists were missing from v0.1.1 bundles). Fail the build if they are not there.
DATA_DIR = os.path.join(APP, "pa_gateway", "data")
for _f in ("common_passwords.txt", "common_pins.txt"):
    if not os.path.isfile(os.path.join(DATA_DIR, _f)):
        raise SystemExit("missing data file: " + _f)
datas = [(os.path.join(DATA_DIR, "*.txt"), os.path.join("pa_gateway", "data"))] + collect_data_files("tzdata")


def analysis(entry):
    return Analysis([os.path.join(HERE, entry)], pathex=[APP], hiddenimports=hidden, datas=datas,
                    excludes=["tkinter", "matplotlib", "IPython", "pytest"], noarchive=False)


a_gw, a_core, a_parser, a_outlook = (analysis(e) for e in ("entry_gateway.py", "entry_core.py", "entry_parser.py", "entry_outlook.py"))


def exe(a, name, console):
    return EXE(PYZ(a.pure), a.scripts, [], exclude_binaries=True, name=name, console=console, upx=False,
               icon=os.path.join(APP, "ui", "src-tauri", "icons", "icon.ico"), version=None)


# gateway runs in the background (no console window); helpers talk over stdin/stdout pipes (console subsystem,
# started with CREATE_NO_WINDOW so no window appears)
e_gw = exe(a_gw, "pa-gateway", False)
e_core = exe(a_core, "pa-core", True)
e_parser = exe(a_parser, "pa-parser", True)
e_outlook = exe(a_outlook, "pa-outlook-worker", True)

coll = COLLECT(e_gw, a_gw.binaries, a_gw.datas, e_core, a_core.binaries, a_core.datas, e_parser, a_parser.binaries, a_parser.datas,
               e_outlook, a_outlook.binaries, a_outlook.datas, strip=False, upx=False, name="PersonalAgent")
