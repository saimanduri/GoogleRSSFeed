"""Portable (copy-and-run) mode: processes are identified by exact paths inside the folder, never by 'any python'."""
from pathlib import Path

from pa_common import portable
from pa_gateway.ipc import pipe_server


def test_source_checkout_is_never_portable():
    assert portable.portable_root() is None


def _make_portable(monkeypatch, tmp_path):
    root = tmp_path / "pa"
    (root / "python").mkdir(parents=True)
    exe = root / "python" / "python.exe"
    exe.write_text("x")
    (root / "pa-ui.exe").write_text("x")
    monkeypatch.setattr(portable, "PORTABLE_BUILD", True)
    monkeypatch.setattr(portable.sys, "executable", str(exe))
    monkeypatch.setattr(portable.sys, "platform", "win32")
    monkeypatch.setattr(pipe_server, "portable_root", portable.portable_root)
    monkeypatch.setattr(pipe_server, "portable_ui_exe", portable.portable_ui_exe)
    monkeypatch.setattr(pipe_server, "portable_core_exe", portable.portable_core_exe)
    return root


def test_portable_root_needs_python_folder(monkeypatch, tmp_path):
    root = _make_portable(monkeypatch, tmp_path)
    assert portable.portable_root() == root.resolve()
    monkeypatch.setattr(portable.sys, "executable", str(tmp_path / "other" / "python.exe"))
    assert portable.portable_root() is None


def test_image_check_accepts_only_exact_files(monkeypatch, tmp_path):
    root = _make_portable(monkeypatch, tmp_path)
    srv = pipe_server.PipeServer.__new__(pipe_server.PipeServer)
    ui, core = str(root / "pa-ui.exe"), str(root / "python" / "python.exe")
    assert srv._image_ok(ui, pipe_server.UI_EXE)
    assert srv._image_ok(core, pipe_server.CORE_EXE)
    # a copy of the UI elsewhere, any other python, or the UI claiming to be the core are all rejected
    assert not srv._image_ok(str(tmp_path / "evil" / "pa-ui.exe"), pipe_server.UI_EXE)
    assert not srv._image_ok(r"C:\Python312\python.exe", pipe_server.CORE_EXE)
    assert not srv._image_ok(ui, pipe_server.CORE_EXE)
    assert not srv._image_ok(core, pipe_server.UI_EXE)
    assert not srv._image_ok("", pipe_server.UI_EXE)


def test_portable_posture_warning_text_present():
    src = (Path(__file__).resolve().parents[2] / "app" / "pa_gateway" / "posture.py").read_text(encoding="utf-8")
    assert "Portable mode: no firewall isolation" in src
