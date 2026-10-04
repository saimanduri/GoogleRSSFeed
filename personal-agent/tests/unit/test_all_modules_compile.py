"""A syntax error in a rarely imported module (a prompt, a worker) must fail the test run, not silently hang a background thread."""
import compileall
from pathlib import Path


def test_every_python_file_compiles():
    app = Path(__file__).resolve().parents[2] / "app"
    assert compileall.compile_dir(str(app), quiet=1, force=True, maxlevels=20)
