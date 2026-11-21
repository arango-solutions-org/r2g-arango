"""Exports of a project's mapping (#54).

Path mode first: ``export-csi`` and ``export-r2rml`` without ``--project`` must keep their
defaults, output bytes, and the way they write through whatever ``--output`` names. The golden
files follow ``test_serialization_compat``: after an intended change to the emitted output,

    R2G_REGEN_COMPAT=1 python -m pytest tests/test_project_exports.py

and review the fixture diff carefully, since federation consumers read these documents.
"""

from __future__ import annotations

import os
import re
import stat
import subprocess
import sys
from pathlib import Path

import pytest
from test_serialization_compat import _REGEN, FIXTURES, MAPPING_GOLDEN, SCHEMA_GOLDEN
from typer.testing import CliRunner

from r2g.main import app

#: command -> (its default --output, its golden file)
COMMANDS = {
    "export-csi": ("mapping.csi.json", FIXTURES / "reference.csi.json"),
    "export-r2rml": ("mapping.r2rml.ttl", FIXTURES / "reference.r2rml.ttl"),
}
INPUTS = ["-c", str(MAPPING_GOLDEN), "-s", str(SCHEMA_GOLDEN)]


@pytest.fixture(autouse=True)
def _reset_structlog():
    """Restore structlog to real stderr after a CliRunner invocation."""
    yield
    import structlog

    structlog.reset_defaults()
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(0),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    )


def _stable(text: str) -> str:
    """The document without the two values that differ per run and per release."""
    return re.sub(r'("(?:generatedAt|producerVersion)": )"[^"]*"', r'\1"<stable>"', text)


def _golden(command: str) -> str:
    return COMMANDS[command][1].read_text(encoding="utf-8")


def _export(command: str, *args: str) -> None:
    result = CliRunner().invoke(app, [command, *INPUTS, *args])
    assert result.exit_code == 0, result.output


@pytest.mark.parametrize("command", COMMANDS)
def test_the_output_matches_its_golden_file(tmp_path, command):
    out = tmp_path / "out"
    _export(command, "-o", str(out))
    text = _stable(out.read_text(encoding="utf-8"))
    if _REGEN:
        COMMANDS[command][1].write_text(text, encoding="utf-8")
    assert text == _golden(command)


@pytest.mark.parametrize("command, says", [
    ("export-csi", '"kind": "relational"'),
    ("export-r2rml", "(source: relational)"),
])
def test_the_defaults_write_one_file_in_the_working_directory(tmp_path, monkeypatch, command, says):
    monkeypatch.chdir(tmp_path)
    _export(command)
    default = COMMANDS[command][0]
    assert [p.name for p in tmp_path.iterdir()] == [default]
    assert says in (tmp_path / default).read_text(encoding="utf-8")


@pytest.mark.parametrize("args, missing", [
    (["export-csi"], "--config"),
    (["export-r2rml"], "--config"),
    (["export-r2rml", "-c", str(MAPPING_GOLDEN)], "--schema"),
])
def test_a_required_option_left_out_exits_2_and_writes_nothing(tmp_path, monkeypatch, args, missing):
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 2
    assert missing in result.output
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("command", COMMANDS)
def test_output_can_be_dev_stdout(command):
    # A real process: CliRunner replaces sys.stdout, which /dev/stdout does not reach.
    code = "import sys; from r2g.main import app; app(sys.argv[1:])"
    done = subprocess.run([sys.executable, "-c", code, command, *INPUTS, "-o", "/dev/stdout"],
                          capture_output=True, text=True, encoding="utf-8", timeout=60)
    assert done.returncode == 0, done.stderr
    assert _stable(done.stdout).startswith(_golden(command))


@pytest.mark.parametrize("command", COMMANDS)
def test_output_through_a_symlink_writes_its_target_and_keeps_the_link(tmp_path, command):
    target = tmp_path / "target"
    target.write_text("old", encoding="utf-8")
    link = tmp_path / "link"
    link.symlink_to(target)
    _export(command, "-o", str(link))
    assert link.is_symlink() and os.readlink(link) == str(target)
    assert _stable(target.read_text(encoding="utf-8")) == _golden(command)


@pytest.mark.parametrize("command", COMMANDS)
def test_an_existing_output_keeps_its_file_and_mode(tmp_path, command):
    out = tmp_path / "out"
    out.write_text("old", encoding="utf-8")
    out.chmod(0o640)
    inode = out.stat().st_ino
    _export(command, "-o", str(out))
    assert out.stat().st_ino == inode
    assert stat.S_IMODE(out.stat().st_mode) == 0o640


@pytest.mark.parametrize("command", COMMANDS)
@pytest.mark.parametrize("umask, mode", [(0o022, 0o644), (0o077, 0o600)])
def test_a_new_output_takes_its_mode_from_the_umask(tmp_path: Path, command, umask, mode):
    out = tmp_path / "out"
    before = os.umask(umask)
    try:
        _export(command, "-o", str(out))
    finally:
        os.umask(before)
    assert stat.S_IMODE(out.stat().st_mode) == mode
