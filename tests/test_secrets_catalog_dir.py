"""`r2g secrets init|status` name the key file the catalog actually reads.

The catalog keeps its key at ``<catalog dir>/secret.key`` and honours
``R2G_CATALOG_DIR``. The secrets commands used to hardwire ``~/.r2g``, so under
``R2G_CATALOG_DIR`` (the control-plane image sets it) ``status`` reported a file that
did not exist and ``init`` wrote a second key the catalog never read.
"""

from __future__ import annotations

import sys

import pytest
from typer.testing import CliRunner

from r2g.catalog import CatalogManager, default_catalog_dir
from r2g.main import app
from r2g.security import SECRET_ENV, SECRET_FILENAME, load_secret_key

runner = CliRunner()


@pytest.fixture(autouse=True)
def _env(tmp_path, monkeypatch):
    import structlog

    def reset():
        structlog.reset_defaults()
        structlog.configure(
            wrapper_class=structlog.make_filtering_bound_logger(0),
            logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        )

    reset()
    monkeypatch.delenv(SECRET_ENV, raising=False)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("R2G_CATALOG_DIR", str(tmp_path / "tenant"))
    yield
    reset()


def _out(result) -> str:
    return " ".join(result.output.split()).replace(" ", "")


def test_default_catalog_dir_follows_the_env(tmp_path, monkeypatch):
    assert default_catalog_dir() == tmp_path / "tenant"
    monkeypatch.delenv("R2G_CATALOG_DIR")
    assert default_catalog_dir() == tmp_path / "home" / ".r2g"


def test_status_names_the_key_the_catalog_created(tmp_path):
    CatalogManager()  # creates <R2G_CATALOG_DIR>/secret.key
    key = tmp_path / "tenant" / SECRET_FILENAME
    assert key.exists()
    result = runner.invoke(app, ["secrets", "status"])
    assert result.exit_code == 0, result.output
    assert f"Usingkeyfile{key}" in _out(result)


def test_init_writes_the_key_the_catalog_then_reads(tmp_path):
    result = runner.invoke(app, ["secrets", "init"])
    assert result.exit_code == 0, result.output
    key = tmp_path / "tenant" / SECRET_FILENAME
    assert key.exists()
    assert not (tmp_path / "home" / ".r2g" / SECRET_FILENAME).exists()
    assert load_secret_key(CatalogManager().dir) == key.read_bytes().strip()


def test_init_refuses_to_replace_the_catalog_key_without_force(tmp_path):
    CatalogManager()
    before = (tmp_path / "tenant" / SECRET_FILENAME).read_bytes()
    result = runner.invoke(app, ["secrets", "init"])
    assert "alreadyexists" in _out(result)
    assert (tmp_path / "tenant" / SECRET_FILENAME).read_bytes() == before
