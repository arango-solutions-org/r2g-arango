"""`r2g target add|list|remove`: how an administrator registers the targets a project
may be created on under a control plane, where the Studio does not add them (P14.18)."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from r2g.catalog import CatalogManager
from r2g.main import app

runner = CliRunner()


@pytest.fixture(autouse=True)
def _tenant(tmp_path, monkeypatch):
    import structlog

    def reset():
        structlog.reset_defaults()
        structlog.configure(
            wrapper_class=structlog.make_filtering_bound_logger(0),
            logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        )

    reset()
    # structlog caches a module's logger on first use; one first used inside CliRunner
    # would keep the runner's closed stream and break every later test that logs.
    monkeypatch.setattr("r2g.catalog.logger", MagicMock())
    monkeypatch.setattr("r2g.main.log", MagicMock())
    monkeypatch.setattr("r2g.security.logger", MagicMock())
    for var in ("ARANGO_ENDPOINT", "ARANGO_DB", "ARANGO_USER", "ARANGO_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("R2G_CATALOG_DIR", str(tmp_path / "tenant"))
    yield
    reset()


def test_add_list_and_remove_a_target_in_the_tenants_catalog(tmp_path):
    r = runner.invoke(app, ["target", "add", "--name", "graph-a", "--endpoint",
                            "http://arango-a:8529", "--database", "graph_a",
                            "--username", "svc", "--password", "s3cret"])
    assert r.exit_code == 0, r.output
    target = CatalogManager(tmp_path / "tenant").get_target("graph-a")
    assert (target.endpoint, target.database, target.username, target.password) == (
        "http://arango-a:8529", "graph_a", "svc", "s3cret")

    r = runner.invoke(app, ["target", "list"])
    assert r.exit_code == 0 and "graph-a" in r.output and "s3cret" not in r.output

    r = runner.invoke(app, ["target", "add", "--name", "graph-a", "--database", "x"])
    assert r.exit_code == 1  # names are unique

    assert runner.invoke(app, ["target", "remove", "graph-a"]).exit_code == 0
    assert CatalogManager(tmp_path / "tenant").get_target("graph-a") is None
    assert runner.invoke(app, ["target", "remove", "graph-a"]).exit_code == 1


def test_a_target_can_be_registered_for_one_project(tmp_path):
    """Under a control plane only that project is created on it and loads to it
    (tests/test_control_plane_mode.py); without --project the field is not written."""
    r = runner.invoke(app, ["target", "add", "--name", "graph-a", "--database", "graph_a",
                            "--project", "customer-360"])
    assert r.exit_code == 0, r.output
    assert CatalogManager(tmp_path / "tenant").get_target("graph-a").project == "customer-360"
    r = runner.invoke(app, ["target", "list"])
    assert "customer-360" in r.output
    runner.invoke(app, ["target", "add", "--name", "graph-b", "--database", "graph_b"])
    saved = (tmp_path / "tenant" / "catalog.json").read_text()
    import json

    targets = json.loads(saved)["targets"]
    assert targets["graph-a"]["project"] == "customer-360"
    assert "project" not in targets["graph-b"]
