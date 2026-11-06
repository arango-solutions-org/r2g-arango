from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from r2g.catalog import CatalogManager
from r2g.config import ConfigManager
from r2g.types import Column, MappingConfig, Schema, Table
from r2g.ui.server import create_app


@pytest.fixture
def catalog_dir(tmp_path):
    return str(tmp_path / "catalog")


@pytest.fixture
def schema():
    return Schema(
        tables={
            "users": Table(
                name="users",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="name", data_type="text"),
                    Column(name="email", data_type="text"),
                ],
                primary_key=["id"],
            ),
        }
    )


@pytest.fixture
def mapping_path(tmp_path, schema):
    config = ConfigManager.generate_default_config(schema)
    path = str(tmp_path / "mapping.yaml")
    ConfigManager.save_config(config, path)
    return path


@pytest.fixture
def seeded_client(catalog_dir, schema, mapping_path):
    """Client with a project already set up (source + snapshot + project)."""
    from starlette.testclient import TestClient

    mgr = CatalogManager(catalog_dir)
    mgr.add_source(
        name="pg_src",
        source_type="postgresql",
        connection_string="postgresql://localhost/testdb",
    )
    mgr.create_snapshot("pg_src", schema)
    mgr.create_project(
        name="demo",
        source_name="pg_src",
        mapping_config_path=mapping_path,
        arango_database="demo_graph",
    )

    app = create_app(catalog_dir=catalog_dir)
    return TestClient(app)


@pytest.fixture
def client(catalog_dir):
    from starlette.testclient import TestClient

    app = create_app(catalog_dir=catalog_dir)
    return TestClient(app)


class TestLoadEndpoint:
    def test_load_missing_project_returns_404(self, client):
        resp = client.post("/api/projects/nonexistent/load", json={})
        assert resp.status_code == 404

    def test_load_no_snapshot_returns_conflict(self, client, catalog_dir):
        mgr = CatalogManager(catalog_dir)
        mgr.add_source("src", "postgresql", "postgresql://localhost/db")
        mgr.create_project("proj", "src", "/fake/path.yaml")
        resp = client.post("/api/projects/proj/load", json={})
        assert resp.status_code == 409

    @patch("r2g.ui.server.StreamingPipeline")
    @patch("r2g.ui.server.ArangoWriter")
    def test_load_returns_202_with_load_id(self, mock_writer_cls, mock_pipeline_cls, seeded_client):
        mock_pipeline = MagicMock()
        mock_pipeline.errors = {}
        mock_pipeline.run.return_value = {
            "documents": [("users", 100)],
            "edges": [],
            "elapsed_seconds": 1.5,
        }
        mock_pipeline_cls.return_value = mock_pipeline

        resp = seeded_client.post("/api/projects/demo/load", json={"dry_run": True})
        assert resp.status_code == 202
        data = resp.json()
        assert "load_id" in data
        assert data["status"] == "started"

        time.sleep(0.5)

    @patch("r2g.ui.server.StreamingPipeline")
    @patch("r2g.ui.server.ArangoWriter")
    def test_load_validation_failure_returns_400(
        self, mock_writer_cls, mock_pipeline_cls, catalog_dir, tmp_path
    ):
        from starlette.testclient import TestClient

        bad_config = MappingConfig(
            collections={
                "bad": __import__("r2g.types", fromlist=["CollectionMapping"]).CollectionMapping(
                    source_table="nonexistent_table",
                    target_collection="bad",
                )
            }
        )
        bad_path = str(tmp_path / "bad_mapping.yaml")
        ConfigManager.save_config(bad_config, bad_path)

        schema = Schema(
            tables={
                "users": Table(
                    name="users",
                    columns=[Column(name="id", data_type="integer", is_primary_key=True)],
                    primary_key=["id"],
                ),
            }
        )

        mgr = CatalogManager(catalog_dir)
        mgr.add_source("src2", "postgresql", "postgresql://localhost/db")
        mgr.create_snapshot("src2", schema)
        mgr.create_project("proj_bad", "src2", bad_path)

        app = create_app(catalog_dir=catalog_dir)
        client = TestClient(app)
        resp = client.post("/api/projects/proj_bad/load", json={})
        assert resp.status_code == 400
        assert "validation_errors" in resp.json()["detail"]

    def test_load_into_system_database_is_refused(self, catalog_dir, schema, mapping_path):
        from starlette.testclient import TestClient

        mgr = CatalogManager(catalog_dir)
        mgr.add_source("sys_src", "postgresql", "postgresql://localhost/db")
        mgr.create_snapshot("sys_src", schema)
        mgr.create_project(
            "sys_proj", "sys_src", mapping_path, arango_database="_system"
        )

        app = create_app(catalog_dir=catalog_dir)
        client = TestClient(app)
        resp = client.post("/api/projects/sys_proj/load", json={})
        assert resp.status_code == 400
        assert "_system" in resp.json()["detail"]


class TestLoadSensitivityGate:
    """Phase 9b: the load gate excludes above-threshold fields by default."""

    def _gated_client(self, catalog_dir, tmp_path):
        from starlette.testclient import TestClient

        from r2g.types import Classification

        schema = Schema(tables={
            "users": Table(name="users", columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="name", data_type="text"),
                Column(name="email", data_type="text",
                       classification=Classification(tags=["PII.Sensitive"])),
            ], primary_key=["id"]),
        })
        config = ConfigManager.generate_default_config(schema)
        path = str(tmp_path / "gated_mapping.yaml")
        ConfigManager.save_config(config, path)
        mgr = CatalogManager(catalog_dir)
        mgr.add_source("gsrc", "postgresql", "postgresql://localhost/db")
        mgr.create_snapshot("gsrc", schema)
        mgr.create_project("gproj", "gsrc", path, arango_database="g_graph")
        return TestClient(create_app(catalog_dir=catalog_dir))

    @patch("r2g.ui.server.StreamingPipeline")
    @patch("r2g.ui.server.ArangoWriter")
    def test_pii_excluded_by_default(self, mock_writer, mock_pipeline, catalog_dir, tmp_path):
        mock_pipeline.return_value = MagicMock(errors={})
        client = self._gated_client(catalog_dir, tmp_path)
        resp = client.post("/api/projects/gproj/load", json={"dry_run": True})
        assert resp.status_code == 202, resp.text
        excluded = resp.json()["excluded_sensitive_fields"]
        props = {e["property"] for e in excluded}
        assert "email" in props
        time.sleep(0.2)

    @patch("r2g.ui.server.StreamingPipeline")
    @patch("r2g.ui.server.ArangoWriter")
    def test_allow_sensitive_loads_everything(self, mock_writer, mock_pipeline, catalog_dir, tmp_path):
        mock_pipeline.return_value = MagicMock(errors={})
        client = self._gated_client(catalog_dir, tmp_path)
        resp = client.post(
            "/api/projects/gproj/load", json={"dry_run": True, "allow_sensitive": True}
        )
        assert resp.status_code == 202, resp.text
        assert resp.json()["excluded_sensitive_fields"] == []
        time.sleep(0.2)

    @patch("r2g.ui.server.StreamingPipeline")
    @patch("r2g.ui.server.ArangoWriter")
    def test_lineage_manifest_written(self, mock_writer, mock_pipeline, catalog_dir, tmp_path):
        import json as _json

        mock_pipeline.return_value = MagicMock(errors={})
        client = self._gated_client(catalog_dir, tmp_path)
        client.post("/api/projects/gproj/load", json={"dry_run": True})
        manifest = tmp_path / "governance" / "lineage.json"
        assert manifest.exists()
        data = _json.loads(manifest.read_text())
        handling = {e["target"]: e["handling"] for e in data["fields"]}
        assert handling["users.email"] == "excluded"
        time.sleep(0.2)

    @patch("r2g.ui.server.StreamingPipeline")
    @patch("r2g.ui.server.ArangoWriter")
    def test_emit_governance_writes_artifacts(self, mock_writer, mock_pipeline, catalog_dir, tmp_path):
        # Phase 9c: a governed load with emit_governance drops the full set.
        mock_pipeline.return_value = MagicMock(errors={})
        client = self._gated_client(catalog_dir, tmp_path)
        resp = client.post(
            "/api/projects/gproj/load",
            json={"dry_run": True, "emit_governance": True, "tier_layout": True},
        )
        assert resp.status_code == 202, resp.text
        gov = tmp_path / "governance"
        assert (gov / "classification-manifest.json").exists()
        assert (gov / "suggested-rbac.json").exists()
        assert (gov / "policy.rego").exists()
        assert (gov / "tier-layout.json").exists()
        time.sleep(0.2)


class TestLoadStatusEndpoint:
    @patch("r2g.ui.server.StreamingPipeline")
    @patch("r2g.ui.server.ArangoWriter")
    def test_status_after_load(self, mock_writer_cls, mock_pipeline_cls, seeded_client):
        mock_pipeline = MagicMock()
        mock_pipeline.errors = {}
        mock_pipeline.run.return_value = {
            "documents": [("users", 50)],
            "edges": [],
            "elapsed_seconds": 0.5,
        }
        mock_pipeline_cls.return_value = mock_pipeline

        resp = seeded_client.post("/api/projects/demo/load", json={})
        load_id = resp.json()["load_id"]

        time.sleep(0.5)

        resp = seeded_client.get(f"/api/projects/demo/load/{load_id}/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["load_id"] == load_id
        assert data["status"] in ("running", "completed")

    def test_status_unknown_load(self, seeded_client):
        resp = seeded_client.get("/api/projects/demo/load/unknown-id/status")
        assert resp.status_code == 200
        assert resp.json()["status"] == "unknown"


class TestAutoMapEndpoint:
    def test_auto_map_returns_config(self, seeded_client):
        resp = seeded_client.post("/api/projects/demo/auto-map")
        assert resp.status_code == 200
        data = resp.json()
        assert "collections" in data
        assert "edges" in data
        assert "users" in data["collections"]

    def test_auto_map_missing_project(self, client):
        resp = client.post("/api/projects/nonexistent/auto-map")
        assert resp.status_code == 404

    def test_auto_map_no_snapshot(self, client, catalog_dir, tmp_path):
        mgr = CatalogManager(catalog_dir)
        mgr.add_source("src", "postgresql", "postgresql://localhost/db")
        mapping_path = str(tmp_path / "m.yaml")
        ConfigManager.save_config(MappingConfig(), mapping_path)
        mgr.create_project("proj", "src", mapping_path)
        resp = client.post("/api/projects/proj/auto-map")
        assert resp.status_code == 409


class TestTargetEndpoints:
    def test_list_targets_empty(self, client):
        resp = client.get("/api/targets")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_add_target(self, client):
        resp = client.post("/api/targets", json={
            "name": "local",
            "endpoint": "http://localhost:8529",
            "database": "_system",
        })
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "local"
        assert data["endpoint"] == "http://localhost:8529"

    def test_add_target_duplicate(self, client):
        client.post("/api/targets", json={"name": "dup", "endpoint": "http://localhost:8529"})
        resp = client.post("/api/targets", json={"name": "dup", "endpoint": "http://localhost:8529"})
        assert resp.status_code == 409

    def test_remove_target_not_found(self, client):
        resp = client.delete("/api/targets/nonexistent")
        assert resp.status_code == 404

    def test_remove_target(self, client):
        client.post("/api/targets", json={"name": "to_delete", "endpoint": "http://localhost:8529"})
        resp = client.delete("/api/targets/to_delete")
        assert resp.status_code == 200

    def test_introspect_target_not_found(self, client):
        resp = client.post("/api/targets/nonexistent/introspect")
        assert resp.status_code == 404


class TestLoadErrorsEndpoint:
    def test_errors_empty(self, seeded_client):
        resp = seeded_client.get("/api/projects/demo/load/fake-id/errors")
        assert resp.status_code == 200
        data = resp.json()
        assert data["errors"] == []
        assert data["count"] == 0
