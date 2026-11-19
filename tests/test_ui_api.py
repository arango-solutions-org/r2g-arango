from __future__ import annotations

import shlex
from types import SimpleNamespace

import pytest

from r2g.catalog import CatalogManager, SchemaSnapshot
from r2g.config import ConfigManager
from r2g.types import (
    CollectionMapping,
    Column,
    LpgLayout,
    MappingConfig,
    Schema,
    Table,
)
from r2g.ui.server import create_app


@pytest.fixture
def catalog_dir(tmp_path):
    return str(tmp_path / "catalog")


@pytest.fixture
def client(catalog_dir):
    from starlette.testclient import TestClient

    app = create_app(catalog_dir=catalog_dir)
    return TestClient(app)


class TestHealthEndpoint:
    def test_health(self, client):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


class TestSourceEndpoints:
    def test_list_sources_empty(self, client):
        resp = client.get("/api/sources")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_add_source(self, client):
        resp = client.post("/api/sources", json={
            "name": "test_pg",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        assert resp.status_code == 201
        assert resp.json()["name"] == "test_pg"

    def test_add_duplicate_source(self, client):
        client.post("/api/sources", json={
            "name": "test_pg",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        resp = client.post("/api/sources", json={
            "name": "test_pg",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        assert resp.status_code == 409

    def test_remove_source(self, client):
        client.post("/api/sources", json={
            "name": "to_remove",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        resp = client.delete("/api/sources/to_remove")
        assert resp.status_code == 200

    def test_remove_nonexistent_source(self, client):
        resp = client.delete("/api/sources/nonexistent")
        assert resp.status_code == 404

    def test_add_snowflake_source_is_accepted(self, client):
        resp = client.post("/api/sources", json={
            "name": "analytics_sf",
            "source_type": "snowflake",
            "connection_string": (
                "snowflake://svc:x@xy12345.us-east-1/ANALYTICS/CORE"
                "?warehouse=WH&role=R"
            ),
        })
        assert resp.status_code == 201
        body = resp.json()
        assert body["source_type"] == "snowflake"
        # connection string should be redacted
        assert "svc:x@" not in body["connection_string"]

    def test_snowflake_key_path_and_passphrase_are_redacted(self, client):
        response = client.post(
            "/api/sources",
            json={
                "name": "keypair_sf",
                "source_type": "snowflake",
                "connection_string": (
                    "snowflake://svc:@xy12345/ANALYTICS/CORE"
                    "?warehouse=WH"
                    "&private_key_file=/Users/demo/private-key.p8"
                    "&private_key_file_pwd=sentinel-passphrase"
                ),
            },
        )
        assert response.status_code == 201
        rendered = response.text
        assert "private-key.p8" not in rendered
        assert "sentinel-passphrase" not in rendered

        listed = client.get("/api/sources")
        assert listed.status_code == 200
        assert "private-key.p8" not in listed.text
        assert "sentinel-passphrase" not in listed.text

    def test_add_csv_source_and_snapshot(self, client, tmp_path):
        csv_dir = tmp_path / "dumps"
        csv_dir.mkdir()
        (csv_dir / "customers.csv").write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
        resp = client.post("/api/sources", json={
            "name": "csv_src",
            "source_type": "csv",
            "connection_string": str(csv_dir),
            "source_params": {"delimiter": ",", "has_header": True},
        })
        assert resp.status_code == 201
        assert resp.json()["source_type"] == "csv"

        snap = client.post("/api/sources/csv_src/snapshot")
        assert snap.status_code == 200, snap.text
        assert snap.json()["tables"] == 1

        schema = client.get("/api/sources/csv_src/schema")
        assert schema.status_code == 200
        assert "customers" in schema.json()["tables"]

    def test_add_kafka_source_accepted(self, client):
        resp = client.post("/api/sources", json={
            "name": "kafka_src",
            "source_type": "kafka",
            "connection_string": "localhost:9092",
            "source_params": {"schema_registry_url": "http://localhost:8081", "topic": "orders"},
        })
        assert resp.status_code == 201
        assert resp.json()["source_type"] == "kafka"

    def test_add_source_rejects_unsupported_type(self, client):
        resp = client.post("/api/sources", json={
            "name": "my_oracle",
            "source_type": "oracle",
            "connection_string": "oracle://u:p@h/db",
        })
        assert resp.status_code in (400, 409)
        assert "oracle" in resp.text.lower() or "unsupported" in resp.text.lower()

    def test_snapshot_snowflake_without_driver_returns_501(self, client, monkeypatch):
        import sys

        client.post("/api/sources", json={
            "name": "analytics_sf",
            "source_type": "snowflake",
            "connection_string": (
                "snowflake://svc:x@xy12345/ANALYTICS/CORE?warehouse=WH"
            ),
        })
        monkeypatch.setitem(sys.modules, "snowflake", None)
        monkeypatch.setitem(sys.modules, "snowflake.connector", None)
        resp = client.post("/api/sources/analytics_sf/snapshot")
        assert resp.status_code == 501
        body = resp.json()
        detail = body.get("detail", "").lower()
        assert "r2g-arango[snowflake]" in detail or "snowflake-connector" in detail


class TestSnowflakeCustomer360Preset:
    @staticmethod
    def _set_environment(monkeypatch, tmp_path):
        key_path = tmp_path / "snowflake-key.p8"
        key_path.touch()
        values = {
            "SNOWFLAKE_ACCOUNT": "example-account",
            "SNOWFLAKE_USER": "r2g-demo",
            "SNOWFLAKE_PRIVATE_KEY_FILE": str(key_path),
            "SNOWFLAKE_WAREHOUSE": "DEMO_WH",
            "SNOWFLAKE_ROLE": "CDF_RO",
        }
        for name, value in values.items():
            monkeypatch.setenv(name, value)

    @staticmethod
    def _fake_capture(catalog, source_name, *, schema_name):
        from relational_schema_analyzer import apply_key_overlay, overlay_summary

        from r2g.demo.snowflake_customer_360 import OVERLAY_SOURCE, load_overlay
        from r2g.snapshot import overlay_fingerprint

        overlay = load_overlay()
        column_names: dict[str, set[str]] = {
            table: set(spec.get("primaryKey", []))
            for table, spec in overlay["tables"].items()
        }
        for table, spec in overlay["tables"].items():
            for fk in spec.get("foreignKeys", []):
                column_names[table].update(fk["columns"])
                ref = fk["references"]
                column_names[ref["table"]].update(ref["columns"])
        keyless = Schema(
            tables={
                table: Table(
                    name=table,
                    columns=[
                        Column(name=column, data_type="varchar", is_nullable=False)
                        for column in sorted(columns)
                    ],
                )
                for table, columns in column_names.items()
            }
        )
        physical = apply_key_overlay(keyless, overlay)
        summary = overlay_summary(physical)
        schema = Schema.model_validate(physical.model_dump())
        snap = catalog.create_snapshot(
            source_name,
            schema,
            pg_schema=schema_name,
            key_overlay_summary=summary,
            key_overlay_source=OVERLAY_SOURCE,
            key_overlay_fingerprint=overlay_fingerprint(overlay),
        )
        return snap, SimpleNamespace(
            schema=schema,
            key_overlay_summary=summary,
            key_overlay_source=OVERLAY_SOURCE,
            key_overlay_fingerprint=overlay_fingerprint(overlay),
            classifications_applied=0,
        )

    def test_requires_environment_without_exposing_values(
        self, client, monkeypatch
    ):
        for name in (
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PRIVATE_KEY_FILE",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_ROLE",
        ):
            monkeypatch.delenv(name, raising=False)

        response = client.post(
            "/api/demo-presets/snowflake-customer-360/install",
            json={},
        )

        assert response.status_code == 400
        detail = response.json()["detail"]
        assert detail["code"] == "snowflake_environment_incomplete"
        assert set(detail["missing"]) == {
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PRIVATE_KEY_FILE",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_ROLE",
        }

    def test_requires_non_system_target(
        self, client, monkeypatch, tmp_path
    ):
        self._set_environment(monkeypatch, tmp_path)

        response = client.post(
            "/api/demo-presets/snowflake-customer-360/install",
            json={},
        )

        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "arango_target_required"

    def test_install_is_idempotent_and_preserves_env_references(
        self, client, catalog_dir, monkeypatch, tmp_path
    ):
        self._set_environment(monkeypatch, tmp_path)
        monkeypatch.setattr(
            "r2g.snapshot.capture_source_snapshot",
            self._fake_capture,
        )
        target = client.post(
            "/api/targets",
            json={
                "name": "demo_arango",
                "endpoint": "http://localhost:8529",
                "database": "customer360_demo",
                "username": "root",
                "password": "target-secret",
            },
        )
        assert target.status_code == 201

        first = client.post(
            "/api/demo-presets/snowflake-customer-360/install",
            json={"target_name": "demo_arango"},
        )
        second = client.post(
            "/api/demo-presets/snowflake-customer-360/install",
            json={"target_name": "demo_arango"},
        )

        assert first.status_code == 200, first.text
        assert first.json()["snapshot"]["tables"] == 5
        assert first.json()["overlay"]["summary"]["foreignKeys"] == 6
        assert first.json()["mapping"] == {
            "status": "created",
            "collections": 5,
            "edges": 6,
        }
        assert second.status_code == 200, second.text
        assert second.json()["source"]["status"] == "reused"
        assert second.json()["snapshot"]["status"] == "reused"
        assert second.json()["project"]["status"] == "reused"
        assert second.json()["mapping"]["status"] == "reused"

        mgr = CatalogManager(catalog_dir)
        source = mgr.get_source("snowflake_customer_360")
        assert source is not None
        assert "$SNOWFLAKE_PRIVATE_KEY_FILE" in source.connection_string
        assert str(tmp_path) not in source.connection_string

        graph = client.get(
            "/api/projects/snowflake_customer_360/graph-data"
        ).json()
        assert len(graph["config"]["edges"]) == 6
        assert {
            edge["keyProvenance"] for edge in graph["config"]["edges"]
        } == {"curated-overlay"}

    def test_install_rebuilds_snapshot_when_overlay_content_changes(
        self, client, catalog_dir, monkeypatch, tmp_path
    ):
        from r2g.demo.snowflake_customer_360 import (
            OVERLAY_SOURCE,
            load_overlay,
        )
        from r2g.snapshot import overlay_fingerprint

        self._set_environment(monkeypatch, tmp_path)
        captures = 0

        def capture(*args, **kwargs):
            nonlocal captures
            captures += 1
            return self._fake_capture(*args, **kwargs)

        monkeypatch.setattr(
            "r2g.snapshot.capture_source_snapshot",
            capture,
        )
        target = client.post(
            "/api/targets",
            json={
                "name": "demo_arango",
                "endpoint": "http://localhost:8529",
                "database": "customer360_demo",
                "username": "root",
                "password": "target-secret",
            },
        )
        assert target.status_code == 201
        first = client.post(
            "/api/demo-presets/snowflake-customer-360/install",
            json={"target_name": "demo_arango"},
        )
        assert first.status_code == 200, first.text

        mgr = CatalogManager(catalog_dir)
        latest = mgr.get_latest_snapshot("snowflake_customer_360")
        assert latest is not None
        changed_overlay = {
            **load_overlay(),
            "description": "Same key counts, different reviewed content",
        }
        stale = mgr.create_snapshot(
            "snowflake_customer_360",
            latest.schema_data,
            pg_schema=latest.pg_schema,
            key_overlay_summary=latest.key_overlay_summary,
            key_overlay_source=OVERLAY_SOURCE,
            key_overlay_fingerprint=overlay_fingerprint(changed_overlay),
        )
        assert mgr.get_latest_snapshot("snowflake_customer_360").id == stale.id

        second = client.post(
            "/api/demo-presets/snowflake-customer-360/install",
            json={"target_name": "demo_arango"},
        )

        assert second.status_code == 200, second.text
        assert second.json()["snapshot"]["status"] == "created"
        assert captures == 2

    def test_install_rebuilds_a_snapshot_that_lost_overlay_unique_keys(
        self, client, catalog_dir, monkeypatch, tmp_path
    ):
        """A format-1 snapshot erased the overlay's UNIQUE keys on save, so reusing
        it -- even with matching source, tables and fingerprint -- would silently
        drop every FK inferred through those natural keys."""
        import r2g.demo.snowflake_customer_360 as demo
        from r2g.catalog import SchemaSnapshot

        bundled = demo.load_overlay()
        with_unique = {
            **bundled,
            "tables": {
                **bundled["tables"],
                "ACCOUNTS": {
                    **bundled["tables"]["ACCOUNTS"],
                    "uniqueConstraints": [["ACCOUNT_ID"]],
                },
            },
        }
        monkeypatch.setattr(demo, "load_overlay", lambda: with_unique)
        self._set_environment(monkeypatch, tmp_path)
        captures = 0

        def capture(*args, **kwargs):
            nonlocal captures
            captures += 1
            return self._fake_capture(*args, **kwargs)

        monkeypatch.setattr("r2g.snapshot.capture_source_snapshot", capture)
        assert (
            client.post(
                "/api/targets",
                json={
                    "name": "demo_arango",
                    "endpoint": "http://localhost:8529",
                    "database": "customer360_demo",
                    "username": "root",
                    "password": "target-secret",
                },
            ).status_code
            == 201
        )
        first = client.post(
            "/api/demo-presets/snowflake-customer-360/install",
            json={"target_name": "demo_arango"},
        )
        assert first.status_code == 200, first.text

        # Plant what an older r2g left behind: identical in every compared field,
        # but format 1, so the overlay's UNIQUE keys are gone from schema_data.
        mgr = CatalogManager(catalog_dir)
        latest = mgr.get_latest_snapshot("snowflake_customer_360")
        assert latest is not None
        legacy = latest.model_dump(mode="json")
        legacy.update(id="legacy", captured_at="2099-01-01T00:00:00Z")
        legacy.pop("schema_format_version", None)
        catalog = mgr._load()
        catalog.snapshots["legacy"] = SchemaSnapshot.model_validate(legacy)
        mgr._save(catalog)
        assert mgr.get_latest_snapshot("snowflake_customer_360").lost_overlay_unique_keys

        second = client.post(
            "/api/demo-presets/snowflake-customer-360/install",
            json={"target_name": "demo_arango"},
        )
        assert second.status_code == 200, second.text
        assert second.json()["snapshot"]["status"] == "created"
        assert captures == 2


class TestInferFksEndpoint:
    def _setup(self, client, catalog_dir):
        client.post("/api/sources", json={
            "name": "src",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        mgr = CatalogManager(catalog_dir)
        schema = Schema(tables={
            "users": Table(name="users", columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
            ], primary_key=["id"]),
            "orders": Table(name="orders", columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="user_id", data_type="integer"),
            ], primary_key=["id"]),
        })
        mgr.create_snapshot("src", schema)

    def test_infer_fks_returns_user_id_candidate(self, client, catalog_dir):
        self._setup(client, catalog_dir)
        resp = client.post("/api/sources/src/infer-fks")
        assert resp.status_code == 200
        body = resp.json()
        assert body["source"] == "src"
        assert body["sample_used"] is False
        cands = body["candidates"]
        assert len(cands) == 1
        c = cands[0]
        assert c["table"] == "orders"
        assert c["columns"] == ["user_id"]
        assert c["foreign_table"] == "users"
        assert c["foreign_columns"] == ["id"]
        assert 0.0 <= c["confidence"] <= 1.0

    def test_infer_fks_without_snapshot_returns_400(self, client):
        client.post("/api/sources", json={
            "name": "empty",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        resp = client.post("/api/sources/empty/infer-fks")
        assert resp.status_code == 400
        assert "snapshot" in resp.json()["detail"].lower()

    def test_infer_fks_unknown_source_returns_404(self, client):
        resp = client.post("/api/sources/nonexistent/infer-fks")
        assert resp.status_code == 404

    def test_infer_fks_min_confidence_filters_results(self, client, catalog_dir):
        self._setup(client, catalog_dir)
        resp = client.post(
            "/api/sources/src/infer-fks",
            json={"min_confidence": 0.99},
        )
        assert resp.status_code == 200
        assert resp.json()["candidates"] == []

    def test_analyze_denorm_reports_repeating_group(self, client, catalog_dir):
        client.post("/api/sources", json={
            "name": "rg",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        mgr = CatalogManager(catalog_dir)
        schema = Schema(tables={
            "contact": Table(name="contact", columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="phone1", data_type="text"),
                Column(name="phone2", data_type="text"),
            ], primary_key=["id"]),
        })
        mgr.create_snapshot("rg", schema)
        resp = client.post("/api/sources/rg/analyze-denorm")
        assert resp.status_code == 200
        body = resp.json()
        assert body["source"] == "rg"
        assert body["sample_used"] is False
        kinds = {f["kind"] for f in body["findings"]}
        assert "repeating_group" in kinds

    def test_analyze_denorm_without_snapshot_returns_400(self, client):
        client.post("/api/sources", json={
            "name": "empty2",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        resp = client.post("/api/sources/empty2/analyze-denorm")
        assert resp.status_code == 400

    def test_analyze_denorm_unknown_source_returns_404(self, client):
        resp = client.post("/api/sources/nope/analyze-denorm")
        assert resp.status_code == 404

    def test_analyze_denorm_skips_sampling_for_snowflake_source(self, client, catalog_dir):
        client.post("/api/sources", json={
            "name": "sf2",
            "source_type": "snowflake",
            "connection_string": "snowflake://u:p@acct/DB/PUBLIC",
        })
        mgr = CatalogManager(catalog_dir)
        schema = Schema(tables={
            "CONTACT": Table(name="CONTACT", columns=[
                Column(name="ID", data_type="number", is_primary_key=True),
                Column(name="PHONE1", data_type="text"),
                Column(name="PHONE2", data_type="text"),
            ], primary_key=["ID"]),
        })
        mgr.create_snapshot("sf2", schema, pg_schema="PUBLIC")
        resp = client.post("/api/sources/sf2/analyze-denorm", json={"sample": True})
        assert resp.status_code == 200
        # Snowflake has no value sampler today; structural detectors still run.
        assert resp.json()["sample_used"] is False

    def test_infer_fks_skips_sampling_for_snowflake_source(self, client, catalog_dir):
        client.post("/api/sources", json={
            "name": "sf",
            "source_type": "snowflake",
            "connection_string": "snowflake://u:p@acct/DB/PUBLIC",
        })
        mgr = CatalogManager(catalog_dir)
        schema = Schema(tables={
            "USERS": Table(
                name="USERS",
                columns=[Column(name="ID", data_type="number", is_primary_key=True)],
                primary_key=["ID"],
            ),
            "ORDERS": Table(
                name="ORDERS",
                columns=[
                    Column(name="ID", data_type="number", is_primary_key=True),
                    Column(name="USER_ID", data_type="number"),
                ],
                primary_key=["ID"],
            ),
        })
        mgr.create_snapshot("sf", schema, pg_schema="PUBLIC")
        resp = client.post("/api/sources/sf/infer-fks", json={"sample": True})
        assert resp.status_code == 200
        body = resp.json()
        # Sampling is PG-only today — requesting it on a Snowflake source
        # must not crash and must not claim sampling was used.
        assert body["sample_used"] is False
        assert len(body["candidates"]) >= 1


class TestProjectEndpoints:
    def _add_source(self, client):
        client.post("/api/sources", json={
            "name": "src",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })

    def test_list_projects_empty(self, client):
        resp = client.get("/api/projects")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_create_project(self, client, tmp_path):
        self._add_source(client)
        mapping_path = str(tmp_path / "mapping.yaml")
        ConfigManager.save_config(MappingConfig(), mapping_path)
        resp = client.post("/api/projects", json={
            "name": "test_proj",
            "source_name": "src",
            "mapping_config_path": mapping_path,
        })
        assert resp.status_code == 201

    def test_create_project_missing_source(self, client):
        resp = client.post("/api/projects", json={
            "name": "test_proj",
            "source_name": "nonexistent",
            "mapping_config_path": "/fake/path",
        })
        assert resp.status_code == 400

    def test_create_project_rejects_traversal_name(self, client):
        self._add_source(client)
        resp = client.post("/api/projects", json={
            "name": "../escape",
            "source_name": "src",
        })
        assert resp.status_code == 400

    def test_mapping_path_is_confined_to_catalog_dir(self, client, tmp_path, catalog_dir):
        """A client-supplied mapping path must never be used as a write target."""
        self._add_source(client)
        evil = tmp_path / "evil.yaml"  # outside the catalog dir
        resp = client.post("/api/projects", json={
            "name": "confined",
            "source_name": "src",
            "mapping_config_path": str(evil),
        })
        assert resp.status_code == 201
        stored = resp.json()["mapping_config_path"]
        # Persisted path is derived under <catalog>/projects/<name>/, not the
        # client path.
        assert str(catalog_dir) in stored
        assert stored.endswith("projects/confined/mapping.yaml")
        # Saving the mapping writes to the safe path, never to the evil path.
        client.put("/api/projects/confined/mapping", json=MappingConfig().model_dump())
        assert not evil.exists()

    def test_create_project_with_metadata(self, client, tmp_path):
        self._add_source(client)
        mapping_path = str(tmp_path / "mapping.yaml")
        ConfigManager.save_config(MappingConfig(), mapping_path)
        resp = client.post("/api/projects", json={
            "name": "meta_proj",
            "source_name": "src",
            "mapping_config_path": mapping_path,
            "mapping_description": "A described mapping",
        })
        assert resp.status_code == 201
        assert resp.json()["mapping_description"] == "A described mapping"

    def test_patch_project_metadata(self, client, tmp_path):
        self._add_source(client)
        mapping_path = str(tmp_path / "mapping.yaml")
        ConfigManager.save_config(MappingConfig(), mapping_path)
        client.post("/api/projects", json={
            "name": "patch_proj",
            "source_name": "src",
            "mapping_config_path": mapping_path,
        })
        resp = client.patch("/api/projects/patch_proj", json={
            "mapping_name": "Renamed",
            "mapping_description": "new desc",
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["mapping_name"] == "Renamed"
        assert body["mapping_description"] == "new desc"

    def test_patch_project_not_found(self, client):
        resp = client.patch("/api/projects/nope", json={"mapping_name": "x"})
        assert resp.status_code == 404

    def test_patch_project_no_fields(self, client, tmp_path):
        self._add_source(client)
        mapping_path = str(tmp_path / "mapping.yaml")
        ConfigManager.save_config(MappingConfig(), mapping_path)
        client.post("/api/projects", json={
            "name": "empty_patch",
            "source_name": "src",
            "mapping_config_path": mapping_path,
        })
        resp = client.patch("/api/projects/empty_patch", json={})
        assert resp.status_code == 400

    def test_delete_project(self, client, tmp_path):
        self._add_source(client)
        mapping_path = str(tmp_path / "mapping.yaml")
        ConfigManager.save_config(MappingConfig(), mapping_path)
        client.post("/api/projects", json={
            "name": "del_proj",
            "source_name": "src",
            "mapping_config_path": mapping_path,
        })
        resp = client.delete("/api/projects/del_proj")
        assert resp.status_code == 200
        assert resp.json() == {"deleted": "del_proj"}
        # Gone from the listing.
        names = [p["name"] for p in client.get("/api/projects").json()]
        assert "del_proj" not in names

    def test_delete_project_not_found(self, client):
        resp = client.delete("/api/projects/nope")
        assert resp.status_code == 404


class TestApiAuth:
    def _app_client(self, catalog_dir, **kwargs):
        from starlette.testclient import TestClient

        return TestClient(create_app(catalog_dir=catalog_dir, **kwargs))

    def test_loopback_no_token_is_open(self, catalog_dir):
        client = self._app_client(catalog_dir)
        assert client.get("/api/sources").status_code == 200

    def test_token_required_when_configured(self, catalog_dir):
        client = self._app_client(catalog_dir, api_token="s3cr3t")
        assert client.get("/api/sources").status_code == 401
        assert client.get("/api/health").status_code == 200  # health stays open
        ok = client.get("/api/sources", headers={"Authorization": "Bearer s3cr3t"})
        assert ok.status_code == 200

    def test_non_loopback_bind_generates_token(self, catalog_dir):
        app = create_app(catalog_dir=catalog_dir, host="0.0.0.0")
        assert app.state.api_auth_required is True
        assert app.state.api_token


class TestMappingEndpoints:
    def _setup_project(self, client, tmp_path, catalog_dir):
        client.post("/api/sources", json={
            "name": "src",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        config = ConfigManager.generate_default_config(
            Schema(tables={
                "users": Table(name="users", columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="name", data_type="text"),
                ], primary_key=["id"]),
            })
        )
        mapping_path = str(tmp_path / "mapping.yaml")
        ConfigManager.save_config(config, mapping_path)
        mgr = CatalogManager(catalog_dir)
        schema = Schema(tables={
            "users": Table(name="users", columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="name", data_type="text"),
            ], primary_key=["id"]),
        })
        mgr.create_snapshot("src", schema)
        client.post("/api/projects", json={
            "name": "proj",
            "source_name": "src",
            "mapping_config_path": mapping_path,
        })

    def test_get_mapping(self, client, tmp_path, catalog_dir):
        self._setup_project(client, tmp_path, catalog_dir)
        resp = client.get("/api/projects/proj/mapping")
        assert resp.status_code == 200
        assert "collections" in resp.json()

    def test_get_mapping_not_found(self, client):
        resp = client.get("/api/projects/nonexistent/mapping")
        assert resp.status_code == 404

    def test_validate_mapping(self, client, tmp_path, catalog_dir):
        self._setup_project(client, tmp_path, catalog_dir)
        resp = client.post("/api/projects/proj/validate")
        assert resp.status_code == 200
        assert "valid" in resp.json()

    def test_arangoimport_preview_uses_saved_mapping(
        self, client, tmp_path, catalog_dir
    ):
        self._setup_project(client, tmp_path, catalog_dir)
        resp = client.post(
            "/api/projects/proj/arangoimport-preview",
            json={"mode": "jsonl"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["mapping_source"] == "saved"
        assert body["data_dir"] == "./output"
        assert body["documents"][0]["mapping_id"] == "users"
        assert "Studio Load streams data directly over HTTP" in body["load_note"]
        # @ARANGO_PASSWORD@ is resolved from the environment by the client
        # tool, so the secret never reaches the process list.
        assert "@ARANGO_PASSWORD@" in body["script"]

    def test_arangoimport_preview_accepts_shell_quoted_artifact_directory(
        self, client, tmp_path, catalog_dir
    ):
        self._setup_project(client, tmp_path, catalog_dir)
        artifact_dir = "./demo artifacts;touch should-not-run"

        resp = client.post(
            "/api/projects/proj/arangoimport-preview",
            json={"mode": "jsonl", "artifact_dir": artifact_dir},
        )

        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["data_dir"] == artifact_dir
        command = shlex.split(body["documents"][0]["command"])
        assert command[command.index("--file") + 1] == (
            f"{artifact_dir}/users.jsonl"
        )

    @pytest.mark.parametrize("artifact_dir", ["", "bad\npath", "bad\u0000path"])
    def test_arangoimport_preview_rejects_invalid_artifact_directory(
        self, client, tmp_path, catalog_dir, artifact_dir
    ):
        self._setup_project(client, tmp_path, catalog_dir)

        resp = client.post(
            "/api/projects/proj/arangoimport-preview",
            json={"artifact_dir": artifact_dir},
        )

        assert resp.status_code == 422

    def test_arangoimport_preview_reports_scrubbed_internal_error_as_500(
        self, client, tmp_path, catalog_dir, monkeypatch
    ):
        self._setup_project(client, tmp_path, catalog_dir)

        def fail(*args, **kwargs):
            raise RuntimeError(
                "Failed postgresql://demo:sentinel-secret@localhost/db"
            )

        monkeypatch.setattr(
            "r2g.ui.server.ArangoImportGenerator.build_plan",
            fail,
        )

        resp = client.post(
            "/api/projects/proj/arangoimport-preview",
            json={"mode": "jsonl"},
        )

        assert resp.status_code == 500
        detail = resp.json()["detail"]
        assert detail["message"] == "Could not render import preview"
        assert "sentinel-secret" not in detail["diagnostic"]

    def test_arangoimport_preview_uses_draft_and_focuses_collection(
        self, client, tmp_path, catalog_dir
    ):
        self._setup_project(client, tmp_path, catalog_dir)
        draft = client.get("/api/projects/proj/mapping").json()
        draft["collections"]["users"]["target_collection"] = "Customer"
        resp = client.post(
            "/api/projects/proj/arangoimport-preview",
            json={
                "mode": "csv",
                "scope": "collection",
                "mapping_id": "users",
                "mapping": draft,
            },
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["mapping_source"] == "draft"
        assert body["focused_command"]["target_collection"] == "Customer"
        assert "./dumps/users.csv" in body["focused_command"]["command"]

    def test_arangoimport_preview_refuses_csv_for_excluded_columns(
        self, client, tmp_path, catalog_dir
    ):
        self._setup_project(client, tmp_path, catalog_dir)
        draft = client.get("/api/projects/proj/mapping").json()
        draft["collections"]["users"]["exclude_fields"] = ["name"]
        resp = client.post(
            "/api/projects/proj/arangoimport-preview",
            json={"mode": "csv", "scope": "all", "mapping": draft},
        )
        assert resp.status_code == 422, resp.text
        assert "cannot reproduce this mapping" in resp.text
        assert "users: excluded fields" in resp.text

        jsonl = client.post(
            "/api/projects/proj/arangoimport-preview",
            json={"mode": "jsonl", "scope": "all", "mapping": draft},
        )
        assert jsonl.status_code == 200, jsonl.text

    def test_arangoimport_preview_rejects_unknown_identity_and_paths(
        self, client, tmp_path, catalog_dir
    ):
        self._setup_project(client, tmp_path, catalog_dir)
        unknown = client.post(
            "/api/projects/proj/arangoimport-preview",
            json={
                "scope": "collection",
                "mapping_id": "../../secret",
            },
        )
        assert unknown.status_code == 404

        path = client.post(
            "/api/projects/proj/arangoimport-preview",
            json={"output_path": "/tmp/import.sh"},
        )
        assert path.status_code == 422

    def test_arangoimport_preview_blocks_lpg_and_does_not_leak_source(
        self, client, tmp_path, catalog_dir
    ):
        self._setup_project(client, tmp_path, catalog_dir)
        draft = client.get("/api/projects/proj/mapping").json()
        draft["graph_layout"] = "lpg"
        draft["lpg"] = {}
        resp = client.post(
            "/api/projects/proj/arangoimport-preview",
            json={"mapping": draft},
        )
        assert resp.status_code == 422
        assert "does not support LPG" in resp.text
        assert "postgresql://localhost/test" not in resp.text

    def test_apply_naming_collections_pascal(self, client, tmp_path, catalog_dir):
        self._setup_project(client, tmp_path, catalog_dir)
        resp = client.post("/api/projects/proj/apply-naming", json={"collections": "pascal"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["collections"]["users"]["target_collection"] == "Users"
        assert data["naming_convention"]["collections"] == "pascal"

    def test_apply_naming_properties_camel(self, client, tmp_path, catalog_dir):
        client.post("/api/sources", json={
            "name": "src2",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        schema = Schema(tables={
            "order_line": Table(name="order_line", columns=[
                Column(name="line_id", data_type="integer", is_primary_key=True),
                Column(name="unit_price", data_type="numeric"),
            ], primary_key=["line_id"]),
        })
        config = ConfigManager.generate_default_config(schema)
        mapping_path = str(tmp_path / "mapping2.yaml")
        ConfigManager.save_config(config, mapping_path)
        CatalogManager(catalog_dir).create_snapshot("src2", schema)
        client.post("/api/projects", json={
            "name": "proj2", "source_name": "src2", "mapping_config_path": mapping_path,
        })
        resp = client.post(
            "/api/projects/proj2/apply-naming",
            json={"collections": "pascal", "properties": "camel"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["collections"]["order_line"]["target_collection"] == "OrderLine"
        fm = data["collections"]["order_line"]["field_mappings"]
        assert fm["unit_price"] == "unitPrice"

    def test_apply_naming_not_found(self, client):
        resp = client.post("/api/projects/nope/apply-naming", json={"collections": "pascal"})
        assert resp.status_code == 404

    def test_migration_plan_when_never_loaded(self, client, tmp_path, catalog_dir):
        self._setup_project(client, tmp_path, catalog_dir)
        resp = client.get("/api/projects/proj/migration-plan")
        assert resp.status_code == 200
        assert resp.json()["loaded"] is False

    def test_migrate_when_never_loaded_rejected(self, client, tmp_path, catalog_dir):
        self._setup_project(client, tmp_path, catalog_dir)
        resp = client.post("/api/projects/proj/migrate", json={"dry_run": True})
        assert resp.status_code == 400

    def test_migration_plan_detects_property_rename(self, client, tmp_path, catalog_dir):
        self._setup_project(client, tmp_path, catalog_dir)
        # Pretend the current mapping was loaded into the DB.
        loaded = client.get("/api/projects/proj/mapping").json()
        CatalogManager(catalog_dir).set_loaded_mapping("proj", loaded)
        # Save an edited mapping that renames a property.
        edited = client.get("/api/projects/proj/mapping").json()
        edited["collections"]["users"]["field_mappings"] = {"name": "fullName"}
        assert client.put("/api/projects/proj/mapping", json=edited).status_code == 200

        resp = client.get("/api/projects/proj/migration-plan")
        assert resp.status_code == 200
        data = resp.json()
        assert data["loaded"] is True
        kinds = {c["change_type"] for c in data["plan"]["changes"]}
        assert "field_mapping_added" in kinds

    def test_migrate_dry_run_reports_plan(self, client, tmp_path, catalog_dir):
        self._setup_project(client, tmp_path, catalog_dir)
        # Point at a real (non-system) database so the migration guard passes.
        CatalogManager(catalog_dir).update_project("proj", arango_database="proj_graph")
        loaded = client.get("/api/projects/proj/mapping").json()
        CatalogManager(catalog_dir).set_loaded_mapping("proj", loaded)
        edited = client.get("/api/projects/proj/mapping").json()
        edited["collections"]["users"]["field_mappings"] = {"name": "fullName"}
        client.put("/api/projects/proj/mapping", json=edited)

        resp = client.post("/api/projects/proj/migrate", json={"dry_run": True})
        assert resp.status_code == 200
        body = resp.json()
        assert body["migrated"] is False  # dry run never reports success
        assert len(body["report"]["actions_skipped"]) >= 1

    def test_save_mapping(self, client, tmp_path, catalog_dir):
        self._setup_project(client, tmp_path, catalog_dir)
        resp = client.put("/api/projects/proj/mapping", json={
            "source_schema": "public",
            "collections": {},
            "edges": [],
            "type_overrides": {},
            "key_separator": "_",
        })
        assert resp.status_code == 200
        assert resp.json()["saved"] is True

    def test_get_graph_data(self, client, tmp_path, catalog_dir):
        self._setup_project(client, tmp_path, catalog_dir)
        resp = client.get("/api/projects/proj/graph-data")
        assert resp.status_code == 200
        data = resp.json()
        assert "graph" in data
        assert "tables" in data

    def test_graph_data_carries_unique_keys_and_the_capture(self, client, tmp_path, catalog_dir):
        """P14.15: a reader outside the Studio sees each table's UNIQUE keys and which
        capture the tables are, with when it was taken."""
        self._setup_project(client, tmp_path, catalog_dir)
        mgr = CatalogManager(catalog_dir)
        snap = mgr.create_snapshot("src", Schema(tables={
            "users": Table(name="users", columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="email", data_type="text"),
                Column(name="tenant", data_type="text"),
                Column(name="handle", data_type="text"),
            ], primary_key=["id"], unique_constraints=[["email"], ["tenant", "handle"]]),
        }))
        data = client.get("/api/projects/proj/graph-data").json()
        users = next(t for t in data["tables"] if t["name"] == "users")
        assert users["uniqueConstraints"] == [["email"], ["tenant", "handle"]]
        assert data["snapshot"] == {
            "id": snap.id,
            "capturedAt": snap.captured_at.isoformat(),
            "keepsUniqueConstraints": True,
        }

    def test_graph_data_says_an_old_capture_cannot_hold_unique_keys(
            self, client, tmp_path, catalog_dir):
        """A snapshot saved before UNIQUE keys were kept has none to give: its empty lists
        are not "none", and the answer says so."""
        self._setup_project(client, tmp_path, catalog_dir)
        mgr = CatalogManager(catalog_dir)
        legacy = mgr.get_latest_snapshot("src").model_dump(mode="json")
        legacy.update(id="legacy", captured_at="2099-01-01T00:00:00Z")
        legacy.pop("schema_format_version", None)
        catalog = mgr._load()
        catalog.snapshots["legacy"] = SchemaSnapshot.model_validate(legacy)
        mgr._save(catalog)
        data = client.get("/api/projects/proj/graph-data").json()
        assert data["snapshot"]["id"] == "legacy"
        assert data["snapshot"]["capturedAt"].startswith("2099-01-01T00:00:00")
        assert data["snapshot"]["keepsUniqueConstraints"] is False
        assert all(t["uniqueConstraints"] == [] for t in data["tables"])

    def test_diff_mapping(self, client, tmp_path, catalog_dir):
        self._setup_project(client, tmp_path, catalog_dir)
        resp = client.post("/api/projects/proj/diff", json={
            "source_schema": "public",
            "collections": {},
            "edges": [],
            "type_overrides": {},
            "key_separator": "-",
        })
        assert resp.status_code == 200
        assert "changes" in resp.json()


class TestHistoryEndpoint:
    def test_empty_history(self, client):
        resp = client.get("/api/projects/any/history")
        assert resp.status_code == 200
        assert resp.json() == []


class TestSecretRedaction:
    def test_source_connection_string_is_redacted(self, client):
        resp = client.post("/api/sources", json={
            "name": "pg",
            "source_type": "postgresql",
            "connection_string": "postgresql://u:hunter2@db.example.com:5432/app",
        })
        assert resp.status_code == 201
        body = resp.json()
        assert "hunter2" not in body["connection_string"]
        assert "u:***@db.example.com:5432/app" in body["connection_string"]

        lst = client.get("/api/sources").json()
        assert "hunter2" not in lst[0]["connection_string"]

    def test_target_password_is_redacted(self, client):
        resp = client.post("/api/targets", json={
            "name": "arango",
            "endpoint": "http://localhost:8529",
            "database": "_system",
            "username": "root",
            "password": "VERY-SECRET-VALUE",
        })
        assert resp.status_code == 201
        body = resp.json()
        assert "VERY-SECRET-VALUE" not in body["password"]
        assert body["password"].startswith("***")

        lst = client.get("/api/targets").json()
        assert "VERY-SECRET-VALUE" not in lst[0]["password"]


class TestExpressionEndpoints:
    def test_list_functions_advertises_subset(self, client):
        resp = client.get("/api/expressions/functions")
        assert resp.status_code == 200
        body = resp.json()
        assert body["engine"] == "aql"
        assert "CONCAT" in body["functions"]
        assert "UPPER" in body["functions"]
        assert body["bind_syntax"] == "@column_name"

    def test_compile_valid_expression_returns_references(self, client):
        resp = client.post(
            "/api/expressions/compile",
            json={"expression": 'CONCAT(@first, " ", @last)'},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["valid"] is True
        assert body["references"] == ["first", "last"]

    def test_compile_invalid_expression_returns_error(self, client):
        resp = client.post(
            "/api/expressions/compile",
            json={"expression": "UNKNOWN_FN(@a)"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["valid"] is False
        assert "UNKNOWN_FN" in body["error"]

    def test_compile_rejects_non_aql_engine(self, client):
        resp = client.post(
            "/api/expressions/compile",
            json={"expression": "@a + 1", "engine": "python"},
        )
        assert resp.status_code == 200
        assert resp.json()["valid"] is False

    def test_preview_evaluates_against_sample_row(self, client):
        resp = client.post(
            "/api/expressions/preview",
            json={
                "expression": 'CONCAT(UPPER(@first), " ", @last)',
                "row": {"first": "ada", "last": "Lovelace"},
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["valid"] is True
        assert body["result"] == "ADA Lovelace"
        assert body["references"] == ["first", "last"]

    def test_preview_invalid_expression_returns_error(self, client):
        resp = client.post(
            "/api/expressions/preview",
            json={"expression": "UNKNOWN_FN(@a)", "row": {}},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["valid"] is False
        assert "UNKNOWN_FN" in body["error"]

    def test_preview_missing_row_yields_null_passthrough(self, client):
        resp = client.post(
            "/api/expressions/preview",
            json={"expression": "@missing"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["valid"] is True
        assert body["result"] is None

    def test_preview_rejects_non_aql_engine(self, client):
        resp = client.post(
            "/api/expressions/preview",
            json={"expression": "@a", "engine": "python", "row": {}},
        )
        assert resp.status_code == 200
        assert resp.json()["valid"] is False


class TestDraftValidationWithExpressions:
    def _setup(self, client, tmp_path, catalog_dir):
        client.post("/api/sources", json={
            "name": "src",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        mgr = CatalogManager(catalog_dir)
        schema = Schema(tables={
            "users": Table(name="users", columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="first_name", data_type="text"),
                Column(name="last_name", data_type="text"),
            ], primary_key=["id"]),
        })
        mgr.create_snapshot("src", schema)
        mapping_path = str(tmp_path / "mapping.yaml")
        ConfigManager.save_config(
            ConfigManager.generate_default_config(schema), mapping_path
        )
        client.post("/api/projects", json={
            "name": "proj",
            "source_name": "src",
            "mapping_config_path": mapping_path,
        })

    def test_draft_with_bad_expression_is_reported(self, client, tmp_path, catalog_dir):
        self._setup(client, tmp_path, catalog_dir)
        resp = client.post(
            "/api/projects/proj/validate-draft",
            json={
                "source_schema": "public",
                "collections": {
                    "users": {
                        "source_table": "users",
                        "target_collection": "users",
                        "field_expressions": [
                            {
                                "target": "full_name",
                                "sources": ["first_name", "last_name"],
                                "expression": "UNKNOWN_FN(@first_name)",
                            }
                        ],
                    }
                },
                "edges": [],
                "type_overrides": {},
                "key_separator": "_",
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["valid"] is False
        assert any("full_name" in m and "UNKNOWN_FN" in m for m in body["issues"])

    def test_draft_with_valid_expression_passes(self, client, tmp_path, catalog_dir):
        self._setup(client, tmp_path, catalog_dir)
        resp = client.post(
            "/api/projects/proj/validate-draft",
            json={
                "source_schema": "public",
                "collections": {
                    "users": {
                        "source_table": "users",
                        "target_collection": "users",
                        "field_expressions": [
                            {
                                "target": "full_name",
                                "sources": ["first_name", "last_name"],
                                "expression": 'CONCAT(@first_name, " ", @last_name)',
                            }
                        ],
                    }
                },
                "edges": [],
                "type_overrides": {},
                "key_separator": "_",
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["valid"] is True, body

    def test_draft_expression_referencing_unknown_column_is_reported(
        self, client, tmp_path, catalog_dir
    ):
        self._setup(client, tmp_path, catalog_dir)
        resp = client.post(
            "/api/projects/proj/validate-draft",
            json={
                "source_schema": "public",
                "collections": {
                    "users": {
                        "source_table": "users",
                        "target_collection": "users",
                        "field_expressions": [
                            {
                                "target": "oops",
                                "sources": [],
                                "expression": "UPPER(@nope)",
                            }
                        ],
                    }
                },
                "edges": [],
                "type_overrides": {},
                "key_separator": "_",
            },
        )
        body = resp.json()
        assert body["valid"] is False
        assert any("@nope" in m for m in body["issues"])


class TestSharedKeysEndpoint:
    """Cross-source shared-key inference over the catalog (PRD P6.7)."""

    def _seed(self, client, catalog_dir):
        for name, source_type in (("ch", "clickhouse"), ("crm", "postgresql")):
            client.post("/api/sources", json={
                "name": name,
                "source_type": source_type,
                "connection_string": f"{source_type}://localhost/{name}",
            })
        mgr = CatalogManager(catalog_dir)
        mgr.create_snapshot("ch", Schema(tables={
            "query_events": Table(name="query_events", columns=[
                Column(name="event_id", data_type="uint64", is_primary_key=True),
                Column(name="account_id", data_type="string"),
            ], primary_key=["event_id"]),
        }))
        mgr.create_snapshot("crm", Schema(tables={
            "accounts": Table(name="accounts", columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="account_id", data_type="character varying"),
            ], primary_key=["id"]),
        }))

    def test_infers_cross_source_hub(self, client, catalog_dir):
        self._seed(client, catalog_dir)
        resp = client.post("/api/shared-keys/infer", json={"min_confidence": 0.0})
        assert resp.status_code == 200
        body = resp.json()
        assert sorted(body["sources_considered"]) == ["ch", "crm"]
        cand = next(c for c in body["candidates"] if c["key"] == "account_id")
        assert cand["hub"]["kind"] == "entity"
        assert cand["hub"]["concept"] == "Account"
        assert (cand["hub"]["source"], cand["hub"]["table"]) == ("crm", "accounts")
        assert [(r["source"], r["table"]) for r in cand["references"]] == [
            ("ch", "query_events")
        ]

    def test_reports_suppressed_rather_than_dropping_silently(self, client, catalog_dir):
        self._seed(client, catalog_dir)
        body = client.post("/api/shared-keys/infer", json={"min_confidence": 0.0}).json()
        reasons = {s["reason"] for s in body["suppressed"]}
        # The CRM's own accounts row is the hub, not a reference.
        assert "hub_table" in reasons

    def test_single_source_returns_400(self, client, catalog_dir):
        client.post("/api/sources", json={
            "name": "only",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/only",
        })
        CatalogManager(catalog_dir).create_snapshot("only", Schema(tables={}))
        resp = client.post("/api/shared-keys/infer")
        assert resp.status_code == 400
        assert "at least two" in resp.json()["detail"]

    def test_explicit_source_list_is_honoured(self, client, catalog_dir):
        self._seed(client, catalog_dir)
        resp = client.post(
            "/api/shared-keys/infer",
            json={"sources": ["ch", "missing"], "min_confidence": 0.0},
        )
        assert resp.status_code == 400
        assert "at least two" in resp.json()["detail"]

    def test_accepted_key_persists_through_the_mapping_put(self, client, catalog_dir):
        """Accept mirrors P6.6: the client writes it into the mapping payload."""
        self._seed(client, catalog_dir)
        client.post("/api/projects", json={"name": "proj", "source_name": "ch"})
        payload = {
            "source_schema": "analytics",
            "collections": {
                "query_events": {
                    "source_table": "query_events",
                    "target_collection": "QueryEvent",
                }
            },
            "edges": [],
            "shared_keys": [
                {
                    "key": "account_id",
                    "concept": "Account",
                    "hub_kind": "entity",
                    "hub_source": "crm",
                    "hub_table": "accounts",
                    "hub_column": "account_id",
                    "bindings": [
                        {"source": "ch", "table": "query_events", "column": "account_id"}
                    ],
                    "confidence": 0.8,
                    "method": "hub_named_table",
                }
            ],
        }
        assert client.put("/api/projects/proj/mapping", json=payload).status_code == 200

        got = client.get("/api/projects/proj/mapping").json()
        assert got["shared_keys"][0]["hub_table"] == "accounts"
        assert got["shared_keys"][0]["bindings"][0]["source"] == "ch"


class TestStaticAssetSafety:
    def test_no_inline_handler_js_string_interpolation(self):
        """Guard against DOM XSS: inline event handlers must not interpolate a
        value into a JS string literal (e.g. onclick="fn('${x}')"), since that
        vector survives HTML-escaping (the parser decodes entities before the JS
        runs). Untrusted values must flow through data-* + this.dataset instead.
        """
        import re

        from r2g.ui.server import _STATIC_DIR

        html = (_STATIC_DIR / "index.html").read_text(encoding="utf-8")
        pat = re.compile(r"on\w+=\"[^\"]*'\$\{")
        offenders = [
            ln.strip()
            for ln in html.splitlines()
            if pat.search(ln) and not ln.lstrip().startswith("//")
        ]
        assert offenders == [], offenders


class TestCatalogEndpoints:
    """External data catalog discovery endpoints (Phase 8b)."""

    def _fake_provider(self):
        from r2g.catalogs.base import CatalogAsset, ResolvedSource

        class _Fake:
            provider_type = "openmetadata"
            name = "corp"

            def list_data_sources(self):
                return [CatalogAsset(
                    provider="corp", provider_type="openmetadata", fqn="pg",
                    kind="service", name="pg", source_type="postgresql",
                )]

            def list_children(self, asset):
                return [CatalogAsset(
                    provider="corp", provider_type="openmetadata", fqn="pg.shop",
                    kind="database", name="shop", source_type="postgresql",
                )]

            def search(self, q, *, limit=50):
                return self.list_children(None)

            def get_asset(self, fqn):
                return CatalogAsset(
                    provider="corp", provider_type="openmetadata", fqn=fqn,
                    kind="database", name="shop", source_type="postgresql",
                )

            def resolve_source(self, asset):
                return ResolvedSource(
                    source_type="postgresql",
                    connection_string="postgresql://$R2G_DB_USER:$R2G_DB_PASSWORD@h:5432/shop",
                    notes="set creds via env",
                )

        return _Fake()

    def test_add_and_list_catalog_redacts_token(self, client):
        resp = client.post("/api/catalogs", json={
            "name": "corp", "provider_type": "openmetadata",
            "endpoint": "http://localhost:8585", "token": "supersecret",
        })
        assert resp.status_code == 201
        listing = client.get("/api/catalogs").json()
        assert listing[0]["name"] == "corp"
        assert "supersecret" not in listing[0]["token"]  # redacted

    def test_add_unsupported_type_409(self, client):
        resp = client.post("/api/catalogs", json={
            "name": "x", "provider_type": "collibra", "endpoint": "http://h",
        })
        assert resp.status_code == 409

    def test_remove_catalog(self, client):
        client.post("/api/catalogs", json={
            "name": "corp", "provider_type": "openmetadata", "endpoint": "http://h:8585",
        })
        assert client.delete("/api/catalogs/corp").status_code == 200
        assert client.delete("/api/catalogs/corp").status_code == 404

    def test_browse(self, client, monkeypatch):
        client.post("/api/catalogs", json={
            "name": "corp", "provider_type": "openmetadata", "endpoint": "http://h:8585",
        })
        monkeypatch.setattr(
            "r2g.catalogs.base.create_catalog_provider",
            lambda *a, **k: self._fake_provider(),
        )
        resp = client.get("/api/catalogs/corp/browse")
        assert resp.status_code == 200
        assert resp.json()[0]["name"] == "pg"

    def test_browse_unknown_catalog_404(self, client):
        assert client.get("/api/catalogs/nope/browse").status_code == 404

    def test_import_source_creates_source(self, client, monkeypatch):
        client.post("/api/catalogs", json={
            "name": "corp", "provider_type": "openmetadata", "endpoint": "http://h:8585",
        })
        monkeypatch.setattr(
            "r2g.catalogs.base.create_catalog_provider",
            lambda *a, **k: self._fake_provider(),
        )
        resp = client.post("/api/catalogs/corp/import-source", json={
            "asset_fqn": "pg.shop", "source_name": "shop_src",
        })
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["source"]["name"] == "shop_src"
        # the imported source is now a normal source
        names = [s["name"] for s in client.get("/api/sources").json()]
        assert "shop_src" in names


class TestEntitlementsApi:
    """Phase 9b: GET /api/projects/{name}/entitlements."""

    def _setup(self, client, tmp_path, catalog_dir):
        from r2g.types import Classification

        schema = Schema(tables={
            "customer": Table(name="customer", columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="email", data_type="text",
                       classification=Classification(tags=["PII.Sensitive"])),
                Column(name="name", data_type="text"),
            ], primary_key=["id"]),
        })
        config = ConfigManager.generate_default_config(schema)
        mapping_path = str(tmp_path / "ent_mapping.yaml")
        ConfigManager.save_config(config, mapping_path)
        mgr = CatalogManager(catalog_dir)
        mgr.add_source("esrc", "postgresql", "postgresql://localhost/db")
        mgr.create_snapshot("esrc", schema)
        client.post("/api/projects", json={
            "name": "eproj", "source_name": "esrc", "mapping_config_path": mapping_path,
        })

    def test_report_flags_pii(self, client, tmp_path, catalog_dir):
        self._setup(client, tmp_path, catalog_dir)
        resp = client.get("/api/projects/eproj/entitlements")
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["threshold"] == "confidential"
        above = {f["target_property"] for f in data["above_threshold"]}
        assert "email" in above
        levels = {f["target_property"]: f["level"] for f in data["fields"]}
        assert levels["email"] == "restricted"
        assert levels["name"] == "public"

    def test_unknown_project_404(self, client):
        assert client.get("/api/projects/nope/entitlements").status_code == 404

    def test_invalid_threshold_400(self, client, tmp_path, catalog_dir):
        self._setup(client, tmp_path, catalog_dir)
        resp = client.get("/api/projects/eproj/entitlements?threshold=secret")
        assert resp.status_code == 400


class TestGovernanceEmitApi:
    """Phase 9c: POST /api/projects/{name}/governance/emit."""

    def _setup(self, client, tmp_path, catalog_dir):
        from r2g.types import Classification

        schema = Schema(tables={
            "customer": Table(name="customer", columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="email", data_type="text",
                       classification=Classification(tags=["PII.Sensitive"])),
                Column(name="name", data_type="text"),
            ], primary_key=["id"]),
        })
        config = ConfigManager.generate_default_config(schema)
        mapping_path = str(tmp_path / "gov_mapping.yaml")
        ConfigManager.save_config(config, mapping_path)
        mgr = CatalogManager(catalog_dir)
        mgr.add_source("gsrc", "postgresql", "postgresql://localhost/db")
        mgr.create_snapshot("gsrc", schema)
        client.post("/api/projects", json={
            "name": "gproj", "source_name": "gsrc", "mapping_config_path": mapping_path,
        })
        return tmp_path

    def test_emit_writes_artifacts(self, client, tmp_path, catalog_dir):
        self._setup(client, tmp_path, catalog_dir)
        resp = client.post("/api/projects/gproj/governance/emit?tier_layout=true")
        assert resp.status_code == 200, resp.text
        data = resp.json()
        arts = data["artifacts"]
        assert "classification-manifest.json" in arts
        assert "suggested-rbac.json" in arts
        assert "policy.rego" in arts
        assert "tier-layout.json" in arts
        # the returned paths are authoritative (server may manage the project dir)
        from pathlib import Path as _P
        assert _P(arts["classification-manifest.json"]).exists()

    def test_unknown_project_404(self, client):
        assert client.post("/api/projects/nope/governance/emit").status_code == 404

    def test_invalid_threshold_400(self, client, tmp_path, catalog_dir):
        self._setup(client, tmp_path, catalog_dir)
        resp = client.post("/api/projects/gproj/governance/emit?threshold=secret")
        assert resp.status_code == 400


class TestSuggestOntologyApi:
    """Phase 10b: POST /api/projects/{name}/suggest-ontology + apply-ontology."""

    def _setup(self, client, tmp_path, catalog_dir):
        from r2g.types import Classification

        # orders has NO declared FK; the "LLM" surfaces the implicit edge.
        schema = Schema(tables={
            "customer": Table(name="customer", columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="email", data_type="text",
                       classification=Classification(tags=["PII.Sensitive"])),
            ], primary_key=["id"]),
            "orders": Table(name="orders", columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="customer_id", data_type="integer"),
            ], primary_key=["id"]),
        })
        config = ConfigManager.generate_default_config(schema)
        mapping_path = str(tmp_path / "onto_mapping.yaml")
        ConfigManager.save_config(config, mapping_path)
        mgr = CatalogManager(catalog_dir)
        mgr.add_source("osrc", "postgresql", "postgresql://localhost/db")
        mgr.create_snapshot("osrc", schema)
        client.post("/api/projects", json={
            "name": "oproj", "source_name": "osrc", "mapping_config_path": mapping_path,
        })
        return mapping_path

    def _proposal(self):
        return {
            "edges": [{
                "edge_collection": "orders_to_customer",
                "from_collection": "orders",
                "to_collection": "customer",
                "from_fields": ["customer_id"],
                "to_fields": ["id"],
                "rationale": "customer_id references customer.id",
                "confidence": 0.9,
            }],
            "renames": [{
                "source_table": "customer",
                "column": "email",
                "target_property": "email_address",
                "confidence": 0.7,
            }],
        }

    def _patch(self, monkeypatch):
        from r2g.llm.base import OntologyProposal

        proposal = OntologyProposal.model_validate(self._proposal())

        class _Fake:
            provider_type = "fake"
            calls: list = []

            def propose_ontology(self, request):
                _Fake.calls.append(request)
                return proposal

        _Fake.calls = []
        monkeypatch.setattr("r2g.llm.create_llm_provider", lambda *a, **k: _Fake())
        return _Fake

    def test_suggest_returns_proposal_and_diff_without_writing(
        self, client, tmp_path, catalog_dir, monkeypatch
    ):
        self._setup(client, tmp_path, catalog_dir)
        fake = self._patch(monkeypatch)
        before = client.get("/api/projects/oproj/mapping").json()

        resp = client.post("/api/projects/oproj/suggest-ontology", json={"domain": "shop"})
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["provenance"]["proposed_edges"] == 1
        assert any(e["edge_collection"] == "orders_to_customer" for e in data["mapping"]["edges"])
        assert data["diff"]["changes"]
        # Redacted, metadata-only digest was actually sent.
        assert fake.calls and "email : [redacted" in fake.calls[0].schema_digest
        # Read-only: nothing persisted.
        assert client.get("/api/projects/oproj/mapping").json() == before

    def test_apply_returns_draft_without_persisting(
        self, client, tmp_path, catalog_dir, monkeypatch
    ):
        self._setup(client, tmp_path, catalog_dir)
        self._patch(monkeypatch)
        suggest = client.post("/api/projects/oproj/suggest-ontology", json={}).json()
        before = client.get("/api/projects/oproj/mapping").json()

        resp = client.post(
            "/api/projects/oproj/apply-ontology", json={"proposal": suggest["proposal"]}
        )
        assert resp.status_code == 200, resp.text
        draft = resp.json()["mapping"]
        assert any(e["edge_collection"] == "orders_to_customer" for e in draft["edges"])
        assert draft["collections"]["customer"]["field_mappings"].get("email") == "email_address"
        # Draft only — not persisted until the client Saves via PUT /mapping.
        assert client.get("/api/projects/oproj/mapping").json() == before

    def test_apply_subset_only_includes_accepted_items(
        self, client, tmp_path, catalog_dir, monkeypatch
    ):
        self._setup(client, tmp_path, catalog_dir)
        self._patch(monkeypatch)
        # Client rejects the rename, keeps only the edge.
        subset = {"edges": self._proposal()["edges"]}
        resp = client.post("/api/projects/oproj/apply-ontology", json={"proposal": subset})
        assert resp.status_code == 200, resp.text
        draft = resp.json()["mapping"]
        assert any(e["edge_collection"] == "orders_to_customer" for e in draft["edges"])
        assert draft["collections"]["customer"]["field_mappings"] == {}

    def test_suggest_with_grounding_passes_evidence(
        self, client, tmp_path, catalog_dir, monkeypatch
    ):
        self._setup(client, tmp_path, catalog_dir)
        fake = self._patch(monkeypatch)
        monkeypatch.setattr(
            "r2g.llm.grounding.build_grounding",
            lambda schema, **k: "GROUNDING: customer_id -> customer.id",
        )
        resp = client.post("/api/projects/oproj/suggest-ontology", json={"ground": True})
        assert resp.status_code == 200, resp.text
        assert resp.json()["provenance"]["grounded"] is True
        assert fake.calls[0].grounding == "GROUNDING: customer_id -> customer.id"

    def test_suggest_rsa_engine_uses_analyzer(
        self, client, tmp_path, catalog_dir, monkeypatch
    ):
        self._setup(client, tmp_path, catalog_dir)
        fake = self._patch(monkeypatch)  # must NOT be called for engine=rsa
        from r2g.llm.base import OntologyProposal

        proposal = OntologyProposal.model_validate(self._proposal())
        captured: dict = {}

        def _fake_propose(schema, *, provider=None, model=None, api_key=None):
            captured["provider"] = provider
            return proposal, {"confidence": 0.9, "detectedPatterns": ["join_table"]}

        monkeypatch.setattr("r2g.rsa_ontology.propose_ontology_from_schema", _fake_propose)
        resp = client.post("/api/projects/oproj/suggest-ontology", json={"engine": "rsa"})
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["provenance"]["engine"] == "relational-schema-analyzer"
        assert data["provenance"]["refined"] is False
        assert data["provenance"]["analyzer_confidence"] == 0.9
        assert any(
            e["edge_collection"] == "orders_to_customer" for e in data["mapping"]["edges"]
        )
        assert not fake.calls, "LLM must not be called for engine=rsa"
        assert captured["provider"] is None

    def test_suggest_rsa_refine_passes_provider(
        self, client, tmp_path, catalog_dir, monkeypatch
    ):
        self._setup(client, tmp_path, catalog_dir)
        self._patch(monkeypatch)
        from r2g.llm.base import OntologyProposal

        proposal = OntologyProposal.model_validate(self._proposal())
        captured: dict = {}

        def _fake_propose(schema, *, provider=None, model=None, api_key=None):
            captured["provider"] = provider
            return proposal, {"confidence": 0.8}

        monkeypatch.setattr("r2g.rsa_ontology.propose_ontology_from_schema", _fake_propose)
        resp = client.post(
            "/api/projects/oproj/suggest-ontology",
            json={"engine": "rsa", "refine": True, "provider": "anthropic"},
        )
        assert resp.status_code == 200, resp.text
        assert captured["provider"] == "anthropic"
        assert resp.json()["provenance"]["refined"] is True

    def test_suggest_unknown_engine_400(self, client, tmp_path, catalog_dir):
        self._setup(client, tmp_path, catalog_dir)
        resp = client.post(
            "/api/projects/oproj/suggest-ontology", json={"engine": "bogus"}
        )
        assert resp.status_code == 400
        assert "engine" in resp.text.lower()

    def test_suggest_rsa_real_library_end_to_end(
        self, client, tmp_path, catalog_dir
    ):
        pytest.importorskip("relational_schema_analyzer")
        self._setup(client, tmp_path, catalog_dir)
        resp = client.post("/api/projects/oproj/suggest-ontology", json={"engine": "rsa"})
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["provenance"]["engine"] == "relational-schema-analyzer"
        cols = data["proposal"]["collections"]
        assert any(c["target_collection"] == "Customer" for c in cols)

    def test_suggest_unknown_project_404(self, client, monkeypatch):
        self._patch(monkeypatch)
        assert client.post("/api/projects/nope/suggest-ontology", json={}).status_code == 404

    def _built(self, monkeypatch):
        """Build the real provider, keep it, and answer with the fake proposal."""
        import r2g.llm
        from r2g.llm.base import OntologyProposal

        real, built = r2g.llm.create_llm_provider, []
        proposal = OntologyProposal.model_validate(self._proposal())

        def factory(*a, **k):
            built.append(real(*a, **k))
            return type("_Fake", (), {"provider_type": "fake",
                                      "propose_ontology": lambda s, r: proposal})()

        monkeypatch.setattr("r2g.llm.create_llm_provider", factory)
        return built

    @pytest.mark.parametrize("extra", [
        {"api_key": "$R2G_REVIEW_SECRET"},
        {"api_key": "$OPENAI_API_KEY", "base_url": "http://collector.test/v1"},
        {"base_url": "http://collector.test/v1"},
    ])
    def test_suggest_sends_no_server_secret_to_a_requested_address(
        self, client, tmp_path, catalog_dir, monkeypatch, extra
    ):
        self._setup(client, tmp_path, catalog_dir)
        monkeypatch.setenv("R2G_REVIEW_SECRET", "s3cret")
        monkeypatch.setenv("OPENAI_API_KEY", "server-key")
        built = self._built(monkeypatch)
        resp = client.post("/api/projects/oproj/suggest-ontology",
                           json={"provider": "openai", **extra})
        assert resp.status_code == 400 and built == [], resp.text

    def test_suggest_server_keys_still_reach_the_provider(
        self, client, tmp_path, catalog_dir, monkeypatch
    ):
        self._setup(client, tmp_path, catalog_dir)
        monkeypatch.setenv("OPENAI_API_KEY", "server-key")
        built = self._built(monkeypatch)
        url = "/api/projects/oproj/suggest-ontology"
        # A named key, to the provider's own address.
        assert client.post(url, json={"api_key": "$OPENAI_API_KEY"}).status_code == 200
        assert built[-1]._api_key == "server-key"
        assert built[-1].base_url == "https://api.openai.com/v1"
        # The request's own key, to its own address.
        body = {"api_key": "own-key", "base_url": "http://collector.test/v1"}
        assert client.post(url, json=body).status_code == 200
        assert built[-1]._api_key == "own-key"
        # A keyless local server: allowed, and the server's key does not go with it.
        body = {"provider": "ollama", "base_url": "http://localhost:11434/v1"}
        assert client.post(url, json=body).status_code == 200
        assert built[-1]._api_key is None

    def test_suggest_unknown_provider_400(self, client, tmp_path, catalog_dir):
        self._setup(client, tmp_path, catalog_dir)
        resp = client.post(
            "/api/projects/oproj/suggest-ontology", json={"provider": "bogus"}
        )
        assert resp.status_code == 400
        assert "Unsupported LLM provider" in resp.text


class TestGraphModelSelection:
    """P13: the Studio must be able to target PG or LPG, and the choice must
    survive the round trip — the layout decides how every document is written,
    so a setting that silently reverts would load the wrong shape."""

    def _project(self, client, tmp_path, name="gm_proj"):
        client.post("/api/sources", json={
            "name": "src", "source_type": "postgresql",
            "connection_string": "postgresql://u:p@localhost/db",
        })
        ConfigManager.save_config(MappingConfig(), str(tmp_path / "mapping.yaml"))
        client.post("/api/projects", json={
            "name": name, "source_name": "src",
            "mapping_config_path": str(tmp_path / "mapping.yaml"),
        })
        return name

    def test_lpg_selection_round_trips(self, client, tmp_path):
        name = self._project(client, tmp_path)
        payload = MappingConfig(source_schema="public", graph_layout="lpg").model_dump()
        assert client.put(f"/api/projects/{name}/mapping", json=payload).status_code == 200
        back = client.get(f"/api/projects/{name}/mapping").json()
        assert back["graph_layout"] == "lpg"
        assert back["lpg"]["node_collection"] == "nodes"
        assert back["lpg"]["edge_collection"] == "edges"

    def test_custom_collection_names_round_trip(self, client, tmp_path):
        name = self._project(client, tmp_path, "gm_custom")
        cfg = MappingConfig(
            source_schema="public",
            graph_layout="lpg",
            lpg=LpgLayout(node_collection="v", edge_collection="e"),
        )
        client.put(f"/api/projects/{name}/mapping", json=cfg.model_dump())
        back = client.get(f"/api/projects/{name}/mapping").json()
        assert (back["lpg"]["node_collection"], back["lpg"]["edge_collection"]) == ("v", "e")

    def test_pg_default_is_not_persisted(self, client, tmp_path):
        """A project that never touched the setting keeps its historical shape."""
        name = self._project(client, tmp_path, "gm_pg")
        client.put(f"/api/projects/{name}/mapping", json=MappingConfig().model_dump())
        back = client.get(f"/api/projects/{name}/mapping").json()
        assert "graph_layout" not in back or back["graph_layout"] == "pg"

    def test_switching_back_to_pg_clears_the_layout(self, client, tmp_path):
        name = self._project(client, tmp_path, "gm_switch")
        client.put(f"/api/projects/{name}/mapping",
                   json=MappingConfig(graph_layout="lpg").model_dump())
        assert client.get(f"/api/projects/{name}/mapping").json()["graph_layout"] == "lpg"
        client.put(f"/api/projects/{name}/mapping", json=MappingConfig().model_dump())
        back = client.get(f"/api/projects/{name}/mapping").json()
        assert back.get("graph_layout", "pg") == "pg"


class TestExtraLabelsRoundTrip:
    """P13.13: extra labels set in the Studio must survive the save/load cycle,
    and a mapping that never used them must keep its on-disk shape."""

    def _project(self, client, tmp_path, name):
        client.post("/api/sources", json={
            "name": "src", "source_type": "postgresql",
            "connection_string": "postgresql://u:p@localhost/db",
        })
        ConfigManager.save_config(MappingConfig(), str(tmp_path / "mapping.yaml"))
        client.post("/api/projects", json={
            "name": name, "source_name": "src",
            "mapping_config_path": str(tmp_path / "mapping.yaml"),
        })
        return name

    def _cfg(self, *extra):
        cfg = MappingConfig(source_schema="public", graph_layout="lpg")
        cfg.collections = {
            "accounts": CollectionMapping(source_table="accounts",
                                          target_collection="Account",
                                          extra_labels=list(extra))
        }
        return cfg

    def test_extra_labels_survive_save_and_load(self, client, tmp_path):
        name = self._project(client, tmp_path, "lbl_proj")
        client.put(f"/api/projects/{name}/mapping",
                   json=self._cfg("Customer", "Premium").model_dump())
        back = client.get(f"/api/projects/{name}/mapping").json()
        assert back["collections"]["accounts"]["extra_labels"] == ["Customer", "Premium"]

    def test_absent_when_unused(self, client, tmp_path):
        name = self._project(client, tmp_path, "lbl_none")
        client.put(f"/api/projects/{name}/mapping", json=self._cfg().model_dump())
        back = client.get(f"/api/projects/{name}/mapping").json()
        assert "extra_labels" not in back["collections"]["accounts"]
