from __future__ import annotations

import json
from pathlib import Path

import pytest

from r2g.catalog import (
    CURRENT_SCHEMA_FORMAT,
    CatalogManager,
    OntologyArtifact,
    OntologyDependencyError,
)
from r2g.types import Column, Schema, Table


class TestSourceCRUD:
    def test_add_source(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        src = mgr.add_source("pg1", "postgresql", "postgresql://u:p@host/db", description="test db", owner="alice")
        assert src.name == "pg1"
        assert src.source_type == "postgresql"
        assert src.description == "test db"
        assert src.owner == "alice"
        assert src.created_at is not None

    def test_list_sources(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("a", "postgresql", "conn_a")
        mgr.add_source("b", "postgresql", "conn_b")
        sources = mgr.list_sources()
        names = {s.name for s in sources}
        assert names == {"a", "b"}

    def test_get_source(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        assert mgr.get_source("pg1") is not None
        assert mgr.get_source("pg1").name == "pg1"

    def test_get_source_missing(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        assert mgr.get_source("nonexistent") is None

    def test_remove_source(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        assert mgr.remove_source("pg1") is True
        assert mgr.get_source("pg1") is None
        assert mgr.list_sources() == []

    def test_remove_nonexistent_source(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        assert mgr.remove_source("ghost") is False

    def test_duplicate_source_name_raises(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        with pytest.raises(ValueError, match="already exists"):
            mgr.add_source("pg1", "postgresql", "conn2")


class TestSnapshots:
    def test_create_snapshot(self, tmp_path, sample_schema):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        snap = mgr.create_snapshot("pg1", sample_schema, pg_schema="public")
        assert snap.source_name == "pg1"
        assert snap.pg_schema == "public"
        assert len(snap.schema_data.tables) == 2
        assert snap.id

    def test_get_latest_snapshot(self, tmp_path, sample_schema):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        mgr.create_snapshot("pg1", sample_schema)
        snap2 = mgr.create_snapshot("pg1", sample_schema)
        latest = mgr.get_latest_snapshot("pg1")
        assert latest is not None
        assert latest.id == snap2.id

    def test_get_latest_snapshot_no_snapshots(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        assert mgr.get_latest_snapshot("pg1") is None

    def test_get_snapshot_returns_exact_snapshot(self, tmp_path, sample_schema):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        first = mgr.create_snapshot("pg1", sample_schema)
        mgr.create_snapshot("pg1", sample_schema)

        assert mgr.get_snapshot(first.id) == first
        assert mgr.get_snapshot("missing") is None

    def test_list_snapshots(self, tmp_path, sample_schema):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        mgr.create_snapshot("pg1", sample_schema)
        mgr.create_snapshot("pg1", sample_schema)
        snaps = mgr.list_snapshots("pg1")
        assert len(snaps) == 2
        assert snaps[0].captured_at <= snaps[1].captured_at

    def test_list_snapshots_filters_by_source(self, tmp_path, sample_schema):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn1")
        mgr.add_source("pg2", "postgresql", "conn2")
        mgr.create_snapshot("pg1", sample_schema)
        mgr.create_snapshot("pg2", sample_schema)
        assert len(mgr.list_snapshots("pg1")) == 1
        assert len(mgr.list_snapshots("pg2")) == 1

    def test_view_identity_survives_snapshot_reload(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("snow", "snowflake", "snowflake://example")
        schema = Schema(
            tables={
                "CUSTOMER_VIEW": Table(
                    name="CUSTOMER_VIEW",
                    columns=[Column(name="ID", data_type="NUMBER")],
                    is_view=True,
                )
            }
        )
        created = mgr.create_snapshot("snow", schema, pg_schema="ANALYTICS")

        loaded = CatalogManager(catalog_dir=tmp_path).get_snapshot(created.id)
        assert loaded is not None
        assert loaded.schema_format_version == CURRENT_SCHEMA_FORMAT
        assert loaded.schema_data.tables["CUSTOMER_VIEW"].is_view is True


class TestProjectCRUD:
    def test_create_project(self, tmp_path, sample_schema):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        mgr.create_snapshot("pg1", sample_schema)
        proj = mgr.create_project("my_project", "pg1", "mapping.yaml")
        assert proj.name == "my_project"
        assert proj.source_name == "pg1"
        assert proj.schema_snapshot_id != ""
        assert proj.mapping_config_path == "mapping.yaml"

    def test_create_project_missing_source_raises(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        with pytest.raises(ValueError, match="not found"):
            mgr.create_project("proj", "no_source", "mapping.yaml")

    @pytest.mark.parametrize(
        "name",
        [
            "../escape",
            "nested/project",
            "project; echo injected",
            "project\ninjected",
            ".hidden",
        ],
    )
    def test_create_project_rejects_unsafe_names(self, tmp_path, name):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")

        with pytest.raises(ValueError, match="Invalid project name"):
            mgr.create_project(name, "pg1", "mapping.yaml")

    def test_create_project_no_snapshot(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        proj = mgr.create_project("proj", "pg1", "mapping.yaml")
        assert proj.schema_snapshot_id == ""

    def test_list_projects(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        mgr.create_project("proj_a", "pg1", "a.yaml")
        mgr.create_project("proj_b", "pg1", "b.yaml")
        projects = mgr.list_projects()
        names = {p.name for p in projects}
        assert names == {"proj_a", "proj_b"}

    def test_get_project(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        mgr.create_project("proj", "pg1", "mapping.yaml")
        proj = mgr.get_project("proj")
        assert proj is not None
        assert proj.name == "proj"

    def test_create_project_with_metadata(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        proj = mgr.create_project(
            "proj", "pg1", "mapping.yaml",
            mapping_name="Customer 360",
            mapping_description="People + orders graph",
        )
        assert proj.mapping_name == "Customer 360"
        assert proj.mapping_description == "People + orders graph"

    def test_update_project_metadata_bumps_updated_at(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        created = mgr.create_project("proj", "pg1", "mapping.yaml")
        updated = mgr.update_project(
            "proj", mapping_name="Renamed", mapping_description="desc"
        )
        assert updated.mapping_name == "Renamed"
        assert updated.mapping_description == "desc"
        assert updated.updated_at >= created.updated_at
        # name is immutable through update_project
        assert updated.name == "proj"

    def test_update_project_ignores_name_kwarg(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        mgr.create_project("proj", "pg1", "mapping.yaml")
        updated = mgr.update_project("proj", name="hacked", mapping_name="ok")
        assert updated.name == "proj"
        assert updated.mapping_name == "ok"

    def test_update_project_missing_raises(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        with pytest.raises(ValueError, match="not found"):
            mgr.update_project("nope", mapping_name="x")

    def test_touch_project_updates_timestamp(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        created = mgr.create_project("proj", "pg1", "mapping.yaml")
        mgr.touch_project("proj")
        touched = mgr.get_project("proj")
        assert touched.updated_at >= created.updated_at

    def test_touch_project_missing_is_noop(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.touch_project("nope")  # should not raise

    def test_delete_project_removes_record(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        mgr.create_project("proj", "pg1", "mapping.yaml")
        assert mgr.delete_project("proj") is True
        assert mgr.get_project("proj") is None
        # Persisted: a fresh manager should not see it either.
        assert CatalogManager(catalog_dir=tmp_path).get_project("proj") is None

    def test_delete_project_cascades_load_history(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        mgr.create_project("proj", "pg1", "mapping.yaml")
        mgr.start_load("proj", "full")
        mgr.start_load("other", "full")
        assert mgr.delete_project("proj") is True
        history = mgr.get_history()
        assert all(r.project_name != "proj" for r in history)
        assert any(r.project_name == "other" for r in history)

    def test_delete_project_missing_returns_false(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        assert mgr.delete_project("nope") is False

    def test_get_project_missing(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        assert mgr.get_project("missing") is None

    def test_create_project_custom_arango_settings(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        proj = mgr.create_project(
            "proj", "pg1", "mapping.yaml",
            arango_endpoint="http://arango:8529",
            arango_database="mydb",
        )
        assert proj.arango_endpoint == "http://arango:8529"
        assert proj.arango_database == "mydb"

    def test_create_project_binds_registered_target_and_snapshot(self, tmp_path, sample_schema):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        pinned = mgr.create_snapshot("pg1", sample_schema)
        mgr.create_snapshot("pg1", sample_schema)
        mgr.add_target(
            "graph",
            endpoint="http://arango.internal:8529",
            database="customer360",
            username="operator",
        )

        project = mgr.create_project(
            "proj",
            "pg1",
            "mapping.yaml",
            target_name="graph",
            schema_snapshot_id=pinned.id,
        )

        assert project.target_name == "graph"
        assert project.schema_snapshot_id == pinned.id
        assert project.arango_endpoint == "http://arango.internal:8529"
        assert project.arango_database == "customer360"
        assert project.arango_username == "operator"

    def test_create_project_rejects_system_target(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        mgr.add_target("system", database="_system")

        with pytest.raises(ValueError, match="dedicated target database"):
            mgr.create_project("proj", "pg1", "mapping.yaml", target_name="system")

    def test_create_project_rejects_snapshot_from_another_source(
        self, tmp_path, sample_schema
    ):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        mgr.add_source("pg2", "postgresql", "conn")
        wrong = mgr.create_snapshot("pg2", sample_schema)

        with pytest.raises(ValueError, match="does not belong"):
            mgr.create_project(
                "proj",
                "pg1",
                "mapping.yaml",
                schema_snapshot_id=wrong.id,
            )


class TestOntologyCatalog:
    @staticmethod
    def _artifact() -> OntologyArtifact:
        return OntologyArtifact(
            filename="specific.ttl",
            ontology_iri="https://example.test/specific",
            serialization="turtle",
            sha256="a" * 64,
            size_bytes=123,
        )

    def test_add_list_get_and_remove_ontology(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        record = mgr.add_ontology(
            name="Customer ontology",
            root_ontology_iri="https://example.test/specific",
            canonical_graph_digest="sha256:" + "b" * 64,
            mapping_digest="sha256:" + "c" * 64,
            artifacts=[self._artifact()],
            ontology_id="customer",
            equivalent_serializations=True,
        )

        assert record.id == "customer"
        assert mgr.get_ontology("customer") == record
        assert mgr.list_ontologies() == [record]
        assert mgr.remove_ontology("customer") is True
        assert mgr.get_ontology("customer") is None

    def test_project_records_ontology_fingerprint_and_blocks_removal(
        self, tmp_path, sample_schema
    ):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        mgr.create_snapshot("pg1", sample_schema)
        ontology = mgr.add_ontology(
            name="Customer ontology",
            root_ontology_iri="https://example.test/specific",
            canonical_graph_digest="sha256:" + "b" * 64,
            mapping_digest="sha256:" + "c" * 64,
            artifacts=[self._artifact()],
            ontology_id="customer",
        )

        project = mgr.create_project(
            "proj",
            "pg1",
            "mapping.yaml",
            ontology_id=ontology.id,
        )

        assert project.ontology_id == ontology.id
        assert project.ontology_fingerprint == ontology.canonical_graph_digest
        with pytest.raises(OntologyDependencyError, match="depend"):
            mgr.remove_ontology(ontology.id)


class TestLoadHistory:
    def test_start_load(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        record = mgr.start_load("proj", "full", mapping_hash="abc123")
        assert record.project_name == "proj"
        assert record.load_type == "full"
        assert record.status == "running"
        assert record.mapping_hash == "abc123"
        assert record.completed_at is None

    def test_complete_load(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        record = mgr.start_load("proj", "full")
        completed = mgr.complete_load(record.id, rows_loaded=1000, errors=2, collections_loaded=["users", "orders"])
        assert completed.status == "completed"
        assert completed.rows_loaded == 1000
        assert completed.errors == 2
        assert completed.collections_loaded == ["users", "orders"]
        assert completed.completed_at is not None

    def test_complete_load_failed_status(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        record = mgr.start_load("proj", "streaming")
        completed = mgr.complete_load(record.id, rows_loaded=50, errors=10, collections_loaded=[], status="failed")
        assert completed.status == "failed"

    def test_complete_load_not_found_raises(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        with pytest.raises(ValueError, match="not found"):
            mgr.complete_load("nonexistent-id", rows_loaded=0, errors=0, collections_loaded=[])

    def test_get_history_all(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.start_load("proj_a", "full")
        mgr.start_load("proj_b", "streaming")
        history = mgr.get_history()
        assert len(history) == 2

    def test_get_history_by_project(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.start_load("proj_a", "full")
        mgr.start_load("proj_b", "streaming")
        mgr.start_load("proj_a", "cdc")
        history = mgr.get_history(project_name="proj_a")
        assert len(history) == 2
        assert all(r.project_name == "proj_a" for r in history)

    def test_get_history_limit(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        for i in range(10):
            mgr.start_load("proj", "full")
        history = mgr.get_history(limit=3)
        assert len(history) == 3

    def test_get_history_sorted_newest_first(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        r1 = mgr.start_load("proj", "full")
        r2 = mgr.start_load("proj", "streaming")
        history = mgr.get_history()
        assert history[0].id == r2.id
        assert history[1].id == r1.id


class TestCatalogPersistence:
    def test_save_and_load_round_trip(self, tmp_path, sample_schema):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "postgresql://u:p@h/d", description="main", owner="bob")
        mgr.create_snapshot("pg1", sample_schema)
        mgr.create_project("proj", "pg1", "mapping.yaml")
        mgr.start_load("proj", "full")

        mgr2 = CatalogManager(catalog_dir=tmp_path)
        assert len(mgr2.list_sources()) == 1
        assert mgr2.get_source("pg1").owner == "bob"
        assert len(mgr2.list_snapshots("pg1")) == 1
        assert len(mgr2.list_projects()) == 1
        assert len(mgr2.get_history()) == 1

    def test_empty_catalog_on_fresh_dir(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        assert mgr.list_sources() == []
        assert mgr.list_projects() == []
        assert mgr.get_history() == []

    def test_catalog_json_is_valid(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        catalog_path = tmp_path / "catalog.json"
        assert catalog_path.exists()
        data = json.loads(catalog_path.read_text(encoding="utf-8"))
        assert "sources" in data
        assert "pg1" in data["sources"]


class TestMappingPathExpansion:
    """A leading `~` must never reach disk unexpanded.

    Storing it raw made the path resolve relative to the process cwd, so r2g
    wrote projects into a literal `~` directory wherever it happened to be run
    — and the CLI then failed with FileNotFoundError while in-process callers
    (whose cwd happened to contain that directory) kept working.
    """

    def _mk(self, tmp_path):
        mgr = CatalogManager(str(tmp_path / "catalog"))
        mgr.add_source("src", "postgresql", "postgresql://localhost/db")
        return mgr

    def test_tilde_is_expanded_on_create(self, tmp_path):
        mgr = self._mk(tmp_path)
        project = mgr.create_project("p", "src", "~/.r2g/projects/p/mapping.yaml")
        assert not project.mapping_config_path.startswith("~")
        assert project.mapping_config_path == str(
            Path("~/.r2g/projects/p/mapping.yaml").expanduser()
        )

    def test_tilde_in_an_existing_catalog_is_expanded_on_load(self, tmp_path):
        """Catalogs written before the validator existed self-heal."""
        mgr = self._mk(tmp_path)
        mgr.create_project("p", "src", "/tmp/p/mapping.yaml")
        catalog_file = Path(mgr.catalog_path) if hasattr(mgr, "catalog_path") else None
        if catalog_file is None or not catalog_file.exists():
            candidates = list((tmp_path / "catalog").rglob("catalog.json"))
            assert candidates, "catalog.json not found"
            catalog_file = candidates[0]
        raw = json.loads(catalog_file.read_text())
        raw["projects"]["p"]["mapping_config_path"] = "~/.r2g/projects/p/mapping.yaml"
        catalog_file.write_text(json.dumps(raw))

        reloaded = CatalogManager(str(tmp_path / "catalog")).get_project("p")
        assert reloaded.mapping_config_path == str(
            Path("~/.r2g/projects/p/mapping.yaml").expanduser()
        )

    def test_absolute_and_relative_paths_are_untouched(self, tmp_path):
        mgr = self._mk(tmp_path)
        assert mgr.create_project("a", "src", "/abs/x.yaml").mapping_config_path == "/abs/x.yaml"
        assert mgr.create_project("r", "src", "rel/x.yaml").mapping_config_path == "rel/x.yaml"
