"""Exports of a project's mapping (#54).

Path mode first: ``export-csi`` and ``export-r2rml`` without ``--project`` must keep their
defaults, output bytes, and the way they write through whatever ``--output`` names. The golden
files follow ``test_serialization_compat``: after an intended change to the emitted output,

    R2G_REGEN_COMPAT=1 python -m pytest tests/test_project_exports.py

and review the fixture diff carefully, since federation consumers read these documents.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from cryptography.fernet import Fernet
from test_serialization_compat import _REGEN, FIXTURES, MAPPING_GOLDEN, SCHEMA_GOLDEN
from typer.testing import CliRunner

from r2g.catalog import CatalogManager
from r2g.config import ConfigManager
from r2g.governed import keep_version
from r2g.main import app
from r2g.project_exports import (
    ProjectExportError,
    check_mapping_coverage,
    prepare_export_directory,
    resolve_project_export,
)
from r2g.security import ENC_PREFIX, SECRET_FILENAME, SecretKeyError
from r2g.types import Column, ForeignKey, MappingConfig, Schema, Table

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


# --- project mode: what is checked before anything is written --------------------------------


def _shop_schema(*extra_columns: str) -> Schema:
    def table(name: str, *columns: str) -> Table:
        return Table(name=name, primary_key=["id"],
                     columns=[Column(name=c, data_type="integer") for c in ("id", *columns)])

    return Schema(tables={"users": table("users", *extra_columns),
                          "orders": table("orders", "user_id")})


def _shop_mapping(**changes: Any) -> MappingConfig:
    doc: dict[str, Any] = {
        "collections": {t: {"source_table": t, "target_collection": t} for t in ("users", "orders")},
        "edges": [{"edge_collection": "placed", "from_collection": "orders",
                   "to_collection": "users", "from_field": "user_id", "to_field": "id"}],
    }
    return MappingConfig.model_validate({**doc, **changes})


@pytest.fixture
def shop(tmp_path, monkeypatch) -> CatalogManager:
    """A catalog holding project ``shop``, the way the Studio lays one out."""
    for name in ("CONTROL_PLANE_URL", "R2G_TENANT", "R2G_SECRET_KEY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("R2G_CATALOG_DIR", str(tmp_path / "catalog"))
    catalog = CatalogManager(tmp_path / "catalog")
    catalog.add_source("src", "postgresql", "postgresql://u:p@db/shop")
    catalog.create_snapshot("src", _shop_schema())
    mapping = tmp_path / "catalog" / "projects" / "shop" / "mapping.yaml"
    ConfigManager.save_config(_shop_mapping(), mapping)
    catalog.create_project("shop", "src", str(mapping))
    return catalog


def _mapping_path(catalog: CatalogManager) -> Path:
    project = catalog.get_project("shop")
    assert project is not None
    return Path(project.mapping_config_path)


def test_a_project_resolves_to_its_mapping_and_latest_snapshot(shop, monkeypatch):
    pinned = shop.get_project("shop").schema_snapshot_id
    latest = shop.create_snapshot("src", _shop_schema("added_later"))
    fetches: list[str] = []
    real = CatalogManager.get_latest_snapshot
    monkeypatch.setattr(CatalogManager, "get_latest_snapshot",
                        lambda self, source: (fetches.append(source), real(self, source))[1])
    export = resolve_project_export("shop")
    assert fetches == ["src"] and export.snapshot_id == latest.id != pinned
    assert "added_later" in {c.name for c in export.schema.tables["users"].columns}
    assert export.schema_sha256 == hashlib.sha256(export.schema.model_dump_json().encode()).hexdigest()
    assert export.source_type == "postgresql" and export.tenant is None
    assert export.directory == shop.dir / "projects" / "shop"


def test_the_mapping_hash_is_the_one_the_saved_versions_are_named_by(shop, tmp_path):
    export = resolve_project_export("shop")
    reference = keep_version(tmp_path / "p", _mapping_path(shop).read_text(encoding="utf-8"))
    assert reference.endswith(export.mapping_sha256)


def test_resolving_writes_nothing_to_the_catalog(shop):
    before = (shop.dir / "catalog.json").read_bytes()
    resolve_project_export("shop")
    assert (shop.dir / "catalog.json").read_bytes() == before


def test_an_unknown_project_is_refused(shop):
    with pytest.raises(ProjectExportError, match="not found"):
        resolve_project_export("nope")


def test_a_relative_mapping_path_is_refused(shop):
    shop.update_project("shop", mapping_config_path="mapping.yaml")
    with pytest.raises(ProjectExportError, match="relative"):
        resolve_project_export("shop")


def test_a_source_without_a_snapshot_is_refused(shop, tmp_path):
    shop.add_source("bare", "csv", str(tmp_path))
    shop.update_project("shop", source_name="bare")
    with pytest.raises(ProjectExportError, match="No schema snapshot"):
        resolve_project_export("shop")


@pytest.mark.parametrize("content, says", [
    (None, "Cannot read the mapping"),
    ("a: [", "Cannot read the mapping"),
    ("- a list\n", "Cannot read the mapping"),
    ("", "empty mapping"),
    ("collections: {}\nedges: []\n", "empty mapping"),
])
def test_an_unusable_mapping_is_refused(shop, content, says):
    path = _mapping_path(shop)
    path.unlink() if content is None else path.write_text(content, encoding="utf-8")
    with pytest.raises(ProjectExportError, match=says):
        resolve_project_export("shop")


def test_a_mapping_saved_while_it_is_read_is_refused(shop, monkeypatch):
    path = _mapping_path(shop)
    real = ConfigManager.load_config

    def saved_meanwhile(target):
        config = real(target)
        path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
        return config

    monkeypatch.setattr(ConfigManager, "load_config", staticmethod(saved_meanwhile))
    with pytest.raises(ProjectExportError, match="changed while"):
        resolve_project_export("shop")


def test_a_mistyped_catalog_directory_creates_nothing(shop, tmp_path, monkeypatch):
    typo = tmp_path / "catalgo"
    monkeypatch.setenv("R2G_CATALOG_DIR", str(typo))
    with pytest.raises(ProjectExportError, match="No catalog"):
        resolve_project_export("shop")
    assert not typo.exists()


def test_an_invalid_catalog_is_refused(shop):
    (shop.dir / "catalog.json").write_text("{not json", encoding="utf-8")
    with pytest.raises(ProjectExportError, match="Cannot read the catalog"):
        resolve_project_export("shop")


def test_an_encrypted_catalog_without_its_key_gets_no_new_key(shop):
    (shop.dir / SECRET_FILENAME).unlink()
    with pytest.raises(ProjectExportError, match="encrypted"):
        resolve_project_export("shop")
    assert not (shop.dir / SECRET_FILENAME).exists()


@pytest.mark.parametrize("section, name, field, value", [
    ("targets", "t", "password", ENC_PREFIX + "x"),
    ("catalog_providers", "c", "token", ENC_PREFIX + "x"),
])
def test_each_decrypted_field_counts_as_encrypted(shop, section, name, field, value):
    path = shop.dir / "catalog.json"
    doc = json.loads(path.read_text(encoding="utf-8"))
    doc["sources"]["src"]["connection_string"] = "postgresql://plain/shop"
    when = "2026-01-01T00:00:00+00:00"
    doc[section][name] = {"name": name, "endpoint": "http://db", "created_at": when,
                          "updated_at": when, field: value}
    path.write_text(json.dumps(doc), encoding="utf-8")
    (shop.dir / SECRET_FILENAME).unlink()
    with pytest.raises(ProjectExportError, match="encrypted"):
        resolve_project_export("shop")


def test_the_key_may_come_from_the_environment(shop, monkeypatch):
    key = (shop.dir / SECRET_FILENAME).read_text(encoding="utf-8").strip()
    (shop.dir / SECRET_FILENAME).unlink()
    monkeypatch.setenv("R2G_SECRET_KEY", key)
    assert resolve_project_export("shop").name == "shop"
    assert not (shop.dir / SECRET_FILENAME).exists()


def test_a_wrong_key_keeps_the_existing_error(shop):
    (shop.dir / SECRET_FILENAME).write_bytes(Fernet.generate_key())
    with pytest.raises(SecretKeyError):
        resolve_project_export("shop")


def test_a_plaintext_catalog_still_initializes_even_if_a_description_mentions_the_prefix(shop):
    path = shop.dir / "catalog.json"
    doc = json.loads(path.read_text(encoding="utf-8"))
    doc["sources"]["src"].update(connection_string="postgresql://plain/shop",
                                 description=f"see {ENC_PREFIX} in the docs")
    path.write_text(json.dumps(doc), encoding="utf-8")
    (shop.dir / SECRET_FILENAME).unlink()
    assert resolve_project_export("shop").name == "shop"
    assert (shop.dir / SECRET_FILENAME).exists()   # the normal first run


@pytest.mark.parametrize("missing", ["R2G_CATALOG_DIR", "R2G_TENANT"])
def test_a_control_plane_needs_its_catalog_and_tenant_named(shop, monkeypatch, missing):
    monkeypatch.setenv("CONTROL_PLANE_URL", "http://cp.test/api")
    monkeypatch.setenv("R2G_TENANT", "acme")
    monkeypatch.delenv(missing)
    with pytest.raises(ProjectExportError, match="must name"):
        resolve_project_export("shop")


def test_a_control_plane_export_records_its_tenant(shop, monkeypatch):
    monkeypatch.setenv("CONTROL_PLANE_URL", "http://cp.test/api")
    monkeypatch.setenv("R2G_TENANT", "acme")
    assert resolve_project_export("shop").tenant == "acme"


# --- the coverage check ---


def _shared(**hub: Any) -> dict[str, Any]:
    return {"key": "k", "concept": "Customer", **hub}


def _binding(source: str, table: str, column: str) -> dict[str, str]:
    return {"source": source, "table": table, "column": column}


def _edge(**changes: Any) -> list[dict[str, Any]]:
    return [{"edge_collection": "placed", "from_collection": "orders", "to_collection": "users",
             "from_fields": ["user_id"], "to_fields": ["id"], **changes}]


def _collection(**changes: Any) -> dict[str, Any]:
    return {"users": {"source_table": "users", "target_collection": "users", **changes}}


@pytest.mark.parametrize("changes, says", [
    ({"collections": _collection(source_table="gone")}, "table 'gone'"),
    ({"collections": _collection(field_mappings={"gone": "x"})}, "column users.gone"),
    ({"collections": _collection(include_fields=["id", "gone"])}, "column users.gone"),
    ({"edges": _edge(from_collection="gone")}, "table 'gone'"),
    ({"edges": _edge(to_collection="gone")}, "table 'gone'"),
    ({"edges": _edge(from_fields=["gone"])}, "column orders.gone"),
    ({"edges": _edge(to_fields=["gone"])}, "column users.gone"),
    ({"shared_keys": [_shared(bindings=[_binding("src", "users", "gone")])]}, "column users.gone"),
    ({"shared_keys": [_shared(bindings=[_binding("src", "gone", "id")])]}, "table 'gone'"),
    ({"shared_keys": [_shared(hub_kind="entity", hub_source="src", hub_table="users",
                              hub_column="gone")]}, "column users.gone"),
])
def test_coverage_refuses_what_the_snapshot_lacks(changes, says):
    assert any(says in p for p in check_mapping_coverage(_shop_mapping(**changes), _shop_schema(), "src"))


@pytest.mark.parametrize("changes", [
    {},
    {"collections": _collection(exclude_fields=["gone"], field_terms={"gone": "https://ex.org/p"})},
    {"edges": [{"edge_collection": "placed", "from_collection": "orders", "to_collection": "users",
                "from_field": "user_id", "to_field": "id"}]},
    {"shared_keys": [_shared()]},   # a virtual hub has no table
    {"shared_keys": [_shared(hub_kind="entity", hub_source="crm", hub_table="accounts",
                             hub_column="ref", bindings=[_binding("crm", "accounts", "ref"),
                                                         _binding("src", "users", "id")])]},
])
def test_coverage_accepts_what_it_should(changes):
    assert check_mapping_coverage(_shop_mapping(**changes), _shop_schema(), "src") == []


def test_coverage_accepts_generated_many_to_many_edges():
    schema = Schema(tables={
        "students": Table(name="students", primary_key=["id"],
                          columns=[Column(name="id", data_type="integer")]),
        "courses": Table(name="courses", primary_key=["id"],
                         columns=[Column(name="id", data_type="integer")]),
        "enroll": Table(
            name="enroll", primary_key=["student_id", "course_id"],
            columns=[Column(name="student_id", data_type="integer"),
                     Column(name="course_id", data_type="integer")],
            foreign_keys=[
                ForeignKey(name="f1", columns=["student_id"], foreign_table="students",
                           foreign_columns=["id"]),
                ForeignKey(name="f2", columns=["course_id"], foreign_table="courses",
                           foreign_columns=["id"])]),
    })
    config = ConfigManager.generate_default_config(schema)
    assert config.edges and check_mapping_coverage(config, schema, "src") == []


def test_coverage_reports_every_problem_once(shop):
    changes = {"collections": _collection(include_fields=["a", "b"]),
               "edges": _edge(to_fields=["a"])}
    problems = check_mapping_coverage(_shop_mapping(**changes), _shop_schema(), "src")
    assert len(problems) == 3 and len(set(problems)) == 3


def test_an_uncovered_mapping_is_refused_before_anything_is_exported(shop):
    ConfigManager.save_config(_shop_mapping(collections=_collection(source_table="gone")),
                              _mapping_path(shop))
    with pytest.raises(ProjectExportError, match="table 'gone'"):
        resolve_project_export("shop")


# --- the project directory ---


def test_the_directory_is_accepted_when_it_is_the_projects_own(shop):
    prepare_export_directory(resolve_project_export("shop"))


def test_a_directory_owned_by_someone_else_is_accepted(shop, monkeypatch):
    # A root-owned, group-writable volume is a normal mount.
    monkeypatch.setattr(os, "geteuid", lambda: shop.dir.stat().st_uid + 1)
    prepare_export_directory(resolve_project_export("shop"))


def test_root_may_not_publish(shop, monkeypatch):
    export = resolve_project_export("shop")
    monkeypatch.setattr(os, "geteuid", lambda: 0)
    with pytest.raises(ProjectExportError, match="root"):
        prepare_export_directory(export)


def test_a_symlinked_project_directory_is_refused(shop, tmp_path):
    export = resolve_project_export("shop")
    moved = tmp_path / "outside"
    export.directory.rename(moved)
    export.directory.symlink_to(moved)
    with pytest.raises(ProjectExportError, match="not a real directory"):
        prepare_export_directory(export)


def test_two_projects_on_one_directory_are_refused(shop):
    other = shop.dir / "projects" / "shop2"
    other.symlink_to(shop.dir / "projects" / "shop")
    shop.create_project("shop2", "src", str(_mapping_path(shop)))
    with pytest.raises(ProjectExportError, match="share one directory"):
        prepare_export_directory(resolve_project_export("shop"))


def test_a_missing_directory_is_created_outside_a_control_plane(shop):
    export = resolve_project_export("shop")
    moved = export.directory.with_name("moved")
    export.directory.rename(moved)
    prepare_export_directory(export)
    assert export.directory.is_dir()


def test_a_missing_directory_is_not_created_under_a_control_plane(shop, monkeypatch):
    export = resolve_project_export("shop")
    export.directory.rename(export.directory.with_name("moved"))
    monkeypatch.setenv("CONTROL_PLANE_URL", "http://cp.test/api")
    with pytest.raises(ProjectExportError, match="open it in the Studio"):
        prepare_export_directory(export)
    assert not export.directory.exists()
