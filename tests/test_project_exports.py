"""Exports of a project's mapping (#54).

Path mode first: ``export-csi`` and ``export-r2rml`` without ``--project`` must keep their
defaults, output bytes, and the way they write through whatever ``--output`` names. The golden
files follow ``test_serialization_compat``: after an intended change to the emitted output,

    R2G_REGEN_COMPAT=1 python -m pytest tests/test_project_exports.py

and review the fixture diff carefully, since federation consumers read these documents.
"""

from __future__ import annotations

import errno
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
from fastapi.testclient import TestClient
from test_serialization_compat import _REGEN, FIXTURES, MAPPING_GOLDEN, SCHEMA_GOLDEN
from typer.testing import CliRunner

import r2g
from r2g import control_plane, project_exports
from r2g.catalog import CatalogManager
from r2g.config import ConfigManager
from r2g.csi import validate_csi
from r2g.governed import keep_version
from r2g.main import app
from r2g.project_exports import (
    ARTIFACTS,
    ProjectExportError,
    check_mapping_coverage,
    prepare_export_directory,
    resolve_project_export,
)
from r2g.security import ENC_PREFIX, SECRET_FILENAME, SecretKeyError
from r2g.types import Column, ForeignKey, MappingConfig, Schema, Table
from r2g.ui.server import create_app

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


@pytest.fixture(autouse=True)
def _log_to_stderr_uncached(monkeypatch):
    """Keep the CLI from pointing loggers at CliRunner's stream, which closes: these tests go
    on to use the catalog and the API after an export."""
    import structlog

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(0),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=False,
    )
    monkeypatch.setattr("r2g.main.setup_logging", lambda *_args, **_kwargs: None)


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


# --- export-csi --project ----------------------------------------------------------------------


EMITTERS = {"csi": "r2g.csi.mapping_to_csi", "r2rml": "r2g.r2rml.mapping_to_r2rml"}
#: An option that changes each export's bytes.
CHANGED = {"csi": ["--source-ref", "changed"], "r2rml": ["--resource-base", "http://changed/"]}


def _files(catalog: CatalogManager) -> Path:
    return catalog.dir / "projects" / "shop"


def _csi(*args: str):
    return CliRunner().invoke(app, ["export-csi", "--project", "shop", *args])


def _project(kind: str, *args: str):
    return CliRunner().invoke(app, [f"export-{kind}", "--project", "shop", *args])


def _receipt(catalog: CatalogManager, kind: str = "csi") -> dict[str, Any]:
    return json.loads((_files(catalog) / ARTIFACTS[kind][1]).read_text(encoding="utf-8"))


def _published_pair(catalog: CatalogManager, kind: str = "csi") -> tuple[bytes, bytes]:
    return ((_files(catalog) / ARTIFACTS[kind][0]).read_bytes(),
            (_files(catalog) / ARTIFACTS[kind][1]).read_bytes())


def test_a_project_export_publishes_the_file_and_its_receipt(shop, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = _csi()
    assert result.exit_code == 0, result.output
    assert not (tmp_path / "mapping.csi.json").exists()   # no second copy in the cwd
    path = _files(shop) / "mapping.csi.json"
    text = path.read_text(encoding="utf-8")
    validate_csi(json.loads(text))
    assert stat.S_IMODE(path.stat().st_mode) == 0o640
    latest = shop.get_latest_snapshot("src")
    receipt = _receipt(shop)
    assert receipt == {
        "format_version": 1, "project": "shop", "tenant": None, "kind": "csi",
        "artifact_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "mapping_sha256": hashlib.sha256(_mapping_path(shop).read_bytes()).hexdigest(),
        "schema_sha256": hashlib.sha256(latest.schema_data.model_dump_json().encode()).hexdigest(),
        "snapshot_id": latest.id,
        "generated_at": json.loads(text)["provenance"]["generatedAt"],
        "r2g_version": r2g.__version__,
        "options": {"label_policy": "qualify", "source_type": "postgresql",
                    "source_ref": json.loads(text)["provenance"]["source"]["ref"],
                    "validated": True, "curated_labels_sha256": None,
                    "allow_unapplied_labels": False},
    }


def test_a_project_export_is_what_the_path_export_of_the_same_inputs_writes(shop, tmp_path):
    assert _csi().exit_code == 0
    schema = tmp_path / "schema.json"
    shop.get_latest_snapshot("src").schema_data.save_to_file(str(schema))
    out = tmp_path / "path.json"
    result = CliRunner().invoke(app, ["export-csi", "-c", str(_mapping_path(shop)), "-s", str(schema),
                                      "--source-type", "postgresql", "-o", str(out)])
    assert result.exit_code == 0, result.output
    published = (_files(shop) / "mapping.csi.json").read_text(encoding="utf-8")
    assert _stable(published) == _stable(out.read_text(encoding="utf-8"))


def test_options_reach_the_document_and_the_receipt(shop):
    assert _csi("--source-type", "mysql", "--source-ref", "sales", "--label-policy", "warn",
                "--no-validate").exit_code == 0
    options = _receipt(shop)["options"]
    assert (options["source_type"], options["source_ref"], options["label_policy"],
            options["validated"]) == ("mysql", "sales", "warn", False)
    source = json.loads((_files(shop) / "mapping.csi.json").read_text())["provenance"]["source"]
    assert (source["kind"], source["ref"]) == ("mysql", "sales")


def test_curated_labels_are_recorded_by_hash_not_path(shop, tmp_path):
    labels = tmp_path / "private-labels.json"
    labels.write_text(json.dumps({"curatedLabelsVersion": "1", "entities": [
        {"name": "User", "label": "Customer"}]}), encoding="utf-8")
    result = _csi("--curated-labels", str(labels))
    assert result.exit_code == 0, result.output
    receipt = _receipt(shop)
    assert receipt["options"]["curated_labels_sha256"] == hashlib.sha256(labels.read_bytes()).hexdigest()
    assert "private-labels" not in json.dumps(receipt)


def test_a_project_export_invents_no_bitemporal_stamps(shop):
    assert _csi().exit_code == 0
    provenance = json.loads((_files(shop) / "mapping.csi.json").read_text())["provenance"]
    for stamp in ("transactionTime", "validTime", "validTimeSource", "predecessorFingerprint"):
        assert stamp not in provenance


@pytest.mark.parametrize("kind", ARTIFACTS)
@pytest.mark.parametrize("extra", [["-c", "m.yaml"], ["-s", "s.json"]])
def test_project_with_path_inputs_exits_2_and_writes_nothing(shop, kind, extra):
    result = _project(kind, *extra)
    assert result.exit_code == 2 and "cannot be combined" in result.output
    assert not (_files(shop) / ARTIFACTS[kind][0]).exists()


def test_a_missing_config_still_reads_as_a_missing_option():
    result = CliRunner().invoke(app, ["export-csi"])
    assert result.exit_code == 2 and "Missing option '--config' / '-c'." in result.output


@pytest.mark.parametrize("args, code", [
    (["-c", "", "-o", "out"], 1),               # an empty path is an unreadable file, not absent
    ([*INPUTS, "-o", ""], 1),                   # and an empty output is not the default
])
@pytest.mark.parametrize("command", COMMANDS)
def test_an_empty_path_option_is_not_treated_as_left_out(tmp_path, monkeypatch, command, args, code):
    monkeypatch.chdir(tmp_path)
    args = [*args, "-s", str(SCHEMA_GOLDEN)] if "-s" not in args else args
    assert CliRunner().invoke(app, [command, *args]).exit_code == code
    assert list(tmp_path.iterdir()) == []


def test_an_empty_schema_path_is_not_treated_as_left_out(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    args = ["export-r2rml", "-c", str(MAPPING_GOLDEN), "-s", ""]
    assert CliRunner().invoke(app, args).exit_code == 1
    assert list(tmp_path.iterdir()) == []


def test_an_empty_source_type_still_reads_as_relational(tmp_path):
    out = tmp_path / "out"
    _export("export-csi", "--source-type", "", "-o", str(out))
    assert json.loads(out.read_text())["provenance"]["source"]["kind"] == "relational"


def test_the_receipt_records_the_source_type_the_document_has(shop):
    assert _csi("--source-type", "").exit_code == 0
    assert _receipt(shop)["options"]["source_type"] == "relational"


@pytest.mark.parametrize("kind", ARTIFACTS)
def test_project_with_output_leaves_the_studio_copy_alone(shop, kind, tmp_path):
    assert _project(kind).exit_code == 0
    before = _published_pair(shop, kind)
    out = tmp_path / "elsewhere.json"
    result = _project(kind, "-o", str(out))
    assert result.exit_code == 0 and "not updated" in result.output
    assert out.is_file() and _published_pair(shop, kind) == before
    assert not list(tmp_path.glob("elsewhere*.export.json"))


@pytest.mark.parametrize("kind, other", [("csi", "r2rml"), ("r2rml", "csi")])
def test_one_export_leaves_the_others_files_alone(shop, kind, other):
    files = [_files(shop) / name for name in ARTIFACTS[other]]
    for path in files:
        path.write_text("the other export", encoding="utf-8")
    assert _project(kind).exit_code == 0
    assert all(path.read_text(encoding="utf-8") == "the other export" for path in files)


@pytest.mark.parametrize("kind", ARTIFACTS)
def test_a_refused_project_says_why_and_writes_nothing(shop, kind):
    ConfigManager.save_config(_shop_mapping(collections=_collection(source_table="gone")),
                              _mapping_path(shop))
    result = _project(kind)
    assert result.exit_code == 1 and "Not exported" in result.output and "gone" in result.output
    assert not (_files(shop) / ARTIFACTS[kind][0]).exists()


@pytest.mark.parametrize("kind", ARTIFACTS)
def test_root_publishes_nothing_but_may_write_elsewhere(shop, kind, tmp_path, monkeypatch):
    monkeypatch.setattr(os, "geteuid", lambda: 0)
    assert _project(kind).exit_code == 1
    assert not (_files(shop) / ARTIFACTS[kind][0]).exists()
    out = tmp_path / "elsewhere.json"
    assert _project(kind, "-o", str(out)).exit_code == 0 and out.is_file()


@pytest.mark.parametrize("kind", ARTIFACTS)
def test_a_failed_generation_keeps_the_published_pair(shop, kind, monkeypatch):
    assert _project(kind).exit_code == 0
    before = _published_pair(shop, kind)

    def fails(*args, **kwargs):
        raise RuntimeError("generator failed")

    monkeypatch.setattr(EMITTERS[kind], fails)
    assert _project(kind).exit_code == 1
    assert _published_pair(shop, kind) == before


@pytest.mark.parametrize("kind", ARTIFACTS)
def test_curated_labels_not_applied_keep_the_published_pair(shop, kind, tmp_path):
    assert _project(kind).exit_code == 0
    before = _published_pair(shop, kind)
    labels = tmp_path / "labels.json"
    labels.write_text(json.dumps({"curatedLabelsVersion": "1", "entities": [
        {"name": "Nobody", "label": "Customer"}]}), encoding="utf-8")
    assert _project(kind, "--curated-labels", str(labels)).exit_code == 1
    assert _published_pair(shop, kind) == before


@pytest.mark.parametrize("kind", ARTIFACTS)
def test_a_failed_directory_sync_exits_1_and_writes_no_receipt(shop, kind, monkeypatch):
    fsync_fails = OSError(errno.EIO, "no")
    real = os.fsync

    def synced(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise fsync_fails
        real(fd)

    monkeypatch.setattr(os, "fsync", synced)
    assert _project(kind).exit_code == 1
    assert (_files(shop) / ARTIFACTS[kind][0]).is_file()
    assert not (_files(shop) / ARTIFACTS[kind][1]).exists()


@pytest.mark.parametrize("kind", ARTIFACTS)
def test_a_failure_between_the_two_writes_leaves_a_receipt_that_does_not_match(shop, kind, monkeypatch):
    assert _project(kind).exit_code == 0
    old = _receipt(shop, kind)
    real = project_exports._write_atomically

    def receipt_fails(path, text, **kwargs):
        if path.name.endswith(".export.json"):
            raise OSError(errno.ENOSPC, "no")
        return real(path, text, **kwargs)

    monkeypatch.setattr(project_exports, "_write_atomically", receipt_fails)
    assert _project(kind, *CHANGED[kind]).exit_code == 1
    text = (_files(shop) / ARTIFACTS[kind][0]).read_text(encoding="utf-8")
    assert _receipt(shop, kind) == old and old["artifact_sha256"] != hashlib.sha256(text.encode()).hexdigest()


@pytest.mark.parametrize("kind", ARTIFACTS)
def test_a_warning_from_the_write_reaches_the_user(shop, kind):
    path = _files(shop) / ARTIFACTS[kind][0]
    path.write_text("old", encoding="utf-8")
    path.chmod(0o600)
    result = _project(kind)
    assert result.exit_code == 0 and "readable only by the user who wrote it" in result.output


@pytest.mark.parametrize("kind", ARTIFACTS)
def test_a_control_plane_export_records_its_tenant_in_the_receipt(shop, kind, monkeypatch):
    monkeypatch.setenv("CONTROL_PLANE_URL", "http://cp.test/api")
    monkeypatch.setenv("R2G_TENANT", "acme")
    result = _project(kind)
    assert result.exit_code == 0 and "tenant acme" in result.output
    assert _receipt(shop, kind)["tenant"] == "acme"


def test_r2rml_project_export_publishes_the_file_and_its_receipt(shop, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = _project("r2rml")
    assert result.exit_code == 0, result.output
    assert not (tmp_path / "mapping.r2rml.ttl").exists()
    path = _files(shop) / "mapping.r2rml.ttl"
    text = path.read_text(encoding="utf-8")
    assert "(source: postgresql)" in text
    assert stat.S_IMODE(path.stat().st_mode) == 0o640
    latest = shop.get_latest_snapshot("src")
    receipt = _receipt(shop, "r2rml")
    generated_at = receipt.pop("generated_at")
    assert generated_at.endswith("+00:00")
    assert receipt == {
        "format_version": 1, "project": "shop", "tenant": None, "kind": "r2rml",
        "artifact_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "mapping_sha256": hashlib.sha256(_mapping_path(shop).read_bytes()).hexdigest(),
        "schema_sha256": hashlib.sha256(latest.schema_data.model_dump_json().encode()).hexdigest(),
        "snapshot_id": latest.id,
        "r2g_version": r2g.__version__,
        "options": {"concept_base": "urn:arango-sparql:concept#",
                    "resource_base": "http://r2g.example/resource/", "source_type": "postgresql",
                    "curated_labels_sha256": None, "allow_unapplied_labels": False},
    }


def test_r2rml_project_export_is_byte_identical_to_the_path_export(shop, tmp_path):
    assert _project("r2rml").exit_code == 0
    schema = tmp_path / "schema.json"
    shop.get_latest_snapshot("src").schema_data.save_to_file(str(schema))
    out = tmp_path / "path.ttl"
    result = CliRunner().invoke(app, ["export-r2rml", "-c", str(_mapping_path(shop)), "-s", str(schema),
                                      "--source-type", "postgresql", "-o", str(out)])
    assert result.exit_code == 0, result.output
    assert (_files(shop) / "mapping.r2rml.ttl").read_bytes() == out.read_bytes()


def test_r2rml_uses_the_latest_snapshot_not_the_pin(shop):
    shop.create_snapshot("src", _shop_schema("added_later"))
    assert _project("r2rml").exit_code == 0
    assert "added_later" in (_files(shop) / "mapping.r2rml.ttl").read_text(encoding="utf-8")


def test_r2rml_options_reach_the_document_and_the_receipt(shop):
    assert _project("r2rml", "--concept-base", "urn:ex:", "--resource-base", "http://ex/",
                    "--source-type", "mysql").exit_code == 0
    text = (_files(shop) / "mapping.r2rml.ttl").read_text(encoding="utf-8")
    assert "<urn:ex:User>" in text and "http://ex/User/" in text and "(source: mysql)" in text
    options = _receipt(shop, "r2rml")["options"]
    assert (options["concept_base"], options["resource_base"], options["source_type"]) == (
        "urn:ex:", "http://ex/", "mysql")


def test_both_receipts_record_the_same_inputs_and_curated_labels(shop, tmp_path):
    labels = tmp_path / "private-labels.json"
    labels.write_text(json.dumps({"curatedLabelsVersion": "1", "entities": [
        {"name": "User", "label": "Customer"}]}), encoding="utf-8")
    for kind in ARTIFACTS:
        assert _project(kind, "--curated-labels", str(labels)).exit_code == 0
    csi, r2rml = _receipt(shop, "csi"), _receipt(shop, "r2rml")
    for field in ("mapping_sha256", "schema_sha256", "snapshot_id"):
        assert csi[field] == r2rml[field]
    assert csi["options"]["curated_labels_sha256"] == r2rml["options"]["curated_labels_sha256"]
    assert "label_policy" not in r2rml["options"] and "concept_base" not in csi["options"]
    assert "private-labels" not in json.dumps(r2rml)


def test_a_missing_schema_still_reads_as_a_missing_option():
    result = CliRunner().invoke(app, ["export-r2rml", "-c", str(MAPPING_GOLDEN)])
    assert result.exit_code == 2 and "Missing option '--schema' / '-s'." in result.output


def test_an_empty_r2rml_source_type_stays_in_the_header(tmp_path):
    out = tmp_path / "out"
    _export("export-r2rml", "--source-type", "", "-o", str(out))
    assert "(source: )" in out.read_text(encoding="utf-8")


def test_labels_changed_while_read_are_refused(tmp_path, monkeypatch):
    labels = tmp_path / "labels.json"
    labels.write_text(json.dumps({"curatedLabelsVersion": "1", "entities": []}), encoding="utf-8")
    real = project_exports.load_curated_labels

    def changed_meanwhile(path):
        doc = real(path)
        labels.write_text(json.dumps({"curatedLabelsVersion": "1", "entities": [
            {"name": "User", "label": "Customer"}]}), encoding="utf-8")
        return doc

    monkeypatch.setattr(project_exports, "load_curated_labels", changed_meanwhile)
    with pytest.raises(ProjectExportError, match="changed while"):
        project_exports.read_curated_labels(str(labels))


# --- GET /api/projects/{name}/exports ---------------------------------------------------------


@pytest.fixture
def api(shop, monkeypatch) -> TestClient:
    monkeypatch.delenv("R2G_API_TOKEN", raising=False)
    monkeypatch.delenv("R2G_EXPORT_MAX_BYTES", raising=False)
    return TestClient(create_app(catalog_dir=str(shop.dir)))


def _read(api: TestClient) -> dict[str, Any]:
    response = api.get("/api/projects/shop/exports")
    assert response.status_code == 200, response.text
    return response.json()


def _export_both(*args: str) -> None:
    for kind in ARTIFACTS:
        result = _project(kind, *args)
        assert result.exit_code == 0, result.output


def _labels_file(tmp_path: Path, name: str, label: str) -> str:
    path = tmp_path / name
    path.write_text(json.dumps({"curatedLabelsVersion": "1", "entities": [
        {"name": "User", "label": label}]}), encoding="utf-8")
    return str(path)


def test_nothing_exported_reads_as_none(api):
    body = _read(api)
    assert body["state"] == "none" and body["same_inputs"] is None
    assert body["csi"]["status"] == body["r2rml"]["status"] == "missing"
    assert body["comparison"]["status"] == "not_comparable"


def test_a_project_without_a_directory_reads_as_none(shop, api):
    moved = _files(shop).with_name("moved")
    _files(shop).rename(moved)
    assert _read(api)["state"] == "none"


@pytest.mark.parametrize("kind, other", [("csi", "r2rml"), ("r2rml", "csi")])
def test_one_export_reads_as_partial(shop, api, kind, other):
    assert _project(kind).exit_code == 0
    body = _read(api)
    assert body["state"] == "partial"
    assert body[kind]["status"] == "current" and body[other]["status"] == "missing"


def test_both_exports_read_as_complete_with_their_exact_content(shop, api):
    _export_both()
    response = api.get("/api/projects/shop/exports")
    assert response.headers["cache-control"] == "no-store"
    body = response.json()
    assert body["state"] == "complete" and body["same_inputs"] and body["same_curated_labels"]
    for kind, (name, _) in ARTIFACTS.items():
        assert body[kind]["status"] == "current"
        assert body[kind]["content"] == (_files(shop) / name).read_text(encoding="utf-8")
        assert body[kind]["receipt"] == _receipt(shop, kind)
    assert body["current"]["snapshot_id"] == shop.get_latest_snapshot("src").id


def test_the_structure_holds_the_tables_and_columns_of_both(shop, api):
    _export_both()
    body = _read(api)
    tables = {t["table"]: t for t in body["r2rml"]["structure"]["tables"]}
    assert set(tables) == {"users", "orders"}
    assert {c["column"] for c in tables["orders"]["columns"]} == {"id", "user_id"}
    assert tables["orders"]["joins"][0]["conditions"] == [{"child": "user_id", "parent": "id"}]
    entities = {e["name"]: e for e in body["csi"]["structure"]["entities"]}
    assert set(entities) == {"User", "Order"} and entities["Order"]["collection"] == "orders"
    assert {p["field"] for p in entities["Order"]["properties"]} == {"id", "user_id"}
    assert body["csi"]["structure"]["relationships"][0]["edge_collection"] == "placed"


def test_bitemporal_stamps_are_reported_as_unavailable(shop, api):
    _export_both()
    assert _read(api)["csi"]["bitemporal"] == {
        "status": "unavailable",
        "fields": ["transactionTime", "validTime", "validTimeSource", "predecessorFingerprint"]}


def test_a_changed_mapping_reads_as_stale(shop, api):
    _export_both()
    path = _mapping_path(shop)
    path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    body = _read(api)
    assert body["state"] == "attention" and body["same_inputs"] is True
    for kind in ARTIFACTS:
        assert (body[kind]["status"], body[kind]["reasons"]) == ("stale", ["mapping_changed"])


def test_a_new_snapshot_with_another_schema_reads_as_stale(shop, api):
    _export_both()
    shop.create_snapshot("src", _shop_schema("added_later"))
    body = _read(api)
    assert body["csi"]["reasons"] == body["r2rml"]["reasons"] == ["schema_changed"]


def test_a_snapshot_changed_in_place_reads_as_stale(shop, api):
    _export_both()
    snapshot = shop.get_latest_snapshot("src")
    shop.update_snapshot_schema(snapshot.id, _shop_schema("changed_in_place"))
    body = _read(api)
    assert body["current"]["snapshot_id"] == body["csi"]["receipt"]["snapshot_id"]
    assert body["csi"]["status"] == "stale" and body["state"] == "attention"


def test_the_same_schema_under_a_new_snapshot_stays_current(shop, api):
    _export_both()
    old = shop.get_latest_snapshot("src").id
    new = shop.create_snapshot("src", _shop_schema()).id
    body = _read(api)
    assert body["state"] == "complete"
    assert body["csi"]["receipt"]["snapshot_id"] == old and body["current"]["snapshot_id"] == new


def test_exports_from_different_inputs_are_not_complete_or_compared(shop, api):
    assert _project("csi").exit_code == 0
    shop.create_snapshot("src", _shop_schema("added_later"))
    assert _project("r2rml").exit_code == 0
    body = _read(api)
    assert body["state"] == "attention" and body["same_inputs"] is False
    assert body["comparison"] == {"status": "not_comparable", "classes": None, "properties": None,
                                  "reason": "the exports came from different inputs"}


def test_different_curated_labels_are_not_complete_or_compared(shop, api, tmp_path):
    assert _project("csi", "--curated-labels", _labels_file(tmp_path, "a", "Customer")).exit_code == 0
    assert _project("r2rml", "--curated-labels", _labels_file(tmp_path, "b", "Client")).exit_code == 0
    body = _read(api)
    assert body["csi"]["status"] == body["r2rml"]["status"] == "current"
    assert body["same_inputs"] is True and body["same_curated_labels"] is False
    assert body["state"] == "attention" and body["comparison"]["status"] == "not_comparable"


def test_one_side_with_curated_labels_is_not_complete(shop, api, tmp_path):
    assert _project("csi", "--curated-labels", _labels_file(tmp_path, "a", "Customer")).exit_code == 0
    assert _project("r2rml").exit_code == 0
    assert _read(api)["state"] == "attention"


def test_the_same_curated_labels_are_complete_and_pair_the_renamed_class(shop, api, tmp_path):
    _export_both("--curated-labels", _labels_file(tmp_path, "a", "Customer"))
    body = _read(api)
    assert body["state"] == "complete"
    assert body["comparison"]["classes"]["paired"] == ["Customer", "Order"]


def test_issue_17_is_shown_without_pairing_properties(shop, api):
    schema = _shop_schema("status")
    schema.tables["orders"].columns.append(Column(name="status", data_type="text"))
    shop.create_snapshot("src", schema)
    _export_both()
    comparison = _read(api)["comparison"]
    assert comparison["status"] == "comparable"
    assert comparison["classes"] == {"paired": ["Order", "User"], "csi_only": [], "r2rml_only": []}
    assert {"class": "Order", "csi_only": ["orderStatus"], "r2rml_only": ["status"]} in comparison["properties"]
    assert {"class": "User", "csi_only": ["userStatus"], "r2rml_only": ["status"]} in comparison["properties"]


def test_a_custom_concept_base_without_a_hash_still_pairs_classes(shop, api):
    assert _project("csi").exit_code == 0
    assert _project("r2rml", "--concept-base", "urn:ex:onto/").exit_code == 0
    assert _read(api)["comparison"]["classes"]["paired"] == ["Order", "User"]


def test_a_class_on_one_side_is_in_that_sides_list_only():
    entry = {"receipt": {"options": {"concept_base": "urn:c:"}}}
    csi = {**entry, "structure": {"entities": [
        {"name": "Both", "properties": []}, {"name": "OnlyCsi", "properties": []}]}}
    r2rml = {**entry, "structure": {"tables": [
        {"class": "urn:c:Both", "columns": []}, {"class": "urn:c:OnlyR2rml", "columns": []},
        {"class": "urn:other:Both", "columns": []}]}}
    assert project_exports._compare(csi, r2rml, True, True)["classes"] == {
        "paired": ["Both"], "csi_only": ["OnlyCsi"], "r2rml_only": ["OnlyR2rml", "urn:other:Both"]}


@pytest.mark.parametrize("kind", ARTIFACTS)
def test_a_changed_artifact_reads_as_unverified(shop, api, kind):
    _export_both()
    path = _files(shop) / ARTIFACTS[kind][0]
    path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    body = _read(api)
    assert (body[kind]["status"], body[kind]["reasons"]) == ("unverified", ["artifact_changed"])
    assert body[kind]["receipt"] is None and body[kind]["content"] is not None
    assert body["state"] == "attention" and body["same_inputs"] is None
    assert body["comparison"]["status"] == "not_comparable"


@pytest.mark.parametrize("change, reason", [
    (None, "receipt_missing"),
    ("{not json", "receipt_invalid"),
    ("[]", "receipt_invalid"),
    ({"unknown": 1}, "receipt_invalid"),
    ({"format_version": 2}, "receipt_invalid"),
    ({"kind": "other"}, "receipt_invalid"),
    ({"options": "x"}, "receipt_invalid"),
    ({"project": "other"}, "project_mismatch"),
    ({"tenant": "other"}, "tenant_mismatch"),
])
def test_a_receipt_that_cannot_vouch_reads_as_unverified(shop, api, change, reason):
    _export_both()
    path = _files(shop) / ARTIFACTS["csi"][1]
    if change is None:
        path.unlink()
    elif isinstance(change, str):
        path.write_text(change, encoding="utf-8")
    else:
        path.write_text(json.dumps({**_receipt(shop), **change}), encoding="utf-8")
    entry = _read(api)["csi"]
    assert (entry["status"], entry["reasons"], entry["receipt"]) == ("unverified", [reason], None)


@pytest.mark.parametrize("kind, content, reason", [
    ("csi", b"{not json", "invalid_csi"),
    ("csi", b'{"csiVersion": "1"}', "invalid_csi"),
    ("csi", b"[" * 100000, "invalid_csi"),
    ("csi", b"\xff\xfe", "not_utf8"),
    ("r2rml", b"not turtle", "invalid_r2rml"),
    ("r2rml", b"\xff\xfe", "not_utf8"),
])
def test_a_malformed_artifact_says_so(shop, api, kind, content, reason):
    _export_both()
    (_files(shop) / ARTIFACTS[kind][0]).write_bytes(content)
    body = _read(api)
    assert body[kind]["status"] == "malformed" and body[kind]["reasons"][0].startswith(reason)
    assert body[kind]["structure"] is None and body["state"] == "attention"


def test_malformed_r2rml_stays_visible_with_its_line(shop, api):
    _export_both()
    path = _files(shop) / ARTIFACTS["r2rml"][0]
    path.write_text("bad 'turtle'", encoding="utf-8")
    entry = _read(api)["r2rml"]
    assert entry["content"] == "bad 'turtle'" and "line 1" in entry["reasons"][0]


def test_an_oversized_artifact_is_withheld(shop, api, monkeypatch):
    _export_both()
    monkeypatch.setenv("R2G_EXPORT_MAX_BYTES", "10")
    body = _read(api)
    assert body["csi"]["status"] == body["r2rml"]["status"] == "too_large"
    assert body["csi"]["content"] is None and body["state"] == "attention"


@pytest.mark.parametrize("value", ["many", "0", "-1"])
def test_an_invalid_size_limit_uses_the_default(shop, api, monkeypatch, value):
    _export_both()
    monkeypatch.setenv("R2G_EXPORT_MAX_BYTES", value)
    assert _read(api)["state"] == "complete"


def test_a_symlinked_artifact_is_not_followed(shop, api, tmp_path):
    _export_both()
    secret = tmp_path / "secret.txt"
    secret.write_text("outside the project", encoding="utf-8")
    path = _files(shop) / ARTIFACTS["csi"][0]
    path.unlink()
    path.symlink_to(secret)
    response = api.get("/api/projects/shop/exports")
    assert response.json()["csi"]["status"] == "malformed"
    assert "outside the project" not in response.text


def test_a_symlinked_receipt_is_not_followed(shop, api, tmp_path):
    _export_both()
    path = _files(shop) / ARTIFACTS["csi"][1]
    real = tmp_path / "receipt.json"
    real.write_bytes(path.read_bytes())
    path.unlink()
    path.symlink_to(real)
    entry = _read(api)["csi"]
    assert (entry["status"], entry["reasons"]) == ("unverified", ["receipt_invalid"])


def test_a_symlinked_project_directory_is_not_followed(shop, api, tmp_path):
    _export_both()
    moved = tmp_path / "outside"
    _files(shop).rename(moved)
    _files(shop).symlink_to(moved)
    body = _read(api)
    assert body["csi"]["status"] == body["r2rml"]["status"] == "malformed"
    assert body["csi"]["content"] is None


def test_a_file_swapped_for_a_symlink_after_the_check_is_not_followed(tmp_path, monkeypatch):
    secret, regular, link = tmp_path / "secret", tmp_path / "regular", tmp_path / "export"
    secret.write_text("outside", encoding="utf-8")
    regular.write_text("x", encoding="utf-8")
    link.symlink_to(secret)
    real = os.lstat
    monkeypatch.setattr(os, "lstat", lambda p: real(regular if Path(p) == link else p))
    assert project_exports._read_file(tmp_path, "export", 100) == ("malformed", None)


def test_a_file_that_grows_after_the_check_is_withheld(tmp_path, monkeypatch):
    path, small = tmp_path / "export", tmp_path / "small"
    path.write_text("x" * 50, encoding="utf-8")
    small.write_text("x", encoding="utf-8")
    real = os.lstat
    monkeypatch.setattr(os, "lstat", lambda p: real(small if Path(p) == path else p))
    assert project_exports._read_file(tmp_path, "export", 10) == ("too_large", None)


def test_a_fifo_is_not_opened(shop, api):
    _export_both()
    path = _files(shop) / ARTIFACTS["csi"][0]
    path.unlink()
    os.mkfifo(path)
    assert _read(api)["csi"]["status"] == "malformed"


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores file modes")
def test_an_unreadable_artifact_is_attention_even_if_the_other_is_missing(shop, api, tmp_path):
    assert _project("csi").exit_code == 0
    path = _files(shop) / ARTIFACTS["csi"][0]
    path.chmod(0)
    try:
        response = api.get("/api/projects/shop/exports")
    finally:
        path.chmod(0o640)
    body = response.json()
    assert body["csi"]["status"] == "unreadable" and body["csi"]["content"] is None
    assert body["r2rml"]["status"] == "missing" and body["state"] == "attention"
    assert str(tmp_path) not in response.text


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores file modes")
def test_an_unreadable_receipt_reads_as_unverified(shop, api, tmp_path):
    _export_both()
    path = _files(shop) / ARTIFACTS["csi"][1]
    path.chmod(0)
    try:
        response = api.get("/api/projects/shop/exports")
    finally:
        path.chmod(0o640)
    entry = response.json()["csi"]
    assert (entry["status"], entry["reasons"]) == ("unverified", ["receipt_unreadable"])
    assert entry["content"] is not None and str(tmp_path) not in response.text


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores file modes")
def test_a_directory_that_cannot_be_searched_is_never_none(shop, api, tmp_path):
    _export_both()
    mapping = _mapping_path(shop)
    (tmp_path / "mapping.yaml").write_bytes(mapping.read_bytes())
    shop.update_project("shop", mapping_config_path=str(tmp_path / "mapping.yaml"))
    _files(shop).chmod(0)
    try:
        response = api.get("/api/projects/shop/exports")
    finally:
        _files(shop).chmod(0o755)
    body = response.json()
    assert response.status_code == 200 and body["state"] == "attention"
    assert body["csi"]["status"] == body["r2rml"]["status"] == "unreadable"
    assert str(tmp_path) not in response.text


def test_any_other_read_error_says_nothing_about_the_path(shop, api, tmp_path, monkeypatch):
    _export_both()

    def fails(*args, **kwargs):
        raise OSError(errno.EIO, "disk error", str(tmp_path / "private"))

    monkeypatch.setattr(project_exports, "_read_file", fails)
    response = TestClient(api.app, raise_server_exceptions=False).get("/api/projects/shop/exports")
    assert response.status_code == 500
    assert response.json() == {"detail": "Could not read this project's exports"}


def test_an_unknown_project_is_404(api):
    assert api.get("/api/projects/nope/exports").status_code == 404


def test_reading_regenerates_and_writes_nothing(shop, api, monkeypatch):
    _export_both()

    def snapshot() -> dict[str, tuple[bytes, int]]:
        return {str(p): (p.read_bytes(), p.stat().st_mtime_ns)
                for p in shop.dir.rglob("*") if p.is_file()}

    def must_not_run(*args, **kwargs):
        raise AssertionError("a read must not generate an export")

    monkeypatch.setattr("r2g.csi.mapping_to_csi", must_not_run)
    monkeypatch.setattr("r2g.r2rml.mapping_to_r2rml", must_not_run)
    before = snapshot()
    assert _read(api)["state"] == "complete"
    assert snapshot() == before


def test_the_response_holds_no_filesystem_path(shop, api, tmp_path):
    _export_both()
    assert str(tmp_path) not in api.get("/api/projects/shop/exports").text


def test_a_receipt_written_for_another_tenant_is_not_trusted(shop, api, monkeypatch):
    _export_both()
    monkeypatch.setattr(control_plane, "mode", lambda: True)
    monkeypatch.setenv("R2G_TENANT", "acme")
    body = project_exports.read_project_exports(shop, shop.get_project("shop"))
    assert body["csi"]["reasons"] == ["tenant_mismatch"] and body["state"] == "attention"
