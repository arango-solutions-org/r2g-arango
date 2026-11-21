"""A project's published exports (#54): what ``export-csi --project`` and
``export-r2rml --project`` read, and the checks made before either writes anything.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from pydantic import ValidationError

from r2g import control_plane
from r2g.catalog import Catalog, CatalogManager
from r2g.config import ConfigManager
from r2g.security import ENC_PREFIX, SECRET_ENV, SECRET_FILENAME
from r2g.types import MappingConfig, Schema


class ProjectExportError(ValueError):
    """Why a project's mapping cannot be exported; nothing has been written."""


@dataclass(frozen=True)
class ProjectExport:
    """One command's inputs: the schema here is the object its emitter must be given."""

    name: str
    directory: Path
    other_directories: tuple[Path, ...]
    config: MappingConfig
    mapping_sha256: str
    schema: Schema
    schema_sha256: str
    snapshot_id: str
    source_type: str
    tenant: str | None


def resolve_project_export(name: str) -> ProjectExport:
    """Resolve a project's mapping and the latest snapshot of its source, which is the schema
    the Studio edits against, or refuse."""
    catalog_dir = _catalog_dir()
    _require_usable_catalog(catalog_dir)
    catalog = CatalogManager(catalog_dir)
    project = catalog.get_project(name)
    if project is None:
        raise ProjectExportError(f"Project '{name}' not found in catalog {catalog_dir}")
    if not Path(project.mapping_config_path).is_absolute():
        raise ProjectExportError(
            f"Project '{name}' has a relative mapping path ({project.mapping_config_path}), "
            "which would depend on the working directory")
    source = catalog.get_source(project.source_name)
    if source is None:
        raise ProjectExportError(f"Source '{project.source_name}' not found")
    snapshot = catalog.get_latest_snapshot(project.source_name)
    if snapshot is None:
        raise ProjectExportError(
            f"No schema snapshot for source '{project.source_name}'; run `r2g source snapshot`")
    config, mapping_sha256 = _read_mapping(Path(project.mapping_config_path))
    if not config.collections:
        raise ProjectExportError(f"Project '{name}' has an empty mapping")
    problems = check_mapping_coverage(config, snapshot.schema_data, project.source_name)
    if problems:
        raise ProjectExportError(
            f"The mapping names what snapshot {snapshot.id} does not have: " + "; ".join(problems))
    projects_root = catalog_dir.expanduser() / "projects"
    return ProjectExport(
        name=name,
        directory=projects_root / name,
        other_directories=tuple(projects_root / p.name for p in catalog.list_projects()
                                if p.name != name),
        config=config,
        mapping_sha256=mapping_sha256,
        schema=snapshot.schema_data,
        schema_sha256=hashlib.sha256(snapshot.schema_data.model_dump_json().encode()).hexdigest(),
        snapshot_id=snapshot.id,
        source_type=source.source_type,
        tenant=os.environ["R2G_TENANT"].strip() if control_plane.mode() else None,
    )


def check_mapping_coverage(config: MappingConfig, schema: Schema, source_name: str) -> list[str]:
    """Every table and column the mapping names that ``schema`` lacks. The two emitters treat
    these differently, so each is refused here, before either writes. A shared key's binding
    on another source is that source's to check."""
    problems: dict[str, None] = {}

    def need(table: str | None, columns: list[str], where: str) -> None:
        found = schema.tables.get(table or "")
        if found is None:
            problems[f"{where}: table {table!r}"] = None
            return
        present = {c.name for c in found.columns}
        for column in columns:
            if column not in present:
                problems[f"{where}: column {table}.{column}"] = None

    for key, cm in config.collections.items():
        need(cm.source_table, [*cm.field_mappings, *(cm.include_fields or [])], f"collection {key}")
    for edge in config.edges:
        need(edge.from_collection, edge.from_fields, f"edge {edge.edge_collection}")
        need(edge.to_collection, edge.to_fields, f"edge {edge.edge_collection}")
    for shared in config.shared_keys:
        if shared.hub_kind == "entity" and shared.hub_source == source_name:
            need(shared.hub_table, [shared.hub_column or ""], f"shared key {shared.key}")
        for binding in shared.bindings:
            if binding.source == source_name:
                need(binding.table, [binding.column], f"shared key {shared.key}")
    return list(problems)


def prepare_export_directory(export: ProjectExport) -> None:
    """Make sure the published files can go in the project's own directory. Under a control
    plane the Studio creates it, so that a directory made here cannot lock the Studio out."""
    if os.geteuid() == 0:
        raise ProjectExportError(
            "Refusing to publish as root: the Studio could not manage the files")
    try:
        info = os.lstat(export.directory)
    except FileNotFoundError:
        if control_plane.mode():
            raise ProjectExportError(
                f"Project '{export.name}' has no directory yet; open it in the Studio first"
            ) from None
        export.directory.mkdir(parents=True, exist_ok=True)
        info = os.lstat(export.directory)
    if not os.path.isdir(export.directory) or os.path.islink(export.directory):
        raise ProjectExportError(f"Project '{export.name}' directory is not a real directory")
    for other in export.other_directories:
        if other.exists() and os.path.samestat(info, os.stat(other)):
            raise ProjectExportError(
                f"Projects '{export.name}' and '{other.name}' share one directory")


def _catalog_dir() -> Path:
    """The directory :class:`CatalogManager` would use, found without constructing it, since
    constructing it creates the directory and a key."""
    configured = os.environ.get("R2G_CATALOG_DIR", "").strip()
    if control_plane.mode() and not (configured and os.environ.get("R2G_TENANT", "").strip()):
        raise ProjectExportError(
            "CONTROL_PLANE_URL is set: R2G_CATALOG_DIR and R2G_TENANT must name this Studio's "
            "catalog and tenant")
    return Path(configured).expanduser() if configured else Path.home() / ".r2g"


def _require_usable_catalog(directory: Path) -> None:
    """Refuse a missing or invalid catalog, and an encrypted one whose key is gone: opening
    that would write a new key that cannot decrypt it."""
    path = directory / "catalog.json"
    try:
        catalog = Catalog.model_validate_json(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ProjectExportError(f"No catalog at {directory}") from None
    except (OSError, ValueError, ValidationError) as exc:
        raise ProjectExportError(f"Cannot read the catalog at {directory}: {exc}") from exc
    credentials = [
        *(s.connection_string for s in catalog.sources.values()),
        *(t.password for t in catalog.targets.values()),
        *(c.token for c in catalog.catalog_providers.values()),
    ]
    if (any(value and value.startswith(ENC_PREFIX) for value in credentials)
            and not os.environ.get(SECRET_ENV) and not (directory / SECRET_FILENAME).exists()):
        raise ProjectExportError(
            f"The catalog at {directory} is encrypted, but neither {SECRET_FILENAME} nor "
            f"{SECRET_ENV} is available")


def _read_mapping(path: Path) -> tuple[MappingConfig, str]:
    """The mapping and the hash of the bytes it was loaded from, refused if a save moved it."""
    try:
        before = path.read_bytes()
        config = ConfigManager.load_config(path)
        after = path.read_bytes()
    except (OSError, ValueError, yaml.YAMLError) as exc:
        raise ProjectExportError(f"Cannot read the mapping at {path}: {exc}") from exc
    if before != after:
        raise ProjectExportError(f"The mapping at {path} changed while it was read; try again")
    return config, hashlib.sha256(before).hexdigest()
