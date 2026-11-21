"""A project's published exports (#54): what ``export-csi --project`` and
``export-r2rml --project`` read, and the checks made before either writes anything.
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import jsonschema
import yaml
from pydantic import ValidationError

from r2g import __version__, control_plane
from r2g.catalog import Catalog, CatalogManager, Project, validate_project_name
from r2g.config import ConfigManager
from r2g.csi import validate_csi
from r2g.curated_labels import load_curated_labels
from r2g.expressions import extract_bind_references
from r2g.governed import _write_atomically
from r2g.log import get_logger
from r2g.r2rml import R2RMLError, summarize_r2rml
from r2g.security import ENC_PREFIX, SECRET_ENV, SECRET_FILENAME
from r2g.types import FieldExpression, MappingConfig, Schema

#: export kind -> (its file, its receipt), fixed names in the project's directory.
ARTIFACTS = {
    "csi": ("mapping.csi.json", "mapping.csi.export.json"),
    "r2rml": ("mapping.r2rml.ttl", "mapping.r2rml.export.json"),
}
RECEIPT_FORMAT = 1
#: Readable by the writer and the project directory's group, never by everyone.
PUBLISHED_MODE = 0o640
#: The largest export the Studio reads, unless ``R2G_EXPORT_MAX_BYTES`` says otherwise.
DEFAULT_MAX_EXPORT_BYTES = 10 * 1024 * 1024
_RECEIPT_TEXT_FIELDS = ("project", "artifact_sha256", "mapping_sha256", "schema_sha256",
                        "snapshot_id", "generated_at", "r2g_version")
_RECEIPT_FIELDS = frozenset({*_RECEIPT_TEXT_FIELDS, "format_version", "tenant", "kind", "options"})
#: CSI's provenance stamps that a catalog snapshot does not keep.
_BITEMPORAL_FIELDS = ("transactionTime", "validTime", "validTimeSource", "predecessorFingerprint")

logger = get_logger(__name__)


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
        schema_sha256=_schema_sha256(snapshot.schema_data),
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
        computed = [c for fx in cm.field_expressions for c in _expression_columns(fx)]
        need(cm.source_table, [*cm.field_mappings, *(cm.include_fields or []), *computed],
             f"collection {key}")
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


def _expression_columns(fx: FieldExpression) -> list[str]:
    """The source columns an expression can be shown to read: its declared ``sources``, the
    target of a pass-through that declares none (what the transformer reads), and AQL's
    ``@column`` references. KSQL and Python text is opaque, so a column named only there is
    not checked."""
    if not fx.expression.strip():
        return fx.sources or [fx.target]
    return [*fx.sources, *(extract_bind_references(fx.expression) if fx.engine == "aql" else [])]


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


def publish_export(export: ProjectExport, kind: str, text: str, generated_at: str,
                   options: dict[str, Any]) -> tuple[Path, list[str]]:
    """Replace one export, then its receipt. A failure between the two leaves a receipt that
    does not match the file, which reads as unverified. Returns the file and any warnings."""
    prepare_export_directory(export)
    name, receipt_name = ARTIFACTS[kind]
    path = export.directory / name
    warnings = _write_atomically(path, text, mode=PUBLISHED_MODE, durable=True)
    receipt = {
        "format_version": RECEIPT_FORMAT,
        "project": export.name,
        "tenant": export.tenant,
        "kind": kind,
        "artifact_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        "mapping_sha256": export.mapping_sha256,
        "schema_sha256": export.schema_sha256,
        "snapshot_id": export.snapshot_id,
        "generated_at": generated_at,
        "r2g_version": __version__,
        "options": options,
    }
    warnings += _write_atomically(export.directory / receipt_name,
                                  json.dumps(receipt, indent=2) + "\n",
                                  mode=PUBLISHED_MODE, durable=True)
    return path, list(dict.fromkeys(warnings))


def read_curated_labels(path: str) -> tuple[dict[str, Any], str]:
    """The curated labels and the hash of the bytes that hold exactly those labels."""
    labels = load_curated_labels(path)
    try:
        raw = Path(path).read_bytes()
        unchanged = json.loads(raw) == labels
    except (OSError, ValueError) as exc:
        raise ProjectExportError(f"Cannot read the curated labels at {path}: {exc}") from exc
    if not unchanged:
        raise ProjectExportError(f"The curated labels at {path} changed while read; try again")
    return labels, hashlib.sha256(raw).hexdigest()


def read_project_exports(catalog: CatalogManager, project: Project) -> dict[str, Any]:
    """What is saved for this project and how far it can be trusted. Reads only: it never
    generates an export, and returns no filesystem path or operating-system error text."""
    directory = catalog.dir.expanduser() / "projects" / project.name
    limit = _max_export_bytes()
    tenant = os.environ.get("R2G_TENANT", "").strip() if control_plane.mode() else None
    current = _current_inputs(catalog, project)
    csi, r2rml = (_read_artifact(kind, directory, project.name, tenant, current, limit)
                  for kind in ("csi", "r2rml"))
    trusted = all(e["status"] in ("current", "stale") for e in (csi, r2rml))
    same_inputs = same_labels = None
    if trusted:
        same_inputs = all(csi["receipt"][f] == r2rml["receipt"][f]
                          for f in ("mapping_sha256", "schema_sha256"))
        same_labels = (csi["receipt"]["options"].get("curated_labels_sha256")
                       == r2rml["receipt"]["options"].get("curated_labels_sha256"))
    return {
        "project": project.name,
        "state": _overall_state(csi["status"], r2rml["status"], same_inputs, same_labels),
        "same_inputs": same_inputs,
        "same_curated_labels": same_labels,
        "current": current,
        "csi": csi,
        "r2rml": r2rml,
        "comparison": _compare(csi, r2rml, same_inputs, same_labels),
    }


def _overall_state(csi: str, r2rml: str, same_inputs: bool | None, same_labels: bool | None) -> str:
    if "unreadable" in (csi, r2rml):
        return "attention"
    if csi == r2rml == "missing":
        return "none"
    if "missing" in (csi, r2rml):
        return "partial"
    return "complete" if csi == r2rml == "current" and same_inputs and same_labels else "attention"


def _current_inputs(catalog: CatalogManager, project: Project) -> dict[str, str | None]:
    """The hashes an export must match. One that cannot be read is ``None``, so nothing
    is reported as current against it."""
    mapping_sha256 = None
    path = Path(project.mapping_config_path)
    if path.is_absolute():
        try:
            mapping_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
        except (OSError, ValueError):  # ValueError: a path holding a NUL byte
            logger.warning("export_mapping_unreadable", project=project.name)
    snapshot = catalog.get_latest_snapshot(project.source_name)
    return {
        "mapping_sha256": mapping_sha256,
        "schema_sha256": _schema_sha256(snapshot.schema_data) if snapshot else None,
        "snapshot_id": snapshot.id if snapshot else None,
    }


def _read_artifact(kind: str, directory: Path, project: str, tenant: str | None,
                   current: dict[str, str | None], limit: int) -> dict[str, Any]:
    name, receipt_name = ARTIFACTS[kind]
    entry: dict[str, Any] = {"status": "missing", "reasons": [], "receipt": None,
                             "content": None, "structure": None}
    try:
        validate_project_name(project)
    except ValueError:  # never build a path from a name that could leave the projects root
        return {**entry, "status": "malformed"}
    state, raw = _read_file(directory, name, limit)
    if raw is None:
        entry["status"] = state
        return entry
    try:
        entry["content"] = raw.decode("utf-8")
        entry["structure"] = (_csi_structure(entry["content"]) if kind == "csi"
                              else summarize_r2rml(entry["content"]))
    except UnicodeDecodeError:
        return {**entry, "status": "malformed", "reasons": ["not_utf8"]}
    except R2RMLError as exc:
        return {**entry, "status": "malformed", "reasons": [f"invalid_r2rml: {exc}"]}
    except (ValueError, RecursionError, jsonschema.ValidationError):
        return {**entry, "status": "malformed", "reasons": ["invalid_csi"]}
    if kind == "csi":
        document = json.loads(entry["content"])["provenance"]
        missing = [f for f in _BITEMPORAL_FIELDS if document.get(f) is None]
        entry["bitemporal"] = {"status": "unavailable" if missing else "available",
                               "fields": missing}

    receipt_state, receipt_raw = _read_file(directory, receipt_name, limit)
    receipt, problem = _verified_receipt(receipt_state, receipt_raw, kind, project, tenant, raw)
    if receipt is None:
        return {**entry, "status": "unverified", "reasons": [problem]}
    entry["receipt"] = receipt
    entry["reasons"] = [
        f"{what}_changed" for what, key in (("mapping", "mapping_sha256"), ("schema", "schema_sha256"))
        if receipt[key] != current[key]]
    entry["status"] = "stale" if entry["reasons"] else "current"
    return entry


def _read_file(directory: Path, name: str, limit: int) -> tuple[str, bytes | None]:
    """``("ok", bytes)``, or why not. Nothing is resolved first, so a symlink is never read."""
    path = directory / name
    try:
        if not stat.S_ISDIR(os.lstat(directory).st_mode):
            return "malformed", None
        info = os.lstat(path)
        if not stat.S_ISREG(info.st_mode):
            return "malformed", None
        if info.st_size > limit:
            return "too_large", None
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as f:
            if not stat.S_ISREG(os.fstat(f.fileno()).st_mode):
                return "malformed", None
            raw = f.read(limit + 1)
    except FileNotFoundError:
        return "missing", None
    except PermissionError:
        return "unreadable", None
    except OSError as exc:
        if exc.errno in (errno.ELOOP, errno.ENOTDIR, errno.ENAMETOOLONG):
            return "malformed", None
        raise
    except ValueError:  # a NUL byte or lone surrogate: refused before any system call
        return "malformed", None
    return ("too_large", None) if len(raw) > limit else ("ok", raw)


def _verified_receipt(state: str, raw: bytes | None, kind: str, project: str, tenant: str | None,
                      artifact: bytes) -> tuple[dict[str, Any] | None, str]:
    """The receipt if it vouches for exactly these bytes, else why it does not."""
    if raw is None:
        return None, {"missing": "receipt_missing",
                      "unreadable": "receipt_unreadable"}.get(state, "receipt_invalid")
    try:
        receipt = _load_sendable_json(raw)
    except (ValueError, RecursionError):
        return None, "receipt_invalid"
    if not _receipt_is_well_formed(receipt, kind):
        return None, "receipt_invalid"
    if receipt["project"] != project:
        return None, "project_mismatch"
    if receipt["tenant"] != tenant:
        return None, "tenant_mismatch"
    if receipt["artifact_sha256"] != hashlib.sha256(artifact).hexdigest():
        return None, "artifact_changed"
    return receipt, ""


def _load_sendable_json(raw: str | bytes) -> Any:
    """Parsed JSON the API can send back. Python's parser accepts ``NaN``, ``1e999`` and lone
    surrogates, which the response encoder refuses; here they raise ``ValueError``."""
    value = json.loads(raw)
    json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
    return value


def _receipt_is_well_formed(receipt: Any, kind: str) -> bool:
    """Every field has the type :func:`publish_export` writes, so the rest can rely on it."""
    if not isinstance(receipt, dict) or set(receipt) != _RECEIPT_FIELDS:
        return False
    options = receipt["options"]
    return (type(receipt["format_version"]) is int
            and receipt["format_version"] == RECEIPT_FORMAT
            and receipt["kind"] == kind
            and all(isinstance(receipt[f], str) for f in _RECEIPT_TEXT_FIELDS)
            and isinstance(receipt["tenant"], (str, type(None)))
            and isinstance(options, dict)
            and isinstance(options.get("curated_labels_sha256"), (str, type(None)))
            and (kind != "r2rml" or isinstance(options.get("concept_base"), str)))


def _csi_structure(text: str) -> dict[str, Any]:
    document = _load_sendable_json(text)
    validate_csi(document)
    model, physical = document["conceptualModel"], document["arangoPhysicalMapping"]
    entities = []
    for entity in model["entities"]:
        spec = physical["entities"].get(entity["name"], {})
        fields = spec.get("properties", {})
        # The CSI schema requires an entity's name but not a property's.
        if not all(isinstance(p.get("name"), str) and p["name"] for p in entity.get("properties", [])):
            raise ValueError("a CSI property has no name")
        entities.append({
            "name": entity["name"],
            "collection": spec.get("collectionName"),
            "properties": [
                {"name": p["name"],
                 "field": fields.get(p["name"], {}).get("field", fields.get(p["name"], {}).get("path"))}
                for p in entity.get("properties", [])],
        })
    relationships = [
        {"type": r["type"], "from": r["fromEntity"], "to": r["toEntity"],
         "edge_collection": physical["relationships"].get(r["type"], {}).get("edgeCollectionName")}
        for r in model["relationships"]]
    return {"entities": entities, "relationships": relationships,
            "join_keys": model.get("joinKeys", [])}


def _compare(csi: dict[str, Any], r2rml: dict[str, Any], same_inputs: bool | None,
             same_labels: bool | None) -> dict[str, Any]:
    """Which classes and property names only one export has. Without one mapping, schema and
    set of curated labels behind both, a difference would say nothing, so none is claimed.
    Properties are listed, never paired: the two exports share no identity for a column."""
    reason = ("an export is not verified" if same_inputs is None
              else "the exports came from different inputs" if not same_inputs
              else "the exports used different curated labels" if not same_labels else None)
    if reason is not None:
        return {"status": "not_comparable", "reason": reason, "classes": None, "properties": None}
    base = r2rml["receipt"]["options"].get("concept_base", "")

    def local(iri: str) -> str:
        return iri[len(base):] if base and iri.startswith(base) else iri

    csi_classes = {e["name"]: {p["name"] for p in e["properties"]}
                   for e in csi["structure"]["entities"]}
    r2rml_classes = {local(t["class"]): {local(c["predicate"]) for c in t["columns"]}
                     for t in r2rml["structure"]["tables"]}
    paired = sorted(csi_classes.keys() & r2rml_classes.keys())
    properties = []
    for name in paired:
        csi_only, r2rml_only = (sorted(csi_classes[name] - r2rml_classes[name]),
                                sorted(r2rml_classes[name] - csi_classes[name]))
        if csi_only or r2rml_only:
            properties.append({"class": name, "csi_only": csi_only, "r2rml_only": r2rml_only})
    return {
        "status": "comparable",
        "reason": None,
        "classes": {"paired": paired, "csi_only": sorted(csi_classes.keys() - r2rml_classes.keys()),
                    "r2rml_only": sorted(r2rml_classes.keys() - csi_classes.keys())},
        "properties": properties,
    }


def _max_export_bytes() -> int:
    configured = os.environ.get("R2G_EXPORT_MAX_BYTES", "").strip()
    if configured:
        try:
            if (limit := int(configured)) > 0:
                return limit
        except ValueError:
            pass
        logger.warning("export_max_bytes_invalid", value=configured)
    return DEFAULT_MAX_EXPORT_BYTES


def _schema_sha256(schema: Schema) -> str:
    return hashlib.sha256(schema.model_dump_json().encode()).hexdigest()


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
