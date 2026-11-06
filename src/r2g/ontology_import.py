"""Curated ontology import, storage, and physical-schema reconciliation.

Parsing belongs to ``relational-schema-analyzer`` because RSA owns the
``phys:`` vocabulary.  This module owns r2g's catalog boundary: it stores only
validated local bundles, compiles RSA's normalized mapping into
``MappingConfig``, and proves that every physical reference exists in one exact
source snapshot.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Literal
from uuid import uuid4

from pydantic import BaseModel, Field

from r2g.catalog import CatalogManager, OntologyArtifact, OntologyBundle
from r2g.types import CollectionMapping, EdgeDefinition, MappingConfig, Schema

MAX_ONTOLOGY_FILE_BYTES = 50 * 1024 * 1024
MAX_ONTOLOGY_FILES = 8
_SAFE_FILENAME = re.compile(r"[^A-Za-z0-9_.-]+")


class OntologyImportError(ValueError):
    """A curated ontology bundle could not be imported safely."""


class ReconciliationIssue(BaseModel):
    severity: Literal["warning", "error"]
    code: str
    message: str
    ontology_term: str | None = None
    source_object: str | None = None


class ReconciliationReport(BaseModel):
    ontology_id: str | None = None
    snapshot_id: str | None = None
    matched_entities: int = 0
    matched_properties: int = 0
    matched_relationships: int = 0
    projected_edges_required: int = 0
    issues: list[ReconciliationIssue] = Field(default_factory=list)

    @property
    def executable(self) -> bool:
        return not any(issue.severity == "error" for issue in self.issues)


@dataclass(frozen=True)
class UploadedOntology:
    filename: str
    content: bytes


@dataclass(frozen=True)
class ImportedOntology:
    record: OntologyBundle
    normalized_bundle: dict[str, Any]


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _safe_filename(filename: str, index: int) -> str:
    leaf = Path(filename).name
    cleaned = _SAFE_FILENAME.sub("_", leaf).strip("._")
    if not cleaned:
        cleaned = f"ontology-{index}.rdf"
    return f"{index:02d}-{cleaned}"


def _validate_uploads(files: Iterable[UploadedOntology]) -> list[UploadedOntology]:
    items = list(files)
    if not items:
        raise OntologyImportError("At least one ontology document is required")
    if len(items) > MAX_ONTOLOGY_FILES:
        raise OntologyImportError(
            f"At most {MAX_ONTOLOGY_FILES} ontology documents may be imported"
        )
    for item in items:
        suffix = Path(item.filename).suffix.lower()
        if suffix not in {".ttl", ".owl", ".rdf", ".xml"}:
            raise OntologyImportError(
                f"Unsupported ontology serialization for {item.filename!r}"
            )
        if not item.content:
            raise OntologyImportError(f"Ontology document {item.filename!r} is empty")
        if len(item.content) > MAX_ONTOLOGY_FILE_BYTES:
            raise OntologyImportError(
                f"Ontology document {item.filename!r} exceeds "
                f"{MAX_ONTOLOGY_FILE_BYTES} bytes"
            )
    return items


def _rsa_import(root: Path, imports: list[Path]) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        import relational_schema_analyzer as rsa
    except ImportError as exc:  # pragma: no cover - package is a core dependency
        raise OntologyImportError(
            "relational-schema-analyzer with the 'owl' extra is required"
        ) from exc
    importer = getattr(rsa, "import_owl_bundle", None)
    if importer is None:
        raise OntologyImportError(
            "This relational-schema-analyzer version cannot import OWL bundles; "
            "install the version required by r2g"
        )
    try:
        result = importer(root, local_imports=imports)
    except Exception as exc:
        error_type = getattr(rsa, "OwlImportError", ValueError)
        if isinstance(exc, error_type):
            raise OntologyImportError(str(exc)) from exc
        raise

    analysis = getattr(result, "analysis", result)
    if hasattr(analysis, "to_bundle"):
        bundle = analysis.to_bundle()
    elif isinstance(analysis, dict):
        bundle = analysis
    else:
        raise OntologyImportError("RSA returned an unsupported ontology result")
    provenance = getattr(result, "provenance", None)
    if provenance is None:
        provenance = (bundle.get("metadata") or {}).get("ontologyImport") or {}
    if hasattr(provenance, "model_dump"):
        provenance = provenance.model_dump()
    if not isinstance(provenance, dict):
        provenance = {}
    return bundle, provenance


def _provenance_digest(provenance: dict[str, Any], key: str) -> str:
    value = provenance.get(key)
    if isinstance(value, str) and value:
        return value if value.startswith("sha256:") else f"sha256:{value}"
    raise OntologyImportError(f"RSA ontology provenance is missing {key}")


def import_ontology_bundle(
    catalog: CatalogManager,
    *,
    name: str,
    root: UploadedOntology,
    imports: Iterable[UploadedOntology] = (),
    equivalent_root: UploadedOntology | None = None,
    equivalent_imports: Iterable[UploadedOntology] = (),
) -> ImportedOntology:
    """Validate, normalize, and atomically store one curated ontology closure."""
    primary = _validate_uploads([root, *imports])
    equivalent = (
        _validate_uploads([equivalent_root, *equivalent_imports])
        if equivalent_root is not None
        else []
    )
    ontology_id = str(uuid4())
    ontologies_dir = catalog.dir / "ontologies"
    staging = ontologies_dir / f".{ontology_id}.tmp"
    destination = ontologies_dir / ontology_id
    staging.mkdir(parents=True, exist_ok=False)

    def write_set(items: list[UploadedOntology], prefix: str) -> list[Path]:
        paths: list[Path] = []
        for index, item in enumerate(items, 1):
            path = staging / prefix / _safe_filename(item.filename, index)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(item.content)
            paths.append(path)
        return paths

    try:
        primary_paths = write_set(primary, "source")
        bundle, provenance = _rsa_import(primary_paths[0], primary_paths[1:])
        graph_digest = _provenance_digest(provenance, "canonicalGraphDigest")
        mapping_digest = _sha256(_canonical_json(bundle.get("physicalMapping") or {}))

        equivalent_ok: bool | None = None
        if equivalent:
            equivalent_paths = write_set(equivalent, "equivalent")
            equivalent_bundle, equivalent_provenance = _rsa_import(
                equivalent_paths[0], equivalent_paths[1:]
            )
            equivalent_graph = _provenance_digest(
                equivalent_provenance, "canonicalGraphDigest"
            )
            equivalent_mapping = _sha256(
                _canonical_json(equivalent_bundle.get("physicalMapping") or {})
            )
            equivalent_ok = (
                equivalent_graph == graph_digest
                and equivalent_mapping == mapping_digest
            )
            if not equivalent_ok:
                raise OntologyImportError(
                    "The supplied Turtle and RDF/XML bundles are not equivalent"
                )

        normalized = staging / "normalized.json"
        normalized.write_bytes(_canonical_json(bundle) + b"\n")
        documents = provenance.get("documents")
        document_iris: dict[str, str] = {}
        if isinstance(documents, list):
            for document in documents:
                if isinstance(document, dict):
                    filename = str(document.get("filename") or "")
                    iri = str(document.get("ontologyIri") or "")
                    if filename and iri:
                        document_iris[Path(filename).name] = iri

        artifacts: list[OntologyArtifact] = []
        for path, item in zip(primary_paths, primary):
            suffix = Path(item.filename).suffix.lower()
            artifacts.append(
                OntologyArtifact(
                    filename=str(path.relative_to(staging)),
                    ontology_iri=document_iris.get(
                        Path(item.filename).name,
                        str(provenance.get("rootOntologyIri") or ""),
                    ),
                    serialization=(
                        "turtle" if suffix == ".ttl" else "rdfxml"
                    ),
                    sha256=hashlib.sha256(item.content).hexdigest(),
                    size_bytes=len(item.content),
                )
            )
        for path, item in zip(
            list((staging / "equivalent").glob("*")) if equivalent else [],
            equivalent,
        ):
            suffix = Path(item.filename).suffix.lower()
            artifacts.append(
                OntologyArtifact(
                    filename=str(path.relative_to(staging)),
                    ontology_iri=document_iris.get(Path(item.filename).name, ""),
                    serialization=(
                        "turtle" if suffix == ".ttl" else "rdfxml"
                    ),
                    sha256=hashlib.sha256(item.content).hexdigest(),
                    size_bytes=len(item.content),
                )
            )

        metadata = bundle.get("metadata") or {}
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staging, destination)
        try:
            record = catalog.add_ontology(
                name=name.strip() or str(provenance.get("rootOntologyIri") or "Ontology"),
                root_ontology_iri=str(provenance.get("rootOntologyIri") or ""),
                version=str(provenance.get("ontologyVersion") or ""),
                canonical_graph_digest=graph_digest,
                mapping_digest=mapping_digest,
                profile_version=str(provenance.get("profileVersion") or "1"),
                rsa_version=str(metadata.get("analyzerVersion") or ""),
                artifacts=artifacts,
                equivalent_serializations=equivalent_ok,
                ontology_id=ontology_id,
            )
        except Exception:
            shutil.rmtree(destination, ignore_errors=True)
            raise
        return ImportedOntology(record=record, normalized_bundle=bundle)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def load_normalized_ontology(
    catalog: CatalogManager, ontology_id: str
) -> dict[str, Any]:
    record = catalog.get_ontology(ontology_id)
    if record is None:
        raise OntologyImportError(f"Ontology '{ontology_id}' not found")
    path = catalog.dir / "ontologies" / ontology_id / "normalized.json"
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OntologyImportError(
            f"Stored ontology '{ontology_id}' is unavailable or corrupt"
        ) from exc


def remove_ontology_bundle(catalog: CatalogManager, ontology_id: str) -> bool:
    """Remove catalog metadata and its server-owned files."""
    removed = catalog.remove_ontology(ontology_id)
    if removed:
        shutil.rmtree(catalog.dir / "ontologies" / ontology_id, ignore_errors=True)
    return removed


def _concept_index(bundle: dict[str, Any]) -> tuple[dict[str, dict], dict[str, dict]]:
    conceptual = bundle.get("conceptualSchema") or {}
    entities = {
        str(entity.get("name")): entity
        for entity in conceptual.get("entities") or []
        if isinstance(entity, dict) and entity.get("name")
    }
    relationships = {
        str(rel.get("type")): rel
        for rel in conceptual.get("relationships") or []
        if isinstance(rel, dict) and rel.get("type")
    }
    return entities, relationships


def compile_ontology_mapping(
    bundle: dict[str, Any],
    schema: Schema,
    *,
    source_schema: str = "public",
    ontology_id: str | None = None,
    snapshot_id: str | None = None,
) -> tuple[MappingConfig, ReconciliationReport]:
    """Compile RSA's normalized mapping and reconcile it with one snapshot."""
    report = ReconciliationReport(
        ontology_id=ontology_id,
        snapshot_id=snapshot_id,
    )
    entities, relationships = _concept_index(bundle)
    physical = bundle.get("physicalMapping") or {}
    physical_entities = physical.get("entities") or {}
    physical_relationships = physical.get("relationships") or {}
    collections: dict[str, CollectionMapping] = {}
    keys_by_table: dict[str, list[str]] = {}

    for entity_name, mapping in physical_entities.items():
        if not isinstance(mapping, dict):
            continue
        table_name = str(mapping.get("tableName") or "")
        concept = entities.get(str(entity_name), {})
        term = concept.get("term")
        if not table_name:
            report.issues.append(
                ReconciliationIssue(
                    severity="error",
                    code="missing-table-mapping",
                    message=f"Mapped entity '{entity_name}' has no physical table",
                    ontology_term=term,
                )
            )
            continue
        table = schema.tables.get(table_name)
        if table is None:
            report.issues.append(
                ReconciliationIssue(
                    severity="error",
                    code="source-object-missing",
                    message=f"Source object '{table_name}' is not in the pinned snapshot",
                    ontology_term=term,
                    source_object=table_name,
                )
            )
            continue
        expected_view = str(mapping.get("style") or "TABLE").upper() == "VIEW"
        if bool(getattr(table, "is_view", False)) != expected_view:
            report.issues.append(
                ReconciliationIssue(
                    severity="error",
                    code="source-object-kind-mismatch",
                    message=(
                        f"Ontology maps '{table_name}' as "
                        f"{'VIEW' if expected_view else 'TABLE'}, but the snapshot does not"
                    ),
                    ontology_term=term,
                    source_object=table_name,
                )
            )

        pm_properties = mapping.get("properties") or {}
        concept_properties = {
            str(prop.get("name")): prop
            for prop in concept.get("properties") or []
            if isinstance(prop, dict) and prop.get("name")
        }
        columns = {column.name for column in table.columns}
        field_mappings: dict[str, str] = {}
        field_terms: dict[str, str] = {}
        for property_name, property_mapping in pm_properties.items():
            if not isinstance(property_mapping, dict):
                continue
            column = str(
                property_mapping.get("field")
                or property_mapping.get("columnName")
                or property_name
            )
            if column not in columns:
                report.issues.append(
                    ReconciliationIssue(
                        severity="error",
                        code="source-column-missing",
                        message=f"Column '{table_name}.{column}' is not in the snapshot",
                        ontology_term=(
                            concept_properties.get(str(property_name), {}).get("term")
                        ),
                        source_object=table_name,
                    )
                )
                continue
            field_mappings[column] = str(property_name)
            property_term = concept_properties.get(str(property_name), {}).get("term")
            if property_term:
                field_terms[column] = str(property_term)
            report.matched_properties += 1

        declared_key = mapping.get("primaryKey") or []
        key_fields = [str(value) for value in declared_key]
        if not key_fields:
            key_fields = list(table.primary_key)
        if not key_fields and len(table.unique_constraints) == 1:
            key_fields = list(table.unique_constraints[0])
        if not key_fields:
            report.issues.append(
                ReconciliationIssue(
                    severity="error",
                    code="identity-unresolved",
                    message=(
                        f"Source object '{table_name}' has no single approved PK/UNIQUE "
                        "identity"
                    ),
                    ontology_term=term,
                    source_object=table_name,
                )
            )
        elif any(column not in columns for column in key_fields):
            report.issues.append(
                ReconciliationIssue(
                    severity="error",
                    code="identity-column-missing",
                    message=f"Identity for '{table_name}' references a missing column",
                    ontology_term=term,
                    source_object=table_name,
                )
            )
        keys_by_table[table_name] = key_fields
        collection_data: dict[str, Any] = {
            "source_table": table_name,
            "target_collection": str(entity_name),
            "collection_type": "document",
            "field_mappings": field_mappings,
            "field_terms": field_terms,
            "term": term,
            "key_fields": key_fields,
        }
        collections[table_name] = CollectionMapping.model_validate(collection_data)
        report.matched_entities += 1

    edges: list[EdgeDefinition] = []
    for relationship_name, mapping in physical_relationships.items():
        if not isinstance(mapping, dict):
            continue
        if mapping.get("style") != "FOREIGN_KEY":
            report.issues.append(
                ReconciliationIssue(
                    severity="warning",
                    code="relationship-style-not-materialized",
                    message=(
                        f"Relationship '{relationship_name}' uses "
                        f"{mapping.get('style')}; review its graph materialization"
                    ),
                )
            )
            continue
        source_table = str(mapping.get("fromTable") or "")
        target_table = str(mapping.get("toTable") or "")
        from_columns = [str(value) for value in mapping.get("fromColumns") or []]
        to_columns = [str(value) for value in mapping.get("toColumns") or []]
        relationship = relationships.get(str(relationship_name), {})
        term = relationship.get("term")
        if source_table not in collections or target_table not in collections:
            report.issues.append(
                ReconciliationIssue(
                    severity="error",
                    code="relationship-endpoint-missing",
                    message=(
                        f"Relationship '{relationship_name}' references an unmapped "
                        "source object"
                    ),
                    ontology_term=term,
                )
            )
            continue
        if not from_columns or len(from_columns) != len(to_columns):
            report.issues.append(
                ReconciliationIssue(
                    severity="error",
                    code="relationship-columns-invalid",
                    message=(
                        f"Relationship '{relationship_name}' must have equally sized, "
                        "ordered source and target columns"
                    ),
                    ontology_term=term,
                )
            )
            continue
        source_columns = {column.name for column in schema.tables[source_table].columns}
        target_columns = {column.name for column in schema.tables[target_table].columns}
        if any(column not in source_columns for column in from_columns) or any(
            column not in target_columns for column in to_columns
        ):
            report.issues.append(
                ReconciliationIssue(
                    severity="error",
                    code="relationship-column-missing",
                    message=(
                        f"Relationship '{relationship_name}' references a missing column"
                    ),
                    ontology_term=term,
                )
            )
            continue
        target_key = keys_by_table.get(target_table, [])
        if to_columns != target_key:
            unique_keys = [
                list(unique)
                for unique in schema.tables[target_table].unique_constraints
            ]
            if to_columns not in unique_keys:
                report.issues.append(
                    ReconciliationIssue(
                        severity="error",
                        code="relationship-target-not-unique",
                        message=(
                            f"Relationship '{relationship_name}' targets columns that "
                            "are not an approved PK/UNIQUE key"
                        ),
                        ontology_term=term,
                    )
                )
                continue
            report.projected_edges_required += 1
            report.issues.append(
                ReconciliationIssue(
                    severity="warning",
                    code="edge-projection-required",
                    message=(
                        f"Relationship '{relationship_name}' targets an alternate "
                        "UNIQUE key; the execution bundle must project canonical _to"
                    ),
                    ontology_term=term,
                )
            )
        edges.append(
            EdgeDefinition(
                edge_collection=str(relationship_name),
                from_collection=source_table,
                to_collection=target_table,
                from_fields=from_columns,
                to_fields=to_columns,
                term=term,
            )
        )
        report.matched_relationships += 1

    return (
        MappingConfig(
            source_schema=source_schema,
            collections=collections,
            edges=edges,
        ),
        report,
    )
