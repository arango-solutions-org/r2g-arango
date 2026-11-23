"""Shared source introspection and snapshot construction.

All catalog-facing entry points use this module so reviewed key overlays and
classifications are applied in the same order in the CLI, Studio, and MCP.

A snapshot is normally introspected through the source's connection. It can
instead be read from a schema file (RSA's ``PhysicalSchema`` JSON, the format
``r2g ingest-schema`` and ``Schema.save_to_file`` write) for a source we cannot
connect to: the file goes through the same overlay and classification steps,
and the snapshot records which file it came from and that file's digest.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError
from relational_schema_analyzer.types import PhysicalSchema

from r2g.types import Schema

if TYPE_CHECKING:
    from r2g.catalog import CatalogManager, SchemaSnapshot, SourceConfig


#: ``SourceConfig.source_params`` key that marks a source registered with no
#: connection. Its snapshots can only be read from a schema file.
OFFLINE_PARAM = "offline"

#: Largest schema file read for an offline snapshot (the same bound as an
#: imported ontology file). A schema of thousands of tables is a few MB.
MAX_SCHEMA_FILE_BYTES = 50 * 1024 * 1024


class SchemaFileError(ValueError):
    """A schema file could not be read as a physical schema."""


def is_offline(source: SourceConfig) -> bool:
    """True when the source was registered without a connection."""
    return bool((source.source_params or {}).get(OFFLINE_PARAM))


@dataclass(frozen=True)
class SchemaFile:
    """A physical schema read from a file, with where it came from."""

    schema: PhysicalSchema
    source: str
    fingerprint: str


def read_schema_file(path: str | Path) -> SchemaFile:
    """Read RSA ``PhysicalSchema`` JSON from *path* for an offline snapshot.

    Refuses a missing, oversized, non-JSON or table-less file with a message
    naming the file, so a bad handoff fails before anything is stored.
    """
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise SchemaFileError(f"Schema file '{resolved}' does not exist or is not a file")
    size = resolved.stat().st_size
    if size > MAX_SCHEMA_FILE_BYTES:
        raise SchemaFileError(
            f"Schema file '{resolved}' is {size} bytes; the limit is {MAX_SCHEMA_FILE_BYTES}"
        )
    raw = resolved.read_bytes()
    try:
        physical = PhysicalSchema.model_validate_json(raw)
    except ValidationError as exc:
        raise SchemaFileError(
            f"Schema file '{resolved}' is not a physical schema "
            f"(RSA PhysicalSchema JSON): {exc.error_count()} error(s), first: "
            f"{exc.errors()[0]['msg']}"
            + (f" at {where}" if (where := ".".join(str(p) for p in exc.errors()[0]["loc"])) else "")
        ) from exc
    if not physical.tables:
        raise SchemaFileError(f"Schema file '{resolved}' has no tables")
    return SchemaFile(
        schema=physical,
        source=str(resolved),
        fingerprint=hashlib.sha256(raw).hexdigest(),
    )


@dataclass(frozen=True)
class SnapshotBuildResult:
    """An introspected schema plus non-secret overlay audit metadata."""

    schema: Schema
    key_overlay_summary: dict[str, int] = field(default_factory=dict)
    key_overlay_source: str | None = None
    key_overlay_fingerprint: str | None = None
    classifications_applied: int = 0
    #: Set when the schema was read from a file rather than introspected.
    schema_file_source: str | None = None
    schema_file_fingerprint: str | None = None


def overlay_fingerprint(overlay: dict[str, Any] | None) -> str | None:
    """Return a stable SHA-256 digest for parsed overlay content."""
    if overlay is None:
        return None
    canonical = json.dumps(
        overlay,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def build_source_schema(
    source: SourceConfig,
    *,
    schema_name: str,
    schema_file: SchemaFile | None = None,
) -> SnapshotBuildResult:
    """Introspect (or take *schema_file*), apply a reviewed key overlay, then
    add classifications.

    An offline source has no connection, so it must be given a schema file.
    """
    physical: PhysicalSchema
    if schema_file is not None:
        physical = schema_file.schema
    else:
        if is_offline(source):
            raise ValueError(
                f"Source '{source.name}' is offline (registered without a connection); "
                "take its snapshot from a schema file"
            )
        from r2g.connectors.base import create_source_connector

        connector = create_source_connector(
            source.source_type or "postgresql",
            source.connection_string,
            schema_name=schema_name,
            source_params=source.source_params,
        )
        physical = connector.get_schema()
    summary: dict[str, int] = {}

    if source.key_overlay is not None:
        from relational_schema_analyzer import apply_key_overlay, overlay_summary

        physical = apply_key_overlay(physical, source.key_overlay)
        summary = overlay_summary(physical)

    # Re-validate into r2g's compatibility subclasses before classifications
    # and persistence. ForeignKey provenance (overlay:* / enforced=False)
    # survives; RSA Table.extra is summarized above because r2g omits it.
    schema = Schema.model_validate(physical.model_dump())
    # RSA's dump writes False for a table whose source never said; keep it unknown.
    for table_name, table in physical.tables.items():
        if "is_view" not in table.model_fields_set:
            schema.tables[table_name].is_view = None
    classified = 0
    if source.classifications:
        from r2g.classification import annotate_schema

        classified = annotate_schema(schema, source.classifications)

    return SnapshotBuildResult(
        schema=schema,
        key_overlay_summary=summary,
        key_overlay_source=source.key_overlay_source if summary else None,
        key_overlay_fingerprint=overlay_fingerprint(source.key_overlay),
        classifications_applied=classified,
        schema_file_source=schema_file.source if schema_file else None,
        schema_file_fingerprint=schema_file.fingerprint if schema_file else None,
    )


def capture_source_snapshot(
    catalog: CatalogManager,
    source_name: str,
    *,
    schema_name: str,
    schema_file: SchemaFile | None = None,
) -> tuple[SchemaSnapshot, SnapshotBuildResult]:
    """Build and persist one source snapshot through the shared pipeline.

    With *schema_file* the schema is read from that file instead of the
    source's connection; everything after that is the same.
    """
    source = catalog.get_source(source_name)
    if source is None:
        raise ValueError(f"Source '{source_name}' not found")
    result = build_source_schema(source, schema_name=schema_name, schema_file=schema_file)
    snapshot = catalog.create_snapshot(
        source_name,
        result.schema,
        pg_schema=schema_name,
        key_overlay_summary=result.key_overlay_summary,
        key_overlay_source=result.key_overlay_source,
        key_overlay_fingerprint=result.key_overlay_fingerprint,
        schema_file_source=result.schema_file_source,
        schema_file_fingerprint=result.schema_file_fingerprint,
    )
    return snapshot, result

