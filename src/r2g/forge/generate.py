"""The ADR-0006 D-4 seam: ``generate(ontology, dialect, seed) -> {ddl, loader, rows}``,
and its volume twin :func:`generate_bulk` (PLAN F-11)."""

from __future__ import annotations

from typing import Any, Dict, Optional

from .bulk import DEFAULT_CHUNK_ROWS, write_bulk
from .core import ForgeArtifacts, ForgeError, ForgeOntology, plan_schema, synthesize_rows
from .dialects import get_dialect
from .scale import ForgeScale
from .synthesis import planned_row_counts, stream_rows


def generate(
    ontology: ForgeOntology,
    dialect: str = "postgres",
    seed: int = 0,
    *,
    rows_per_entity: int = 10,
    scale: Optional[ForgeScale] = None,
) -> ForgeArtifacts:
    """Ontology -> ``ForgeArtifacts(ddl, load_sql, rows)`` for one dialect.

    Deterministic end to end (PLAN F-5): the same ``(ontology, dialect, seed,
    rows_per_entity, scale)`` reproduces byte-identical artifacts. The schema
    plan and the rows are dialect-independent — data is synthesized once and
    only *projected* per system (ADR-0006 D-2), so ``rows`` is identical across
    dialects for the same ontology and seed. Unknown dialects and refused
    ontologies or scales raise :class:`~r2g.forge.core.ForgeError` before
    anything is rendered. Everything is held in memory; at volume use
    :func:`generate_bulk`.
    """
    target = get_dialect(dialect)
    if rows_per_entity < 1:
        raise ForgeError("rows_per_entity must be >= 1")
    plan = plan_schema(ontology, scale)
    rows = synthesize_rows(plan, seed, rows_per_entity, scale)
    return ForgeArtifacts(
        dialect=target.name,
        seed=seed,
        ddl=target.render_ddl(plan),
        load_sql=target.render_loader(plan, rows, seed),
        rows=rows,
    )


def generate_bulk(
    ontology: ForgeOntology,
    out_dir: str,
    dialect: str = "postgres",
    seed: int = 0,
    *,
    rows_per_entity: int = 10,
    scale: Optional[ForgeScale] = None,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
) -> Dict[str, Any]:
    """Stream the same rows :func:`generate` would hold into chunked files
    under ``out_dir`` (schema, data chunks, bulk loader, ``forge.bulk.json``)
    and return the manifest. Memory does not grow with the rows (PLAN F-11)."""
    get_dialect(dialect)
    plan = plan_schema(ontology, scale)
    rows = stream_rows(plan, seed, rows_per_entity, scale)
    provenance = {
        "rowsPerEntity": rows_per_entity,
        "scale": (scale or ForgeScale()).document(),
        "plannedRows": planned_row_counts(plan, rows_per_entity, scale),
    }
    return write_bulk(
        plan, dialect, rows, out_dir, seed=seed, chunk_rows=chunk_rows, provenance=provenance
    )


__all__ = ["generate", "generate_bulk"]
