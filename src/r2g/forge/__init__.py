"""Federation Forge — the reverse generator (contextual-data-fabric ADR-0006, D-4).

Public seam (the only thing the fabric's orchestration may depend on)::

    generate(ontology, dialect, seed) -> ForgeArtifacts(ddl, load_sql, rows)

Layout: :mod:`.core` holds the ontology contract and the dialect-independent
schema plan; :mod:`.scale` the volume knobs (row counts, fan-out skew, NULL
rates); :mod:`.synthesis` the seeded synthesis, as a stream; :mod:`.dialects`
the per-system projections behind one registry; :mod:`.bulk` the chunked-file
writer for volume; :mod:`.generate` composes them.
"""

from .bulk import BULK_MANIFEST_FILE, DEFAULT_CHUNK_ROWS, BulkWriter, write_bulk
from .core import (
    JSON_TYPES,
    SURROGATE_KEY,
    ColumnPlan,
    EdgePlan,
    ForgeArtifacts,
    ForgeEntity,
    ForgeError,
    ForgeOntology,
    ForgeProperty,
    ForgeRelationship,
    SchemaPlan,
    TablePlan,
    column_name,
    edge_collection_name,
    expected_relationship_type,
    foreign_key_column,
    load_ontology_file,
    plan_schema,
    synthesize_rows,
    table_name,
)
from .dialects import DIALECTS, SUPPORTED_DIALECTS, ForgeDialect, get_dialect, split_sql_statements
from .dialects.postgres import PG_TYPE_FOR_JSON_TYPE
from .generate import generate, generate_bulk
from .scale import ForgeScale, RelationshipScale
from .synthesis import ZipfSampler, planned_row_counts, stream_rows

__all__ = [
    "BULK_MANIFEST_FILE",
    "DEFAULT_CHUNK_ROWS",
    "DIALECTS",
    "JSON_TYPES",
    "PG_TYPE_FOR_JSON_TYPE",
    "SUPPORTED_DIALECTS",
    "SURROGATE_KEY",
    "BulkWriter",
    "ColumnPlan",
    "EdgePlan",
    "ForgeArtifacts",
    "ForgeDialect",
    "ForgeEntity",
    "ForgeError",
    "ForgeOntology",
    "ForgeProperty",
    "ForgeRelationship",
    "ForgeScale",
    "RelationshipScale",
    "SchemaPlan",
    "TablePlan",
    "ZipfSampler",
    "column_name",
    "edge_collection_name",
    "expected_relationship_type",
    "foreign_key_column",
    "generate",
    "generate_bulk",
    "get_dialect",
    "load_ontology_file",
    "plan_schema",
    "planned_row_counts",
    "split_sql_statements",
    "stream_rows",
    "synthesize_rows",
    "table_name",
    "write_bulk",
]
