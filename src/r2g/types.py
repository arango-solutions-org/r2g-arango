from __future__ import annotations

import re
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_serializer, model_validator
from relational_schema_analyzer.types import Column as _RsaColumn
from relational_schema_analyzer.types import ForeignKey as ForeignKey  # noqa: F401  (re-export)
from relational_schema_analyzer.types import PhysicalSchema as _RsaSchema
from relational_schema_analyzer.types import Table as _RsaTable

# ArangoDB system attributes. These are managed by ArangoDB / the R2G
# transformers (``_key`` is derived from the source PK; ``_from``/``_to`` are
# built from FK data) and must never be renamed or used as a mapping target.
RESERVED_ATTRIBUTES: frozenset[str] = frozenset({"_id", "_key", "_rev", "_from", "_to"})

# PostgreSQL's default schema: on every connection's default search path, so a
# table there is named bare and its snapshot does not record it.
DEFAULT_SQL_SCHEMA = "public"


# ``ForeignKey`` is re-exported from ``relational_schema_analyzer.types`` (imported
# above). RSA's ForeignKey is a superset of r2g's original — identical fields,
# accessors (``column``/``foreign_column``/``is_composite``), singular-form
# acceptance, and serializer key order — plus an ``is_unique`` cardinality hint
# that r2g never sets and RSA omits from output when false. It therefore
# serializes byte-identically to r2g's historical shape (asserted by the
# serialization compat corpus), so unifying it here is a no-op on disk.


class Classification(BaseModel):
    """Governance classification carried from an external catalog (PRD Phase 9).

    ``tags`` are catalog tag FQNs (e.g. ``"PII.Sensitive"``); ``tier`` is a
    confidentiality-tier FQN if present (e.g. ``"Tier.Tier1"``); ``glossary_terms``
    are business-glossary references; ``source`` records provenance. Everything is
    optional/empty by default, so sources not imported from a catalog carry no
    classification and behave exactly as before.
    """

    tags: List[str] = Field(default_factory=list)
    tier: Optional[str] = None
    glossary_terms: List[str] = Field(default_factory=list)
    source: str = "catalog"

    @property
    def is_empty(self) -> bool:
        return not self.tags and self.tier is None and not self.glossary_terms


class Column(_RsaColumn):
    """Physical column: the shared RSA column + r2g's Phase-9 governance.

    Subclasses ``relational_schema_analyzer.types.Column`` so RSA's own code paths
    (typemap, baseline, FK heuristics) accept r2g columns directly — the basis for
    the dependency reversal (see ``docs/internal/DESIGN-rsa-compat-layer.md``).
    r2g re-adds ``classification`` as a first-class field and serializes to its
    **historical 5-key shape** (dropping RSA's enrichment fields, the computed
    ``type_category``, and the ``extra`` passthrough) so existing snapshots and
    ``catalog.json`` stay byte-identical. On input, a ``classification`` carried in
    RSA's ``extra['classification']`` (an RSA-native producer) is lifted onto the
    field so both representations round-trip.
    """

    # Governance classification (PRD Phase 9). ``None`` for non-catalog sources
    # and untagged columns; populated at ``source snapshot`` for catalog-imported
    # sources by merging the resolved classification map onto the schema.
    classification: Optional[Classification] = None

    @model_validator(mode="before")
    @classmethod
    def _accept_classification_from_extra(cls, data: Any) -> Any:
        if isinstance(data, dict) and not data.get("classification"):
            extra = data.get("extra")
            if isinstance(extra, dict) and extra.get("classification"):
                data = {**data, "classification": extra["classification"]}
        return data

    @model_serializer(mode="wrap")
    def _serialize(self, handler: Any) -> dict[str, Any]:
        full = handler(self)
        return {
            "name": full["name"],
            "data_type": full["data_type"],
            "is_nullable": full["is_nullable"],
            "is_primary_key": full["is_primary_key"],
            "classification": full.get("classification"),
        }


class Table(_RsaTable):
    """Physical table: the shared RSA table narrowed to r2g's :class:`Column`.

    Serializes to r2g's historical key set (dropping RSA's ``comment`` /
    constraint / index enrichment and the ``extra`` passthrough) for byte-stable
    snapshots. ``schema_name`` is kept only outside :data:`DEFAULT_SQL_SCHEMA`
    (see :meth:`qualified_name`), and ``is_view`` only when known.
    """

    # Narrowing the inherited RSA field to r2g's Column subclass (safe: every
    # r2g Column is an RSA Column; the invariance warning does not apply at runtime).
    columns: List[Column]  # type: ignore[assignment]
    # True for a view, False for a confirmed base table, None when nothing recorded
    # which. RSA's bool cannot say "unknown"; its readers only test truthiness.
    is_view: Optional[bool] = None  # type: ignore[assignment]

    @model_serializer(mode="wrap")
    def _serialize(self, handler: Any) -> dict[str, Any]:
        full = handler(self)
        out = {
            "name": full["name"],
            "columns": full["columns"],
            "primary_key": full["primary_key"],
            "foreign_keys": full["foreign_keys"],
            "is_partitioned": full["is_partitioned"],
            "partition_of": full["partition_of"],
        }
        # UNIQUE keys are FK-inference *targets* (RSA indexes candidate keys as
        # PK or UNIQUE), so dropping them on save silently erased every FK
        # inferred through a natural key once the catalog reloaded the snapshot.
        # Written last and only when present, so every table without one --
        # i.e. every snapshot persisted before this -- stays byte-identical.
        if full.get("unique_constraints"):
            out["unique_constraints"] = full["unique_constraints"]
        # The SQL schema a table lives in, so a reloaded snapshot still names it
        # (R2RML's rr:tableName needs it). Written last and only outside the
        # default schema, so every snapshot of a `public`-only source -- and every
        # snapshot persisted before this -- stays byte-identical.
        if full.get("schema_name") not in (None, DEFAULT_SQL_SCHEMA):
            out["schema_name"] = full["schema_name"]
        # Written last and only when known, so every snapshot persisted before
        # this, and every source that does not record it, stays byte-identical.
        if full.get("is_view") is not None:
            out["is_view"] = full["is_view"]
        return out

    def qualified_name(self) -> str:
        """The table's SQL name, schema-qualified outside :data:`DEFAULT_SQL_SCHEMA`.

        Each part is a delimited identifier (``"sales"."orders"``), so the name
        resolves without depending on the connection's search path and keeps its
        case. A table in the default schema, or with no recorded schema, keeps its
        bare name, as before.
        """
        if self.schema_name in (None, DEFAULT_SQL_SCHEMA):
            return self.name
        return ".".join(
            '"' + part.replace('"', '""') + '"' for part in (self.schema_name, self.name)
        )

    @model_validator(mode="after")
    def _derive_column_uniqueness(self) -> "Table":
        # Column.is_unique is not persisted (r2g's Column keeps its historical
        # key set), but RSA sets it at capture for exactly two cases -- a
        # single-column PK and a single-column UNIQUE key -- and its ontology
        # baseline reads only that flag. Both sources are persisted at table
        # level, so re-derive it here: a reloaded table then answers "is this a
        # key column?" the same way it did when captured, with no change on disk.
        # Only ever sets the flag; never clears one a capture set for another reason.
        single = {u[0] for u in self.unique_constraints if len(u) == 1}
        if len(self.primary_key) == 1:
            single.add(self.primary_key[0])
        for col in self.columns:
            if col.name in single:
                col.is_unique = True
        return self


class Schema(_RsaSchema):
    """Physical schema: the shared RSA :class:`PhysicalSchema` narrowed to r2g's
    :class:`Table`. Serializes to ``{"tables": …}`` only (dropping RSA's optional
    ``source`` provenance) for byte-stable snapshots."""

    # Narrowing the inherited RSA field to r2g's Table subclass (safe, as above).
    tables: Dict[str, Table] = {}  # type: ignore[assignment]

    @model_serializer(mode="wrap")
    def _serialize(self, handler: Any) -> dict[str, Any]:
        full = handler(self)
        return {"tables": full["tables"]}

    def save_to_file(self, path: str) -> None:
        with open(path, "w") as f:
            f.write(self.model_dump_json(indent=2))

    @classmethod
    def load_from_file(cls, path: str) -> Schema:
        with open(path, "r") as f:
            return cls.model_validate_json(f.read())


#: An absolute IRI: a scheme, a colon, and no whitespace or characters RFC 3987 excludes.
_IRI = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*:[^\s<>\"{}|\\^`]+$")


def _ontology_term(value: Optional[str]) -> Optional[str]:
    """An ontology term a mapping element names (P14.8): an absolute IRI, or none."""
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    if not _IRI.match(value):
        raise ValueError(f"an ontology term must be an absolute IRI, not {value!r}")
    return value


class EdgeDefinition(BaseModel):
    """Defines how a foreign key becomes an edge collection.

    Supports single-column (``from_field``/``to_field``) and composite
    (``from_fields``/``to_fields``) FK relationships.
    """
    edge_collection: str
    from_collection: str
    to_collection: str
    from_fields: List[str]
    to_fields: List[str]
    #: The ontology property this relationship stands for, an IRI from the project
    #: ontology (P14.8). Optional and omitted while unset; nothing reads it outside a
    #: control plane.
    term: Optional[str] = None

    @field_validator("term")
    @classmethod
    def _check_term(cls, value: Optional[str]) -> Optional[str]:
        return _ontology_term(value)

    @model_validator(mode="before")
    @classmethod
    def _accept_singular(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "from_field" in data and "from_fields" not in data:
                data["from_fields"] = cls._split_field_spec(data.pop("from_field"))
            if "to_field" in data and "to_fields" not in data:
                data["to_fields"] = cls._split_field_spec(data.pop("to_field"))
            # Also normalize pre-existing list forms in case a caller passed
            # ``from_fields=["a, b"]`` (a single comma-joined entry) rather
            # than a proper list.
            for plural in ("from_fields", "to_fields"):
                if plural in data and isinstance(data[plural], list):
                    flat: list[str] = []
                    for item in data[plural]:
                        flat.extend(cls._split_field_spec(item))
                    data[plural] = flat
        return data

    @staticmethod
    def _split_field_spec(value: Any) -> list[str]:
        """Accept a single column name or a comma-separated list and return
        a clean list of column names.

        The UI represents composite FKs as ``"order_id, product_id"`` in a
        single string field; normalize that here so validation and loading
        treat each column individually.
        """
        if value is None:
            return []
        if isinstance(value, list):
            out: list[str] = []
            for v in value:
                out.extend(EdgeDefinition._split_field_spec(v))
            return out
        s = str(value).strip()
        if not s:
            return []
        if "," in s:
            return [part.strip() for part in s.split(",") if part.strip()]
        return [s]

    @property
    def from_field(self) -> str:
        return self.from_fields[0]

    @property
    def to_field(self) -> str:
        return self.to_fields[0]

    @property
    def is_composite(self) -> bool:
        return len(self.from_fields) > 1

    @model_serializer
    def _serialize(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "edge_collection": self.edge_collection,
            "from_collection": self.from_collection,
            "to_collection": self.to_collection,
        }
        if len(self.from_fields) <= 1 and len(self.to_fields) <= 1:
            d["from_field"] = self.from_fields[0] if self.from_fields else ""
            d["to_field"] = self.to_fields[0] if self.to_fields else ""
        else:
            d["from_fields"] = self.from_fields
            d["to_fields"] = self.to_fields
        if self.term:
            d["term"] = self.term
        return d


class FieldExpression(BaseModel):
    """A mapping function that computes a target property from one or more source columns.

    The default (identity) mapping has ``expression=""`` and ``sources=[target]`` (or a single
    rename when ``sources`` is set to a different column). Non-identity mappings carry an
    expression string in the selected engine. Multiple entries in ``sources`` represent
    fan-in: several source columns feed a single target property.
    """

    target: str
    sources: List[str] = Field(default_factory=list)
    expression: str = ""
    engine: Literal["aql", "ksql", "python"] = "aql"
    description: str = ""

    @property
    def is_identity(self) -> bool:
        """True when this mapping is a pure pass-through (no expression, single source)."""
        return self.expression.strip() == "" and len(self.sources) <= 1


class CollectionMapping(BaseModel):
    """Maps a PostgreSQL table to an ArangoDB collection."""
    source_table: str
    target_collection: str
    collection_type: str = "document"  # "document" or "edge"
    is_join_table: bool = False
    field_mappings: Dict[str, str] = Field(default_factory=dict)
    exclude_fields: List[str] = Field(default_factory=list)
    include_fields: Optional[List[str]] = None
    field_expressions: List[FieldExpression] = Field(default_factory=list)
    #: Additional LPG labels this collection's nodes carry, beyond
    #: ``target_collection`` (P13.13). Empty means the node carries exactly one
    #: label, its ``target_collection`` — the historical behaviour.
    #:
    #: ``target_collection`` stays the **primary** label and is not repeated
    #: here: it remains the identity used for the ``_key`` namespace and for the
    #: CSI physical mapping, so adding labels never moves an existing document.
    #: Declared last and omitted while empty, so mappings written before P13.13
    #: serialize byte-identically.
    extra_labels: List[str] = Field(default_factory=list)
    #: The ontology class this table stands for, and per source column the property it
    #: stands for: IRIs from the project ontology (P14.8). Optional, declared last and
    #: omitted while empty, so mappings written before P14.8 serialize byte-identically.
    #: Nothing reads them outside a control plane.
    term: Optional[str] = None
    field_terms: Dict[str, str] = Field(default_factory=dict)

    @field_validator("term")
    @classmethod
    def _check_term(cls, value: Optional[str]) -> Optional[str]:
        return _ontology_term(value)

    @field_validator("field_terms")
    @classmethod
    def _check_field_terms(cls, value: Dict[str, str]) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for column, term in value.items():
            checked = _ontology_term(term)
            if checked:
                out[column] = checked
        return out

    @model_serializer(mode="wrap")
    def _serialize_collection(self, handler: Any) -> dict[str, Any]:
        full = handler(self)
        if not full.get("extra_labels"):
            full.pop("extra_labels", None)
        if not full.get("term"):
            full.pop("term", None)
        if not full.get("field_terms"):
            full.pop("field_terms", None)
        return full

    @property
    def labels(self) -> List[str]:
        """The full label set: the primary label first, then extras, deduped."""
        out = [self.target_collection]
        for label in self.extra_labels:
            if label and label not in out:
                out.append(label)
        return out


NameCase = Literal["preserve", "snake", "camel", "pascal"]


class NamingConvention(BaseModel):
    """A naming convention applied across a mapping.

    Each field selects the case style for a class of identifiers. ``preserve``
    leaves names untouched (the historical pass-through behaviour). Stored on
    :class:`MappingConfig` as a record of the last convention applied; the
    actual names are *materialized* into ``target_collection`` /
    ``field_mappings`` / ``edge_collection`` so they remain visible and editable.
    """

    collections: NameCase = "preserve"
    properties: NameCase = "preserve"
    edges: NameCase = "preserve"


class SharedKeyBinding(BaseModel):
    """One table bound to a shared key (PRD P6.7).

    ``source`` is the *catalog source name*, not a schema name — a shared key
    spans sources by definition, so a binding is only meaningful when it says
    which source it came from.
    """

    source: str
    table: str
    column: str


class SharedKey(BaseModel):
    """An accepted cross-source shared key — a **declared join key** (PRD P6.7).

    This is deliberately *not* an :class:`EdgeDefinition`. A project maps exactly
    one source, so a key shared with another source cannot become a materialized
    ArangoDB edge collection; it is federation metadata that flows into the
    CSI / R2RML export, where the federated executor bind-joins on it.

    ``hub_kind="entity"`` records the source/table/column that owns the key;
    ``hub_kind="virtual"`` records only the concept, because P6.7 forbids
    inventing a table for a key no source owns.
    """

    key: str
    concept: str
    hub_kind: Literal["entity", "virtual"] = "virtual"
    hub_source: Optional[str] = None
    hub_table: Optional[str] = None
    hub_column: Optional[str] = None
    bindings: List[SharedKeyBinding] = Field(default_factory=list)
    confidence: float = 0.0
    method: str = ""


class LpgLayout(BaseModel):
    """Physical field/collection names for the **LPG** graph target (P13).

    In the default ``pg`` layout a node's type *is* its collection and an edge's
    type *is* its edge collection. The ``lpg`` layout collapses everything into
    one node collection and one edge collection, so both types must move **into
    the documents**: nodes carry a ``labels`` array, edges carry a ``type``.

    Edges additionally carry **copies of their endpoints' labels**
    (``fromLabels`` / ``toLabels``). That denormalization is the whole point: it
    is what lets a *vertex-centric index* — a persistent index led by ``_from``
    or ``_to`` — answer "outbound edges of this node, of this type, landing on
    that kind of node" entirely from the index, instead of loading every
    incident edge and filtering afterwards. Without the copies the labels live
    on the neighbour document, which an index on the edge collection cannot
    reach.
    """

    node_collection: str = "nodes"
    edge_collection: str = "edges"
    label_field: str = "labels"
    type_field: str = "type"
    from_labels_field: str = "fromLabels"
    to_labels_field: str = "toLabels"
    #: Originating relational table, kept so an LPG graph stays self-describing
    #: and reversible back to the ``pg`` layout.
    source_field: str = "sourceTable"
    #: Also index the endpoint-label arrays on the edge collection
    #: (``[_from, type, toLabels[*]]`` and its inbound mirror).
    #:
    #: **Off by default, and leave it off unless you run label-filtered
    #: *pattern-match* edge queries** (``FOR e IN edges FILTER e._from == …``).
    #: In that access path the index is selected and each edge is returned once.
    #:
    #: **In a traversal the same index duplicates results** — a persistent index
    #: over an array-expansion field stores one entry per element and the
    #: traversal scans every entry under the ``_from``/``type`` prefix, so each
    #: edge comes back once per label on its target. The label filter does not
    #: narrow the scan and ``uniqueEdges: 'global'`` is rejected, so there is no
    #: query-side repair. Enabling this on a deployment that also traverses
    #: silently inflates counts.
    #:
    #: It also costs write throughput on every load (one index entry per label),
    #: which is why it is not provisioned by default.
    index_edge_labels: bool = False


class MappingConfig(BaseModel):
    """Top-level mapping configuration."""
    source_schema: str = "public"
    collections: Dict[str, CollectionMapping] = Field(default_factory=dict)
    edges: List[EdgeDefinition] = Field(default_factory=list)
    type_overrides: Dict[str, str] = Field(default_factory=dict)
    key_separator: str = "_"
    naming_convention: Optional[NamingConvention] = None
    # Accepted P6.7 cross-source join keys. Declared last and omitted from
    # output when empty so every mapping written before P6.7 serializes
    # byte-identically (guarded by tests/test_serialization_compat.py).
    shared_keys: List[SharedKey] = Field(default_factory=list)
    # Physical graph layout (P13). ``pg`` (default) = a collection per node type
    # and per edge type; ``lpg`` = one node collection + one edge collection
    # with types carried as ``labels`` / ``type`` fields. Declared last and
    # omitted from output while default, so every mapping written before P13
    # serializes byte-identically (guarded by tests/test_serialization_compat.py).
    graph_layout: Literal["pg", "lpg"] = "pg"
    lpg: LpgLayout = Field(default_factory=LpgLayout)

    @model_serializer(mode="wrap")
    def _serialize(self, handler: Any) -> dict[str, Any]:
        full = handler(self)
        if not full.get("shared_keys"):
            full.pop("shared_keys", None)
        # Only a project that actually opted into the LPG layout carries these.
        if full.get("graph_layout") == "pg":
            full.pop("graph_layout", None)
            full.pop("lpg", None)
        return full

    def save_to_file(self, path: str):
        with open(path, 'w') as f:
            f.write(self.model_dump_json(indent=2))

    @classmethod
    def load_from_file(cls, path: str) -> "MappingConfig":
        with open(path, 'r') as f:
            return cls.model_validate_json(f.read())
