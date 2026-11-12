"""Federation Forge core — the ontology contract, the dialect-independent
schema plan, and seeded data synthesis.

Commissioned by contextual-data-fabric ADR-0006 (D-4) and specified in
``docs/internal/PLAN-federation-forge.md``: from a conceptual ontology,
*generate* a physical schema plus seeded synthetic data, such that running the
REAL forward pipeline over the loaded result reproduces the ontology::

    introspect(generate(O)) == O      (up to CC-12 normalization and plumbing)

Everything in this module is dialect-independent. :func:`plan_schema` turns an
ontology into a :class:`SchemaPlan` — canonical snake_case tables and columns
with their roles (surrogate PK, FK spine, conceptual property) — and
:func:`synthesize_rows` fills that plan with seeded data **once** (ADR-0006
D-2). The per-system projections live in :mod:`r2g.forge.dialects`; the seam
that composes the two is :func:`r2g.forge.generate.generate`.

Declared PK/FK constraints are always emitted where the target records them
(the constraint-stripped variant is the S3 denormalizer's job); input
ontologies are collision-free by construction (F-6) and are *refused*
otherwise — the forge fails loudly at generate time, never at compare time.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

from r2g.naming import edge_collection_name as _edge_name

from ..csi import owl_entity_name, owl_property_name
from ..naming import convert_identifier, pluralize

if TYPE_CHECKING:  # pragma: no cover - typing only; scale imports this module
    from .scale import ForgeScale

#: Conceptual JSON types the forge synthesizes (PLAN F-1/F-3). Every dialect
#: must declare one roundtrip-stable physical type per entry; the forward
#: pipeline's ``config.pg_type_to_json_type`` must map that physical type back
#: to the same JSON type. Temporal/decimal/uuid are deliberately absent: the
#: forward map collapses them to ``string``, so no honest roundtrip exists yet.
JSON_TYPES: Tuple[str, ...] = ("integer", "float", "boolean", "string")

#: Name of the surrogate primary key every generated table carries (F-4).
SURROGATE_KEY = "id"

#: Words a generated column may not be named. The forge emits identifiers
#: **unquoted** on purpose — Snowflake folds unquoted names to uppercase and the
#: CC-12 roundtrip depends on that folding (P12.7/P12.11), so quoting to dodge a
#: reserved word would change the spelling that comes back and break the very
#: equivalence the Forge exists to check. Refusing is therefore the only option
#: that keeps `introspect(generate(O)) == O` honest, and it matches how F-6
#: already refuses a colliding property label.
#:
#: DERIVED, NOT WRITTEN. ``scripts/derive_reserved_words.py`` probes every word
#: in Postgres's own keyword catalogue (``pg_get_keywords()``, 471 entries) as a
#: real column name against live Postgres and live ClickHouse, and unions what
#: they reject with Snowflake's documented reserved words. One plan is projected
#: onto every dialect (D-2), so a name must be safe in all of them.
#:
#: The first version of this list was written from memory as "the SQL:2016
#: reserved words" and was wrong in BOTH directions: it accepted ``current_date``,
#: ``array``, ``do``, ``returning`` and ``ilike`` — every one rejected by live
#: Postgres — while refusing ``key``, ``range``, ``session`` and ``filter``,
#: which Postgres and ClickHouse both accept. Its validation probed only the
#: words already in the list, so it could not fail. Hence deriving.
#:
#: ``tests/integration/test_reserved_words.py`` re-probes the live engines and
#: fails if this list has drifted from them, so the check is testable rather
#: than asserted.
RESERVED_COLUMN_NAMES = frozenset(
    """
    account all alter analyse analyze and
    any array as asc asymmetric authorization
    between binary both by case cast
    check collate collation column concurrently connect
    connection constraint create cross current current_catalog
    current_date current_role current_schema current_time current_timestamp current_user
    database default deferrable delete desc distinct
    do drop else end except exists
    false fetch following for foreign freeze
    from full grant group gscluster having
    ilike in increment index initially inner
    insert intersect into is isnull issue
    join lateral leading left like limit
    localtime localtimestamp minus natural not notnull
    null of offset on only or
    order organization outer overlaps placing primary
    qualify references regexp returning revoke right
    rlike row rows sample schema select
    session_user set similar some start symmetric
    system_user table tablesample then to trailing
    trigger true try_cast union unique update
    user using values variadic verbose view
    when whenever where window with
    """.split()
)

#: Roles a planned column can play; the plumbing roles are the only extras the
#: roundtrip tolerates over the conceptual properties.
ROLE_PRIMARY_KEY = "pk"
ROLE_FOREIGN_KEY = "fk"
ROLE_PROPERTY = "property"

Rows = Dict[str, List[Dict[str, Any]]]


class ForgeError(ValueError):
    """A refused ontology or unsupported request — always at generate time."""


class ForgeProperty(BaseModel):
    """One conceptual property: lowerCamel name + JSON type (PLAN F-1)."""

    name: str
    type: str


class ForgeRelationship(BaseModel):
    """One conceptual relationship; ``type`` must equal the name the forward
    pipeline will re-derive (``<from_table>_to_<to_table>`` in lowerCamel), so
    the roundtrip comparison stays exact."""

    type: str
    from_entity: str = Field(alias="fromEntity")
    to_entity: str = Field(alias="toEntity")

    model_config = {"populate_by_name": True}


class ForgeEntity(BaseModel):
    """One conceptual class: singular PascalCase name + typed properties."""

    name: str
    properties: List[ForgeProperty]


def _refuse_unsafe_identifier(identifier: str, owner: str, kind: str) -> None:
    """Refuse a generated identifier that cannot appear unquoted (PLAN F-7).

    The forge emits identifiers unquoted so Snowflake's uppercase folding is
    what the CC-12 roundtrip asserts against, so neither of these can be quoted
    around: the spelling that came back would no longer match the one declared.

    Applied to **tables as well as columns**. The check used to cover only
    property columns, so an entity named ``Value`` generated ``CREATE TABLE
    values (``, which Snowflake reserves, and ``3dModel`` generated a table
    beginning with a digit that no target accepts.
    """
    if identifier in RESERVED_COLUMN_NAMES:
        raise ForgeError(
            f"{owner} generates the {kind} {identifier!r}, a SQL reserved word. "
            f"The forge emits identifiers unquoted so Snowflake's folding keeps "
            f"the CC-12 roundtrip honest, so this cannot be quoted around - "
            f"rename it (PLAN F-7)."
        )
    if not (identifier[:1].isalpha() or identifier.startswith("_")):
        raise ForgeError(
            f"{owner} generates the {kind} {identifier!r}, which does not start "
            f"with a letter or underscore. Postgres and Snowflake refuse such an "
            f"identifier unquoted, and it cannot be quoted around for the same "
            f"reason a reserved word cannot (PLAN F-7)."
        )


class ForgeOntology(BaseModel):
    """A validated conceptual ontology — the forge's only input contract.

    Build one with :meth:`from_conceptual`, which accepts either a bare
    ``{"entities": …, "relationships": …}`` object or a full CSI v1 document
    (the ``conceptualModel`` is taken) and refuses anything the skeleton
    cannot roundtrip byte-honestly.
    """

    entities: List[ForgeEntity]
    relationships: List[ForgeRelationship] = Field(default_factory=list)

    @classmethod
    def from_conceptual(cls, document: Dict[str, Any]) -> "ForgeOntology":
        try:
            return cls._from_conceptual(document)
        except ForgeError:
            raise
        except Exception as exc:  # malformed input is a caller error, not a crash
            # Indexing raw dicts leaked KeyError / TypeError / AttributeError /
            # pydantic ValidationError past `except ForgeError`, so the CLI
            # logged a traceback and exited 1 ("Failed to generate") where the
            # documented behaviour is exit 2 ("Forge refused").
            raise ForgeError(
                f"malformed conceptual document: {type(exc).__name__}: {exc}"
            ) from exc

    @classmethod
    def _from_conceptual(cls, document: Dict[str, Any]) -> "ForgeOntology":
        conceptual = document.get("conceptualModel", document)
        entities = [
            ForgeEntity(
                name=e["name"],
                properties=[
                    ForgeProperty(name=p["name"], type=p.get("type", ""))
                    for p in e.get("properties", [])
                ],
            )
            for e in conceptual.get("entities", [])
        ]
        relationships = [
            ForgeRelationship.model_validate(r)
            for r in conceptual.get("relationships", [])
        ]
        ontology = cls(entities=entities, relationships=relationships)
        ontology.validate_for_forge()
        return ontology

    def entity(self, name: str) -> ForgeEntity:
        for e in self.entities:
            if e.name == name:
                return e
        raise ForgeError(f"unknown entity {name!r}")

    def validate_for_forge(self) -> None:
        """Refuse anything that cannot survive the roundtrip (PLAN F-2/F-6)."""
        if not self.entities:
            raise ForgeError("ontology declares no entities")

        seen_entities: set[str] = set()
        label_owner: Dict[str, str] = {}
        for e in self.entities:
            if e.name in seen_entities:
                raise ForgeError(f"duplicate entity {e.name!r}")
            seen_entities.add(e.name)

            table = table_name(e.name)
            _refuse_unsafe_identifier(table, f"entity {e.name!r}", "table")
            if owl_entity_name(table) != e.name:
                raise ForgeError(
                    f"entity {e.name!r} does not survive the CC-12 naming "
                    f"roundtrip: table {table!r} normalizes back to "
                    f"{owl_entity_name(table)!r}. Rename the class so that "
                    "generate/introspect agree (PLAN F-2)."
                )

            seen_props: set[str] = set()
            for p in e.properties:
                if p.type not in JSON_TYPES:
                    raise ForgeError(
                        f"{e.name}.{p.name}: unsupported type {p.type!r} "
                        f"(the forge supports {sorted(JSON_TYPES)})"
                    )
                if p.name in seen_props:
                    raise ForgeError(f"duplicate property {e.name}.{p.name}")
                seen_props.add(p.name)
                column = column_name(p.name)
                if owl_property_name(column) != p.name:
                    raise ForgeError(
                        f"property {e.name}.{p.name!r} does not survive the "
                        f"CC-12 naming roundtrip: column {column!r} normalizes "
                        f"back to {owl_property_name(column)!r} (PLAN F-2)."
                    )
                if p.name == SURROGATE_KEY:
                    raise ForgeError(
                        f"{e.name}.id collides with the generated surrogate "
                        "primary key; declare a domain identifier instead"
                    )
                _refuse_unsafe_identifier(column, f"{e.name}.{p.name!r}", "column")
                # Collision-free by construction (F-6): deliberate collisions
                # are an S3 denormalizer feature, not a skeleton input.
                owner = label_owner.get(p.name)
                if owner is not None:
                    raise ForgeError(
                        f"property label {p.name!r} appears on both {owner} "
                        f"and {e.name}; skeleton ontologies must be "
                        "collision-free (PLAN F-6)"
                    )
                label_owner[p.name] = e.name

        seen_edges: set[tuple[str, str]] = set()
        for r in self.relationships:
            if r.from_entity not in seen_entities:
                raise ForgeError(f"relationship {r.type!r}: unknown fromEntity {r.from_entity!r}")
            if r.to_entity not in seen_entities:
                raise ForgeError(f"relationship {r.type!r}: unknown toEntity {r.to_entity!r}")
            if (r.from_entity, r.to_entity) in seen_edges:
                raise ForgeError(
                    f"duplicate relationship {r.from_entity} -> {r.to_entity}; "
                    "the skeleton supports one relationship per entity pair"
                )
            seen_edges.add((r.from_entity, r.to_entity))

            expected = expected_relationship_type(r.from_entity, r.to_entity)
            if r.type != expected:
                raise ForgeError(
                    f"relationship {r.from_entity} -> {r.to_entity} must be "
                    f"named {expected!r} (the name the forward pipeline "
                    f"re-derives), got {r.type!r}"
                )

            fk_column = foreign_key_column(r.to_entity)
            for p in self.entity(r.from_entity).properties:
                if column_name(p.name) == fk_column:
                    raise ForgeError(
                        f"{r.from_entity}.{p.name} collides with the "
                        f"generated foreign-key column {fk_column!r} for "
                        f"relationship {r.type!r}"
                    )


class ForgeArtifacts(BaseModel):
    """What ``generate`` returns: a schema definition, a loader, and the
    synthesized rows (the pre-partition dataset expected answers are computed
    on).

    ``ddl`` / ``load_sql`` keep their skeleton names for backward compatibility
    but are *dialect-shaped*: SQL dialects put DDL and INSERT statements there;
    the ``arango`` dialect puts a JSON collection manifest in ``ddl`` and a
    standalone Python loader script in ``load_sql``. ``rows`` is always the
    same canonical dataset regardless of dialect (ADR-0006 D-2) — keyed by
    canonical snake_case table and column names, never by the dialect's
    physical spelling.
    """

    dialect: str
    seed: int
    ddl: str
    load_sql: str
    rows: Rows

    def write_to(self, out_dir: str) -> List[str]:
        """Write the schema definition, loader, and ``forge.rows.json`` under
        ``out_dir`` using the dialect's file names (``forge.sql`` /
        ``forge.load.sql`` for SQL dialects) and return the paths written."""
        import os

        from .dialects import get_dialect

        dialect = get_dialect(self.dialect)
        os.makedirs(out_dir, exist_ok=True)
        paths = []
        for filename, content in (
            (dialect.ddl_filename, self.ddl),
            (dialect.loader_filename, self.load_sql),
            ("forge.rows.json", json.dumps(self.rows, indent=2, sort_keys=True) + "\n"),
        ):
            path = os.path.join(out_dir, filename)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(content)
            paths.append(path)
        return paths


def table_name(entity: str) -> str:
    """CC-12 inverse for classes: singular PascalCase -> plural snake_case."""
    return pluralize(convert_identifier(entity, "snake"))


def column_name(prop: str) -> str:
    """CC-12 inverse for properties: lowerCamel -> snake_case."""
    return convert_identifier(prop, "snake")


def foreign_key_column(to_entity: str) -> str:
    """The generated FK column pointing at ``to_entity``: ``account_id``."""
    return f"{convert_identifier(to_entity, 'snake')}_id"


def edge_collection_name(from_entity: str, to_entity: str) -> str:
    """The edge collection r2g's forward Auto-Map derives for an FK
    (``config.ConfigManager.generate_default_config``): ``<from>_to_<to>``.
    The ``arango`` dialect emits exactly this name so ASA reads it back.

    Delegates to :func:`r2g.naming.edge_collection_name` rather than restating
    the rule: the roundtrip only holds while the Forge and Auto-Map agree, and
    they cannot agree by coincidence across two hand-written copies."""
    return _edge_name(table_name(from_entity), table_name(to_entity))


def expected_relationship_type(from_entity: str, to_entity: str) -> str:
    """The relationship name the forward pipeline re-derives for an FK:
    Auto-Map names the edge ``<from_table>_to_<to_table>`` and the CSI emitter
    lowerCamels it."""
    return owl_property_name(edge_collection_name(from_entity, to_entity))


# ── The dialect-independent schema plan ──────────────────────────────


@dataclass(frozen=True)
class ColumnPlan:
    """One planned column in canonical snake_case spelling.

    ``role`` is one of :data:`ROLE_PRIMARY_KEY`, :data:`ROLE_FOREIGN_KEY`,
    :data:`ROLE_PROPERTY`; ``references`` names the parent *table* for an FK and
    ``references_column`` the column it points at **in that parent** — not in this
    table. The two coincide only while every table shares :data:`SURROGATE_KEY`,
    which is why rendering ``table.primary_key`` here looked right and was not;
    ``prop`` is the conceptual lowerCamel property for a property column.
    Key columns are NOT NULL, property columns nullable — the same across
    every dialect so the introspected nullability agrees. The one exception is
    an ``optional`` foreign key (PLAN F-9/F-10): a self-link, whose roots have
    no parent, or a relationship the scale gives a ``nullRate``.
    """

    name: str
    json_type: str
    role: str
    references: Optional[str] = None
    references_column: Optional[str] = None
    prop: Optional[str] = None
    optional: bool = False

    @property
    def nullable(self) -> bool:
        return self.role == ROLE_PROPERTY or self.optional


@dataclass(frozen=True)
class TablePlan:
    """One entity's table: surrogate PK first, then FK spine columns (sorted),
    then the conceptual properties in declared order."""

    entity: str
    table: str
    columns: Tuple[ColumnPlan, ...]

    @property
    def primary_key(self) -> ColumnPlan:
        return self.columns[0]

    @property
    def foreign_keys(self) -> Tuple[ColumnPlan, ...]:
        return tuple(c for c in self.columns if c.role == ROLE_FOREIGN_KEY)

    @property
    def properties(self) -> Tuple[ColumnPlan, ...]:
        return tuple(c for c in self.columns if c.role == ROLE_PROPERTY)


@dataclass(frozen=True)
class EdgePlan:
    """One relationship as the forward pipeline sees it: an FK column on the
    from-table pointing at the to-table's surrogate key, re-derived as the
    edge collection ``<from_table>_to_<to_table>``."""

    relationship: str
    from_entity: str
    to_entity: str
    from_table: str
    to_table: str
    fk_column: str
    edge_collection: str
    #: The column ``fk_column`` points at in ``to_table``. Carried rather than
    #: assumed so a non-uniform primary key cannot silently mis-render an FK.
    to_key: str = SURROGATE_KEY
    #: Whether ``fk_column`` may be NULL (a self-link's roots, or a scale
    #: ``nullRate``); mirrors the FK ColumnPlan's ``optional``.
    optional: bool = False

    @property
    def self_link(self) -> bool:
        """A class linked to itself (employee -> manager), PLAN F-9."""
        return self.from_table == self.to_table


@dataclass(frozen=True)
class SchemaPlan:
    """The whole generated schema, tables in parent-before-child order.

    A pure function of the ontology (never of the seed), so every dialect
    projects the same plan and the same rows."""

    tables: Tuple[TablePlan, ...]
    edges: Tuple[EdgePlan, ...]

    def table(self, name: str) -> TablePlan:
        for t in self.tables:
            if t.table == name:
                return t
        raise ForgeError(f"unknown planned table {name!r}")


def _topological_entity_order(ontology: ForgeOntology) -> List[str]:
    """Parents before children (FK targets first), deterministic tie-break by
    name. Cycles are refused — the forge generates trees/DAGs only.

    A self-link (``Employee -> Employee``, PLAN F-9) is not a cycle between
    tables: its rows are ordered inside the one table instead, each pointing
    at an earlier row, so it adds no dependency here."""
    names = sorted(e.name for e in ontology.entities)
    depends_on: Dict[str, set[str]] = {n: set() for n in names}
    for r in ontology.relationships:
        if r.from_entity != r.to_entity:
            depends_on[r.from_entity].add(r.to_entity)

    ordered: List[str] = []
    placed: set[str] = set()
    while len(ordered) < len(names):
        progress = False
        for n in names:
            if n in placed or not depends_on[n] <= placed:
                continue
            ordered.append(n)
            placed.add(n)
            progress = True
        if not progress:
            cyclic = sorted(set(names) - placed)
            raise ForgeError(f"relationship cycle among {cyclic}; the forge generates DAGs only")
    return ordered


def plan_schema(ontology: ForgeOntology, scale: Optional["ForgeScale"] = None) -> SchemaPlan:
    """Lay out the canonical physical schema for ``ontology`` (F-2/F-4).

    Refuses cyclic relationship graphs (via :func:`_topological_entity_order`)
    and re-runs :meth:`ForgeOntology.validate_for_forge` so a hand-built
    ontology object gets the same guarantees as one from
    :meth:`ForgeOntology.from_conceptual`.

    ``scale`` (PLAN F-10) matters to the schema in one way only: a relationship
    it gives a ``nullRate`` gets a nullable foreign-key column. A self-link's
    column is nullable regardless (its roots have no parent, F-9). With no
    scale, or one without a rate, the plan is exactly the skeleton's.
    """
    ontology.validate_for_forge()
    order = _topological_entity_order(ontology)

    fk_by_entity: Dict[str, List[ColumnPlan]] = {e.name: [] for e in ontology.entities}
    edges: List[EdgePlan] = []
    for r in ontology.relationships:
        null_rate = scale.relationship(r.type).null_rate if scale is not None else 0.0
        fk_column = ColumnPlan(
            name=foreign_key_column(r.to_entity),
            json_type="integer",
            role=ROLE_FOREIGN_KEY,
            references=table_name(r.to_entity),
            references_column=SURROGATE_KEY,
            optional=r.from_entity == r.to_entity or null_rate > 0,
        )
        fk_by_entity[r.from_entity].append(fk_column)
        edges.append(
            EdgePlan(
                relationship=r.type,
                from_entity=r.from_entity,
                to_entity=r.to_entity,
                from_table=table_name(r.from_entity),
                to_table=table_name(r.to_entity),
                fk_column=foreign_key_column(r.to_entity),
                # Read off the FK ColumnPlan built just above rather than
                # restated: they are one fact, and when they were two stores a
                # DDL could name `accounts(account_pk)` in a column comment and
                # `accounts(id)` in its own header.
                to_key=fk_column.references_column or SURROGATE_KEY,
                edge_collection=edge_collection_name(r.from_entity, r.to_entity),
                optional=fk_column.optional,
            )
        )

    tables: List[TablePlan] = []
    for name in order:
        entity = ontology.entity(name)
        columns: List[ColumnPlan] = [
            ColumnPlan(name=SURROGATE_KEY, json_type="integer", role=ROLE_PRIMARY_KEY)
        ]
        columns.extend(sorted(fk_by_entity[name], key=lambda c: c.name))
        columns.extend(
            ColumnPlan(name=column_name(p.name), json_type=p.type, role=ROLE_PROPERTY, prop=p.name)
            for p in entity.properties
        )
        tables.append(TablePlan(entity=name, table=table_name(name), columns=tuple(columns)))

    # Edges in the same deterministic order as the tables they hang off.
    table_rank = {t.table: i for i, t in enumerate(tables)}
    edges.sort(key=lambda e: (table_rank[e.from_table], e.fk_column))
    plan = SchemaPlan(tables=tuple(tables), edges=tuple(edges))
    if scale is not None:
        scale.check_against(plan)
    return plan


# ── Seeded synthesis: once, dialect-independent (F-5, ADR-0006 D-2/D-5) ──
#
# The synthesizer itself lives in :mod:`.synthesis` (it streams, F-11); this
# wrapper keeps the skeleton's materializing entry point where callers import it.


def synthesize_rows(
    plan: SchemaPlan,
    seed: int,
    rows_per_entity: int,
    scale: Optional["ForgeScale"] = None,
) -> Rows:
    """Fill ``plan`` with ``random.Random(seed)`` end to end, held in memory.

    Rows are produced per table in plan (topological) order, every FK value
    drawn from the parent's ids — join-spine agreement by construction. Keys
    are the canonical snake_case names from the plan, so the result is
    byte-identical across dialects for the same
    ``(ontology, seed, rows_per_entity, scale)``; dialects only *project* it.

    This collects :func:`r2g.forge.synthesis.stream_rows`, so it holds every
    row; at volume, consume the stream instead (PLAN F-11).
    """
    from .synthesis import stream_rows

    rows: Rows = {t.table: [] for t in plan.tables}
    for table, row in stream_rows(plan, seed, rows_per_entity, scale):
        rows[table].append(row)
    return rows


def load_ontology_file(path: str) -> ForgeOntology:
    """Read a conceptual ontology (bare or full CSI v1 document) from JSON.

    Unreadable or malformed input is a caller problem and raises
    :class:`ForgeError` (never a bare ``OSError``), so the CLI reports it as a
    refusal with the reason attached.
    """
    try:
        with open(path, "r", encoding="utf-8") as fh:
            try:
                document = json.load(fh)
            except json.JSONDecodeError as exc:
                raise ForgeError(f"{path} is not valid JSON: {exc}") from exc
    except OSError as exc:
        raise ForgeError(f"cannot read ontology file {path}: {exc}") from exc
    if not isinstance(document, dict):
        raise ForgeError(f"{path} must contain a JSON object")
    return ForgeOntology.from_conceptual(document)


__all__ = [
    "JSON_TYPES",
    "ROLE_FOREIGN_KEY",
    "ROLE_PRIMARY_KEY",
    "ROLE_PROPERTY",
    "SURROGATE_KEY",
    "ColumnPlan",
    "EdgePlan",
    "ForgeArtifacts",
    "ForgeEntity",
    "ForgeError",
    "ForgeOntology",
    "ForgeProperty",
    "ForgeRelationship",
    "Rows",
    "SchemaPlan",
    "TablePlan",
    "column_name",
    "edge_collection_name",
    "expected_relationship_type",
    "foreign_key_column",
    "load_ontology_file",
    "plan_schema",
    "synthesize_rows",
    "table_name",
]
