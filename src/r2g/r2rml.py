"""R2RML mapping emitter (relational -> RDF, for Ontop SPARQL->SQL pushdown).

r2g owns the R2RML capability because R2RML is fundamentally a *relational-source*
artifact: it names the actual SQL tables, columns and primary keys, and r2g is
the only component that knows the source relational schema. A Virtual Knowledge
Graph engine (Ontop) reads this R2RML to answer SPARQL over the live relational
database by rewriting to SQL — no data movement.

Concept/property IRIs default to the same ``urn:arango-sparql:concept#`` namespace
the AQL leg (``arango-sparql-py``) synthesizes from a MappingBundle, so a federated
SPARQL query means the *same thing* on both the relational (Ontop) and graph
(ArangoDB) legs — the two legs share one vocabulary by construction.

See contextual-data-fabric ADR-0001 / M5 implementation-plan WP-A4 (Option 2:
r2g-internal serializer off ``MappingConfig`` + ``Schema``), and the sibling
forward-CSI emitter in :mod:`r2g.csi`.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

from .security import one_line
from .types import MappingConfig, Schema, Table

# Class/property IRI namespace. Defaults to the AQL leg's synthetic concept
# namespace so both federation legs use identical IRIs (see module docstring).
DEFAULT_CONCEPT_BASE = "urn:arango-sparql:concept#"
# Instance/subject IRI namespace (one resource IRI per source row).
DEFAULT_RESOURCE_BASE = "http://r2g.example/resource/"

_PREFIXES = (
    "@prefix rr: <http://www.w3.org/ns/r2rml#> .\n"
    "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
    "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
)


class R2RMLError(ValueError):
    """Raised when a mapping cannot be serialized to valid R2RML."""


def _ttl_string(value: str) -> str:
    """Quote+escape a Turtle string literal."""
    escaped = (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f'"{escaped}"'


# ClickHouse wrappers that say nothing about the value's type. r2g's ClickHouse
# connector strips them, but a schema.json written by hand or another tool may
# not have. Matched after lower-casing.
_TYPE_WRAPPER_RE = re.compile(r"^(?:nullable|lowcardinality)\((.*)\)$")

# Base type names (lower-cased, parameters removed) whose values are text:
# PostgreSQL (information_schema and udt names), MySQL, SQL Server, Snowflake
# (VARCHAR/TEXT/STRING and their synonyms) and ClickHouse (String/FixedString).
# Emitted as an explicit xsd:string so a missing rr:datatype keeps one meaning,
# "r2g did not classify this column" (r2g-arango#24).
_STRING_TYPES = frozenset(
    {
        "char",
        "character",
        "varchar",
        "character varying",
        "bpchar",
        "text",
        "string",
        "fixedstring",
        "nchar",
        "nvarchar",
        "ntext",
        "tinytext",
        "mediumtext",
        "longtext",
        "national character",
        "national character varying",
        "char varying",
    }
)

# Fixed-point types whose scale decides integer vs decimal. Their parameters are
# (precision[, scale]); scale defaults to 0 when only precision is given.
_FIXED_POINT_TYPES = frozenset({"number", "numeric", "decimal"})

# An integer type name: int, int4, Int32, UInt64, "int unsigned".
_INT_NAME_RE = re.compile(r"^u?int\d*(?:\s|$)")

# ClickHouse DecimalN(S): the one parameter is the scale.
_CH_SCALED_DECIMAL_RE = re.compile(r"^decimal(?:32|64|128|256)$")


def _unwrap_type(t: str) -> str:
    """Strip ``nullable(…)`` / ``lowcardinality(…)``, however nested."""
    while True:
        m = _TYPE_WRAPPER_RE.match(t)
        if not m:
            return t
        t = m.group(1).strip()


def _split_type(t: str) -> Tuple[str, List[str]]:
    """``"number(38,0)"`` -> ``("number", ["38", "0"])``; no parameters -> ``[]``."""
    if "(" not in t or not t.endswith(")"):
        return t, []
    base, _, rest = t.partition("(")
    return base.strip(), [p.strip() for p in rest[:-1].split(",")]


def _fixed_point_datatype(scale: Optional[str]) -> str:
    """``xsd:integer`` at scale 0, ``xsd:decimal`` otherwise or when unknown."""
    if scale is not None and scale.isdigit() and int(scale) == 0:
        return "xsd:integer"
    return "xsd:decimal"


def _xsd_datatype(sql_type: str) -> Optional[str]:
    """Map a SQL type name to an XSD datatype IRI.

    ``None`` means only "r2g does not classify this type" (``uuid``, ``json``,
    ``bytea``, arrays, …): the emitter then writes no ``rr:datatype``, and a
    consumer that needs a known type can refuse on its absence. Text columns get
    an explicit ``xsd:string`` rather than relying on R2RML's natural mapping,
    so the two cases stay distinguishable (r2g-arango#24).
    """
    t = _unwrap_type(sql_type.strip().lower())
    if t.endswith("[]") or t.startswith("array"):
        return None
    base, params = _split_type(t)
    if base in _STRING_TYPES:
        return "xsd:string"
    if base in _FIXED_POINT_TYPES:
        # NUMBER(p,s) / DECIMAL(p,s): integer only when the type proves scale 0.
        # A bare NUMBER (Snowflake's DATA_TYPE carries no scale) may hold
        # fractions, so it is a decimal.
        if not params:
            return "xsd:decimal"
        return _fixed_point_datatype(params[1] if len(params) > 1 else "0")
    if _CH_SCALED_DECIMAL_RE.match(base):
        return _fixed_point_datatype(params[0] if params else None)
    if (
        base in {"int", "int2", "int4", "int8", "tinyint", "mediumint"}
        or any(k in t for k in ("bigint", "smallint", "integer", "serial"))
        # ClickHouse Int8/…/256, UInt8/…, MySQL "int unsigned"; not "interval"
        # or a range type such as "int4range", which only start with "int".
        or _INT_NAME_RE.match(base)
    ):
        return "xsd:integer"
    if any(k in t for k in ("numeric", "decimal", "money")):
        return "xsd:decimal"
    if any(k in t for k in ("double", "real", "float")):
        return "xsd:double"
    if "bool" in t:
        return "xsd:boolean"
    if "timestamp" in t or "datetime" in t:
        return "xsd:dateTime"
    if t.startswith("date"):
        return "xsd:date"
    if t.startswith("time"):
        return "xsd:time"
    return None


def _mapped_columns(cm, table: Table) -> List[Tuple[str, str, str]]:
    """(source_column, target_property, sql_type) for each emitted column."""
    include = set(cm.include_fields) if cm.include_fields is not None else None
    exclude = set(cm.exclude_fields)
    out: List[Tuple[str, str, str]] = []
    for col in table.columns:
        if include is not None and col.name not in include:
            continue
        if col.name in exclude:
            continue
        prop = cm.field_mappings.get(col.name, col.name)
        out.append((col.name, prop, col.data_type))
    return out


def mapping_to_r2rml(
    config: MappingConfig,
    schema: Schema,
    *,
    concept_base: str = DEFAULT_CONCEPT_BASE,
    resource_base: str = DEFAULT_RESOURCE_BASE,
    source_type: str = "relational",
    curated_labels: Optional[dict] = None,
) -> str:
    """Serialize an r2g mapping to an R2RML Turtle document.

    Args:
        config: the r2g mapping (source tables -> collections, FK edges).
        schema: the source :class:`Schema` (supplies columns, types, PKs —
            required, since R2RML names them).
        concept_base: IRI namespace for classes/properties (default aligns with
            the AQL leg for federation).
        resource_base: IRI namespace for row-level subject IRIs.
        source_type: recorded in the document header for provenance.
        curated_labels: an optional curated-labels document (P12.12). Classes,
            predicates and relationships take their curated names from the same
            plan :func:`r2g.csi.mapping_to_csi` applies, so the two legs agree;
            rr:tableName / rr:column keep the physical names.

    Returns:
        An R2RML Turtle document as a string.

    Raises:
        R2RMLError: when a document entity's source table is absent from the
            schema (R2RML cannot be emitted without it).
    """
    # Conceptual entity name per source table (CC-12: singular PascalCase —
    # the class IRI the fabric queries with), for resolving FK edge endpoints
    # to the TriplesMaps that host them. Physical table/column names stay in
    # rr:tableName / rr:column / rr:template.
    from .csi import curated_plan_for_mapping, entity_names_for_mapping, owl_property_name

    generated_names, _ = entity_names_for_mapping(config, schema)

    plan = (
        curated_plan_for_mapping(config, schema, curated_labels)
        if curated_labels is not None
        else None
    )

    def _entity(generated: str) -> str:
        return plan.entity(generated) if plan is not None else generated

    def _predicate(generated_entity: str, generated: str) -> str:
        return plan.prop(generated_entity, generated) if plan is not None else generated

    def _relationship(generated: str) -> str:
        return plan.relationship(generated) if plan is not None else generated

    entity_by_source: Dict[str, str] = {
        cm.source_table: _entity(generated_names[cm.target_collection])
        for cm in config.collections.values()
        if cm.collection_type != "edge" and not cm.is_join_table
    }

    # Group FK relationships by the entity that hosts the referencing object
    # map (the "from" side — the table carrying the foreign key).
    rels_by_entity: Dict[str, List[dict]] = {}
    skipped_edges: List[str] = []
    for edge in config.edges:
        from_entity = entity_by_source.get(edge.from_collection)
        to_entity = entity_by_source.get(edge.to_collection)
        if from_entity is None or to_entity is None:
            skipped_edges.append(
                f"{edge.edge_collection} ({edge.from_collection}->{edge.to_collection})"
            )
            continue
        rels_by_entity.setdefault(from_entity, []).append(
            {
                "type": _relationship(owl_property_name(edge.edge_collection)),
                "to_entity": to_entity,
                "joins": list(zip(edge.from_fields, edge.to_fields)),
            }
        )

    lines: List[str] = [
        f"# R2RML mapping generated by r2g (source: {source_type}).",
        "# Relational -> RDF for SPARQL->SQL pushdown (e.g. Ontop). No data movement.",
        f"# Concept IRIs: <{concept_base}...>   Resource IRIs: <{resource_base}...>",
        "",
        _PREFIXES,
    ]

    for cm in config.collections.values():
        if cm.collection_type == "edge" or cm.is_join_table:
            continue
        # CC-12 conceptual class name; physical names stay in the logical table.
        generated_entity = generated_names[cm.target_collection]
        entity = _entity(generated_entity)
        table = schema.tables.get(cm.source_table)
        if table is None:
            raise R2RMLError(
                f"entity {entity!r} maps to source table {cm.source_table!r}, "
                f"which is not present in the schema"
            )

        notes: List[str] = []
        pk = list(table.primary_key or [])
        if not pk:
            # R2RML needs a subject; with no declared PK, template over every
            # column so rows still get distinct (if verbose) IRIs.
            pk = [c.name for c in table.columns]
            notes.append(
                f"# NOTE: {cm.source_table!r} has no primary key; "
                f"subject template uses all columns"
            )

        template = resource_base + entity + "".join(f"/{{{c}}}" for c in pk)

        # Each entry is a complete predicate-object phrase (no trailing
        # separator); the top-level list is joined with " ;" and closed with
        # " .". Turtle permits a trailing ";" before "]" inside bracketed
        # nodes, so inner statements can carry one uniformly.
        preds: List[str] = [
            "    a rr:TriplesMap",
            # Schema-qualified outside `public`: Ontop resolves a bare name in the
            # connection's first schema only, so two schemas cannot share one mapping.
            f"    rr:logicalTable [ rr:tableName {_ttl_string(table.qualified_name())} ]",
            (
                "    rr:subjectMap [\n"
                f"        rr:template {_ttl_string(template)} ;\n"
                f"        rr:class <{concept_base}{entity}> ;\n"
                "    ]"
            ),
        ]

        for source_col, prop, sql_type in _mapped_columns(cm, table):
            dtype = _xsd_datatype(sql_type)
            obj = f"rr:column {_ttl_string(source_col)}"
            if dtype is not None:
                obj += f" ; rr:datatype {dtype}"
            preds.append(
                "    rr:predicateObjectMap [\n"
                # Predicate IRI is the CC-12 conceptual name; rr:column above
                # keeps the physical column — Ontop reads the column, queries
                # use the concept.
                f"        rr:predicate <{concept_base}"
                f"{_predicate(generated_entity, owl_property_name(prop))}> ;\n"
                f"        rr:objectMap [ {obj} ] ;\n"
                "    ]"
            )

        for rel in rels_by_entity.get(entity, []):
            join_lines = "".join(
                "            rr:joinCondition [ "
                f"rr:child {_ttl_string(child)} ; "
                f"rr:parent {_ttl_string(parent)} ] ;\n"
                for child, parent in rel["joins"]
            )
            preds.append(
                "    rr:predicateObjectMap [\n"
                f"        rr:predicate <{concept_base}{rel['type']}> ;\n"
                "        rr:objectMap [\n"
                f"            rr:parentTriplesMap <#{rel['to_entity']}> ;\n"
                f"{join_lines}"
                "        ] ;\n"
                "    ]"
            )

        if cm.field_expressions:
            computed = ", ".join(
                fe.target for fe in cm.field_expressions if not fe.is_identity
            )
            if computed:
                notes.append(
                    f"# NOTE: computed properties omitted (need SQL): {one_line(computed)}"
                )

        block = f"<#{entity}>\n" + " ;\n".join(preds) + " ."
        lines.append(block)
        lines.extend(notes)
        lines.append("")

    if skipped_edges:
        lines.append(
            "# Skipped edges (endpoint not a mapped document entity — e.g. "
            "join-table M2M):"
        )
        lines.extend(f"#   {one_line(e)}" for e in skipped_edges)
        lines.append("")

    return "\n".join(lines) + "\n"
