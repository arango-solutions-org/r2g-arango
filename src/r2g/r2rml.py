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
from typing import Any, Dict, List, Optional, Tuple

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
                    f"# NOTE: computed properties omitted (need SQL): {computed}"
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
        lines.extend(f"#   {e}" for e in skipped_edges)
        lines.append("")

    return "\n".join(lines) + "\n"


# --- reading back what mapping_to_r2rml wrote --------------------------------------------

_TTL_TOKEN = re.compile(
    r"""(?P<space>\s+)
    |(?P<comment>\#[^\n]*)
    |(?P<string>"(?:[^"\\\n]|\\[\\"nrt])*")
    |(?P<iri><[^<>\s]*>)
    |(?P<directive>@prefix\b)
    |(?P<name>[A-Za-z]+:[A-Za-z]*|a(?=\s))
    |(?P<punct>[\[\];.])""",
    re.VERBOSE,
)
_TTL_ESCAPES = {"\\": "\\", '"': '"', "n": "\n", "r": "\r", "t": "\t"}
#: The deepest ``[ ]`` the emitter writes: map > predicate-object > object > join condition.
_TTL_MAX_DEPTH = 3

# What each node may hold: predicate -> (kind of object, how many: "1", "?" or "*").
_TRIPLES_MAP = {
    "a": ("name", "1"),
    "rr:logicalTable": ("node", "1"),
    "rr:subjectMap": ("node", "1"),
    "rr:predicateObjectMap": ("node", "*"),
}
_LOGICAL_TABLE = {"rr:tableName": ("string", "1")}
_SUBJECT_MAP = {"rr:template": ("string", "1"), "rr:class": ("iri", "1")}
_PREDICATE_OBJECT = {"rr:predicate": ("iri", "1"), "rr:objectMap": ("node", "1")}
_COLUMN_OBJECT = {"rr:column": ("string", "1"), "rr:datatype": ("name", "?")}
_JOIN_OBJECT = {"rr:parentTriplesMap": ("iri", "1"), "rr:joinCondition": ("node", "*")}
_JOIN_CONDITION = {"rr:child": ("string", "1"), "rr:parent": ("string", "1")}


def _ttl_tokens(text: str) -> Tuple[List[Tuple[str, str, int]], List[str]]:
    """(kind, text, line) of each token, and r2g's notes among the comments."""
    tokens: List[Tuple[str, str, int]] = []
    notes: List[str] = []
    position, line = 0, 1
    while position < len(text):
        match = _TTL_TOKEN.match(text, position)
        if match is None:
            raise R2RMLError(f"line {line}: not r2g's R2RML at {text[position:position + 20]!r}")
        kind, value = str(match.lastgroup), match.group()
        if kind == "comment" and value.startswith(("# NOTE:", "# Skipped edges", "#   ")):
            notes.append(value.lstrip("# "))
        elif kind not in ("space", "comment"):
            tokens.append((kind, value, line))
        line += value.count("\n")
        position = match.end()
    return tokens, notes


def _ttl_node(node: Tuple[List[Any], int], shape: Dict[str, Tuple[str, str]]) -> Dict[str, Any]:
    """A parsed node's values by predicate, refusing anything ``shape`` does not allow."""
    pairs, line = node
    found: Dict[str, List[Any]] = {}
    for verb, kind, value, at in pairs:
        if verb not in shape or shape[verb][0] != kind:
            raise R2RMLError(f"line {at}: unexpected {verb} ({kind})")
        found.setdefault(verb, []).append(value)
    out: Dict[str, Any] = {}
    for verb, (_, count) in shape.items():
        values = found.get(verb, [])
        if count == "*":
            out[verb] = values
        elif len(values) > 1 or (count == "1" and not values):
            raise R2RMLError(f"line {line}: expected one {verb}, found {len(values)}")
        else:
            out[verb] = values[0] if values else None
    return out


def _ttl_object_entry(predicate: str, node: Tuple[List[Any], int]) -> Tuple[str, Dict[str, Any]]:
    """A predicate-object map as a ``columns`` or ``joins`` entry."""
    pairs, line = node
    if any(verb == "rr:parentTriplesMap" for verb, *_ in pairs):
        join = _ttl_node(node, _JOIN_OBJECT)
        parent = join["rr:parentTriplesMap"]
        if not parent.startswith("#"):
            raise R2RMLError(f"line {line}: parent map {parent!r} is not in this document")
        conditions = [_ttl_node(c, _JOIN_CONDITION) for c in join["rr:joinCondition"]]
        return "joins", {"predicate": predicate, "parent": parent[1:], "conditions": [
            {"child": c["rr:child"], "parent": c["rr:parent"]} for c in conditions]}
    column = _ttl_node(node, _COLUMN_OBJECT)
    datatype = column["rr:datatype"]
    if datatype is not None and not datatype.startswith("xsd:"):
        raise R2RMLError(f"line {line}: unexpected datatype {datatype}")
    return "columns", {"column": column["rr:column"], "predicate": predicate, "datatype": datatype}


def summarize_r2rml(text: str) -> Dict[str, Any]:
    """The tables, columns and joins of a document :func:`mapping_to_r2rml` wrote.

    This reads that emitter's output and nothing wider, so r2g needs no RDF library at
    runtime. Anything else, even valid Turtle, raises :class:`R2RMLError`.
    """
    tokens, notes = _ttl_tokens(text)
    prefixes = [t[:2] for t in _ttl_tokens(_PREFIXES)[0]]
    if [t[:2] for t in tokens[:len(prefixes)]] != prefixes:
        raise R2RMLError("line 1: not r2g's R2RML prefixes")
    position = len(prefixes)

    def take() -> Tuple[str, str, int]:
        nonlocal position
        if position >= len(tokens):
            raise R2RMLError("the document ends early")
        position += 1
        return tokens[position - 1]

    def pairs(depth: int) -> List[Tuple[str, str, Any, int]]:
        """``verb object (; verb object)* ;?``, up to the closing token (not consumed)."""
        out: List[Tuple[str, str, Any, int]] = []
        while True:
            kind, verb, line = take()
            if kind != "name":
                raise R2RMLError(f"line {line}: expected a predicate, found {verb!r}")
            kind, value, at = take()
            parsed: Any
            if value == "[" and kind == "punct":
                if depth >= _TTL_MAX_DEPTH:
                    raise R2RMLError(f"line {at}: nested too deeply")
                parsed = (pairs(depth + 1), at)
                closing = take()
                if closing[1] != "]":
                    raise R2RMLError(f"line {closing[2]}: expected ]")
                kind = "node"
            elif kind == "string":
                parsed = re.sub(r"\\(.)", lambda m: _TTL_ESCAPES[m.group(1)], value[1:-1])
            elif kind == "iri":
                parsed = value[1:-1]
            elif kind == "name":
                parsed = value
            else:
                raise R2RMLError(f"line {at}: unexpected {value!r}")
            out.append((verb, kind, parsed, line))
            if position >= len(tokens) or tokens[position][1] != ";":
                return out
            take()
            if position < len(tokens) and tokens[position][1] in ("]", "."):
                return out

    tables: List[Dict[str, Any]] = []
    while position < len(tokens):
        kind, subject, line = take()
        if kind != "iri" or not subject.startswith("<#") or len(subject) == 3:
            raise R2RMLError(f"line {line}: expected a <#TriplesMap>, found {subject!r}")
        triples_map = _ttl_node((pairs(0), line), _TRIPLES_MAP)
        closing = take()
        if closing[1] != ".":
            raise R2RMLError(f"line {closing[2]}: expected .")
        if triples_map["a"] != "rr:TriplesMap":
            raise R2RMLError(f"line {line}: not a rr:TriplesMap")
        table = _ttl_node(triples_map["rr:logicalTable"], _LOGICAL_TABLE)
        subject_map = _ttl_node(triples_map["rr:subjectMap"], _SUBJECT_MAP)
        entry: Dict[str, Any] = {
            "subject": subject[2:-1],
            "table": table["rr:tableName"],
            "template": subject_map["rr:template"],
            "class": subject_map["rr:class"],
            "columns": [],
            "joins": [],
        }
        for node in triples_map["rr:predicateObjectMap"]:
            predicate_object = _ttl_node(node, _PREDICATE_OBJECT)
            where, item = _ttl_object_entry(predicate_object["rr:predicate"],
                                            predicate_object["rr:objectMap"])
            entry[where].append(item)
        tables.append(entry)

    subjects = [t["subject"] for t in tables]
    if len(set(subjects)) != len(subjects):
        raise R2RMLError("a TriplesMap is defined twice")
    for entry in tables:
        for join in entry["joins"]:
            if join["parent"] not in subjects:
                raise R2RMLError(f"{entry['subject']} joins to unknown map {join['parent']!r}")
    return {"tables": tables, "notes": notes}
