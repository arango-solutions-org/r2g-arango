from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Set

import yaml

from r2g.expressions import ExpressionError, compile_expression
from r2g.naming import edge_collection_name as _edge_name
from r2g.types import (
    RESERVED_ATTRIBUTES,
    CollectionMapping,
    EdgeDefinition,
    MappingConfig,
    Schema,
    Table,
)

DEFAULT_TYPE_MAP: Dict[str, str] = {
    "integer": "integer",
    "int": "integer",
    "int4": "integer",
    "int8": "integer",
    "int2": "integer",
    "bigint": "integer",
    "smallint": "integer",
    "serial": "integer",
    "bigserial": "integer",
    "smallserial": "integer",
    "oid": "integer",
    "numeric": "float",
    "decimal": "float",
    "real": "float",
    "float4": "float",
    "float8": "float",
    "double precision": "float",
    "money": "string",
    "boolean": "boolean",
    "bool": "boolean",
    "json": "object",
    "jsonb": "object",
    "uuid": "string",
    "text": "string",
    "varchar": "string",
    "character varying": "string",
    "char": "string",
    "character": "string",
    "bpchar": "string",
    "name": "string",
    "bytea": "string",
    "date": "string",
    "time": "string",
    "time without time zone": "string",
    "time with time zone": "string",
    "timetz": "string",
    "timestamp": "string",
    "timestamp without time zone": "string",
    "timestamp with time zone": "string",
    "timestamptz": "string",
    "interval": "string",
    "inet": "string",
    "cidr": "string",
    "macaddr": "string",
    "macaddr8": "string",
    "xml": "string",
    "point": "string",
    "line": "string",
    "lseg": "string",
    "box": "string",
    "path": "string",
    "polygon": "string",
    "circle": "string",
    "tsvector": "string",
    "tsquery": "string",
    "bit": "string",
    "bit varying": "string",
    "varbit": "string",
    # MySQL / MariaDB integer + text + blob variants (DATA_TYPE base names).
    "tinyint": "integer",
    "mediumint": "integer",
    "year": "integer",
    "tinytext": "string",
    "mediumtext": "string",
    "longtext": "string",
    "tinyblob": "string",
    "blob": "string",
    "mediumblob": "string",
    "longblob": "string",
    "enum": "string",
    "set": "string",
    # SQL Server variants (DATA_TYPE names). `bit` is boolean in SQL Server but
    # a bit-string in PostgreSQL, so the conflict is resolved in the connector
    # rather than here. `tinyint`/`smallmoney`/`money`/`real` are already mapped.
    "nvarchar": "string",
    "nchar": "string",
    "ntext": "string",
    "datetime2": "string",
    "smalldatetime": "string",
    "datetimeoffset": "string",
    "uniqueidentifier": "string",
    "smallmoney": "string",
    "image": "string",
    "number": "float",
    "fixed": "float",
    "float": "float",
    "double": "float",
    "float32": "float",
    "float64": "float",
    "binary": "string",
    "varbinary": "string",
    "string": "string",
    "datetime": "string",
    "timestamp_ltz": "string",
    "timestamp_ntz": "string",
    "timestamp_tz": "string",
    "variant": "object",
    "object": "object",
    "array": "array",
    "geography": "object",
    "geometry": "object",
    "vector": "array",
    # ClickHouse variants (base names after ``Nullable(…)`` / ``LowCardinality(…)``
    # are stripped by ``connectors.clickhouse._clean_type``). Without these the
    # unsigned/sized integer families fall through to the ``"string"`` default,
    # which silently breaks JSON-level type comparisons — notably the P6.7
    # cross-source shared-key type-compatibility gate, where a ClickHouse
    # ``UInt64`` key would look incompatible with a PostgreSQL ``bigint``.
    # ``Int8``/``Int4``/``Int2``, ``Float32``/``Float64``, ``Bool``, ``String``,
    # ``UUID``, ``Date``, ``DateTime``, and ``Decimal`` are already covered above.
    "uint8": "integer",
    "uint16": "integer",
    "uint32": "integer",
    "uint64": "integer",
    "uint128": "integer",
    "uint256": "integer",
    "int16": "integer",
    "int32": "integer",
    "int64": "integer",
    "int128": "integer",
    "int256": "integer",
    "decimal32": "float",
    "decimal64": "float",
    "decimal128": "float",
    "decimal256": "float",
    "date32": "string",
    "datetime64": "string",
    "fixedstring": "string",
    "enum8": "string",
    "enum16": "string",
    "ipv4": "string",
    "ipv6": "string",
    "map": "object",
    "tuple": "object",
}


def _base_pg_type_name(pg_type: str) -> str:
    t = pg_type.strip().lower()
    if "(" in t:
        t = t.split("(", 1)[0].strip()
    return t


def _is_array_pg_type(pg_type: str) -> bool:
    s = pg_type.strip().lower()
    return "[]" in s or s.startswith("array") or s.endswith("[]")


#: Fixed-point types whose declared scale tells a whole number from a decimal.
#: Every Snowflake integer is ``NUMBER(38,0)`` (recorded as ``number(38,0)``,
#: r2g-arango#24), and ``numeric(10)`` has scale 0 by the SQL standard.
_FIXED_POINT_TYPES = frozenset({"number", "numeric", "decimal", "fixed"})
#: ClickHouse's sized decimals take the scale as their only argument.
_SCALE_ONLY_DECIMALS = frozenset({"decimal32", "decimal64", "decimal128", "decimal256"})
_SIZED_TYPE = re.compile(r"\s*([a-z0-9_ ]+?)\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)\s*")


def _declared_scale(pg_type: str) -> int | None:
    """The scale a fixed-point type declares, or None when it declares none.

    A bare ``number`` / ``numeric`` / ``decimal`` declares no scale, so its
    values may carry a fraction and it stays a float.
    """
    m = _SIZED_TYPE.fullmatch(pg_type.lower())
    if m is None:
        return None
    base, first, second = m.groups()
    if base in _SCALE_ONLY_DECIMALS:
        return int(first) if second is None else None
    if base in _FIXED_POINT_TYPES:
        return int(second) if second is not None else 0
    return None


def pg_type_to_json_type(pg_type: str) -> str:
    if _is_array_pg_type(pg_type):
        return "array"
    # A fixed-point column with scale 0 holds whole numbers. Mapped to float, the
    # load converted each value to a double, which cannot hold a whole number
    # above 2**53: a 64-bit id silently lost its last digits.
    if _declared_scale(pg_type) == 0:
        return "integer"
    base = _base_pg_type_name(pg_type)
    if base in DEFAULT_TYPE_MAP:
        return DEFAULT_TYPE_MAP[base]
    return "string"


def _is_likely_join_table(table: Table) -> bool:
    """Heuristic: a join table has exactly 2 FKs and no non-FK, non-PK data columns
    (or only typical junction metadata like quantity, created_at, etc.)."""
    if len(table.foreign_keys) != 2:
        return False
    fk_cols: set[str] = set()
    for fk in table.foreign_keys:
        fk_cols.update(fk.columns)
    pk_cols = set(table.primary_key)
    structural = fk_cols | pk_cols
    data_cols = [c for c in table.columns if c.name not in structural]
    if not data_cols:
        return True
    _JUNCTION_META = {"quantity", "qty", "count", "sort_order", "position", "rank",
                      "created_at", "updated_at", "created", "updated"}
    return all(c.name.lower() in _JUNCTION_META for c in data_cols)


def validate_config(schema: Schema, config: MappingConfig) -> list[str]:
    """Validate a mapping config against a schema, returning a list of issues.

    Checks that every collection references a known table, every edge
    references valid collections and columns, and field lists only name
    columns that exist in the source table.
    """
    issues: list[str] = []
    col_names_by_table: dict[str, set[str]] = {
        name: {c.name for c in table.columns} for name, table in schema.tables.items()
    }
    collection_tables = set()

    for key, cm in config.collections.items():
        src = cm.source_table
        if src not in schema.tables:
            issues.append(f"Collection '{key}': source_table '{src}' not found in schema")
            continue
        collection_tables.add(src)
        cols = col_names_by_table[src]
        for fm_src, fm_tgt in cm.field_mappings.items():
            if fm_src not in cols:
                issues.append(
                    f"Collection '{key}': field_mapping source '{fm_src}' "
                    f"is not a column in table '{src}'"
                )
            if fm_tgt in RESERVED_ATTRIBUTES:
                issues.append(
                    f"Collection '{key}': field_mapping target '{fm_tgt}' is a "
                    f"reserved ArangoDB attribute and cannot be used"
                )
        for ef in cm.exclude_fields:
            if ef not in cols:
                issues.append(
                    f"Collection '{key}': exclude_field '{ef}' "
                    f"is not a column in table '{src}'"
                )
        if cm.include_fields is not None:
            for incl in cm.include_fields:
                if incl not in cols:
                    issues.append(
                        f"Collection '{key}': include_field '{incl}' "
                        f"is not a column in table '{src}'"
                    )

        for fx in cm.field_expressions:
            if fx.target in RESERVED_ATTRIBUTES:
                issues.append(
                    f"Collection '{key}': expression target '{fx.target}' is a "
                    f"reserved ArangoDB attribute and cannot be used"
                )
            for s in fx.sources:
                if s not in cols:
                    issues.append(
                        f"Collection '{key}': expression for '{fx.target}' "
                        f"references source '{s}' which is not a column in table '{src}'"
                    )
            if fx.engine == "aql" and fx.expression.strip():
                try:
                    compiled = compile_expression(fx.expression)
                except ExpressionError as err:
                    issues.append(
                        f"Collection '{key}': expression for '{fx.target}' is invalid: {err}"
                    )
                    continue
                for ref in compiled.references:
                    if ref not in cols:
                        issues.append(
                            f"Collection '{key}': expression for '{fx.target}' "
                            f"references @{ref} which is not a column in table '{src}'"
                        )

    for key, cm in config.collections.items():
        src = cm.source_table
        if src in schema.tables and not schema.tables[src].primary_key:
            issues.append(
                f"Collection '{key}': source table '{src}' has no primary key; "
                f"documents are keyed by all column values; identical rows "
                f"collapse to one document"
            )

    for idx, edge in enumerate(config.edges):
        label = edge.edge_collection or f"edges[{idx}]"
        if edge.from_collection not in schema.tables:
            issues.append(
                f"Edge '{label}': from_collection '{edge.from_collection}' "
                f"not found in schema"
            )
        else:
            src_cols = col_names_by_table[edge.from_collection]
            for ff in edge.from_fields:
                if ff not in src_cols:
                    issues.append(
                        f"Edge '{label}': from_field '{ff}' "
                        f"is not a column in table '{edge.from_collection}'"
                    )
        if edge.to_collection not in schema.tables:
            issues.append(
                f"Edge '{label}': to_collection '{edge.to_collection}' "
                f"not found in schema"
            )
        else:
            tgt_cols = col_names_by_table[edge.to_collection]
            for tf in edge.to_fields:
                if tf not in tgt_cols:
                    issues.append(
                        f"Edge '{label}': to_field '{tf}' "
                        f"is not a column in table '{edge.to_collection}'"
                    )

    return issues


class ConfigManager:
    """Load, save, and synthesize table-to-graph mapping configuration."""

    @staticmethod
    def target_by_source_table(config: MappingConfig) -> dict[str, str]:
        """Map each collection's ``source_table`` to its ``target_collection``.

        Edge ``from_collection`` / ``to_collection`` reference *source-table*
        keys; this lookup resolves them to the collection names that actually
        hold the data, so renames don't break edge endpoints. Shared by the
        named-graph builder and every edge-transformer construction site.
        """
        return {
            cm.source_table: cm.target_collection
            for cm in config.collections.values()
        }

    @staticmethod
    def graph_edge_definitions(config: MappingConfig) -> list[dict[str, Any]]:
        """Build ArangoDB named-graph edge definitions from a mapping config.

        Resolves ``from_collection`` / ``to_collection`` (which are *source-table*
        keys on the edge) to the corresponding ``target_collection`` names, so
        the named graph references the collections that actually hold the data
        even after collections are renamed.
        """
        if config.graph_layout == "lpg":
            # One edge collection, both endpoints the one node collection: any
            # type may connect to any type, with the constraint carried by the
            # edge's `type` / endpoint-label fields rather than by the schema.
            from r2g.lpg import graph_edge_definition

            return [graph_edge_definition(config.lpg)]
        target_by_source = ConfigManager.target_by_source_table(config)
        defs: list[dict[str, Any]] = []
        for edge in config.edges:
            defs.append({
                "edge_collection": edge.edge_collection,
                "from_vertex_collections": [
                    target_by_source.get(edge.from_collection, edge.from_collection)
                ],
                "to_vertex_collections": [
                    target_by_source.get(edge.to_collection, edge.to_collection)
                ],
            })
        return defs

    @staticmethod
    def generate_default_config(
        schema: Schema,
        source_schema: str = "public",
        expand_partitions: bool = False,
    ) -> MappingConfig:
        collections: Dict[str, CollectionMapping] = {}
        edges: list[EdgeDefinition] = []
        edge_collection_names: Set[str] = set()

        def _is_partition_child(table) -> bool:
            # Collapse partition children into their parent by default: the
            # parent (which carries the rolled-up FKs from introspection) is the
            # single logical collection and a query against it returns every
            # partition's rows. ``expand_partitions`` opts back into per-shard
            # collections.
            return bool(getattr(table, "partition_of", None)) and not expand_partitions

        for table_name, table in schema.tables.items():
            if _is_partition_child(table):
                continue
            is_join = _is_likely_join_table(table)
            collections[table_name] = CollectionMapping(
                source_table=table_name,
                target_collection=table_name,
                collection_type="document",
                is_join_table=is_join,
            )

        for table_name, table in schema.tables.items():
            if _is_partition_child(table):
                continue
            for fk in table.foreign_keys:
                base = _edge_name(table_name, fk.foreign_table)
                edge_name = base
                if edge_name in edge_collection_names:
                    suffix = "_".join(fk.columns)
                    edge_name = f"{base}_{suffix}"
                edge_collection_names.add(edge_name)
                edges.append(
                    EdgeDefinition(
                        edge_collection=edge_name,
                        from_collection=table_name,
                        to_collection=fk.foreign_table,
                        from_fields=fk.columns,
                        to_fields=fk.foreign_columns,
                    )
                )

        return MappingConfig(
            source_schema=source_schema,
            collections=collections,
            edges=edges,
        )

    @staticmethod
    def load_config(path: str | Path) -> MappingConfig:
        p = Path(path)
        with p.open("r", encoding="utf-8") as f:
            data: Any = yaml.safe_load(f)
        if data is None:
            return MappingConfig()
        if not isinstance(data, dict):
            raise ValueError(f"Mapping config must be a YAML mapping, got {type(data).__name__}")
        return MappingConfig.model_validate(data)

    @staticmethod
    def dump_config(config: MappingConfig) -> str:
        """The text :meth:`save_config` writes for ``config``."""
        return yaml.safe_dump(
            config.model_dump(mode="python"),
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )

    @staticmethod
    def save_config(config: MappingConfig, path: str | Path) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("w", encoding="utf-8") as f:
            f.write(ConfigManager.dump_config(config))
