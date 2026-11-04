from __future__ import annotations

import json
from typing import Any, Dict, Optional

from r2g.config import pg_type_to_json_type
from r2g.expressions import (
    CompiledExpression,
    ExpressionError,
    compile_expression,
    extract_bind_references,
    rewrite_bind_params,
)
from r2g.keys import sanitize_key_component
from r2g.log import get_logger
from r2g.types import CollectionMapping, Column, FieldExpression, LpgLayout, Table

logger = get_logger(__name__)


class NodeTransformer:
    def __init__(
        self,
        table_def: Table,
        collection_mapping: Optional[CollectionMapping] = None,
        key_separator: str = "_",
        type_overrides: Optional[Dict[str, str]] = None,
        lpg: Optional[LpgLayout] = None,
        label: Optional[str] = None,
    ) -> None:
        self.table_def = table_def
        self._mapping = collection_mapping
        self.key_separator = key_separator
        self._type_overrides = type_overrides or {}
        # LPG layout (P13): one collection for every node type, so the type has
        # to travel inside the document as a label — and inside the key, which
        # would otherwise collide across former tables.
        self.lpg = lpg
        self.label = label or (
            collection_mapping.target_collection
            if collection_mapping is not None
            else table_def.name
        )
        # The full label set written to the document (P13.13). ``self.label``
        # stays the PRIMARY label and remains the sole basis for the ``_key``
        # namespace, so adding labels to a mapping never relocates an existing
        # document — only widens what it answers to.
        if label is not None or collection_mapping is None:
            self.labels = [self.label]
        else:
            self.labels = collection_mapping.labels
        self._compiled_expressions: list[tuple[FieldExpression, Optional[CompiledExpression]]] = []
        # Expressions that compile-fail locally but are valid AQL are pushed
        # down to ArangoDB per batch (P5c.1.5). They are *not* added to
        # ``_compiled_expressions`` — :meth:`transform_row` leaves their target
        # field unset so the pipeline can fill it from the server result.
        self._delegated: list[FieldExpression] = []
        self._delegated_targets: set[str] = set()
        if collection_mapping is not None:
            for fx in collection_mapping.field_expressions:
                if fx.is_identity or not fx.expression.strip():
                    self._compiled_expressions.append((fx, None))
                    continue
                if fx.engine != "aql":
                    logger.warning(
                        "field_expression_engine_unsupported",
                        target=fx.target,
                        engine=fx.engine,
                    )
                    self._compiled_expressions.append((fx, None))
                    continue
                try:
                    compiled = compile_expression(fx.expression)
                except ExpressionError as err:
                    logger.info(
                        "field_expression_delegated_to_aql",
                        target=fx.target,
                        engine=fx.engine,
                        reason=str(err),
                    )
                    self._delegated.append(fx)
                    self._delegated_targets.add(fx.target)
                    continue
                self._compiled_expressions.append((fx, compiled))

    @property
    def has_delegated_expressions(self) -> bool:
        """True when one or more expressions must be evaluated server-side."""
        return bool(self._delegated)

    @property
    def delegated_expressions(self) -> list[FieldExpression]:
        """The expressions evaluated server-side, in mapping order."""
        return list(self._delegated)

    def delegated_reference_columns(self) -> set[str]:
        """Source columns the delegated expressions read (for row projection)."""
        cols: set[str] = set()
        for fx in self._delegated:
            cols.update(fx.sources or [])
            cols.update(extract_bind_references(fx.expression))
        return cols

    def build_delegation_query(self, expressions: Optional[list[FieldExpression]] = None) -> str:
        """Build an AQL query computing all delegated targets for a row batch.

        The query takes a ``@rows`` bind parameter (a list of projected source
        rows) and returns one object per row with each delegated target mapped
        to its server-evaluated value, in input order. ``expressions`` limits it
        to those (default: every delegated expression).
        """
        parts = []
        for fx in self._delegated if expressions is None else expressions:
            key = json.dumps(fx.target)
            parts.append(f"    {key}: {rewrite_bind_params(fx.expression)}")
        body = ",\n".join(parts)
        return f"FOR row IN @rows\n  RETURN {{\n{body}\n  }}"

    def _json_type_for_column(self, column: Column) -> str:
        if column.name in self._type_overrides:
            return self._type_overrides[column.name]
        return pg_type_to_json_type(column.data_type)

    def _coerce_value(self, value: Any, column: Column) -> Any:
        json_type = self._json_type_for_column(column)
        if value is None:
            return None
        if isinstance(value, str) and value.strip() == "" and column.is_nullable:
            return None

        if json_type == "integer":
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, int):
                return value
            if isinstance(value, float):
                return int(value)
            return int(str(value).strip())

        if json_type == "float":
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return float(value)
            return float(str(value).strip())

        if json_type == "boolean":
            if isinstance(value, str):
                return value.lower() in ("true", "1", "t", "yes")
            return bool(value)

        if json_type == "object":
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    logger.warning("json_decode_failed", column=column.name)
                    return value
            return value

        if json_type == "array":
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    pass
                pg = value.strip()
                if pg.startswith("{") and pg.endswith("}"):
                    inner = pg[1:-1]
                    if inner == "":
                        return []
                    return [elem.strip('"') for elem in inner.split(",")]
                return value
            return value

        return str(value)

    def _generate_key(self, row: Dict[str, Any]) -> Optional[str]:
        if not self.table_def.primary_key:
            return None
        pk_values: list[str] = []
        for pk_col in self.table_def.primary_key:
            val = row.get(pk_col)
            if val is None:
                raise ValueError(f"Row missing PK value for column '{pk_col}': {row}")
            pk_values.append(sanitize_key_component(val))
        key = self.key_separator.join(pk_values)
        if self.lpg is not None:
            # ``customers/1`` and ``orders/1`` are distinct in the pg layout but
            # both become ``nodes/1`` once collapsed into one collection — the
            # second silently overwrites the first. Namespace by label.
            key = f"{self.label}{self.key_separator}{key}"
        return key

    def _decorate_lpg(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Attach the LPG type carriers: the label array and the origin table."""
        if self.lpg is None:
            return doc
        doc[self.lpg.label_field] = list(self.labels)
        doc[self.lpg.source_field] = self.table_def.name
        return doc

    def transform_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        key = self._generate_key(row)

        if self._mapping is None:
            passthrough = row.copy()
            if key:
                passthrough["_key"] = key
            return self._decorate_lpg(passthrough)

        col_by_name: Dict[str, Column] = {c.name: c for c in self.table_def.columns}
        known = set(col_by_name.keys())
        names = set(row.keys()) & known
        if self._mapping.include_fields is not None:
            names &= set(self._mapping.include_fields)
        names -= set(self._mapping.exclude_fields)

        expression_targets = {
            fx.target for fx, _ in self._compiled_expressions
        } | self._delegated_targets

        ordered_names = [k for k in row if k in names]
        doc: Dict[str, Any] = {}
        for src_name in ordered_names:
            raw = row.get(src_name)
            column = col_by_name[src_name]
            try:
                coerced = self._coerce_value(raw, column)
            except (TypeError, ValueError) as e:
                logger.warning("coerce_failed", column=src_name, error=str(e))
                coerced = raw
            tgt_name = self._mapping.field_mappings.get(src_name, src_name)
            if tgt_name in expression_targets:
                continue
            doc[tgt_name] = coerced

        for fx, compiled in self._compiled_expressions:
            doc[fx.target] = self._apply_field_expression(fx, compiled, row, col_by_name)

        if key:
            doc["_key"] = key
        return self._decorate_lpg(doc)

    def _apply_field_expression(
        self,
        fx: FieldExpression,
        compiled: Optional[CompiledExpression],
        row: Dict[str, Any],
        col_by_name: Dict[str, Column],
    ) -> Any:
        """Evaluate a ``FieldExpression`` against the raw source row.

        Identity and un-compilable expressions fall back to a pass-through
        read of ``sources`` (or the target name if ``sources`` is empty).
        """

        if compiled is None:
            src_name = fx.sources[0] if fx.sources else fx.target
            raw = row.get(src_name)
            column = col_by_name.get(src_name)
            if column is None:
                return raw
            try:
                return self._coerce_value(raw, column)
            except (TypeError, ValueError) as e:
                logger.warning("coerce_failed", column=src_name, error=str(e))
                return raw

        env: Dict[str, Any] = {}
        source_names = fx.sources or list(compiled.references)
        for src_name in source_names:
            raw = row.get(src_name)
            column = col_by_name.get(src_name)
            if column is None:
                env[src_name] = raw
                continue
            try:
                env[src_name] = self._coerce_value(raw, column)
            except (TypeError, ValueError) as e:
                logger.warning("coerce_failed", column=src_name, error=str(e))
                env[src_name] = raw
        for ref in compiled.references:
            env.setdefault(ref, row.get(ref))
        try:
            return compiled.evaluate(env)
        except ExpressionError as err:
            logger.warning(
                "field_expression_eval_failed",
                target=fx.target,
                error=str(err),
            )
            return None
