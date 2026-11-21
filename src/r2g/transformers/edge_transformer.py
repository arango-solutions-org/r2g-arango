from __future__ import annotations

from collections.abc import Generator, Iterable
from typing import Any, Dict, Optional

from r2g.keys import row_key, sanitize_key_component
from r2g.log import get_logger
from r2g.types import CollectionMapping, EdgeDefinition, LpgLayout, Schema, Table

logger = get_logger(__name__)


class EdgeTransformer:
    def __init__(
        self,
        edge_def: EdgeDefinition,
        source_table: Table,
        *,
        key_separator: str = "_",
        join_mode: bool = False,
        from_name: str | None = None,
        to_name: str | None = None,
        lpg: LpgLayout | None = None,
        from_label: str | None = None,
        to_label: str | None = None,
        from_labels: list[str] | None = None,
        to_labels: list[str] | None = None,
        edge_type: str | None = None,
    ) -> None:
        self.edge_def = edge_def
        self.source_table = source_table
        self.key_separator = key_separator
        self.join_mode = join_mode
        # Collection names used to build ``_from`` / ``_to`` references. These
        # default to the source-table keys on ``edge_def`` (historical
        # behaviour), but callers that rename collections should pass the
        # resolved ``target_collection`` names so endpoints stay valid.
        self.from_name = from_name or edge_def.from_collection
        self.to_name = to_name or edge_def.to_collection
        # LPG layout (P13). Capture the endpoint *labels* from the resolved
        # collection names BEFORE redirecting both endpoints at the single node
        # collection — in this layout the type stops being the collection and
        # becomes data, copied onto the edge so a vertex-centric index can
        # filter on it.
        self.lpg = lpg
        self.from_label = from_label or self.from_name
        self.to_label = to_label or self.to_name
        # The endpoints' FULL label sets, copied onto the edge (P13.13). These
        # must match what NodeTransformer wrote: the copies are a
        # denormalization the traversal filters against, so an edge carrying
        # only the primary label while its endpoint carries three makes every
        # filter on the other two silently miss. The single-label default keeps
        # pre-P13.13 behaviour.
        self.from_labels = list(from_labels) if from_labels else [self.from_label]
        self.to_labels = list(to_labels) if to_labels else [self.to_label]
        self.edge_type = edge_type or edge_def.edge_collection
        if lpg is not None:
            self.from_name = lpg.node_collection
            self.to_name = lpg.node_collection

    def _ns(self, label: str, key: str) -> str:
        """Label-namespace a vertex key. Identity in the ``pg`` layout."""
        if self.lpg is None:
            return key
        return f"{label}{self.key_separator}{key}"

    def _edge_key(self, from_ref: str, to_ref: str) -> str:
        if self.lpg is None:
            return f"{from_ref}{self.key_separator}{to_ref}"
        # One edge collection now holds every type, so two different edge types
        # joining the same pair would collide on ``_key``. Lead with the type.
        return self.key_separator.join((self.edge_type, from_ref, to_ref))

    def _decorate_lpg(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Attach the type + endpoint-label copies the VCIs index on."""
        if self.lpg is None:
            return doc
        doc[self.lpg.type_field] = self.edge_type
        doc[self.lpg.from_labels_field] = list(self.from_labels)
        doc[self.lpg.to_labels_field] = list(self.to_labels)
        doc[self.lpg.source_field] = self.source_table.name
        return doc

    @classmethod
    def for_join_table(
        cls,
        table: Table,
        collection_mapping: CollectionMapping,
        schema: Schema,
        key_separator: str = "_",
    ) -> EdgeTransformer:
        if len(table.foreign_keys) != 2:
            raise ValueError(
                f"Join table '{table.name}' must have exactly 2 foreign keys, got {len(table.foreign_keys)}"
            )
        fks = sorted(table.foreign_keys, key=lambda fk: (fk.foreign_table, fk.columns[0]))
        fk_a, fk_b = fks
        for fk in (fk_a, fk_b):
            if fk.foreign_table not in schema.tables:
                logger.warning(
                    "join_fk_unknown_referenced_table",
                    join_table=table.name,
                    referenced=fk.foreign_table,
                )
        edge_def = EdgeDefinition(
            edge_collection=collection_mapping.target_collection,
            from_collection=fk_a.foreign_table,
            to_collection=fk_b.foreign_table,
            from_fields=fk_a.columns,
            to_fields=fk_b.columns,
        )
        return cls(edge_def, table, key_separator=key_separator, join_mode=True)

    def _vertex_key_from_pk(self, row: Dict[str, Any]) -> str:
        return row_key(self.source_table, row, self.key_separator)

    def transform_row(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if self.join_mode:
            return self._transform_join_row(row)
        return self._transform_fk_edge_row(row)

    def _transform_fk_edge_row(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        fk_parts: list[str] = []
        for ff in self.edge_def.from_fields:
            if ff not in row:
                logger.warning("edge_missing_from_field", field=ff, table=self.source_table.name)
                return None
            val = row[ff]
            if val is None:
                return None
            if isinstance(val, str) and val.strip() == "":
                return None
            fk_parts.append(sanitize_key_component(str(val).strip()))

        try:
            src_key = self._vertex_key_from_pk(row)
        except ValueError as e:
            logger.warning("edge_source_key_failed", error=str(e), table=self.source_table.name)
            return None

        to_key = self.key_separator.join(fk_parts)
        from_ref = self._ns(self.from_label, src_key)
        to_ref = self._ns(self.to_label, to_key)
        return self._decorate_lpg({
            "_key": self._edge_key(from_ref, to_ref),
            "_from": f"{self.from_name}/{from_ref}",
            "_to": f"{self.to_name}/{to_ref}",
            "_label": self.edge_def.edge_collection,
        })

    def _transform_join_row(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        from_parts: list[str] = []
        for ff in self.edge_def.from_fields:
            if ff not in row:
                logger.warning("join_edge_missing_field", field=ff, table=self.source_table.name)
                return None
            v = row[ff]
            if v is None:
                return None
            if isinstance(v, str) and v.strip() == "":
                return None
            from_parts.append(sanitize_key_component(str(v).strip()))

        to_parts: list[str] = []
        for tf in self.edge_def.to_fields:
            if tf not in row:
                logger.warning("join_edge_missing_field", field=tf, table=self.source_table.name)
                return None
            v = row[tf]
            if v is None:
                return None
            if isinstance(v, str) and v.strip() == "":
                return None
            to_parts.append(sanitize_key_component(str(v).strip()))

        from_key = self.key_separator.join(from_parts)
        to_key = self.key_separator.join(to_parts)
        from_ref = self._ns(self.from_label, from_key)
        to_ref = self._ns(self.to_label, to_key)
        return self._decorate_lpg({
            "_key": self._edge_key(from_ref, to_ref),
            "_from": f"{self.from_name}/{from_ref}",
            "_to": f"{self.to_name}/{to_ref}",
            "_label": self.edge_def.edge_collection,
        })

    def transform_rows(self, rows: Iterable[Dict[str, Any]]) -> Generator[Dict[str, Any], None, None]:
        for row in rows:
            out = self.transform_row(row)
            if out is not None:
                yield out
