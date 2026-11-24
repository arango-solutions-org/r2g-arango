"""Transform CDC ChangeEvents into ArangoDB deltas.

Reuses the existing NodeTransformer and EdgeTransformer to produce
properly typed, correctly keyed documents and edges from row-level
change events.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from r2g.cdc.models import (
    ArangoDelta,
    ArangoOperation,
    ChangeEvent,
    ChangeOperation,
)
from r2g.keys import row_key
from r2g.log import get_logger
from r2g.transformers.edge_transformer import EdgeTransformer
from r2g.transformers.node_transformer import NodeTransformer
from r2g.types import EdgeDefinition, MappingConfig, Schema

logger = get_logger(__name__)

_OP_MAP = {
    ChangeOperation.INSERT: ArangoOperation.INSERT,
    ChangeOperation.UPDATE: ArangoOperation.REPLACE,
    ChangeOperation.DELETE: ArangoOperation.DELETE,
}


@dataclass(frozen=True)
class OwnedScope:
    """The documents one source table writes into one collection.

    ``match_field``/``match_value`` select them inside a shared (LPG)
    collection; ``None`` means the whole collection is the table's.
    ``shared_with`` lists other source tables that also write to the
    collection without any field telling their documents apart.
    """

    collection: str
    is_edge: bool
    match_field: str | None = None
    match_value: str | None = None
    shared_with: tuple[str, ...] = ()


class DeltaTransformer:
    """Convert a stream of ChangeEvents into ArangoDeltas.

    For each change event the transformer:
    1. Looks up the target collection mapping for the source table.
    2. Transforms the row through NodeTransformer (for documents).
    3. Finds all edges sourced from the affected table and builds
       edge deltas through EdgeTransformer.

    On DELETE the transformer emits delete operations for both the
    document and any edges that originated from the deleted row.
    On UPDATE edges are replaced to reflect new FK values.
    """

    def __init__(
        self,
        schema: Schema,
        config: MappingConfig,
    ) -> None:
        self.schema = schema
        self.config = config
        # Target layout (P13.9). In the LPG layout every delta is written to the
        # one shared node or edge collection and the type travels in the
        # document, exactly as the streaming load does — so a CDC stream keeps
        # the graph in the same shape the initial load produced. Indexes are
        # provisioned by that initial load; CDC only ensures the collections.
        self._lpg = config.lpg if config.graph_layout == "lpg" else None
        self._cm_by_table = {
            cm.source_table: (key, cm)
            for key, cm in config.collections.items()
        }
        self._edges_by_source: dict[str, list[EdgeDefinition]] = {}
        for edge in config.edges:
            self._edges_by_source.setdefault(edge.from_collection, []).append(edge)

    def _build_node_transformer(self, table_name: str) -> NodeTransformer | None:
        table_def = self.schema.tables.get(table_name)
        mapping_entry = self._cm_by_table.get(table_name)
        if table_def is None or mapping_entry is None:
            return None
        _, cm = mapping_entry
        return NodeTransformer(
            table_def,
            collection_mapping=cm,
            key_separator=self.config.key_separator,
            type_overrides=self.config.type_overrides,
            lpg=self._lpg,
        )

    def _build_edge_transformer(
        self, table_name: str, edge_def: EdgeDefinition
    ) -> EdgeTransformer | None:
        table_def = self.schema.tables.get(table_name)
        if table_def is None:
            return None
        from r2g.config import ConfigManager

        target_by_source = ConfigManager.target_by_source_table(self.config)
        return EdgeTransformer(
            edge_def,
            table_def,
            key_separator=self.config.key_separator,
            from_name=target_by_source.get(edge_def.from_collection),
            to_name=target_by_source.get(edge_def.to_collection),
            # Full endpoint label sets, matching what the node documents carry
            # (P13.13) — a CDC-written edge must be filterable identically to a
            # bulk-loaded one, or the two paths disagree on the same graph.
            from_labels=self._labels_for_source_table(edge_def.from_collection),
            to_labels=self._labels_for_source_table(edge_def.to_collection),
            lpg=self._lpg,
        )

    def _labels_for_source_table(self, source_table: str) -> list[str] | None:
        entry = self._cm_by_table.get(source_table)
        return entry[1].labels if entry is not None else None

    def _node_collection(self, cm: Any) -> str:
        """Where a document delta is written for this mapping."""
        return self._lpg.node_collection if self._lpg is not None else cm.target_collection

    def _edge_collection(self, edge_def: EdgeDefinition) -> str:
        """Where an edge delta is written for this edge definition."""
        return self._lpg.edge_collection if self._lpg is not None else edge_def.edge_collection

    def collections_for_table(self, table_name: str) -> set[str]:
        """Every collection a change to *table_name* can write to.

        The table's document collection plus the edge collections of edges
        sourced from it, in the target layout (one shared pair under LPG).
        Empty when the table is not mapped.
        """
        mapping_entry = self._cm_by_table.get(table_name)
        if mapping_entry is None:
            return set()
        names = {self._node_collection(mapping_entry[1])}
        for edge_def in self._edges_by_source.get(table_name, []):
            names.add(self._edge_collection(edge_def))
        return names

    def owned_scopes(self, table_name: str) -> list[OwnedScope]:
        """Where the documents *table_name* produces live, and how to tell them apart.

        Used to resync a table: every document in a scope that the table's
        current rows no longer produce is one whose row is gone. Under LPG
        each node and edge records its source table (``LpgConfig.source_field``),
        so the shared collections are matched on it. Under the ``pg`` layout a
        collection belongs to the table outright, unless the mapping has
        another table writing to the same collection; ``shared_with`` names
        those, and such a scope cannot be swept safely.
        """
        entry = self._cm_by_table.get(table_name)
        if entry is None:
            return []
        _, cm = entry
        edge_defs = self._edges_by_source.get(table_name, [])
        if self._lpg is not None:
            scopes = [OwnedScope(self._lpg.node_collection, False, self._lpg.source_field, table_name)]
            if edge_defs:
                scopes.append(
                    OwnedScope(self._lpg.edge_collection, True, self._lpg.source_field, table_name)
                )
            return scopes

        others = sorted(
            c.source_table for c in self.config.collections.values()
            if c.target_collection == cm.target_collection and c.source_table != table_name
        )
        scopes = [OwnedScope(cm.target_collection, False, shared_with=tuple(others))]
        for coll in sorted({e.edge_collection for e in edge_defs}):
            writers = {
                e.from_collection for e in self.config.edges
                if e.edge_collection == coll and e.from_collection != table_name
            } | {
                c.source_table for c in self.config.collections.values()
                if c.is_join_table and c.target_collection == coll
            }
            scopes.append(OwnedScope(coll, True, shared_with=tuple(sorted(writers))))
        return scopes

    def _document_key_from_row(
        self, table_name: str, row: dict[str, Any]
    ) -> str | None:
        table_def = self.schema.tables.get(table_name)
        if table_def is None:
            return None
        if not table_def.primary_key and any(c.name not in row for c in table_def.columns):
            # A keyless table is keyed by every column, so only the full old row
            # (Postgres: REPLICA IDENTITY FULL) can address its document.
            return None
        try:
            key = row_key(table_def, row, self.config.key_separator)
        except ValueError:  # a PK column missing from the old row
            return None
        if self._lpg is not None:
            # This key is built here rather than by NodeTransformer (a delete
            # carries only the old row), so the label namespace has to be
            # applied here too. Without it a DELETE would address ``nodes/42``
            # instead of ``nodes/Account_42`` — deleting nothing, or, if another
            # table happens to own that key, the wrong document. Deletes are
            # idempotent, so the miss would leave no trace.
            mapping_entry = self._cm_by_table.get(table_name)
            if mapping_entry is None:
                return None
            _, cm = mapping_entry
            key = f"{cm.target_collection}{self.config.key_separator}{key}"
        return key

    def transform(self, event: ChangeEvent) -> list[ArangoDelta]:
        """Produce zero or more ArangoDeltas from a single ChangeEvent."""
        deltas: list[ArangoDelta] = []
        table_name = event.table_name

        mapping_entry = self._cm_by_table.get(table_name)
        if mapping_entry is None:
            logger.debug("cdc_skip_unmapped_table", table=table_name)
            return deltas

        _, cm = mapping_entry
        arango_op = _OP_MAP[event.operation]

        if event.is_delete:
            doc_key = self._document_key_from_row(table_name, event.old_row or {})
            if doc_key:
                deltas.append(ArangoDelta(
                    operation=ArangoOperation.DELETE,
                    collection=self._node_collection(cm),
                    key=doc_key,
                ))
            self._emit_edge_deletes(deltas, table_name, event.old_row or {})
        else:
            node_xform = self._build_node_transformer(table_name)
            if node_xform is None:
                return deltas
            row = event.new_row or {}
            doc = node_xform.transform_row(row)
            deltas.append(ArangoDelta(
                operation=arango_op,
                collection=self._node_collection(cm),
                document=doc,
                key=doc.get("_key"),
            ))
            self._emit_edge_upserts(deltas, table_name, row, arango_op)

        return deltas

    def _emit_edge_upserts(
        self,
        deltas: list[ArangoDelta],
        table_name: str,
        row: dict[str, Any],
        op: ArangoOperation,
    ) -> None:
        edge_defs = self._edges_by_source.get(table_name, [])
        for edge_def in edge_defs:
            edge_xform = self._build_edge_transformer(table_name, edge_def)
            if edge_xform is None:
                continue
            edge_doc = edge_xform.transform_row(row)
            if edge_doc is not None:
                edge_op = ArangoOperation.REPLACE if op == ArangoOperation.REPLACE else ArangoOperation.INSERT
                deltas.append(ArangoDelta(
                    operation=edge_op,
                    collection=self._edge_collection(edge_def),
                    is_edge=True,
                    document=edge_doc,
                    key=edge_doc.get("_key"),
                ))

    def _emit_edge_deletes(
        self,
        deltas: list[ArangoDelta],
        table_name: str,
        old_row: dict[str, Any],
    ) -> None:
        edge_defs = self._edges_by_source.get(table_name, [])
        for edge_def in edge_defs:
            edge_xform = self._build_edge_transformer(table_name, edge_def)
            if edge_xform is None:
                continue
            edge_doc = edge_xform.transform_row(old_row)
            if edge_doc is not None:
                deltas.append(ArangoDelta(
                    operation=ArangoOperation.DELETE,
                    collection=self._edge_collection(edge_def),
                    is_edge=True,
                    key=edge_doc.get("_key"),
                ))
