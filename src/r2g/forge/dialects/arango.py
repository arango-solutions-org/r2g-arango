"""``arango`` — ArangoDB collections: one document collection per entity, one
edge collection per relationship.

This is what r2g's *forward* pipeline would materialize from the relational
shape (``ConfigManager.generate_default_config``): vertex collections named
after the tables, edge collections named ``<from_table>_to_<to_table>`` with
``_from``/``_to`` on the spine ids — so ``arangodb-schema-analyzer`` (ASA)
reads the edges back as relationships between the two entities.

There is no SQL here, so the two text artifacts are reinterpreted (the
:class:`~r2g.forge.core.ForgeArtifacts` field names stay for backward
compatibility):

- ``ddl``      -> a JSON **collection manifest** (:func:`render_manifest`)
  describing collections, the edge projections, and a named graph;
- ``load_sql`` -> a standalone **Python loader script** (python-arango, env
  driven) that reads the manifest and ``forge.rows.json`` beside it and loads
  the database. :func:`project_documents` is the same projection as a library
  call, for callers that already hold the artifacts.

Documents are the canonical rows with ``_key = str(row["id"])``; edges carry
only ``_key``/``_from``/``_to`` so ASA reports no relationship properties. A row
whose foreign key is NULL (a self-link's root, or a scale ``nullRate``,
PLAN F-9/F-10) has no edge: there is nothing for ``_to`` to name.

At volume (PLAN F-11) the bulk path writes each collection as chunked JSON
lines and ``forge.bulk.py`` imports them a chunk at a time.
"""

from __future__ import annotations

import json
from typing import Any, ClassVar, Dict, Iterable, List, Tuple

from ..core import SURROGATE_KEY, EdgePlan, Rows, SchemaPlan, TablePlan
from .base import BulkProjector, BulkTarget, ForgeDialect

MANIFEST_VERSION = 1
GRAPH_NAME = "forge"

#: ArangoDB is schemaless; the "type" a document field takes is whatever JSON
#: value lands in it. Kept so the shared type-table contract holds.
ARANGO_TYPE_FOR_JSON_TYPE: Dict[str, str] = {
    "integer": "number",
    "float": "number",
    "boolean": "bool",
    "string": "string",
}


def edge_projection(edge: EdgePlan, from_key: str = SURROGATE_KEY) -> Dict[str, Any]:
    """How one edge collection is derived from the from-table's rows.

    ``from_key`` is the from-table's primary key, passed in rather than assumed:
    the edge's ``_key`` and ``_from`` are built from it, so hardcoding
    SURROGATE_KEY produced dangling edges (and a silent exit 0) for any plan
    whose key is named otherwise.
    """
    return {
        "name": edge.edge_collection,
        "relationship": edge.relationship,
        "from": {"collection": edge.from_table, "field": from_key},
        "to": {"collection": edge.to_table, "field": edge.fk_column},
    }


def render_manifest(plan: SchemaPlan) -> Dict[str, Any]:
    """The collection manifest as data (``ddl`` is its JSON serialization)."""
    return {
        "forgeManifestVersion": MANIFEST_VERSION,
        "dialect": ArangoDialect.name,
        # Kept for manifests written before per-collection keys; every reader
        # should prefer the collection's own ``keyField``.
        "keyField": SURROGATE_KEY,
        "collections": [
            {
                "name": t.table,
                "entity": t.entity,
                "type": "document",
                "keyField": t.primary_key.name,
            }
            for t in plan.tables
        ],
        "edgeCollections": [
            edge_projection(e, plan.table(e.from_table).primary_key.name) for e in plan.edges
        ],
        "graph": {
            "name": GRAPH_NAME,
            "edgeDefinitions": [
                {"collection": e.edge_collection, "from": [e.from_table], "to": [e.to_table]}
                for e in plan.edges
            ],
        },
    }


def project_documents(manifest: Dict[str, Any], rows: Rows) -> Dict[str, List[Dict[str, Any]]]:
    """Project canonical rows onto ArangoDB documents and edges per ``manifest``.

    Pure and deterministic; the generated loader script applies the identical
    rule, so the library and script paths cannot drift.
    """
    default_key = manifest.get("keyField", "id")
    out: Dict[str, List[Dict[str, Any]]] = {}
    for collection in manifest["collections"]:
        name = collection["name"]
        key_field = collection.get("keyField", default_key)
        out[name] = [{"_key": str(row[key_field]), **row} for row in rows[name]]
    for edge in manifest["edgeCollections"]:
        src, dst = edge["from"], edge["to"]
        out[edge["name"]] = [
            {
                "_key": str(row[src["field"]]),
                "_from": f"{src['collection']}/{row[src['field']]}",
                "_to": f"{dst['collection']}/{row[dst['field']]}",
            }
            for row in rows[src["collection"]]
            if row[dst["field"]] is not None
        ]
    return out


_LOADER_TEMPLATE = '''#!/usr/bin/env python3
"""Federation Forge — ArangoDB loader (dialect: arango; seed: {seed}).

Loads forge.collections.json + forge.rows.json (from this script's directory,
or --dir) into an ArangoDB database. Connection comes from the environment:
ARANGO_ENDPOINT (default http://localhost:8529), ARANGO_DB (default _system),
ARANGO_USER (default root), ARANGO_PASSWORD (default empty). Idempotent:
collections are created if missing and documents replaced on duplicate key.
Requires python-arango.
"""

from __future__ import annotations

import argparse
import json
import os
import sys


def project_documents(manifest, rows):
    default_key = manifest.get("keyField", "id")
    out = {{}}
    for collection in manifest["collections"]:
        name = collection["name"]
        key_field = collection.get("keyField", default_key)
        out[name] = [{{"_key": str(row[key_field]), **row}} for row in rows[name]]
    for edge in manifest["edgeCollections"]:
        src, dst = edge["from"], edge["to"]
        out[edge["name"]] = [
            {{
                "_key": str(row[src["field"]]),
                "_from": f"{{src['collection']}}/{{row[src['field']]}}",
                "_to": f"{{dst['collection']}}/{{row[dst['field']]}}",
            }}
            for row in rows[src["collection"]]
            if row[dst["field"]] is not None
        ]
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir", default=os.path.dirname(os.path.abspath(__file__)))
    args = parser.parse_args()

    with open(os.path.join(args.dir, "forge.collections.json"), encoding="utf-8") as fh:
        manifest = json.load(fh)
    with open(os.path.join(args.dir, "forge.rows.json"), encoding="utf-8") as fh:
        rows = json.load(fh)

    from arango import ArangoClient

    client = ArangoClient(hosts=os.environ.get("ARANGO_ENDPOINT", "http://localhost:8529"))
    db = client.db(
        os.environ.get("ARANGO_DB", "_system"),
        username=os.environ.get("ARANGO_USER", "root"),
        password=os.environ.get("ARANGO_PASSWORD", ""),
    )

    for collection in manifest["collections"]:
        if not db.has_collection(collection["name"]):
            db.create_collection(collection["name"])
    for edge in manifest["edgeCollections"]:
        if not db.has_collection(edge["name"]):
            db.create_collection(edge["name"], edge=True)
    graph = manifest.get("graph")
    if graph and graph["edgeDefinitions"]:
        # Recreate rather than skip. The graph name is a fixed constant and
        # ARANGO_DB defaults to _system, so loading a second ontology into the
        # same database previously imported the new collections and left the
        # PREVIOUS ontology's edge definitions attached — exiting 0, with the
        # analyzer then reading relationships that no longer exist.
        # drop_collections=False: the documents were just imported above.
        if db.has_graph(graph["name"]):
            db.delete_graph(graph["name"], drop_collections=False)
        db.create_graph(
            graph["name"],
            edge_definitions=[
                {{
                    "edge_collection": d["collection"],
                    "from_vertex_collections": d["from"],
                    "to_vertex_collections": d["to"],
                }}
                for d in graph["edgeDefinitions"]
            ],
        )

    total = 0
    for name, documents in project_documents(manifest, rows).items():
        result = db.collection(name).import_bulk(documents, on_duplicate="replace")
        errors = result.get("errors", 0)
        if errors:
            print(f"{{name}}: {{errors}} import errors", file=sys.stderr)
            return 1
        total += len(documents)
        print(f"{{name}}: {{len(documents)}} documents")
    print(f"loaded {{total}} documents into {{db.name}}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
'''


_BULK_LOADER_TEMPLATE = '''#!/usr/bin/env python3
"""Federation Forge — ArangoDB bulk loader (dialect: arango; seed: {seed}).

Loads the chunked JSON-lines files that forge.bulk.json lists (from this
script's directory, or --dir) into an ArangoDB database, after creating the
collections and graph forge.collections.json declares. Reads one file at a
time and imports it in batches of --batch documents, so memory stays bounded
by the batch, not the data. Connection comes from the environment:
ARANGO_ENDPOINT (default http://localhost:8529), ARANGO_DB (default _system),
ARANGO_USER (default root), ARANGO_PASSWORD (default empty). Idempotent:
collections are created if missing and documents replaced on duplicate key.
Requires python-arango.
"""

from __future__ import annotations

import argparse
import json
import os
import sys


def batches(path, size):
    batch = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            batch.append(json.loads(line))
            if len(batch) >= size:
                yield batch
                batch = []
    if batch:
        yield batch


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir", default=os.path.dirname(os.path.abspath(__file__)))
    parser.add_argument("--batch", type=int, default=10000)
    args = parser.parse_args()
    if args.batch < 1:
        print("--batch must be >= 1", file=sys.stderr)
        return 2

    with open(os.path.join(args.dir, "forge.collections.json"), encoding="utf-8") as fh:
        manifest = json.load(fh)
    with open(os.path.join(args.dir, "forge.bulk.json"), encoding="utf-8") as fh:
        bulk = json.load(fh)

    from arango import ArangoClient

    client = ArangoClient(hosts=os.environ.get("ARANGO_ENDPOINT", "http://localhost:8529"))
    db = client.db(
        os.environ.get("ARANGO_DB", "_system"),
        username=os.environ.get("ARANGO_USER", "root"),
        password=os.environ.get("ARANGO_PASSWORD", ""),
    )

    for collection in manifest["collections"]:
        if not db.has_collection(collection["name"]):
            db.create_collection(collection["name"])
    for edge in manifest["edgeCollections"]:
        if not db.has_collection(edge["name"]):
            db.create_collection(edge["name"], edge=True)
    graph = manifest.get("graph")
    if graph and graph["edgeDefinitions"]:
        # Recreated rather than skipped, as forge.load.py does: a fixed graph
        # name must not keep a previous ontology's edge definitions.
        if db.has_graph(graph["name"]):
            db.delete_graph(graph["name"], drop_collections=False)
        db.create_graph(
            graph["name"],
            edge_definitions=[
                {{
                    "edge_collection": d["collection"],
                    "from_vertex_collections": d["from"],
                    "to_vertex_collections": d["to"],
                }}
                for d in graph["edgeDefinitions"]
            ],
        )

    total = 0
    for target in bulk["targets"]:
        loaded = 0
        for chunk in target["files"]:
            path = os.path.join(args.dir, chunk["path"])
            for batch in batches(path, args.batch):
                result = db.collection(target["physical"]).import_bulk(
                    batch, on_duplicate="replace"
                )
                errors = result.get("errors", 0)
                if errors:
                    print(f"{{target['physical']}}: {{errors}} import errors in {{path}}", file=sys.stderr)
                    return 1
                loaded += len(batch)
        if loaded != target["rows"]:
            print(
                f"{{target['physical']}}: loaded {{loaded}} documents, the manifest lists "
                f"{{target['rows']}}",
                file=sys.stderr,
            )
            return 1
        total += loaded
        print(f"{{target['physical']}}: {{loaded}} documents")
    print(f"loaded {{total}} documents into {{db.name}}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
'''


def _json_line(document: Dict[str, Any]) -> str:
    return json.dumps(document, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


class ArangoDialect(ForgeDialect):
    name: ClassVar[str] = "arango"
    ddl_filename: ClassVar[str] = "forge.collections.json"
    loader_filename: ClassVar[str] = "forge.load.py"
    bulk_loader_filename: ClassVar[str] = "forge.bulk.py"
    bulk_extension: ClassVar[str] = "jsonl"
    type_for_json: ClassVar[Dict[str, str]] = ARANGO_TYPE_FOR_JSON_TYPE

    def render_ddl(self, plan: SchemaPlan) -> str:
        return json.dumps(render_manifest(plan), indent=2, sort_keys=True) + "\n"

    def render_loader(self, plan: SchemaPlan, rows: Rows, seed: int) -> str:
        return _LOADER_TEMPLATE.format(seed=seed)

    def bulk_targets(self, plan: SchemaPlan) -> List[BulkTarget]:
        documents = [BulkTarget(t.table, t.table, "document") for t in plan.tables]
        edges = [BulkTarget(e.edge_collection, e.edge_collection, "edge") for e in plan.edges]
        return documents + edges

    def bulk_projector(self, plan: SchemaPlan, table: TablePlan) -> BulkProjector:
        """A document per row, plus one edge per non-NULL foreign key — the
        rule :func:`project_documents` applies, one row at a time."""
        name = table.table
        key = table.primary_key.name
        edges = [
            (e.edge_collection, e.fk_column, e.to_table) for e in plan.edges if e.from_table == name
        ]

        def project(row: Dict[str, Any]) -> Iterable[Tuple[str, str]]:
            out = [(name, _json_line({"_key": str(row[key]), **row}))]
            for collection, fk, to_table in edges:
                target = row[fk]
                if target is None:
                    continue
                out.append(
                    (
                        collection,
                        _json_line(
                            {
                                "_key": str(row[key]),
                                "_from": f"{name}/{row[key]}",
                                "_to": f"{to_table}/{target}",
                            }
                        ),
                    )
                )
            return out

        return project

    def render_bulk_loader(self, plan: SchemaPlan, manifest: Dict[str, Any]) -> str:
        return _BULK_LOADER_TEMPLATE.format(seed=manifest.get("seed"))


__all__ = [
    "ARANGO_TYPE_FOR_JSON_TYPE",
    "GRAPH_NAME",
    "MANIFEST_VERSION",
    "ArangoDialect",
    "edge_projection",
    "project_documents",
    "render_manifest",
]
