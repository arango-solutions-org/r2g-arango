from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from r2g.catalog import CatalogManager
from r2g.ontology_import import (
    OntologyImportError,
    UploadedOntology,
    compile_ontology_mapping,
    import_ontology_bundle,
    load_normalized_ontology,
)
from r2g.types import Column, Schema, Table


def _bundle() -> dict:
    return {
        "conceptualSchema": {
            "entities": [
                {
                    "name": "Account",
                    "term": "https://example.test/Account",
                    "properties": [
                        {
                            "name": "accountId",
                            "term": "https://example.test/accountId",
                        },
                        {
                            "name": "displayName",
                            "term": "https://example.test/displayName",
                        },
                    ],
                },
                {
                    "name": "Contact",
                    "term": "https://example.test/Contact",
                    "properties": [
                        {
                            "name": "contactId",
                            "term": "https://example.test/contactId",
                        },
                        {
                            "name": "accountId",
                            "term": "https://example.test/contactAccountId",
                        },
                    ],
                },
            ],
            "relationships": [
                {
                    "type": "belongsTo",
                    "term": "https://example.test/belongsTo",
                    "fromEntity": "Contact",
                    "toEntity": "Account",
                }
            ],
        },
        "physicalMapping": {
            "entities": {
                "Account": {
                    "style": "TABLE",
                    "tableName": "ACCOUNTS",
                    "primaryKey": ["ACCOUNT_ID"],
                    "properties": {
                        "accountId": {"columnName": "ACCOUNT_ID"},
                        "displayName": {"columnName": "DISPLAY_NAME"},
                    },
                },
                "Contact": {
                    "style": "VIEW",
                    "tableName": "CONTACTS_VIEW",
                    "primaryKey": ["CONTACT_ID"],
                    "properties": {
                        "contactId": {"columnName": "CONTACT_ID"},
                        "accountId": {"columnName": "ACCOUNT_ID"},
                    },
                },
            },
            "relationships": {
                "belongsTo": {
                    "style": "FOREIGN_KEY",
                    "fromTable": "CONTACTS_VIEW",
                    "fromColumns": ["ACCOUNT_ID"],
                    "toTable": "ACCOUNTS",
                    "toColumns": ["ACCOUNT_ID"],
                }
            },
        },
        "metadata": {"analyzerVersion": "test"},
    }


def _schema() -> Schema:
    return Schema(
        tables={
            "ACCOUNTS": Table(
                name="ACCOUNTS",
                columns=[
                    Column(
                        name="ACCOUNT_ID",
                        data_type="VARCHAR",
                        is_primary_key=True,
                    ),
                    Column(name="DISPLAY_NAME", data_type="VARCHAR"),
                ],
                primary_key=["ACCOUNT_ID"],
            ),
            "CONTACTS_VIEW": Table(
                name="CONTACTS_VIEW",
                columns=[
                    Column(name="CONTACT_ID", data_type="VARCHAR"),
                    Column(name="ACCOUNT_ID", data_type="VARCHAR"),
                ],
                unique_constraints=[["CONTACT_ID"]],
                is_view=True,
            ),
        }
    )


def test_compile_mapping_preserves_terms_renames_views_and_relationships():
    mapping, report = compile_ontology_mapping(
        _bundle(),
        _schema(),
        ontology_id="customer",
        snapshot_id="snapshot",
    )

    assert report.executable
    assert report.matched_entities == 2
    assert report.matched_properties == 4
    assert report.matched_relationships == 1
    assert mapping.collections["ACCOUNTS"].target_collection == "Account"
    assert mapping.collections["ACCOUNTS"].field_mappings == {
        "ACCOUNT_ID": "accountId",
        "DISPLAY_NAME": "displayName",
    }
    assert mapping.collections["ACCOUNTS"].term == "https://example.test/Account"
    assert mapping.collections["ACCOUNTS"].field_terms["ACCOUNT_ID"] == (
        "https://example.test/accountId"
    )
    assert mapping.edges[0].term == "https://example.test/belongsTo"


def test_compile_mapping_reports_object_kind_mismatch():
    schema = _schema()
    schema.tables["CONTACTS_VIEW"].is_view = False

    _, report = compile_ontology_mapping(_bundle(), schema)

    assert not report.executable
    assert any(issue.code == "source-object-kind-mismatch" for issue in report.issues)


def test_compile_mapping_requires_approved_identity():
    bundle = _bundle()
    bundle["physicalMapping"]["entities"]["Contact"]["primaryKey"] = []
    schema = _schema()
    schema.tables["CONTACTS_VIEW"].unique_constraints = []

    _, report = compile_ontology_mapping(bundle, schema)

    assert not report.executable
    assert any(issue.code == "identity-unresolved" for issue in report.issues)


def test_compile_mapping_marks_alternate_unique_target_for_projection():
    bundle = _bundle()
    bundle["physicalMapping"]["entities"]["Account"]["primaryKey"] = ["ACCOUNT_ID"]
    bundle["physicalMapping"]["relationships"]["belongsTo"]["toColumns"] = [
        "DISPLAY_NAME"
    ]
    schema = _schema()
    schema.tables["ACCOUNTS"].unique_constraints = [["DISPLAY_NAME"]]

    _, report = compile_ontology_mapping(bundle, schema)

    assert report.executable
    assert report.projected_edges_required == 1
    assert any(issue.code == "edge-projection-required" for issue in report.issues)


def test_import_stores_only_safe_server_owned_paths(tmp_path, monkeypatch):
    bundle = _bundle()
    digest = "sha256:" + hashlib.sha256(b"same graph").hexdigest()

    def fake_import(root: Path, imports: list[Path]):
        assert root.is_relative_to(tmp_path / "ontologies")
        assert all(path.is_relative_to(tmp_path / "ontologies") for path in imports)
        return bundle, {
            "canonicalGraphDigest": digest,
            "rootOntologyIri": "https://example.test/specific",
            "ontologyVersion": "1",
            "profileVersion": "1",
        }

    monkeypatch.setattr("r2g.ontology_import._rsa_import", fake_import)
    manager = CatalogManager(catalog_dir=tmp_path)
    imported = import_ontology_bundle(
        manager,
        name="Customer",
        root=UploadedOntology("../../specific.ttl", b"@prefix : <x> ."),
        imports=[UploadedOntology("upper.ttl", b"@prefix : <x> .")],
    )

    record = imported.record
    assert record.canonical_graph_digest == digest
    assert all(".." not in artifact.filename for artifact in record.artifacts)
    assert load_normalized_ontology(manager, record.id) == bundle
    assert (tmp_path / "ontologies" / record.id / "normalized.json").is_file()


def test_import_rejects_non_equivalent_serializations(tmp_path, monkeypatch):
    calls = 0

    def fake_import(root: Path, imports: list[Path]):
        nonlocal calls
        calls += 1
        return _bundle(), {
            "canonicalGraphDigest": f"sha256:{calls:064x}",
            "rootOntologyIri": "https://example.test/specific",
        }

    monkeypatch.setattr("r2g.ontology_import._rsa_import", fake_import)
    manager = CatalogManager(catalog_dir=tmp_path)

    with pytest.raises(OntologyImportError, match="not equivalent"):
        import_ontology_bundle(
            manager,
            name="Customer",
            root=UploadedOntology("specific.ttl", b"ttl"),
            imports=[UploadedOntology("upper.ttl", b"ttl")],
            equivalent_root=UploadedOntology("specific.owl", b"<rdf:RDF/>"),
            equivalent_imports=[UploadedOntology("upper.owl", b"<rdf:RDF/>")],
        )

    assert manager.list_ontologies() == []
    assert not any((tmp_path / "ontologies").glob("[!.]*"))
