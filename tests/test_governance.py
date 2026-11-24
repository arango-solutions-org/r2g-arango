"""Unit tests for the entitlement report, gate, masking, and lineage (Phase 9b)."""
from __future__ import annotations

import json

import pytest

from r2g.governance import (
    apply_sensitivity_gate,
    build_entitlement_report,
    classification_manifest,
    key_exposures,
    lineage_manifest,
    policy_rego,
    suggested_rbac,
    tier_layout_recommendation,
    write_governance_artifacts,
    write_lineage_manifest,
)
from r2g.masking import (
    MASK_KINDS,
    is_masking_expression,
    make_mask_expression,
    mask_kind_of,
)
from r2g.types import (
    Classification,
    CollectionMapping,
    Column,
    EdgeDefinition,
    FieldExpression,
    MappingConfig,
    Schema,
    Table,
)


def _col(name, clf=None, pk=False):
    return Column(name=name, data_type="text", is_primary_key=pk, classification=clf)


def _pii():
    return Classification(tags=["PII.Sensitive"])


def _schema():
    return Schema(tables={
        "customer": Table(
            name="customer",
            columns=[
                _col("id", pk=True),
                _col("email", _pii()),                       # restricted
                _col("first_name"),                          # public
                _col("loyalty_tier", Classification(tier="Tier.Tier1")),  # confidential
            ],
            primary_key=["id"],
        ),
        "order": Table(
            name="order",
            columns=[_col("id", pk=True), _col("customer_id"), _col("total")],
            primary_key=["id"],
        ),
    })


def _config():
    return MappingConfig(
        collections={
            "customer": CollectionMapping(source_table="customer", target_collection="Customer"),
            "order": CollectionMapping(source_table="order", target_collection="Order"),
        },
        edges=[
            EdgeDefinition(
                edge_collection="placed",
                from_collection="order",
                to_collection="customer",
                from_fields=["customer_id"],
                to_fields=["id"],
            )
        ],
    )


class TestMaskingHelpers:
    def test_all_kinds_build_tagged_expressions(self):
        for kind in MASK_KINDS:
            fx = make_mask_expression("email", kind)
            assert fx.target == "email"
            assert is_masking_expression(fx)
            assert mask_kind_of(fx) == kind

    def test_hash_and_tokenize_reference_column(self):
        assert "@email" in make_mask_expression("email", "hash").expression
        assert make_mask_expression("email", "redact").expression == '"***"'
        assert make_mask_expression("email", "nullify").expression == "null"

    def test_unknown_kind_rejected(self):
        try:
            make_mask_expression("x", "scramble")
        except ValueError as e:
            assert "scramble" in str(e)
        else:
            raise AssertionError("expected ValueError")

    def test_non_masking_expression_detected(self):
        fx = FieldExpression(target="x", expression="@a + @b", description="sum")
        assert not is_masking_expression(fx)
        assert mask_kind_of(fx) is None


class TestEntitlementReport:
    def test_levels_and_lineage(self):
        report = build_entitlement_report(_config(), _schema(), threshold="confidential")
        by_target = {f"{f.target_collection}.{f.target_property}": f for f in report.fields}
        assert by_target["Customer.email"].level == "restricted"
        assert by_target["Customer.email"].source_columns == ["email"]
        assert by_target["Customer.loyalty_tier"].level == "confidential"
        assert by_target["Customer.first_name"].level == "public"
        # collection + edge rollup come from the mosaic recompute
        assert report.collection_levels["customer"] == "restricted"
        assert report.edge_levels["placed"] == "restricted"

    def test_above_threshold_filters_by_level(self):
        report = build_entitlement_report(_config(), _schema(), threshold="confidential")
        flagged = {f.target_property for f in report.above_threshold}
        assert flagged == {"email", "loyalty_tier"}
        # raising the threshold to restricted drops loyalty_tier (confidential)
        report2 = build_entitlement_report(_config(), _schema(), threshold="restricted")
        assert {f.target_property for f in report2.above_threshold} == {"email"}

    def test_masked_field_excluded_from_above_threshold(self):
        config = _config()
        config.collections["customer"].field_expressions = [
            make_mask_expression("email", "hash")
        ]
        report = build_entitlement_report(config, _schema(), threshold="confidential")
        email = next(f for f in report.fields if f.target_property == "email")
        assert email.masked and email.mask_kind == "hash"
        # masked => not in the gate's above-threshold set, even though restricted
        assert "email" not in {f.target_property for f in report.above_threshold}


class TestSensitivityGate:
    def test_excludes_above_threshold_columns_by_default(self):
        report = build_entitlement_report(_config(), _schema(), threshold="confidential")
        gated, excluded = apply_sensitivity_gate(_config(), report, allow_sensitive=False)
        assert set(gated.collections["customer"].exclude_fields) == {"email", "loyalty_tier"}
        assert {f.target_property for f in excluded} == {"email", "loyalty_tier"}
        assert all(f.excluded for f in excluded)

    def test_allow_sensitive_is_a_noop(self):
        report = build_entitlement_report(_config(), _schema(), threshold="confidential")
        gated, excluded = apply_sensitivity_gate(_config(), report, allow_sensitive=True)
        assert gated.collections["customer"].exclude_fields == []
        assert excluded == []

    def test_gate_does_not_mutate_input_config(self):
        config = _config()
        report = build_entitlement_report(config, _schema(), threshold="confidential")
        apply_sensitivity_gate(config, report, allow_sensitive=False)
        assert config.collections["customer"].exclude_fields == []

    def test_masked_field_passes_gate(self):
        config = _config()
        config.collections["customer"].field_expressions = [
            make_mask_expression("email", "hash")
        ]
        report = build_entitlement_report(config, _schema(), threshold="confidential")
        gated, excluded = apply_sensitivity_gate(config, report, allow_sensitive=False)
        # email is masked -> not excluded; loyalty_tier still excluded
        assert "email" not in gated.collections["customer"].exclude_fields
        assert "loyalty_tier" in gated.collections["customer"].exclude_fields


class TestLineageManifest:
    def test_manifest_records_handling(self):
        config = _config()
        config.collections["customer"].field_expressions = [
            make_mask_expression("email", "nullify")
        ]
        report = build_entitlement_report(config, _schema(), threshold="confidential", project="p")
        gated, excluded = apply_sensitivity_gate(config, report, allow_sensitive=False)
        # reflect exclusion back onto the report fields for the manifest
        excl_targets = {f.target_property for f in excluded}
        for f in report.fields:
            if f.target_property in excl_targets:
                f.excluded = True
        manifest = lineage_manifest(report)
        handling = {
            e["target"]: e["handling"] for e in manifest["fields"]
        }
        assert handling["Customer.email"].startswith("masked")
        assert handling["Customer.loyalty_tier"] == "excluded"
        assert handling["Customer.first_name"] == "loaded"
        assert manifest["project"] == "p"
        assert manifest["summary"]["total_fields"] == len(report.fields)

    def test_write_manifest_to_disk(self, tmp_path):
        report = build_entitlement_report(_config(), _schema(), threshold="confidential")
        path = write_lineage_manifest(report, tmp_path)
        assert path.exists() and path.name == "lineage.json"
        data = json.loads(path.read_text())
        assert "fields" in data and data["threshold"] == "confidential"


# ── Phase 9c: enforcement artifacts ───────────────────────────────────────


class TestClassificationManifest:
    def test_groups_fields_under_collections_with_owners(self):
        report = build_entitlement_report(
            _config(), _schema(), threshold="confidential", project="p"
        )
        manifest = classification_manifest(
            report, owners=["data-team@x.io"], synced_at="2026-06-30T00:00:00+00:00"
        )
        assert manifest["kind"] == "r2g.classification-manifest/v1"
        assert manifest["owners"] == ["data-team@x.io"]
        assert manifest["classifications_synced_at"] == "2026-06-30T00:00:00+00:00"
        cust = manifest["collections"]["Customer"]
        assert cust["level"] == "restricted"
        props = {f["property"]: f for f in cust["fields"]}
        assert props["email"]["level"] == "restricted"
        assert props["email"]["sources"] == ["customer.email"]
        # superset of lineage
        assert "lineage" in manifest and manifest["summary"]["total_fields"]


class TestSuggestedRbac:
    def test_cumulative_roles_by_clearance(self):
        report = build_entitlement_report(_config(), _schema(), threshold="confidential")
        rbac = suggested_rbac(report, database="graph_db")
        roles = {r["role"]: r for r in rbac["roles"]}
        # restricted clearance reads everything; confidential clearance must NOT
        # include the restricted Customer collection. Keyed by target names.
        assert "r2g_clearance_restricted" in roles
        restricted = set(roles["r2g_clearance_restricted"]["collections"])
        assert {"Customer", "Order", "placed"} <= restricted
        if "r2g_clearance_confidential" in roles:
            assert "Customer" not in roles["r2g_clearance_confidential"]["collections"]
        assert rbac["database"] == "graph_db"

    def test_redundant_clearances_deduped(self):
        # A graph with only public collections collapses to a single role.
        schema = Schema(tables={
            "t": Table(name="t", columns=[_col("id", pk=True), _col("name")], primary_key=["id"])
        })
        config = MappingConfig(collections={
            "t": CollectionMapping(source_table="t", target_collection="T")
        })
        report = build_entitlement_report(config, schema, threshold="confidential")
        rbac = suggested_rbac(report)
        assert len(rbac["roles"]) == 1


class TestPolicyRego:
    def test_rego_is_default_deny_and_lists_levels(self):
        report = build_entitlement_report(_config(), _schema(), threshold="confidential")
        rego = policy_rego(report)
        assert "package r2g.authz" in rego
        assert "default allow := false" in rego
        assert '"Customer": "restricted"' in rego
        assert "sensitivity_rank" in rego


class TestTierLayout:
    def test_groups_collections_by_tier(self):
        report = build_entitlement_report(_config(), _schema(), threshold="confidential")
        layout = tier_layout_recommendation(report, database="graph_db")
        assert layout["strategy"] == "separate-database"
        assert "Customer" in layout["tiers"]["restricted"]["collections"]
        assert layout["tiers"]["restricted"]["suggested_database"] == "graph_db_restricted"


class TestWriteGovernanceArtifacts:
    def test_writes_full_artifact_set(self, tmp_path):
        report = build_entitlement_report(
            _config(), _schema(), threshold="confidential", project="p"
        )
        written = write_governance_artifacts(
            report, tmp_path, owners=["o@x.io"], database="db", tier_layout=True
        )
        names = set(written)
        assert names == {
            "lineage.json",
            "classification-manifest.json",
            "suggested-rbac.json",
            "policy.rego",
            "tier-layout.json",
        }
        for p in written.values():
            assert p.exists()
        gov = tmp_path / "governance"
        manifest = json.loads((gov / "classification-manifest.json").read_text())
        assert manifest["project"] == "p"

    def test_rego_and_tier_layout_optional(self, tmp_path):
        report = build_entitlement_report(_config(), _schema(), threshold="confidential")
        written = write_governance_artifacts(
            report, tmp_path, tier_layout=False, emit_rego=False
        )
        assert "policy.rego" not in written
        assert "tier-layout.json" not in written
        assert "classification-manifest.json" in written


class TestCdcGovernanceGate:
    """Phase 9c: the CDC/temporal gate helper carries classification on changes."""

    def test_no_govern_returns_input_unchanged(self):
        from r2g.main import _govern_cdc_mapping

        config = _config()
        out = _govern_cdc_mapping(
            _schema(), config, govern=False, allow_sensitive=False, threshold="confidential"
        )
        assert out is config

    def test_govern_excludes_above_threshold(self):
        from r2g.main import _govern_cdc_mapping

        config = _config()
        out = _govern_cdc_mapping(
            _schema(), config, govern=True, allow_sensitive=False, threshold="confidential"
        )
        # email (restricted) + loyalty_tier (confidential) excluded on the copy
        assert set(out.collections["customer"].exclude_fields) == {"email", "loyalty_tier"}
        # input not mutated
        assert config.collections["customer"].exclude_fields == []

    def test_allow_sensitive_keeps_fields(self):
        from r2g.main import _govern_cdc_mapping

        out = _govern_cdc_mapping(
            _schema(), _config(), govern=True, allow_sensitive=True, threshold="confidential"
        )
        assert out.collections["customer"].exclude_fields == []

    def test_invalid_threshold_exits(self):
        import typer

        from r2g.main import _govern_cdc_mapping

        try:
            _govern_cdc_mapping(
                _schema(), _config(), govern=True, allow_sensitive=False, threshold="secret"
            )
        except typer.Exit as e:
            assert e.exit_code == 1
        else:
            raise AssertionError("expected typer.Exit")


class TestTheGateCoversFieldExpressions:
    """An expression reads its columns from the row whether they are excluded or not,
    and declares them only if its author chose to. The gate rates a field by every
    column its expression references, drops a field it rates sensitive along with its
    columns, and takes a field as masked only when its expression is a mask."""

    ROW = {"id": "1", "email": "a@b.c", "first_name": "Ann", "loyalty_tier": "gold"}

    def _loaded(self, fx):
        from r2g.transformers.node_transformer import NodeTransformer

        cfg = MappingConfig(collections={"customer": CollectionMapping(
            source_table="customer", target_collection="Customer", field_expressions=[fx])})
        gated, _ = apply_sensitivity_gate(cfg, build_entitlement_report(cfg, _schema()))
        return NodeTransformer(_schema().tables["customer"],
                               gated.collections["customer"]).transform_row(self.ROW)

    @pytest.mark.parametrize("fx", [
        FieldExpression(target="contact", sources=["email"], expression="@email"),
        FieldExpression(target="contact", sources=["first_name"], expression="@email"),
        FieldExpression(target="contact", sources=["first_name", "email"],
                        expression="CONCAT(@first_name, @email)"),
        FieldExpression(target="email", sources=["email"], expression="",
                        description="r2g:mask:hash"),
    ])
    def test_a_field_reading_a_sensitive_column_is_not_loaded(self, fx):
        assert "a@b.c" not in str(self._loaded(fx))

    def test_a_second_expression_on_a_masked_field_unmasks_it(self):
        """The transformer writes the last expression for a property, so a mask is only
        a mask if every expression for that property is one."""
        from r2g.transformers.node_transformer import NodeTransformer

        cfg = MappingConfig(collections={"customer": CollectionMapping(
            source_table="customer", target_collection="Customer", field_expressions=[
                make_mask_expression("email", "redact"),
                FieldExpression(target="email", sources=["email"], expression="@email")])})
        gated, _ = apply_sensitivity_gate(cfg, build_entitlement_report(cfg, _schema()))
        doc = NodeTransformer(_schema().tables["customer"],
                              gated.collections["customer"]).transform_row(self.ROW)
        assert "a@b.c" not in str(doc)

    def test_a_mask_is_loaded_and_other_expressions_untouched(self):
        assert self._loaded(make_mask_expression("email", "redact"))["email"] == "***"
        upper = FieldExpression(target="shout", sources=["first_name"],
                                expression="UPPER(@first_name)")
        assert self._loaded(upper)["shout"] == "ANN"

    @pytest.mark.parametrize("fx", [
        FieldExpression(target="email", sources=["email"], expression="",
                        description="r2g:mask:hash"),
        FieldExpression(target="email", sources=["email"], expression="@email",
                        description="r2g:mask:redact"),
        FieldExpression(target="email", sources=["email"], expression="MD5(TO_STRING(@email))",
                        description="r2g:mask:hash", engine="python"),
    ])
    def test_a_mask_marker_alone_does_not_make_a_field_masked(self, fx):
        assert not is_masking_expression(fx)


class TestSensitiveKeys:
    """The gate removes properties, not keys: a sensitive primary key, any column of a
    table without one, or a sensitive foreign key an edge follows would be written into
    ``_key``/``_to``. The report names each such column."""

    def _schema(self, *, pk="ssn"):
        from r2g.types import ForeignKey

        person = Table(name="person", columns=[_col("ssn", _pii()), _col("pid"), _col("name")],
                       primary_key=[pk] if pk else [])
        claim = Table(name="claim", columns=[_col("id", pk=True), _col("person_ssn", _pii())],
                      primary_key=["id"], foreign_keys=[ForeignKey(
                          column="person_ssn", foreign_table="person", foreign_column="ssn")])
        link = Table(name="link", columns=[_col("a"), _col("b_ssn", _pii())], primary_key=[],
                     foreign_keys=[ForeignKey(column="a", foreign_table="person",
                                              foreign_column="pid"),
                                   ForeignKey(column="b_ssn", foreign_table="person",
                                              foreign_column="ssn")])
        return Schema(tables={"person": person, "claim": claim, "link": link})

    def _config(self, *, edge=True, join=True):
        cols = {"person": CollectionMapping(source_table="person", target_collection="Person"),
                "claim": CollectionMapping(source_table="claim", target_collection="Claim")}
        if join:
            cols["link"] = CollectionMapping(source_table="link", target_collection="linked",
                                             collection_type="edge", is_join_table=True)
        edges = [EdgeDefinition(edge_collection="filed_by", from_collection="claim",
                                to_collection="person", from_fields=["person_ssn"],
                                to_fields=["ssn"])] if edge else []
        return MappingConfig(collections=cols, edges=edges)

    def test_each_kind_of_key_is_named(self):
        """A join table mapped as an edge collection writes nothing itself, so its
        foreign keys are not named; the edges in ``config.edges`` are what is written."""
        found = {(e.collection, e.column) for e in key_exposures(self._config(), self._schema())}
        assert found == {("Person", "ssn"), ("filed_by", "person_ssn")}

    def test_an_empty_include_list_means_every_table(self):
        """As the pipeline reads it: an empty list is no filter, not "load nothing"."""
        assert key_exposures(self._config(), self._schema(), include=[]) == \
            key_exposures(self._config(), self._schema())

    def test_an_edge_carries_its_starting_tables_key_mapped_or_not(self):
        """``_from`` and the edge's ``_key`` hold the starting row's key, whether or not
        that table is mapped as documents, or mapped as an edge collection."""
        from r2g.types import ForeignKey

        schema = Schema(tables={
            "person": Table(name="person", columns=[_col("pid", pk=True)],
                            primary_key=["pid"]),
            "claim": Table(name="claim", columns=[_col("ssn", _pii()), _col("person_pid")],
                           primary_key=["ssn"], foreign_keys=[ForeignKey(
                               column="person_pid", foreign_table="person",
                               foreign_column="pid")]),
        })
        edge = EdgeDefinition(edge_collection="filed_by", from_collection="claim",
                              to_collection="person", from_fields=["person_pid"],
                              to_fields=["pid"])
        for extra in ({}, {"claim": CollectionMapping(source_table="claim",
                                                      target_collection="claims",
                                                      collection_type="edge")}):
            cfg = MappingConfig(collections={"person": CollectionMapping(
                source_table="person", target_collection="Person"), **extra}, edges=[edge])
            assert {(e.collection, e.column) for e in key_exposures(cfg, schema)} == {
                ("filed_by", "ssn")}

    def test_a_foreign_key_is_rated_by_the_column_it_points_at(self):
        """``_to`` carries the referenced row's key: an unclassified column holding a
        sensitive key is as sensitive as that key."""
        from r2g.types import ForeignKey

        schema = Schema(tables={
            "person": Table(name="person", columns=[_col("ssn", _pii()), _col("pid")],
                            primary_key=["pid"]),
            "visit": Table(name="visit", columns=[_col("vid", pk=True), _col("person_ref")],
                           primary_key=["vid"], foreign_keys=[ForeignKey(
                               column="person_ref", foreign_table="person",
                               foreign_column="ssn")]),
        })
        cfg = MappingConfig(collections={
            "visit": CollectionMapping(source_table="visit", target_collection="Visit")},
            edges=[EdgeDefinition(edge_collection="of", from_collection="visit",
                                  to_collection="person", from_fields=["person_ref"],
                                  to_fields=["ssn"])])
        assert {(e.collection, e.column) for e in key_exposures(cfg, schema)} == {
            ("of", "person_ref")}

    def test_a_table_without_a_primary_key_is_keyed_by_every_column(self):
        found = key_exposures(self._config(edge=False, join=False), self._schema(pk=None))
        assert {(e.collection, e.column) for e in found} == {("Person", "ssn")}

    def test_a_key_that_is_not_sensitive_and_tables_left_out_are_clean(self):
        assert key_exposures(self._config(edge=False, join=False), self._schema(pk="pid")) == []
        assert key_exposures(self._config(), self._schema(), exclude=["person", "claim",
                                                                       "link"]) == []

    def test_the_report_carries_them(self):
        report = build_entitlement_report(self._config(), self._schema())
        assert {e.column for e in report.key_exposures} == {"ssn", "person_ssn"}
