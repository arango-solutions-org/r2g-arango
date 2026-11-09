from __future__ import annotations

import pytest

from r2g.fk_inference import (
    CsvValueSampler,
    InferenceOptions,
    InferredForeignKey,
    MySQLValueSampler,
    PostgresValueSampler,
    SQLServerValueSampler,
    create_value_sampler,
    infer_foreign_keys,
)
from r2g.types import Column, EdgeDefinition, ForeignKey, Schema, Table

# ── Helpers ─────────────────────────────────────────────────────────


def _tbl(name: str, cols: list[tuple[str, str, bool, bool]], pk: list[str] | None = None,
         fks: list[ForeignKey] | None = None) -> Table:
    """`cols` entries: (name, data_type, is_nullable, is_primary_key)."""
    return Table(
        name=name,
        columns=[
            Column(name=n, data_type=t, is_nullable=nul, is_primary_key=pkf)
            for (n, t, nul, pkf) in cols
        ],
        primary_key=pk or [],
        foreign_keys=fks or [],
    )


def _schema(*tables: Table) -> Schema:
    return Schema(tables={t.name: t for t in tables})


# ── Name-based inference ────────────────────────────────────────────


class TestNameHeuristic:
    def test_single_column_user_id_points_to_users_id(self):
        s = _schema(
            _tbl("users", [("id", "integer", False, True), ("name", "text", True, False)], pk=["id"]),
            _tbl("orders", [("id", "integer", False, True), ("user_id", "integer", False, False)], pk=["id"]),
        )
        out = infer_foreign_keys(s)
        assert len(out) == 1
        c = out[0]
        assert c.table == "orders"
        assert c.columns == ["user_id"]
        assert c.foreign_table == "users"
        assert c.foreign_columns == ["id"]
        assert c.method == "name_suffix"
        assert c.confidence >= 0.75

    def test_plural_and_singular_table_names_both_match(self):
        s_plural = _schema(
            _tbl("users", [("id", "integer", False, True)], pk=["id"]),
            _tbl("orders", [("id", "integer", False, True), ("user_id", "integer", False, False)], pk=["id"]),
        )
        s_singular = _schema(
            _tbl("user", [("id", "integer", False, True)], pk=["id"]),
            _tbl("orders", [("id", "integer", False, True), ("user_id", "integer", False, False)], pk=["id"]),
        )
        out1 = infer_foreign_keys(s_plural)
        out2 = infer_foreign_keys(s_singular)
        assert any(c.foreign_table == "users" for c in out1)
        assert any(c.foreign_table == "user" for c in out2)

    def test_declared_fk_suppresses_suggestion_for_that_column(self):
        s = _schema(
            _tbl("users", [("id", "integer", False, True)], pk=["id"]),
            _tbl(
                "orders",
                [("id", "integer", False, True), ("user_id", "integer", False, False)],
                pk=["id"],
                fks=[ForeignKey(columns=["user_id"], foreign_table="users", foreign_columns=["id"])],
            ),
        )
        out = infer_foreign_keys(s)
        assert out == []

    def test_type_incompatibility_rejects_candidate(self):
        s = _schema(
            _tbl("users", [("id", "integer", False, True)], pk=["id"]),
            _tbl(
                "flags",
                [("id", "integer", False, True), ("user_id", "boolean", False, False)],
                pk=["id"],
            ),
        )
        out = infer_foreign_keys(s)
        assert out == []

    def test_integer_and_float_are_considered_compatible(self):
        # Snowflake NUMBER(38,0) ends up as "number" → float in the map,
        # but joining against integer PKs is a very real workflow.
        s = _schema(
            _tbl("users", [("id", "integer", False, True)], pk=["id"]),
            _tbl(
                "orders",
                [("id", "integer", False, True), ("user_id", "number", False, False)],
                pk=["id"],
            ),
        )
        out = infer_foreign_keys(s)
        assert any(c.columns == ["user_id"] for c in out)

    def test_bare_id_column_does_not_match_random_pks(self):
        # Direct pk_name_match should skip generic names like `id`.
        s = _schema(
            _tbl("users", [("id", "integer", False, True)], pk=["id"]),
            _tbl("sessions", [("id", "integer", False, True)], pk=["id"]),
        )
        out = infer_foreign_keys(s)
        assert out == []

    def test_non_generic_pk_name_triggers_direct_match(self):
        s = _schema(
            _tbl("products", [("sku", "text", False, True), ("name", "text", True, False)], pk=["sku"]),
            _tbl(
                "line_items",
                [("id", "integer", False, True), ("sku", "text", False, False)],
                pk=["id"],
            ),
        )
        out = infer_foreign_keys(s)
        match = [c for c in out if c.foreign_table == "products" and c.columns == ["sku"]]
        assert len(match) == 1
        assert match[0].method in ("pk_name_match", "name_suffix")

    def test_no_underscore_id_gets_lower_confidence(self):
        s = _schema(
            _tbl("users", [("id", "integer", False, True)], pk=["id"]),
            _tbl(
                "orders",
                [("id", "integer", False, True), ("userid", "integer", False, False)],
                pk=["id"],
            ),
        )
        out = infer_foreign_keys(s, options=InferenceOptions(min_confidence=0.3))
        # The "userid" → "users.id" suggestion should exist but be scored
        # below the clean "_id" form.
        assert any(c.columns == ["userid"] for c in out)
        conf = next(c for c in out if c.columns == ["userid"]).confidence
        assert conf < 0.6

    def test_min_confidence_filters_weak_candidates(self):
        s = _schema(
            _tbl("users", [("id", "integer", False, True)], pk=["id"]),
            _tbl(
                "orders",
                [("id", "integer", False, True), ("userid", "integer", False, False)],
                pk=["id"],
            ),
        )
        strict = infer_foreign_keys(s, options=InferenceOptions(min_confidence=0.9))
        assert strict == []

    def test_nullable_target_with_non_nullable_source_is_penalized(self):
        s = _schema(
            _tbl("users", [("id", "integer", True, True)], pk=["id"]),  # nullable PK (weird!)
            _tbl(
                "orders",
                [("id", "integer", False, True), ("user_id", "integer", False, False)],
                pk=["id"],
            ),
        )
        out = infer_foreign_keys(s)
        conf = next(c for c in out if c.columns == ["user_id"]).confidence
        assert conf < 0.8

    def test_results_are_sorted_by_confidence_desc(self):
        s = _schema(
            _tbl("users", [("id", "integer", False, True)], pk=["id"]),
            _tbl("customers", [("id", "integer", False, True)], pk=["id"]),
            _tbl(
                "orders",
                [
                    ("id", "integer", False, True),
                    ("user_id", "integer", False, False),
                    ("customerid", "integer", False, False),
                ],
                pk=["id"],
            ),
        )
        out = infer_foreign_keys(s, options=InferenceOptions(min_confidence=0.3))
        confs = [c.confidence for c in out]
        assert confs == sorted(confs, reverse=True)


# ── Composite inference ────────────────────────────────────────────


class TestCompositeInference:
    def test_composite_pk_gets_grouped_suggestion(self):
        s = _schema(
            _tbl(
                "order_products",
                [("order_id", "integer", False, True), ("product_id", "integer", False, True)],
                pk=["order_id", "product_id"],
            ),
            _tbl(
                "order_lines",
                [
                    ("id", "integer", False, True),
                    ("order_id", "integer", False, False),
                    ("product_id", "integer", False, False),
                ],
                pk=["id"],
            ),
        )
        out = infer_foreign_keys(s)
        composite = [c for c in out if c.method == "composite"]
        assert len(composite) == 1
        c = composite[0]
        assert c.table == "order_lines"
        assert c.foreign_table == "order_products"
        assert c.columns == ["order_id", "product_id"]
        assert c.foreign_columns == ["order_id", "product_id"]

    def test_composite_disabled_returns_only_singles(self):
        s = _schema(
            _tbl(
                "order_products",
                [("order_id", "integer", False, True), ("product_id", "integer", False, True)],
                pk=["order_id", "product_id"],
            ),
            _tbl(
                "order_lines",
                [
                    ("id", "integer", False, True),
                    ("order_id", "integer", False, False),
                    ("product_id", "integer", False, False),
                ],
                pk=["id"],
            ),
        )
        out = infer_foreign_keys(
            s,
            options=InferenceOptions(allow_composite=False, min_confidence=0.0),
        )
        assert all(c.method != "composite" for c in out)


# ── Sampler integration ────────────────────────────────────────────


class TestSamplerIntegration:
    def _schema(self) -> Schema:
        return _schema(
            _tbl("users", [("id", "integer", False, True)], pk=["id"]),
            _tbl(
                "orders",
                [("id", "integer", False, True), ("user_id", "integer", False, False)],
                pk=["id"],
            ),
        )

    def test_high_overlap_boosts_confidence(self):
        s = self._schema()

        def sampler(lt, lc, ft, fc):
            return 1.0

        out_no = infer_foreign_keys(s)
        out_yes = infer_foreign_keys(
            s,
            options=InferenceOptions(sample_overlap=True),
            sampler=sampler,
        )
        assert out_yes[0].confidence > out_no[0].confidence

    def test_zero_overlap_vetoes_when_enabled(self):
        s = self._schema()
        out = infer_foreign_keys(
            s,
            options=InferenceOptions(sample_overlap=True, overlap_veto_on_zero=True),
            sampler=lambda *args: 0.0,
        )
        assert out == []

    def test_zero_overlap_without_veto_still_lowers_score(self):
        s = self._schema()
        out = infer_foreign_keys(
            s,
            options=InferenceOptions(sample_overlap=True, overlap_veto_on_zero=False),
            sampler=lambda *args: 0.0,
        )
        assert len(out) == 1
        # 0.85 base minus 0.25 penalty, rounded = 0.6 — still above the default floor.
        assert out[0].confidence < 0.85

    def test_sampler_exception_keeps_candidate(self):
        s = self._schema()

        def angry(*args):
            raise RuntimeError("boom")

        out = infer_foreign_keys(
            s,
            options=InferenceOptions(sample_overlap=True),
            sampler=angry,
        )
        assert len(out) == 1

    def test_sampler_none_is_neutral(self):
        s = self._schema()
        out_no = infer_foreign_keys(s)
        out_neutral = infer_foreign_keys(
            s,
            options=InferenceOptions(sample_overlap=True),
            sampler=lambda *args: None,
        )
        assert out_neutral[0].confidence == out_no[0].confidence


# ── to_edge_definition ─────────────────────────────────────────────


class TestEdgeDefinitionConversion:
    def test_single_column_edge_round_trip(self):
        c = InferredForeignKey(
            table="orders",
            columns=["user_id"],
            foreign_table="users",
            foreign_columns=["id"],
            confidence=0.85,
            method="name_suffix",
        )
        ed = c.to_edge_definition()
        assert isinstance(ed, EdgeDefinition)
        assert ed.edge_collection == "orders_to_users"
        assert ed.from_collection == "orders"
        assert ed.to_collection == "users"
        assert ed.from_fields == ["user_id"]
        assert ed.to_fields == ["id"]

    def test_composite_edge_keeps_column_order(self):
        c = InferredForeignKey(
            table="order_lines",
            columns=["order_id", "product_id"],
            foreign_table="order_products",
            foreign_columns=["order_id", "product_id"],
            confidence=0.82,
            method="composite",
        )
        ed = c.to_edge_definition(edge_collection="lines_to_order_products")
        assert ed.edge_collection == "lines_to_order_products"
        assert ed.from_fields == ["order_id", "product_id"]
        assert ed.to_fields == ["order_id", "product_id"]


# ── PostgresValueSampler plumbing ──────────────────────────────────


class TestPostgresValueSampler:
    def test_query_failure_is_swallowed_and_returns_none(self, monkeypatch):
        sampler = PostgresValueSampler("postgresql://bogus@localhost/none")

        class _FakeCur:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

            def execute(self, *a, **kw):
                raise RuntimeError("boom")

            def fetchone(self):
                return None

        class _FakeConn:
            def cursor(self):
                return _FakeCur()

            def rollback(self):
                self.rolled_back = True

            def close(self):
                pass

        sampler._conn = _FakeConn()
        res = sampler("orders", "user_id", "users", "id")
        assert res is None
        assert sampler._conn.rolled_back is True

    def test_empty_result_returns_none(self):
        sampler = PostgresValueSampler("postgresql://bogus/none")

        class _FakeCur:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

            def execute(self, *a, **kw):
                pass

            def fetchone(self):
                return None

        class _FakeConn:
            def cursor(self):
                return _FakeCur()

            def close(self):
                pass

        sampler._conn = _FakeConn()
        assert sampler("a", "b", "c", "d") is None

    def test_successful_result_is_coerced_to_float(self):
        sampler = PostgresValueSampler("postgresql://bogus/none")

        class _FakeCur:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

            def execute(self, *a, **kw):
                pass

            def fetchone(self):
                return (0.75,)

        class _FakeConn:
            def cursor(self):
                return _FakeCur()

            def close(self):
                pass

        sampler._conn = _FakeConn()
        assert sampler("a", "b", "c", "d") == 0.75

    def test_limit_is_clamped_to_minimum(self):
        sampler = PostgresValueSampler("postgresql://x/y", limit=10)
        assert sampler.limit >= 100


# ── Parametric coverage ────────────────────────────────────────────


@pytest.mark.parametrize(
    "col_name,expected_prefix",
    [
        ("user_id", "user"),
        ("customer_id", "customer"),
        ("customerid", "customer"),
        ("uuid", None),  # generic UUID column should not auto-match
    ],
)
def test_prefix_extraction_produces_expected_tables(col_name, expected_prefix):
    parent = _tbl(
        expected_prefix or "unrelated",
        [("id", "integer", False, True)],
        pk=["id"],
    ) if expected_prefix else _tbl("foo", [("id", "integer", False, True)], pk=["id"])
    s = _schema(
        parent,
        _tbl(
            "child",
            [("id", "integer", False, True), (col_name, "integer", False, False)],
            pk=["id"],
        ),
    )
    out = infer_foreign_keys(s, options=InferenceOptions(min_confidence=0.3))
    if expected_prefix:
        assert any(c.foreign_table == expected_prefix for c in out)
    else:
        assert all(c.columns != [col_name] for c in out)


# ── CsvValueSampler ─────────────────────────────────────────────────


class TestCsvValueSampler:
    def _make_dir(self, tmp_path, files: dict[str, str]):
        for name, content in files.items():
            (tmp_path / name).write_text(content)
        return CsvValueSampler(str(tmp_path), limit=1000)

    def test_sample_values_skips_blank_cells_and_preserves_first_seen_order(self, tmp_path):
        sampler = self._make_dir(
            tmp_path,
            {"authors.csv": 'row,id\n1,003\n2,\n3,""\n4,001\n5,003\n6,002\n'},
        )

        assert sampler.sample_values("authors", "id") == ["003", "001", "002"]
        assert sampler.sample_values("authors", "id", limit=2) == ["003", "001"]

    def test_sample_values_leaves_out_the_null_marker(self, tmp_path):
        """The CSV source reads ``\\N`` as NULL (#73), so it is never a sample value."""
        sampler = self._make_dir(tmp_path, {"authors.csv": "row,id\n1,\\N\n2,001\n"})
        assert sampler.sample_values("authors", "id") == ["001"]
        kept = create_value_sampler("csv", str(tmp_path), source_params={"null_values": []})
        assert kept.sample_values("authors", "id") == ["\\N", "001"]

    def test_full_overlap_returns_one(self, tmp_path):
        sampler = self._make_dir(
            tmp_path,
            {
                "users.csv": "id,name\n1,a\n2,b\n3,c\n",
                "orders.csv": "id,user_id\n10,1\n11,2\n12,3\n",
            },
        )
        # Every distinct orders.user_id (1,2,3) exists in users.id.
        assert sampler("orders", "user_id", "users", "id") == 1.0

    def test_partial_overlap_is_fraction(self, tmp_path):
        sampler = self._make_dir(
            tmp_path,
            {
                "users.csv": "id\n1\n2\n",
                "orders.csv": "id,user_id\n10,1\n11,2\n12,99\n",
            },
        )
        # Distinct local values {1,2,99}; {1,2} present in foreign → 2/3.
        assert sampler("orders", "user_id", "users", "id") == pytest.approx(2 / 3)

    def test_zero_overlap_returns_zero(self, tmp_path):
        sampler = self._make_dir(
            tmp_path,
            {
                "users.csv": "id\n1\n2\n3\n",
                "orders.csv": "id,user_id\n10,7\n11,8\n",
            },
        )
        assert sampler("orders", "user_id", "users", "id") == 0.0

    def test_int_vs_float_tokens_compared_as_text(self, tmp_path):
        # Raw text comparison: "1" matches "1", "2.5" matches "2.5".
        sampler = self._make_dir(
            tmp_path,
            {
                "parent.csv": "key\n1\n2.5\n",
                "child.csv": "id,key\n10,1\n11,2.5\n",
            },
        )
        assert sampler("child", "key", "parent", "key") == 1.0

    def test_missing_file_returns_none(self, tmp_path):
        sampler = self._make_dir(tmp_path, {"users.csv": "id\n1\n"})
        assert sampler("orders", "user_id", "users", "id") is None
        assert sampler("users", "id", "ghost", "id") is None

    def test_empty_local_column_returns_none(self, tmp_path):
        sampler = self._make_dir(
            tmp_path,
            {
                "users.csv": "id\n1\n",
                "orders.csv": "id,user_id\n",  # header only, no rows
            },
        )
        assert sampler("orders", "user_id", "users", "id") is None

    def test_limit_is_clamped_to_minimum(self, tmp_path):
        sampler = CsvValueSampler(str(tmp_path), limit=10)
        assert sampler.limit >= 100

    def test_integration_vetoes_bad_candidate(self, tmp_path):
        (tmp_path / "users.csv").write_text("id\n1\n2\n3\n")
        (tmp_path / "orders.csv").write_text("id,user_id\n10,7\n11,8\n12,9\n")
        s = _schema(
            _tbl("users", [("id", "integer", False, True)], pk=["id"]),
            _tbl(
                "orders",
                [("id", "integer", False, True), ("user_id", "integer", False, False)],
                pk=["id"],
            ),
        )
        sampler = CsvValueSampler(str(tmp_path), limit=1000)
        out = infer_foreign_keys(
            s,
            options=InferenceOptions(sample_overlap=True, overlap_veto_on_zero=True),
            sampler=sampler,
        )
        # orders.user_id values never appear in users.id → vetoed.
        assert out == []

    def test_integration_boosts_good_candidate(self, tmp_path):
        (tmp_path / "users.csv").write_text("id\n1\n2\n3\n")
        (tmp_path / "orders.csv").write_text("id,user_id\n10,1\n11,2\n12,3\n")
        s = _schema(
            _tbl("users", [("id", "integer", False, True)], pk=["id"]),
            _tbl(
                "orders",
                [("id", "integer", False, True), ("user_id", "integer", False, False)],
                pk=["id"],
            ),
        )
        sampler = CsvValueSampler(str(tmp_path), limit=1000)
        out_no = infer_foreign_keys(s)
        out_yes = infer_foreign_keys(
            s,
            options=InferenceOptions(sample_overlap=True),
            sampler=sampler,
        )
        assert out_yes[0].confidence > out_no[0].confidence


# ── MySQLValueSampler + create_value_sampler dispatch ────────────────


class TestMySQLValueSampler:
    def test_database_taken_from_url(self):
        s = MySQLValueSampler("mysql://u:p@h/shop")
        assert s.schema_name == "shop"

    def test_explicit_schema_overrides_url_database(self):
        s = MySQLValueSampler("mysql://u:p@h/shop", schema_name="analytics")
        assert s.schema_name == "analytics"
        assert s._connect_params["database"] == "analytics"

    def test_public_sentinel_falls_back_to_url_database(self):
        # `infer-fks` forwards snap.pg_schema, which defaults to "public".
        s = MySQLValueSampler("mysql://u:p@h/shop", schema_name="public")
        assert s.schema_name == "shop"

    def test_connect_failure_returns_none(self):
        # Bogus host: the sampler swallows the driver error and yields None
        # so name-based scoring still wins. pymysql may be absent; either the
        # ImportError or a connection error is caught.
        s = MySQLValueSampler("mysql://nobody@127.0.0.1:1/none")
        assert s("orders", "user_id", "users", "id") is None
        s.close()


class TestSQLServerValueSampler:
    def test_schema_defaults_to_dbo(self):
        assert SQLServerValueSampler("mssql://u:p@h/shop").schema_name == "dbo"

    def test_public_sentinel_folds_to_dbo(self):
        assert SQLServerValueSampler("mssql://u:p@h/shop", schema_name="public").schema_name == "dbo"

    def test_explicit_schema_kept(self):
        assert SQLServerValueSampler("mssql://u:p@h/shop", schema_name="sales").schema_name == "sales"

    def test_connect_failure_returns_none(self):
        s = SQLServerValueSampler("mssql://nobody@127.0.0.1:1/none")
        assert s("orders", "user_id", "users", "id") is None
        s.close()


class TestCreateValueSamplerDispatch:
    def test_postgres(self):
        s = create_value_sampler("postgresql", "postgresql://u:p@h/db")
        assert isinstance(s, PostgresValueSampler)

    def test_mysql(self):
        s = create_value_sampler("mysql", "mysql://u:p@h/shop")
        assert isinstance(s, MySQLValueSampler)

    def test_mariadb_alias(self):
        s = create_value_sampler("mariadb", "mariadb://u:p@h/shop")
        assert isinstance(s, MySQLValueSampler)

    def test_sqlserver(self):
        s = create_value_sampler("sqlserver", "mssql://u:p@h/shop")
        assert isinstance(s, SQLServerValueSampler)

    def test_mssql_alias(self):
        s = create_value_sampler("mssql", "mssql://u:p@h/shop")
        assert isinstance(s, SQLServerValueSampler)

    def test_csv(self, tmp_path):
        s = create_value_sampler("csv", str(tmp_path))
        assert isinstance(s, CsvValueSampler)

    def test_unsupported_returns_none(self):
        assert create_value_sampler("snowflake", "snowflake://u:p@a/DB") is None


class TestCandidateKeyTargeting:
    """RSA >= 0.7.1 targets any candidate key, not only the primary key.

    r2g re-exports RSA's engine, so this is r2g's *contract with* the dependency
    rather than a test of the dependency itself: the pin allows any 0.7.x, and
    P6.7 shared-key inference is built on exactly this shape — a surrogate PK
    beside a natural business key. Before 0.7.1 this schema inferred NOTHING.
    """

    def _schema(self):
        return Schema(tables={
            "accounts": Table(name="accounts", columns=[
                Column(name="row_id", data_type="integer", is_primary_key=True),
                Column(name="account_id", data_type="text", is_unique=True),
                Column(name="name", data_type="text")], primary_key=["row_id"]),
            "orders": Table(name="orders", columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="account_id", data_type="text")], primary_key=["id"]),
        })

    def test_targets_a_natural_candidate_key_not_just_the_pk(self):
        res = infer_foreign_keys(self._schema(), options=InferenceOptions(min_confidence=0.0))
        assert res, "no candidates: RSA < 0.7.1 targeted only primary keys"
        assert any(c.foreign_table == "accounts" and c.foreign_columns == ["account_id"]
                   for c in res), [(c.foreign_table, c.foreign_columns) for c in res]

    def test_the_surrogate_pk_is_not_the_chosen_target(self):
        res = infer_foreign_keys(self._schema(), options=InferenceOptions(min_confidence=0.0))
        assert not any(c.foreign_columns == ["row_id"] for c in res)


# ── `source infer-fks --accept`: accepted guesses are labelled, not laundered ──


@pytest.fixture
def accept_catalog(tmp_path, monkeypatch):
    """A catalog whose latest snapshot declares PKs only, built on a reviewed overlay.

    ``contracts.account_id`` and ``contacts.account_id`` both resolve to ``accounts.id``
    by name (``name_suffix``), so --accept has two keys from one method to write.
    """
    import sys

    import structlog

    from r2g.catalog import CatalogManager

    def quiet(*_a, **_k):
        structlog.reset_defaults()
        structlog.configure(
            wrapper_class=structlog.make_filtering_bound_logger(0),
            logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
            cache_logger_on_first_use=False,
        )

    monkeypatch.setattr("r2g.main.setup_logging", quiet)
    root = str(tmp_path / "catalog")
    monkeypatch.setattr("r2g.main._get_catalog", lambda: CatalogManager(root))
    quiet()

    def table(name, *cols):
        return Table(
            name=name,
            columns=[Column(name="id", data_type="integer", is_nullable=False, is_primary_key=True)]
            + [Column(name=c, data_type="integer") for c in cols],
            primary_key=["id"],
            foreign_keys=[],
        )

    schema = Schema(
        tables={
            "accounts": table("accounts"),
            "contracts": table("contracts", "account_id"),
            "contacts": table("contacts", "account_id"),
        }
    )
    mgr = CatalogManager(root)
    mgr.add_source("crm", "postgresql", "postgresql://u@h/db")
    mgr.create_snapshot(
        "crm",
        schema,
        key_overlay_summary={"primaryKeys": 3},
        key_overlay_source="/reviewed/crm.keys.json",
        key_overlay_fingerprint="sha256-reviewed",
    )
    yield CatalogManager(root)
    quiet()


def _accept():
    from typer.testing import CliRunner

    from r2g.main import app

    return CliRunner().invoke(app, ["source", "infer-fks", "crm", "--accept"])


class TestAcceptLabelsInferredKeys:
    def test_an_accepted_key_is_not_enforced_and_says_so_in_json(self, accept_catalog):
        result = _accept()
        assert result.exit_code == 0, result.output
        snap = accept_catalog.get_latest_snapshot("crm")
        (fk,) = snap.schema_data.tables["contracts"].foreign_keys
        assert fk.enforced is False
        # The serializer drops enforced when it is True, so a default would leave no trace.
        assert '"enforced":false' in fk.model_dump_json()

    def test_keys_accepted_by_one_method_get_their_own_names(self, accept_catalog):
        _accept()
        tables = accept_catalog.get_latest_snapshot("crm").schema_data.tables
        names = {t: tables[t].foreign_keys[0].constraint_name for t in ("contracts", "contacts")}
        assert names == {
            "contracts": "inferred:contracts_account_id_fk",
            "contacts": "inferred:contacts_account_id_fk",
        }

    def test_method_and_confidence_are_kept_on_the_snapshot(self, accept_catalog):
        _accept()
        records = accept_catalog.get_latest_snapshot("crm").inferred_keys
        by_table = {r["table"]: r for r in records}
        assert set(by_table) == {"contracts", "contacts"}
        assert by_table["contracts"]["method"] == "name_suffix"
        assert by_table["contracts"]["foreign_table"] == "accounts"
        assert 0 < by_table["contracts"]["confidence"] <= 1

    def test_the_overlay_record_survives_accept(self, accept_catalog):
        _accept()
        snap = accept_catalog.get_latest_snapshot("crm")
        assert snap.key_overlay_summary == {"primaryKeys": 3}
        assert snap.key_overlay_source == "/reviewed/crm.keys.json"
        assert snap.key_overlay_fingerprint == "sha256-reviewed"

    def test_a_second_accept_skips_declared_keys_and_keeps_the_record(self, accept_catalog):
        _accept()
        first = accept_catalog.get_latest_snapshot("crm")
        again = _accept()
        assert again.exit_code == 0, again.output
        # Inference already skips columns a declared key covers, so a second run
        # usually proposes nothing; either way nothing may be added twice.
        assert "Accepted" not in again.output
        latest = accept_catalog.get_latest_snapshot("crm")
        assert latest.id == first.id  # nothing new, so no new snapshot
        assert len(latest.schema_data.tables["contracts"].foreign_keys) == 1
        assert len(latest.inferred_keys) == 2

    def test_snapshots_without_accepted_keys_serialize_as_before(self, accept_catalog):
        snap = accept_catalog.get_latest_snapshot("crm")
        assert "inferred_keys" not in snap.model_dump()
