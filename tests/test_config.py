from __future__ import annotations

import pytest

from r2g.config import ConfigManager, _is_likely_join_table, pg_type_to_json_type, validate_config
from r2g.types import CollectionMapping, Column, EdgeDefinition, ForeignKey, MappingConfig, Schema, Table


class TestPgTypeToJsonType:
    @pytest.mark.parametrize(
        "pg_type, expected",
        [
            ("integer", "integer"),
            ("bigint", "integer"),
            ("smallint", "integer"),
            ("serial", "integer"),
            ("bigserial", "integer"),
            ("numeric", "float"),
            ("decimal", "float"),
            ("real", "float"),
            ("double precision", "float"),
            ("boolean", "boolean"),
            ("json", "object"),
            ("jsonb", "object"),
        ],
    )
    def test_known_types(self, pg_type, expected):
        assert pg_type_to_json_type(pg_type) == expected

    @pytest.mark.parametrize(
        "pg_type",
        ["text", "varchar", "character varying", "uuid", "timestamp", "date",
         "bytea", "interval", "inet", "cidr", "money", "time", "timetz",
         "timestamptz", "macaddr", "xml", "tsvector", "bit", "point"],
    )
    def test_string_types(self, pg_type):
        assert pg_type_to_json_type(pg_type) == "string"

    @pytest.mark.parametrize(
        "pg_type",
        ["hstore", "ltree", "my_custom_enum"],
    )
    def test_unknown_types_fallback_to_string(self, pg_type):
        assert pg_type_to_json_type(pg_type) == "string"

    @pytest.mark.parametrize(
        "pg_type",
        ["integer[]", "text[]", "ARRAY", "boolean[]"],
    )
    def test_array_types(self, pg_type):
        assert pg_type_to_json_type(pg_type) == "array"

    def test_type_with_precision_stripped(self):
        assert pg_type_to_json_type("numeric(10,2)") == "float"

    def test_case_insensitive(self):
        assert pg_type_to_json_type("INTEGER") == "integer"
        assert pg_type_to_json_type("Boolean") == "boolean"

    def test_whitespace_stripped(self):
        assert pg_type_to_json_type("  integer  ") == "integer"

    @pytest.mark.parametrize(
        "mysql_type, expected",
        [
            ("tinyint", "integer"),
            ("mediumint", "integer"),
            ("year", "integer"),
            ("double", "float"),
            ("tinytext", "string"),
            ("mediumtext", "string"),
            ("longtext", "string"),
            ("blob", "string"),
            ("longblob", "string"),
            ("enum", "string"),
            ("set", "string"),
            ("datetime", "string"),
            ("json", "object"),
        ],
    )
    def test_mysql_types(self, mysql_type, expected):
        assert pg_type_to_json_type(mysql_type) == expected

    def test_mysql_tinyint_with_display_width_stripped(self):
        # MySQL DATA_TYPE is "tinyint" but COLUMN_TYPE may be "tinyint(1)";
        # the precision-stripping path handles either form.
        assert pg_type_to_json_type("tinyint(1)") == "integer"

    @pytest.mark.parametrize(
        "mssql_type, expected",
        [
            ("nvarchar", "string"),
            ("nchar", "string"),
            ("ntext", "string"),
            ("datetime2", "string"),
            ("smalldatetime", "string"),
            ("datetimeoffset", "string"),
            ("uniqueidentifier", "string"),
            ("smallmoney", "string"),
            ("image", "string"),
            ("real", "float"),
        ],
    )
    def test_sqlserver_types(self, mssql_type, expected):
        assert pg_type_to_json_type(mssql_type) == expected

    def test_bit_maps_to_string_in_shared_map(self):
        # `bit` stays a (PostgreSQL) bit-string in the shared map; the SQL
        # Server connector overrides it to boolean at introspection time, so
        # the dialect conflict never reaches this function.
        assert pg_type_to_json_type("bit") == "string"


class TestGenerateDefaultConfig:
    def test_produces_document_collections(self, sample_schema):
        config = ConfigManager.generate_default_config(sample_schema)
        assert "users" in config.collections
        assert "orders" in config.collections
        assert len(config.collections) == 2
        assert config.collections["users"].collection_type == "document"
        assert config.collections["orders"].collection_type == "document"

    def test_produces_edge_definitions(self, sample_schema):
        config = ConfigManager.generate_default_config(sample_schema)
        assert len(config.edges) == 1
        edge = config.edges[0]
        assert edge.edge_collection == "orders_to_users"
        assert edge.from_collection == "orders"
        assert edge.to_collection == "users"
        assert edge.from_field == "user_id"
        assert edge.to_field == "id"

    def test_collection_mapping_source_table(self, sample_schema):
        config = ConfigManager.generate_default_config(sample_schema)
        assert config.collections["users"].source_table == "users"
        assert config.collections["users"].target_collection == "users"

    def test_default_schema_is_public(self, sample_schema):
        config = ConfigManager.generate_default_config(sample_schema)
        assert config.source_schema == "public"

    def test_custom_source_schema(self, sample_schema):
        config = ConfigManager.generate_default_config(sample_schema, source_schema="sales")
        assert config.source_schema == "sales"

    def test_self_referential_fk(self):
        employees = Table(
            name="employees",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="name", data_type="text"),
                Column(name="manager_id", data_type="integer", is_nullable=True),
            ],
            primary_key=["id"],
            foreign_keys=[
                ForeignKey(
                    column="manager_id",
                    foreign_table="employees",
                    foreign_column="id",
                    constraint_name="fk_manager",
                ),
            ],
        )
        schema = Schema(tables={"employees": employees})
        config = ConfigManager.generate_default_config(schema)
        assert len(config.edges) == 1
        edge = config.edges[0]
        assert edge.from_collection == "employees"
        assert edge.to_collection == "employees"
        assert edge.edge_collection == "employees_to_employees"

    def test_multiple_fks_to_same_table_disambiguated(self):
        orders = Table(
            name="orders",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="customer_id", data_type="integer"),
                Column(name="referrer_id", data_type="integer", is_nullable=True),
            ],
            primary_key=["id"],
            foreign_keys=[
                ForeignKey(
                    column="customer_id",
                    foreign_table="customers",
                    foreign_column="id",
                    constraint_name="fk_customer",
                ),
                ForeignKey(
                    column="referrer_id",
                    foreign_table="customers",
                    foreign_column="id",
                    constraint_name="fk_referrer",
                ),
            ],
        )
        customers = Table(
            name="customers",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="name", data_type="text"),
            ],
            primary_key=["id"],
            foreign_keys=[],
        )
        schema = Schema(tables={"orders": orders, "customers": customers})
        config = ConfigManager.generate_default_config(schema)
        edge_names = [e.edge_collection for e in config.edges]
        assert len(edge_names) == 2
        assert len(set(edge_names)) == 2
        assert "orders_to_customers" in edge_names
        suffixed = [n for n in edge_names if n != "orders_to_customers"]
        assert len(suffixed) == 1
        assert suffixed[0].startswith("orders_to_customers_")


class TestSaveAndLoadConfig:
    def test_yaml_round_trip(self, sample_schema, tmp_path):
        config = ConfigManager.generate_default_config(sample_schema)
        path = tmp_path / "mapping.yaml"
        ConfigManager.save_config(config, path)
        loaded = ConfigManager.load_config(path)

        assert loaded.source_schema == config.source_schema
        assert set(loaded.collections.keys()) == set(config.collections.keys())
        assert len(loaded.edges) == len(config.edges)
        assert loaded.edges[0].edge_collection == config.edges[0].edge_collection

    def test_load_empty_file_returns_default(self, tmp_path):
        path = tmp_path / "empty.yaml"
        path.write_text("", encoding="utf-8")
        loaded = ConfigManager.load_config(path)

        assert isinstance(loaded, MappingConfig)
        assert loaded.source_schema == "public"
        assert loaded.collections == {}
        assert loaded.edges == []

    def test_load_invalid_yaml_raises(self, tmp_path):
        path = tmp_path / "bad.yaml"
        path.write_text("- just a list item\n", encoding="utf-8")
        with pytest.raises(ValueError, match="YAML mapping"):
            ConfigManager.load_config(path)

    def test_save_creates_parent_dirs(self, tmp_path):
        config = MappingConfig()
        path = tmp_path / "a" / "b" / "config.yaml"
        ConfigManager.save_config(config, path)
        assert path.exists()


class TestJoinTableAutoDetection:
    def _make_table(self, name, cols, pks, fks):
        columns = [
            Column(name=c[0], data_type=c[1], is_nullable=c[2], is_primary_key=c[0] in pks)
            for c in cols
        ]
        foreign_keys = [
            ForeignKey(column=fk[0], foreign_table=fk[1], foreign_column=fk[2], constraint_name=f"fk_{fk[0]}")
            for fk in fks
        ]
        return Table(name=name, columns=columns, primary_key=pks, foreign_keys=foreign_keys)

    def test_pure_join_table_detected(self):
        table = self._make_table(
            "post_tags",
            [("post_id", "integer", False), ("tag_id", "integer", False)],
            ["post_id", "tag_id"],
            [("post_id", "posts", "id"), ("tag_id", "tags", "id")],
        )
        assert _is_likely_join_table(table) is True

    def test_join_table_with_quantity(self):
        table = self._make_table(
            "order_items",
            [("order_id", "integer", False), ("product_id", "integer", False),
             ("quantity", "integer", False)],
            ["order_id", "product_id"],
            [("order_id", "orders", "id"), ("product_id", "products", "id")],
        )
        assert _is_likely_join_table(table) is True

    def test_join_table_with_created_at(self):
        table = self._make_table(
            "user_roles",
            [("user_id", "integer", False), ("role_id", "integer", False),
             ("created_at", "timestamp without time zone", True)],
            ["user_id", "role_id"],
            [("user_id", "users", "id"), ("role_id", "roles", "id")],
        )
        assert _is_likely_join_table(table) is True

    def test_regular_table_not_detected(self):
        table = self._make_table(
            "orders",
            [("id", "integer", False), ("customer_id", "integer", False),
             ("total", "numeric", False), ("notes", "text", True)],
            ["id"],
            [("customer_id", "customers", "id")],
        )
        assert _is_likely_join_table(table) is False

    def test_table_with_one_fk_not_detected(self):
        table = self._make_table(
            "comments",
            [("id", "integer", False), ("post_id", "integer", False),
             ("body", "text", False)],
            ["id"],
            [("post_id", "posts", "id")],
        )
        assert _is_likely_join_table(table) is False

    def test_table_with_extra_data_column_not_detected(self):
        table = self._make_table(
            "enrollments",
            [("student_id", "integer", False), ("course_id", "integer", False),
             ("grade", "text", True)],
            ["student_id", "course_id"],
            [("student_id", "students", "id"), ("course_id", "courses", "id")],
        )
        assert _is_likely_join_table(table) is False

    def test_generate_default_config_flags_join_table(self):
        post_tags = self._make_table(
            "post_tags",
            [("post_id", "integer", False), ("tag_id", "integer", False)],
            ["post_id", "tag_id"],
            [("post_id", "posts", "id"), ("tag_id", "tags", "id")],
        )
        posts = self._make_table(
            "posts",
            [("id", "integer", False), ("title", "text", False)],
            ["id"],
            [],
        )
        tags = self._make_table(
            "tags",
            [("id", "integer", False), ("label", "text", False)],
            ["id"],
            [],
        )
        schema = Schema(tables={"posts": posts, "tags": tags, "post_tags": post_tags})
        config = ConfigManager.generate_default_config(schema)
        assert config.collections["post_tags"].is_join_table is True
        assert config.collections["posts"].is_join_table is False
        assert config.collections["tags"].is_join_table is False


class TestValidateConfig:
    @pytest.fixture
    def schema(self):
        return Schema(tables={
            "users": Table(
                name="users",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="name", data_type="text"),
                ],
                primary_key=["id"],
                foreign_keys=[],
            ),
            "orders": Table(
                name="orders",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="user_id", data_type="integer"),
                ],
                primary_key=["id"],
                foreign_keys=[
                    ForeignKey(column="user_id", foreign_table="users", foreign_column="id"),
                ],
            ),
        })

    def test_valid_config_has_no_issues(self, schema):
        config = ConfigManager.generate_default_config(schema)
        assert validate_config(schema, config) == []

    def test_missing_source_table(self, schema):
        config = MappingConfig(
            collections={
                "ghosts": CollectionMapping(source_table="ghosts", target_collection="ghosts"),
            },
        )
        issues = validate_config(schema, config)
        assert len(issues) == 1
        assert "ghosts" in issues[0]
        assert "not found" in issues[0]

    def test_a_table_mapped_by_two_document_collections_is_refused(self, schema):
        """A load writes one document collection per table, so a second is refused
        rather than silently not loaded; an edge collection beside one is not."""
        config = MappingConfig(collections={
            "users": CollectionMapping(source_table="users", target_collection="users"),
            "people": CollectionMapping(source_table="users", target_collection="people"),
        })
        issues = validate_config(schema, config)
        assert any("more than one document collection" in i and "people" in i
                   for i in issues), issues
        config.collections.pop("people")
        config.collections["user_links"] = CollectionMapping(
            source_table="users", target_collection="user_links", collection_type="edge")
        assert not any("more than one" in i for i in validate_config(schema, config))

    def test_bad_exclude_field(self, schema):
        config = MappingConfig(
            collections={
                "users": CollectionMapping(
                    source_table="users",
                    target_collection="users",
                    exclude_fields=["nonexistent"],
                ),
            },
        )
        issues = validate_config(schema, config)
        assert any("nonexistent" in i for i in issues)

    def test_bad_include_field(self, schema):
        config = MappingConfig(
            collections={
                "users": CollectionMapping(
                    source_table="users",
                    target_collection="users",
                    include_fields=["bogus"],
                ),
            },
        )
        issues = validate_config(schema, config)
        assert any("bogus" in i for i in issues)

    def test_field_mapping_target_reserved_attribute(self, schema):
        config = MappingConfig(
            collections={
                "users": CollectionMapping(
                    source_table="users",
                    target_collection="users",
                    field_mappings={"name": "_key"},
                ),
            },
        )
        issues = validate_config(schema, config)
        assert any("_key" in i and "reserved" in i for i in issues)

    def test_expression_target_reserved_attribute(self, schema):
        from r2g.types import FieldExpression

        config = MappingConfig(
            collections={
                "users": CollectionMapping(
                    source_table="users",
                    target_collection="users",
                    field_expressions=[FieldExpression(target="_from", sources=["id"])],
                ),
            },
        )
        issues = validate_config(schema, config)
        assert any("_from" in i and "reserved" in i for i in issues)

    def test_bad_field_mapping_source(self, schema):
        config = MappingConfig(
            collections={
                "users": CollectionMapping(
                    source_table="users",
                    target_collection="users",
                    field_mappings={"nope": "yep"},
                ),
            },
        )
        issues = validate_config(schema, config)
        assert any("nope" in i for i in issues)

    def test_edge_bad_from_collection(self, schema):
        config = MappingConfig(
            edges=[
                EdgeDefinition(
                    edge_collection="bad_edge",
                    from_collection="phantoms",
                    to_collection="users",
                    from_field="x",
                    to_field="id",
                ),
            ],
        )
        issues = validate_config(schema, config)
        assert any("phantoms" in i for i in issues)

    def test_edge_bad_from_field(self, schema):
        config = MappingConfig(
            edges=[
                EdgeDefinition(
                    edge_collection="bad_edge",
                    from_collection="orders",
                    to_collection="users",
                    from_field="nonexistent_col",
                    to_field="id",
                ),
            ],
        )
        issues = validate_config(schema, config)
        assert any("nonexistent_col" in i for i in issues)

    def test_edge_bad_to_field(self, schema):
        config = MappingConfig(
            edges=[
                EdgeDefinition(
                    edge_collection="bad_edge",
                    from_collection="orders",
                    to_collection="users",
                    from_field="user_id",
                    to_field="phantom_col",
                ),
            ],
        )
        issues = validate_config(schema, config)
        assert any("phantom_col" in i for i in issues)

    def test_multiple_issues_reported(self, schema):
        config = MappingConfig(
            collections={
                "missing": CollectionMapping(source_table="missing", target_collection="m"),
            },
            edges=[
                EdgeDefinition(
                    edge_collection="e",
                    from_collection="also_missing",
                    to_collection="users",
                    from_field="x",
                    to_field="id",
                ),
            ],
        )
        issues = validate_config(schema, config)
        assert len(issues) >= 2

    def test_pkless_table_warning(self):
        pkless_schema = Schema(tables={
            "logs": Table(
                name="logs",
                columns=[
                    Column(name="message", data_type="text"),
                    Column(name="ts", data_type="timestamp"),
                ],
                primary_key=[],
            ),
        })
        config = MappingConfig(
            collections={
                "logs": CollectionMapping(source_table="logs", target_collection="logs"),
            },
        )
        issues = validate_config(pkless_schema, config)
        assert len(issues) == 1
        assert "no primary key" in issues[0]
        assert "keyed by all column values" in issues[0]
        assert "identical rows collapse to one document" in issues[0]
        assert "auto-generated" not in issues[0]
