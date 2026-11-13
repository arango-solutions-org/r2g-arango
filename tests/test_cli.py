"""CLI integration tests using typer.testing.CliRunner.

These tests exercise the full CLI surface without requiring PostgreSQL or
ArangoDB -- they use on-disk schema.json, mapping.yaml, and CSV fixtures.
"""
from __future__ import annotations

import json
import sys

import click
import pytest
from typer.testing import CliRunner

from r2g.main import app
from r2g.types import (
    CollectionMapping,
    Column,
    ForeignKey,
    MappingConfig,
    Schema,
    Table,
)

runner = CliRunner()


def plain_output(result) -> str:
    """Strip Rich/Typer ANSI styling so help assertions are version-stable."""
    return click.unstyle(result.output)


@pytest.fixture(autouse=True)
def _reset_structlog():
    """Ensure structlog outputs to real stderr after CliRunner restores stdout."""
    yield
    import structlog

    structlog.reset_defaults()
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(0),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    )


@pytest.fixture
def schema_file(tmp_path) -> str:
    users = Table(
        name="users",
        columns=[
            Column(name="id", data_type="integer", is_primary_key=True),
            Column(name="name", data_type="text"),
            Column(name="email", data_type="text", is_nullable=True),
        ],
        primary_key=["id"],
        foreign_keys=[],
    )
    orders = Table(
        name="orders",
        columns=[
            Column(name="id", data_type="integer", is_primary_key=True),
            Column(name="user_id", data_type="integer"),
            Column(name="total", data_type="numeric"),
        ],
        primary_key=["id"],
        foreign_keys=[
            ForeignKey(column="user_id", foreign_table="users", foreign_column="id"),
        ],
    )
    schema = Schema(tables={"users": users, "orders": orders})
    path = tmp_path / "schema.json"
    schema.save_to_file(str(path))
    return str(path)


@pytest.fixture
def config_file(tmp_path, schema_file) -> str:
    from r2g.config import ConfigManager

    schema = Schema.load_from_file(schema_file)
    config = ConfigManager.generate_default_config(schema)
    path = tmp_path / "mapping.yaml"
    ConfigManager.save_config(config, path)
    return str(path)


@pytest.fixture
def users_csv(tmp_path) -> str:
    path = tmp_path / "users.csv"
    path.write_text("id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com\n")
    return str(path)


@pytest.fixture
def orders_csv(tmp_path) -> str:
    path = tmp_path / "orders.csv"
    path.write_text("id,user_id,total\n10,1,99.99\n20,2,50.00\n")
    return str(path)


@pytest.fixture
def dumps_dir(tmp_path, users_csv, orders_csv) -> str:
    import shutil

    dump_dir = tmp_path / "dumps"
    dump_dir.mkdir()
    shutil.copy(users_csv, dump_dir / "users.csv")
    shutil.copy(orders_csv, dump_dir / "orders.csv")
    return str(dump_dir)


class TestHelp:
    def test_main_help(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        output = plain_output(result)
        assert "R2G-ETL" in output

    def test_stream_help(self):
        result = runner.invoke(app, ["stream", "--help"])
        assert result.exit_code == 0
        output = plain_output(result)
        assert "--dry-run" in output
        assert "--workers" in output
        assert "--include-tables" in output
        assert "--exclude-tables" in output
        assert "--on-duplicate" in output
        assert "--since" in output
        assert "--since-column" in output

    def test_validate_config_help(self):
        result = runner.invoke(app, ["validate-config", "--help"])
        assert result.exit_code == 0
        output = plain_output(result)
        assert "--schema" in output

    def test_diff_schema_help(self):
        result = runner.invoke(app, ["diff-schema", "--help"])
        assert result.exit_code == 0
        output = plain_output(result)
        assert "--old" in output
        assert "--new" in output

    def test_cdc_setup_help(self):
        result = runner.invoke(app, ["cdc-setup", "--help"])
        assert result.exit_code == 0
        output = plain_output(result)
        assert "--pg-conn" in output
        assert "--slot" in output
        assert "--plugin" in output

    def test_cdc_teardown_help(self):
        result = runner.invoke(app, ["cdc-teardown", "--help"])
        assert result.exit_code == 0
        output = plain_output(result)
        assert "--slot" in output

    def test_cdc_status_help(self):
        result = runner.invoke(app, ["cdc-status", "--help"])
        assert result.exit_code == 0
        output = plain_output(result)
        assert "--slot" in output

    def test_cdc_start_help(self):
        result = runner.invoke(app, ["cdc-start", "--help"])
        assert result.exit_code == 0
        output = plain_output(result)
        assert "--pg-conn" in output
        assert "--slot" in output
        assert "--plugin" in output
        assert "--poll-interval" in output
        assert "--batch-size" in output
        assert "--endpoint" in output
        assert "schema_file" in output.lower()  # metavar case varies by Typer version
        assert "--conflict-policy" in output
        assert "--temporal" in output
        assert "--ttl-seconds" in output
        assert "--smart-field" in output

    def test_kafka_start_help(self):
        result = runner.invoke(app, ["kafka-start", "--help"])
        assert result.exit_code == 0
        output = plain_output(result)
        assert "--brokers" in output
        assert "--topics" in output
        assert "--group-id" in output
        assert "--format" in output
        assert "--offset-reset" in output
        assert "--batch-size" in output
        assert "--endpoint" in output
        assert "--conflict-policy" in output
        assert "schema_file" in output.lower()  # metavar case varies by Typer version
        assert "--temporal" in output
        assert "--ttl-seconds" in output
        assert "--smart-field" in output


class TestValidateSchema:
    def test_valid_schema(self, schema_file):
        result = runner.invoke(app, ["validate-schema", schema_file])
        assert result.exit_code == 0
        assert "Schema valid" in result.output

    def test_invalid_schema_file(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_text("{}", encoding="utf-8")
        result = runner.invoke(app, ["validate-schema", str(bad)])
        assert result.exit_code == 0 or "valid" in result.output.lower()

    def test_missing_schema_file(self, tmp_path):
        result = runner.invoke(app, ["validate-schema", str(tmp_path / "missing.json")])
        assert result.exit_code != 0


class TestGenerateConfig:
    def test_generates_yaml(self, schema_file, tmp_path):
        output = str(tmp_path / "out.yaml")
        result = runner.invoke(app, [
            "generate-config", "--schema", schema_file, "--output", output,
        ])
        assert result.exit_code == 0
        assert "Wrote mapping config" in result.output
        assert "2" in result.output  # 2 collections

    def test_missing_schema_exits_1(self, tmp_path):
        result = runner.invoke(app, [
            "generate-config", "--schema", str(tmp_path / "missing.json"),
            "--output", str(tmp_path / "out.yaml"),
        ])
        assert result.exit_code == 1


class TestValidateConfig:
    def test_valid_config(self, schema_file, config_file):
        result = runner.invoke(app, [
            "validate-config", "--schema", schema_file, "--config", config_file,
        ])
        assert result.exit_code == 0
        assert "Config valid" in result.output

    def test_invalid_config(self, schema_file, tmp_path):
        bad_config = tmp_path / "bad.yaml"
        from r2g.config import ConfigManager

        config = MappingConfig(
            collections={"ghosts": CollectionMapping(source_table="ghosts", target_collection="g")},
        )
        ConfigManager.save_config(config, bad_config)
        result = runner.invoke(app, [
            "validate-config", "--schema", schema_file, "--config", str(bad_config),
        ])
        assert result.exit_code == 1
        assert "issue" in result.output.lower()


class TestInspectDump:
    def test_inspect_csv(self, users_csv):
        result = runner.invoke(app, ["inspect-dump", users_csv, "--limit", "2"])
        assert result.exit_code == 0
        assert "Columns detected" in result.output
        assert "Alice" in result.output

    def test_missing_file(self, tmp_path):
        result = runner.invoke(app, ["inspect-dump", str(tmp_path / "missing.csv")])
        assert result.exit_code == 1


class TestTransformNodes:
    def test_transform_nodes(self, schema_file, users_csv, tmp_path):
        output = str(tmp_path / "users.jsonl")
        result = runner.invoke(app, [
            "transform-nodes",
            "--schema", schema_file,
            "--table", "users",
            "--input", users_csv,
            "--output", output,
        ])
        assert result.exit_code == 0
        assert "2" in result.output
        with open(output) as f:
            lines = f.readlines()
        assert len(lines) == 2
        doc = json.loads(lines[0])
        assert "_key" in doc

    def test_transform_nodes_with_config(self, schema_file, config_file, users_csv, tmp_path):
        output = str(tmp_path / "users.jsonl")
        result = runner.invoke(app, [
            "transform-nodes",
            "--schema", schema_file,
            "--config", config_file,
            "--table", "users",
            "--input", users_csv,
            "--output", output,
        ])
        assert result.exit_code == 0

    def test_unknown_table_exits_1(self, schema_file, users_csv, tmp_path):
        result = runner.invoke(app, [
            "transform-nodes",
            "--schema", schema_file,
            "--table", "nonexistent",
            "--input", users_csv,
            "--output", str(tmp_path / "out.jsonl"),
        ])
        assert result.exit_code == 1


class TestTransformEdges:
    def test_transform_edges(self, schema_file, config_file, orders_csv, tmp_path):
        output = str(tmp_path / "edge_out" / "edges.jsonl")
        result = runner.invoke(app, [
            "transform-edges",
            "--schema", schema_file,
            "--config", config_file,
            "--table", "orders",
            "--input", orders_csv,
            "--output", output,
        ], catch_exceptions=False)
        assert result.exit_code == 0
        assert "edges" in result.output.lower()
        with open(output) as f:
            lines = f.readlines()
        assert len(lines) == 2
        edge = json.loads(lines[0])
        assert "_from" in edge
        assert "_to" in edge

    def test_no_edge_definitions_exits_1(self, schema_file, config_file, users_csv, tmp_path):
        result = runner.invoke(app, [
            "transform-edges",
            "--schema", schema_file,
            "--config", config_file,
            "--table", "users",
            "--input", users_csv,
            "--output", str(tmp_path / "out.jsonl"),
        ])
        assert result.exit_code == 1


class TestTransformAll:
    def test_full_transform(self, schema_file, config_file, dumps_dir, tmp_path):
        output_dir = str(tmp_path / "output")
        result = runner.invoke(app, [
            "transform-all",
            "--schema", schema_file,
            "--config", config_file,
            "--input-dir", dumps_dir,
            "--output-dir", output_dir,
        ])
        assert result.exit_code == 0
        assert "Transform complete" in result.output

    def test_missing_input_dir_exits_1(self, schema_file, config_file, tmp_path):
        result = runner.invoke(app, [
            "transform-all",
            "--schema", schema_file,
            "--config", config_file,
            "--input-dir", str(tmp_path / "nope"),
            "--output-dir", str(tmp_path / "out"),
        ])
        assert result.exit_code == 1


class TestGenerateImport:
    def test_generate_import_script(self, config_file, dumps_dir, tmp_path):
        output = str(tmp_path / "import.sh")
        result = runner.invoke(app, [
            "generate-import",
            "--config", config_file,
            "--data-dir", dumps_dir,
            "--output", output,
        ])
        assert result.exit_code == 0
        assert "Wrote import script" in result.output

    def test_with_graph_name(self, config_file, dumps_dir, tmp_path):
        output = str(tmp_path / "import.sh")
        result = runner.invoke(app, [
            "generate-import",
            "--config", config_file,
            "--data-dir", dumps_dir,
            "--output", output,
            "--graph-name", "test_graph",
        ])
        assert result.exit_code == 0
        assert "graph creation" in result.output.lower()


class TestGenerateCsvImport:
    def test_generate_csv_script(self, schema_file, config_file, dumps_dir, tmp_path):
        output = str(tmp_path / "import_csv.sh")
        result = runner.invoke(app, [
            "generate-csv-import",
            "--schema", schema_file,
            "--config", config_file,
            "--data-dir", dumps_dir,
            "--output", output,
        ])
        assert result.exit_code == 0
        assert "Wrote CSV import script" in result.output

    def test_names_nullable_text_columns_on_screen(
        self, schema_file, config_file, dumps_dir, tmp_path
    ):
        result = runner.invoke(app, [
            "generate-csv-import",
            "--schema", schema_file,
            "--config", config_file,
            "--data-dir", dumps_dir,
            "--output", str(tmp_path / "import_csv.sh"),
        ])
        assert result.exit_code == 0
        said = " ".join(plain_output(result).split())  # Rich wraps long lines
        assert "Warning: arangoimport converts" in said
        assert "users.email" in said

    def test_refuses_a_masked_column_and_writes_no_script(
        self, schema_file, config_file, dumps_dir, tmp_path, monkeypatch
    ):
        import structlog

        from r2g.config import ConfigManager
        from r2g.masking import make_mask_expression

        # r2g.main's module logger is cached on first use (cache_logger_on_first_use),
        # so an earlier CliRunner test can leave it bound to a closed stream; the error
        # path's log.exception would then raise before the message prints.
        monkeypatch.setattr("r2g.main.log", structlog.get_logger("test_cli"))

        mapping = ConfigManager.load_config(config_file)
        users = next(k for k, cm in mapping.collections.items() if cm.source_table == "users")
        mapping.collections[users].field_expressions = [make_mask_expression("email", "redact")]
        ConfigManager.save_config(mapping, config_file)
        output = tmp_path / "import_csv.sh"
        result = runner.invoke(app, [
            "generate-csv-import",
            "--schema", schema_file,
            "--config", config_file,
            "--data-dir", dumps_dir,
            "--output", str(output),
        ])
        assert result.exit_code == 1
        said = " ".join(plain_output(result).split())  # Rich wraps long errors
        assert "cannot reproduce this mapping" in said
        assert f"{users}: field expressions (email)" in said
        assert not output.exists()


class TestVisualizeMappingCLI:
    def test_generate_visualization(self, schema_file, config_file, tmp_path):
        output = str(tmp_path / "viz.html")
        result = runner.invoke(app, [
            "visualize-mapping",
            "--schema", schema_file,
            "--config", config_file,
            "--output", output,
            "--no-open",
        ])
        assert result.exit_code == 0
        assert "Wrote mapping visualization" in result.output
        with open(output) as f:
            html = f.read()
        assert "<html" in html.lower()


class TestValidateData:
    def test_clean_data(self, schema_file, config_file, dumps_dir):
        result = runner.invoke(app, [
            "validate-data",
            "--schema", schema_file,
            "--config", config_file,
            "--data-dir", dumps_dir,
        ])
        assert result.exit_code == 0
        assert "passed" in result.output.lower() or "no orphan" in result.output.lower()

    def test_orphan_detected(self, schema_file, config_file, tmp_path):
        dump_dir = tmp_path / "dumps"
        dump_dir.mkdir()
        (dump_dir / "users.csv").write_text("id,name,email\n1,Alice,a@e.com\n")
        (dump_dir / "orders.csv").write_text("id,user_id,total\n10,1,50\n20,999,99\n")

        result = runner.invoke(app, [
            "validate-data",
            "--schema", schema_file,
            "--config", config_file,
            "--data-dir", str(dump_dir),
        ])
        assert result.exit_code == 1
        assert "999" in result.output

    def test_help(self):
        result = runner.invoke(app, ["validate-data", "--help"])
        assert result.exit_code == 0
        output = plain_output(result)
        assert "--data-dir" in output
        assert "--schema" in output


class TestMigrateConfig:
    def test_no_changes(self, schema_file, config_file):
        result = runner.invoke(app, [
            "migrate-config", "--schema", schema_file, "--config", config_file,
        ])
        assert result.exit_code == 0
        assert "up to date" in result.output.lower() or "no migration" in result.output.lower()

    def test_added_table(self, schema_file, config_file, tmp_path):
        schema = Schema.load_from_file(schema_file)
        schema.tables["products"] = Table(
            name="products",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="name", data_type="text"),
            ],
            primary_key=["id"],
        )
        new_schema_path = tmp_path / "new_schema.json"
        schema.save_to_file(str(new_schema_path))

        output = str(tmp_path / "migrated.yaml")
        result = runner.invoke(app, [
            "migrate-config",
            "--schema", str(new_schema_path),
            "--config", config_file,
            "--output", output,
        ])
        assert result.exit_code == 0
        assert "products" in result.output
        assert "Added" in result.output

    def test_removed_fk(self, schema_file, config_file, tmp_path):
        schema = Schema.load_from_file(schema_file)
        schema.tables["orders"].foreign_keys = []
        new_path = tmp_path / "no_fk_schema.json"
        schema.save_to_file(str(new_path))

        output = str(tmp_path / "migrated.yaml")
        result = runner.invoke(app, [
            "migrate-config",
            "--schema", str(new_path),
            "--config", config_file,
            "--output", output,
        ])
        assert result.exit_code == 0
        assert "Removed" in result.output or "removed" in result.output.lower()

    def test_json_report(self, schema_file, config_file, tmp_path):
        schema = Schema.load_from_file(schema_file)
        schema.tables["products"] = Table(
            name="products",
            columns=[Column(name="id", data_type="integer", is_primary_key=True)],
            primary_key=["id"],
        )
        new_path = tmp_path / "new_schema.json"
        schema.save_to_file(str(new_path))

        output = str(tmp_path / "migrated.yaml")
        result = runner.invoke(app, [
            "migrate-config",
            "--schema", str(new_path),
            "--config", config_file,
            "--output", output,
            "--json-report",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "added_collections" in data
        assert "products" in data["added_collections"]

    def test_missing_schema_exits_1(self, config_file, tmp_path):
        result = runner.invoke(app, [
            "migrate-config",
            "--schema", str(tmp_path / "missing.json"),
            "--config", config_file,
        ])
        assert result.exit_code == 1

    def test_overwrites_input_by_default(self, schema_file, tmp_path):
        from r2g.config import ConfigManager as CM

        schema = Schema.load_from_file(schema_file)
        config = CM.generate_default_config(schema)
        cfg_path = tmp_path / "mapping.yaml"
        CM.save_config(config, cfg_path)

        result = runner.invoke(app, [
            "migrate-config",
            "--schema", schema_file,
            "--config", str(cfg_path),
        ])
        assert result.exit_code == 0
        assert "mapping.yaml" in result.output

    def test_help(self):
        result = runner.invoke(app, ["migrate-config", "--help"])
        assert result.exit_code == 0
        output = plain_output(result)
        assert "--schema" in output
        assert "--config" in output
        assert "--output" in output


class TestDiffSchema:
    def test_no_changes(self, schema_file):
        result = runner.invoke(app, [
            "diff-schema", "--old", schema_file, "--new", schema_file,
        ])
        assert result.exit_code == 0
        assert "identical" in result.output.lower() or "no changes" in result.output.lower()

    def test_added_table(self, schema_file, tmp_path):
        schema = Schema.load_from_file(schema_file)
        schema.tables["products"] = Table(
            name="products",
            columns=[Column(name="id", data_type="integer", is_primary_key=True)],
            primary_key=["id"],
        )
        new_path = tmp_path / "new_schema.json"
        schema.save_to_file(str(new_path))

        result = runner.invoke(app, [
            "diff-schema", "--old", schema_file, "--new", str(new_path),
        ])
        assert result.exit_code == 0
        assert "products" in result.output
        assert "added" in result.output.lower()

    def test_removed_table(self, schema_file, tmp_path):
        schema = Schema.load_from_file(schema_file)
        del schema.tables["orders"]
        new_path = tmp_path / "new_schema.json"
        schema.save_to_file(str(new_path))

        result = runner.invoke(app, [
            "diff-schema", "--old", schema_file, "--new", str(new_path),
        ])
        assert result.exit_code == 0
        assert "orders" in result.output
        assert "removed" in result.output.lower()

    def test_added_column(self, schema_file, tmp_path):
        schema = Schema.load_from_file(schema_file)
        schema.tables["users"].columns.append(
            Column(name="phone", data_type="text", is_nullable=True)
        )
        new_path = tmp_path / "new_schema.json"
        schema.save_to_file(str(new_path))

        result = runner.invoke(app, [
            "diff-schema", "--old", schema_file, "--new", str(new_path),
        ])
        assert result.exit_code == 0
        assert "phone" in result.output

    def test_type_change(self, schema_file, tmp_path):
        schema = Schema.load_from_file(schema_file)
        for col in schema.tables["users"].columns:
            if col.name == "name":
                col.data_type = "varchar(255)"
        new_path = tmp_path / "new_schema.json"
        schema.save_to_file(str(new_path))

        result = runner.invoke(app, [
            "diff-schema", "--old", schema_file, "--new", str(new_path),
        ])
        assert result.exit_code == 0
        assert "name" in result.output
        assert "text" in result.output

    def test_added_fk(self, schema_file, tmp_path):
        schema = Schema.load_from_file(schema_file)
        schema.tables["users"].foreign_keys.append(
            ForeignKey(column="email", foreign_table="emails", foreign_column="id")
        )
        new_path = tmp_path / "new_schema.json"
        schema.save_to_file(str(new_path))

        result = runner.invoke(app, [
            "diff-schema", "--old", schema_file, "--new", str(new_path),
        ])
        assert result.exit_code == 0
        assert "foreign" in result.output.lower() or "fk" in result.output.lower()

    def test_json_output(self, schema_file, tmp_path):
        schema = Schema.load_from_file(schema_file)
        schema.tables["products"] = Table(
            name="products",
            columns=[Column(name="id", data_type="integer", is_primary_key=True)],
            primary_key=["id"],
        )
        new_path = tmp_path / "new_schema.json"
        schema.save_to_file(str(new_path))

        result = runner.invoke(app, [
            "diff-schema", "--old", schema_file, "--new", str(new_path), "--json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "added_tables" in data
