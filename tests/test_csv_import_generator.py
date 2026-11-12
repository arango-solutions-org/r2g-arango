from __future__ import annotations

import json
import os
import shlex
import shutil
import stat
import subprocess

import pytest

from r2g.config import ConfigManager
from r2g.generators.arangoimport import (
    IMPORT_GUARD,
    CsvDirectUnsupportedError,
    CsvImportGenerator,
)
from r2g.masking import make_mask_expression
from r2g.types import (
    CollectionMapping,
    Column,
    EdgeDefinition,
    FieldExpression,
    ForeignKey,
    MappingConfig,
    Schema,
    Table,
)


@pytest.fixture
def ecommerce_schema() -> Schema:
    customers = Table(
        name="customers",
        columns=[
            Column(name="id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="name", data_type="text", is_nullable=False),
            Column(name="email", data_type="text", is_nullable=True),
            Column(name="is_premium", data_type="boolean", is_nullable=False),
            Column(name="balance", data_type="numeric", is_nullable=True),
        ],
        primary_key=["id"],
        foreign_keys=[],
    )
    orders = Table(
        name="orders",
        columns=[
            Column(name="id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="customer_id", data_type="integer", is_nullable=False),
            Column(name="total", data_type="numeric", is_nullable=False),
            Column(name="referrer_id", data_type="integer", is_nullable=True),
            Column(name="notes", data_type="text", is_nullable=True),
        ],
        primary_key=["id"],
        foreign_keys=[
            ForeignKey(column="customer_id", foreign_table="customers", foreign_column="id", constraint_name="fk_cust"),
            ForeignKey(column="referrer_id", foreign_table="customers", foreign_column="id", constraint_name="fk_ref"),
        ],
    )
    order_items = Table(
        name="order_items",
        columns=[
            Column(name="order_id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="product_id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="quantity", data_type="integer", is_nullable=False),
        ],
        primary_key=["order_id", "product_id"],
        foreign_keys=[
            ForeignKey(column="order_id", foreign_table="orders", foreign_column="id", constraint_name="fk_order"),
            ForeignKey(column="product_id", foreign_table="products", foreign_column="id", constraint_name="fk_prod"),
        ],
    )
    products = Table(
        name="products",
        columns=[
            Column(name="id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="name", data_type="text", is_nullable=False),
            Column(name="price", data_type="numeric", is_nullable=False),
        ],
        primary_key=["id"],
        foreign_keys=[],
    )
    return Schema(tables={
        "customers": customers,
        "orders": orders,
        "order_items": order_items,
        "products": products,
    })


@pytest.fixture
def ecommerce_config() -> MappingConfig:
    return MappingConfig(
        collections={
            "customers": CollectionMapping(source_table="customers", target_collection="customers"),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
            "products": CollectionMapping(source_table="products", target_collection="products"),
            "order_items": CollectionMapping(source_table="order_items", target_collection="order_items"),
        },
        edges=[
            EdgeDefinition(
                edge_collection="orders_to_customers",
                from_collection="orders",
                to_collection="customers",
                from_field="customer_id",
                to_field="id",
            ),
            EdgeDefinition(
                edge_collection="orders_to_customers_referrer_id",
                from_collection="orders",
                to_collection="customers",
                from_field="referrer_id",
                to_field="id",
            ),
        ],
    )


@pytest.fixture
def generator(ecommerce_config, ecommerce_schema) -> CsvImportGenerator:
    return CsvImportGenerator(
        ecommerce_config,
        ecommerce_schema,
        endpoint="http://localhost:8529",
        database="test_db",
        username="root",
        password="secret",
        data_dir="./dumps",
    )


class TestInvalidOnDuplicate:
    def test_raises_value_error(self, ecommerce_config, ecommerce_schema):
        with pytest.raises(ValueError, match="on_duplicate"):
            CsvImportGenerator(ecommerce_config, ecommerce_schema, on_duplicate="bad")

    @pytest.mark.parametrize("valid", ["error", "update", "replace", "ignore"])
    def test_valid_values_accepted(self, ecommerce_config, ecommerce_schema, valid):
        gen = CsvImportGenerator(ecommerce_config, ecommerce_schema, on_duplicate=valid)
        assert gen.on_duplicate == valid


class TestDatatypeFlags:
    def test_skips_excluded_columns(self, generator):
        flags = generator._datatype_flags("customers", exclude_cols={"id"})
        joined = " ".join(flags)
        assert "id=" not in joined

    def test_includes_boolean_for_non_nullable(self, generator):
        flags = generator._datatype_flags("customers", exclude_cols={"id"})
        joined = " ".join(flags)
        assert "is_premium=boolean" in joined

    def test_skips_nullable_number(self, generator):
        flags = generator._datatype_flags("customers", exclude_cols={"id"})
        joined = " ".join(flags)
        assert "balance=" not in joined

    def test_returns_empty_for_unknown_table(self, generator):
        flags = generator._datatype_flags("nonexistent")
        assert flags == []

    def test_includes_non_nullable_number(self, generator):
        flags = generator._datatype_flags("orders", exclude_cols={"id", "customer_id", "referrer_id"})
        joined = " ".join(flags)
        assert "total=number" in joined

    def test_skips_nullable_referrer_id_number(self, generator):
        flags = generator._datatype_flags("orders", exclude_cols={"id"})
        joined = " ".join(flags)
        assert "referrer_id=" not in joined


class TestBuildDocCommand:
    def _normalize(self, cmd: str) -> str:
        return cmd.replace(" \\\n    ", " ")

    def test_uses_csv_type(self, generator):
        cmd = self._normalize(generator._build_doc_command("customers", "customers"))
        assert "--type csv" in cmd

    def test_translates_single_pk_to_key(self, generator):
        cmd = self._normalize(generator._build_doc_command("customers", "customers"))
        assert "--translate" in cmd
        assert "id=_key" in cmd

    def test_translates_mapped_fields(
        self, ecommerce_config, ecommerce_schema
    ):
        ecommerce_config.collections["customers"].field_mappings = {
            "name": "displayName"
        }
        gen = CsvImportGenerator(ecommerce_config, ecommerce_schema)
        cmd = self._normalize(
            gen._build_doc_command(
                "customers",
                "customers",
                field_mappings=ecommerce_config.collections[
                    "customers"
                ].field_mappings,
            )
        )
        assert "--translate name=displayName" in cmd

    def test_renamed_pk_is_kept_as_property_and_merged_into_key(
        self, ecommerce_config, ecommerce_schema
    ):
        ecommerce_config.collections["customers"].field_mappings = {
            "id": "customerId"
        }
        gen = CsvImportGenerator(ecommerce_config, ecommerce_schema)
        cmd = self._normalize(
            gen._build_doc_command(
                "customers",
                "customers",
                field_mappings=ecommerce_config.collections[
                    "customers"
                ].field_mappings,
            )
        )
        assert "--merge-attributes '_key=[id]'" in cmd
        assert "--translate id=customerId" in cmd
        assert "id=_key" not in cmd

    def test_forces_pk_to_string(self, generator):
        cmd = self._normalize(generator._build_doc_command("customers", "customers"))
        assert "id=string" in cmd

    def test_composite_pk_uses_merge_attributes(self, generator):
        cmd = self._normalize(generator._build_doc_command("order_items", "order_items"))
        assert "--merge-attributes" in cmd
        assert "[order_id]_[product_id]" in cmd

    def test_composite_pk_forces_string(self, generator):
        cmd = self._normalize(generator._build_doc_command("order_items", "order_items"))
        assert "order_id=string" in cmd
        assert "product_id=string" in cmd

    def test_contains_overwrite(self, generator):
        cmd = self._normalize(generator._build_doc_command("customers", "customers"))
        assert "--overwrite" in cmd

    def test_uses_on_duplicate(self, generator):
        cmd = self._normalize(generator._build_doc_command("customers", "customers"))
        assert "--on-duplicate" in cmd
        assert "replace" in cmd

    def test_includes_server_env_vars(self, generator):
        cmd = self._normalize(generator._build_doc_command("customers", "customers"))
        assert "$ARANGO_ENDPOINT" in cmd
        assert "$ARANGO_DB" in cmd


class TestBuildEdgeCommand:
    def _normalize(self, cmd: str) -> str:
        return cmd.replace(" \\\n    ", " ")

    def _edge_cmd(self, generator) -> str:
        return self._normalize(
            generator._build_edge_command("orders", "orders_to_customers", "orders", "customers", ["customer_id"])
        )

    def test_uses_csv_type(self, generator):
        assert "--type csv" in self._edge_cmd(generator)

    def test_translates_pk_to_from(self, generator):
        assert "id=_from" in self._edge_cmd(generator)

    def test_translates_fk_to_to(self, generator):
        assert "customer_id=_to" in self._edge_cmd(generator)

    def test_from_collection_prefix(self, generator):
        cmd = self._edge_cmd(generator)
        assert "--from-collection-prefix" in cmd
        assert "orders/" in cmd

    def test_to_collection_prefix(self, generator):
        cmd = self._edge_cmd(generator)
        assert "--to-collection-prefix" in cmd
        assert "customers/" in cmd

    def test_removes_non_structural_columns(self, generator):
        cmd = self._edge_cmd(generator)
        assert "--remove-attribute" in cmd
        assert "notes" in cmd

    def test_creates_edge_collection_type(self, generator):
        assert "--create-collection-type edge" in self._edge_cmd(generator)

    def test_forces_fk_to_string(self, generator):
        assert "customer_id=string" in self._edge_cmd(generator)

    def test_composite_fk_uses_merge_attributes(self, generator):
        cmd = self._normalize(
            generator._build_edge_command("orders", "test_edge", "orders", "targets", ["customer_id", "referrer_id"])
        )
        assert "--merge-attributes" in cmd
        assert "[customer_id]_[referrer_id]" in cmd
        assert "customer_id=string" in cmd
        assert "referrer_id=string" in cmd


class TestBuildGraphCreationArangosh:
    def test_contains_graph_name(self, generator):
        lines = generator._build_graph_creation_arangosh("my_graph")
        joined = "\n".join(lines)
        assert "my_graph" in joined

    def test_uses_arangosh(self, generator):
        lines = generator._build_graph_creation_arangosh("my_graph")
        joined = "\n".join(lines)
        assert "arangosh" in joined

    def test_contains_edge_definitions(self, generator):
        lines = generator._build_graph_creation_arangosh("my_graph")
        joined = "\n".join(lines)
        assert "orders_to_customers" in joined

    def test_contains_relation_call(self, generator):
        lines = generator._build_graph_creation_arangosh("my_graph")
        joined = "\n".join(lines)
        assert "graph._relation(" in joined

    def test_contains_create_call(self, generator):
        lines = generator._build_graph_creation_arangosh("my_graph")
        joined = "\n".join(lines)
        assert "graph._create(" in joined

    def test_contains_drop_call(self, generator):
        lines = generator._build_graph_creation_arangosh("my_graph")
        joined = "\n".join(lines)
        assert "graph._drop(" in joined

    def test_graph_name_is_quoted_for_shell_and_javascript(self, generator):
        graph_name = 'demo"); throw new Error("injected'

        echo_line, arangosh_line = generator._build_graph_creation_arangosh(
            graph_name
        )
        js_code = shlex.split(arangosh_line)[-1]

        assert shlex.split(echo_line) == [
            "echo",
            f"Creating named graph {graph_name}...",
        ]
        assert f"graph._create({json.dumps(graph_name)}, edgeDefs)" in js_code
        assert (
            f"print({json.dumps(f'Named graph {graph_name} created.')})"
            in js_code
        )


class TestGenerateCsvScript:
    def test_shebang_present(self, generator, tmp_path):
        path = str(tmp_path / "import.sh")
        content = generator.generate_csv_script(path)
        assert content.startswith("#!/usr/bin/env bash")

    def test_set_pipefail(self, generator, tmp_path):
        path = str(tmp_path / "import.sh")
        content = generator.generate_csv_script(path)
        assert "set -euo pipefail" in content

    def test_file_is_executable(self, generator, tmp_path):
        path = tmp_path / "import.sh"
        generator.generate_csv_script(str(path))
        mode = path.stat().st_mode
        assert mode & stat.S_IXUSR
        assert mode & stat.S_IXGRP
        assert mode & stat.S_IXOTH

    def test_file_written_to_disk(self, generator, tmp_path):
        path = tmp_path / "import.sh"
        generator.generate_csv_script(str(path))
        assert path.exists()
        content = path.read_text(encoding="utf-8")
        assert content.startswith("#!/usr/bin/env bash")

    def test_document_section(self, generator, tmp_path):
        path = str(tmp_path / "import.sh")
        content = generator.generate_csv_script(path)
        assert "Document collections" in content
        assert "customers" in content
        assert "orders" in content

    def test_edge_section(self, generator, tmp_path):
        path = str(tmp_path / "import.sh")
        content = generator.generate_csv_script(path)
        assert "Edge collections" in content
        assert "orders_to_customers" in content

    def test_no_graph_by_default(self, generator, tmp_path):
        path = str(tmp_path / "import.sh")
        content = generator.generate_csv_script(path)
        assert "Named graph" not in content

    def test_graph_included_when_specified(self, generator, tmp_path):
        path = str(tmp_path / "import.sh")
        content = generator.generate_csv_script(path, graph_name="my_graph")
        assert "Named graph" in content
        assert "my_graph" in content
        assert "arangosh" in content

    def test_env_defaults(self, generator, tmp_path):
        path = str(tmp_path / "import.sh")
        content = generator.generate_csv_script(path)
        assert "ARANGO_ENDPOINT" in content
        assert "ARANGO_DB" in content
        assert "ARANGO_USER" in content
        assert "ARANGO_PASSWORD" in content

    def test_uses_csv_not_jsonl(self, generator, tmp_path):
        path = str(tmp_path / "import.sh")
        content = generator.generate_csv_script(path)
        assert "--type csv" in content
        # Comments may name JSONL mode (the nullable-text warning points to
        # it); no command line may use it.
        commands = "\n".join(
            line for line in content.lower().splitlines()
            if not line.lstrip().startswith("#")
        )
        assert "jsonl" not in commands


class TestSilentRowLossGuard:
    """A discarded row must fail the script, not pass unnoticed."""

    def test_every_import_runs_through_the_guard(
        self, ecommerce_config, ecommerce_schema
    ):
        """arangoimport exits 0 after discarding rows, so `set -e` is blind.

        Measured on ArangoDB 3.12.4: an empty field in a column typed
        ``number`` makes arangoimport emit a malformed request body, discard
        that row, print it only as a warning, and exit 0. Fifty discarded rows
        still exit 0 as long as one row loaded. Its ``--max-errors`` option is
        not a usable guard either -- it returns non-zero only when every row
        failed.

        The printed summary is the reliable signal, so every invocation runs
        through ``run_import``, which fails when the count is not zero.
        """
        plan = CsvImportGenerator(
            ecommerce_config, ecommerce_schema, password="pw"
        ).build_plan(graph_name="demo")

        invocations = [
            line
            for line in plan.script.splitlines()
            if line.strip().startswith(("arangoimport", "run_import"))
        ]
        assert invocations, "the plan must contain import commands"
        unguarded = [line for line in invocations if not line.strip().startswith("run_import")]
        assert not unguarded, (
            f"every arangoimport invocation must run through run_import; "
            f"unguarded: {unguarded}"
        )
        assert "run_import() {" in plan.script, "the guard function must be defined"
        # The check must key off the summary line, not the exit status.
        assert "warnings/errors" in plan.script
        assert "exit 1" in plan.script

    def test_guard_is_defined_before_it_is_used(
        self, ecommerce_config, ecommerce_schema
    ):
        """A guard defined after the first call would be a runtime error."""
        plan = CsvImportGenerator(
            ecommerce_config, ecommerce_schema, password="pw"
        ).build_plan(graph_name="demo")
        definition = plan.script.index("run_import() {")
        # shlex.quote leaves a simple collection name unquoted.
        first_call = plan.script.index("\nrun_import ")
        assert definition < first_call, "run_import must be defined before use"

    def test_only_a_nullable_fk_edge_tolerates_null_fk_rejections(
        self, ecommerce_config, ecommerce_schema
    ):
        """orders.customer_id is NOT NULL; orders.referrer_id is nullable."""
        script = CsvImportGenerator(
            ecommerce_config, ecommerce_schema, password="pw"
        ).render_script()

        assert "run_import orders_to_customers strict " in script
        assert "run_import orders_to_customers_referrer_id null-fk-ok " in script
        for table in ("customers", "orders", "products", "order_items"):
            assert f"run_import {table} strict " in script


# Lines copied verbatim from arangoimport 3.12.11 output (an edge import from
# a CSV dump whose nullable FK column holds NULLs and one illegal key).
_REJECTED_NULL_FK = (
    "2026-10-17T01:20:58.219443Z [2202-2] WARNING [e5a29] {general} at position 2: "
    "creating document failed with error 'expecting both `_from` and `_to` attributes "
    "to be defined in the edge document and have the format "
    "`<collectionName>/<vertexKey>` ', offending document: "
    '{"_from":"orders/1001","_to":"customers/"}'
)
_REJECTED_ILLEGAL_KEY = (
    "2026-10-17T01:20:58.219431Z [2202-2] WARNING [e5a29] {general} at position 0: "
    "creating document failed with error 'expecting both `_from` and `_to` attributes "
    "to be defined in the edge document and have the format "
    "`<collectionName>/<vertexKey>` ', offending document: "
    '{"_from":"orders/3000","_to":"customers/a b"}'
)
_DISCARDED_ROW = (
    "2026-10-17T00:14:46.967680Z [653-2] WARNING [e5a29] {general} at position 3: "
    "invalid JSON type (expecting object, probably parse error), "
    'offending context: ["2",]'
)


def _summary(created: int, errors: int) -> str:
    return (
        f"created:          {created}\n"
        f"warnings/errors:  {errors}\n"
        "updated/replaced: 0\n"
        "ignored:          0\n"
    )


def _run_guard(
    output: str, *, policy: str = "strict", status: int = 0
) -> subprocess.CompletedProcess:
    """Run the generated run_import against a stand-in arangoimport.

    The stand-in prints ``output`` and exits ``status``, exactly as the real
    tool does at the end of an import, so this exercises the shell the
    generated script runs rather than looking for text in it.
    """
    script = "\n".join([
        "set -euo pipefail",
        IMPORT_GUARD,
        f"fake_arangoimport() {{ printf '%s\\n' {shlex.quote(output)}; return {status}; }}",
        f"run_import demo {policy} fake_arangoimport",
        'echo "SCRIPT CONTINUED"',
    ])
    return subprocess.run(
        ["bash", "-c", script], capture_output=True, text=True, timeout=30
    )


@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
class TestGuardBehaviour:
    """What run_import does, run in bash against arangoimport's real output."""

    def test_clean_import_continues(self):
        result = _run_guard(_summary(3, 0))
        assert result.returncode == 0
        assert "SCRIPT CONTINUED" in result.stdout

    def test_discarded_row_stops_the_script(self):
        result = _run_guard(_DISCARDED_ROW + "\n" + _summary(2, 1))
        assert result.returncode == 1
        assert "SCRIPT CONTINUED" not in result.stdout
        assert "FAILED: demo discarded 1 row(s)" in result.stderr

    def test_null_fk_rejection_fails_under_strict(self):
        """A NOT NULL FK column must never produce a bare-prefix edge."""
        result = _run_guard(_REJECTED_NULL_FK + "\n" + _summary(2, 1))
        assert result.returncode == 1

    def test_null_fk_rejections_beyond_the_log_cap_are_skipped(self):
        """arangoimport logs 20 of 150 rejections; all logged are NULL FKs."""
        output = "\n".join([_REJECTED_NULL_FK] * 20) + "\n" + _summary(150, 150)
        result = _run_guard(output, policy="null-fk-ok")
        assert result.returncode == 0
        assert "SCRIPT CONTINUED" in result.stdout
        assert "skipped 150 edge(s) whose foreign key is NULL" in result.stdout

    def test_any_other_logged_rejection_fails_a_nullable_fk_edge(self):
        output = "\n".join(
            [_REJECTED_ILLEGAL_KEY] + [_REJECTED_NULL_FK] * 19
        ) + "\n" + _summary(150, 151)
        result = _run_guard(output, policy="null-fk-ok")
        assert result.returncode == 1
        assert "FAILED: demo discarded 151 row(s)" in result.stderr

    def test_errors_with_nothing_logged_fail(self):
        """Rejections that cannot be classified are not assumed harmless."""
        result = _run_guard(_summary(2, 1), policy="null-fk-ok")
        assert result.returncode == 1

    def test_missing_summary_fails_closed(self):
        result = _run_guard("Starting CSV import...")
        assert result.returncode == 1
        assert "no 'warnings/errors:' summary" in result.stderr

    def test_non_zero_exit_fails(self):
        result = _run_guard(_summary(0, 0), status=1)
        assert result.returncode == 1
        assert "arangoimport returned non-zero" in result.stderr


@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
def test_baked_in_password_reaches_the_client_tool(
    ecommerce_config, ecommerce_schema
):
    """@ARANGO_PASSWORD@ is read from the tool's environment, so it must be exported.

    Measured on 3.12.11: without the export, a password baked into the script
    is a shell variable only, never reaches arangoimport, and the import fails
    with HTTP 401.
    """
    script = CsvImportGenerator(
        ecommerce_config, ecommerce_schema, password="baked-pw"
    ).render_script()
    preamble = script[: script.index("# === Document collections ===")]
    probe = preamble + '\nbash -c \'printf "%s" "${ARANGO_PASSWORD-UNSET}"\'\n'
    env = {k: v for k, v in os.environ.items() if k != "ARANGO_PASSWORD"}

    result = subprocess.run(
        ["bash", "-c", probe], capture_output=True, text=True, env=env, timeout=30
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == "baked-pw"


class TestStructuredCsvPreview:
    def test_plan_groups_commands_and_matches_cli_renderer(
        self, generator, tmp_path
    ):
        plan = generator.build_plan(graph_name="ecom_graph")
        written = generator.generate_csv_script(
            str(tmp_path / "import.sh"),
            graph_name="ecom_graph",
        )

        assert len(plan.documents) == 4
        assert len(plan.edges) == 2
        for item in (*plan.documents, *plan.edges):
            assert item.command in written

    def test_overwrite_argument_is_honored(self, generator, tmp_path):
        content = generator.generate_csv_script(
            str(tmp_path / "import.sh"),
            overwrite_on_initial=False,
        )
        assert "--overwrite \\\n    false" in content
        assert "--overwrite \\\n    true" not in content

    def test_secret_safe_plan_and_fixed_csv_root(
        self, ecommerce_config, ecommerce_schema
    ):
        generator = CsvImportGenerator(
            ecommerce_config,
            ecommerce_schema,
            endpoint="http://admin:sentinel-target-secret@localhost:8529",
            password="sentinel-password",
        )
        plan = generator.build_plan()
        assert plan.data_dir == "./dumps"
        assert "./dumps/customers.csv" in plan.documents[0].command
        assert "sentinel-password" not in plan.script
        assert "sentinel-target-secret" not in plan.script
        # The password is referenced as @ARANGO_PASSWORD@, resolved from the
        # environment by the client tool itself. A shell expansion would put
        # the secret in the process list for the duration of the import.
        assert "@ARANGO_PASSWORD@" in plan.script
        assert '--server.password "$ARANGO_PASSWORD"' not in plan.script

    def test_custom_artifact_root_is_shell_quoted(
        self, ecommerce_config, ecommerce_schema
    ):
        artifact_dir = "./csv artifacts;touch should-not-run"
        plan = CsvImportGenerator(
            ecommerce_config,
            ecommerce_schema,
            data_dir=artifact_dir,
        ).build_plan()
        command = shlex.split(
            plan.documents[0].command.replace(" \\\n    ", " ")
        )

        assert command[command.index("--file") + 1] == (
            f"{artifact_dir}/customers.csv"
        )

    def test_graph_artifact_matches_jsonl_shell_contract(
        self, ecommerce_config, ecommerce_schema
    ):
        plan = CsvImportGenerator(
            ecommerce_config,
            ecommerce_schema,
            endpoint="http://admin:target-secret@localhost:8529",
            password="password-secret",
        ).build_plan(graph_name="demo")

        assert plan.graph_script.startswith("#!/usr/bin/env bash")
        assert "arangosh" in plan.graph_script
        assert "@ARANGO_PASSWORD@" in plan.graph_script
        assert '--server.password "$ARANGO_PASSWORD"' not in plan.graph_script
        assert "password-secret" not in plan.graph_script
        assert "target-secret" not in plan.graph_script

    def test_field_renames_emit_translate_without_warning(
        self, ecommerce_config, ecommerce_schema
    ):
        ecommerce_config.collections["customers"].field_mappings = {
            "name": "display_name"
        }
        plan = CsvImportGenerator(
            ecommerce_config, ecommerce_schema
        ).build_plan()
        # The only warning is the nullable-text one, which the fixture's
        # customers.email and orders.notes always produce.
        assert all("nullable text columns" in w for w in plan.warnings)
        command = plan.documents[0].command.replace(" \\\n    ", " ")
        assert "--translate name=display_name" in command


    def test_renamed_collections_drive_edge_prefixes(
        self, ecommerce_config, ecommerce_schema
    ):
        ecommerce_config.collections[
            "orders"
        ].target_collection = "Purchase"
        ecommerce_config.collections[
            "customers"
        ].target_collection = "Customer"
        plan = CsvImportGenerator(
            ecommerce_config, ecommerce_schema
        ).build_plan()
        edge = plan.edges[0]
        assert "Purchase/" in edge.command
        assert "Customer/" in edge.command
        assert '"Purchase"' in plan.graph_script
        assert '"Customer"' in plan.graph_script

    def test_lpg_preview_is_rejected(
        self, ecommerce_config, ecommerce_schema
    ):
        config = ecommerce_config.model_copy(update={"graph_layout": "lpg"})
        with pytest.raises(ValueError, match="does not support LPG"):
            CsvImportGenerator(config, ecommerce_schema).build_plan()


class TestFromSampleSchema:
    def test_generate_from_auto_config(self, ecommerce_schema, tmp_path):
        config = ConfigManager.generate_default_config(ecommerce_schema)
        gen = CsvImportGenerator(config, ecommerce_schema)
        path = str(tmp_path / "import.sh")
        content = gen.generate_csv_script(path, graph_name="ecom_graph")
        assert "customers" in content
        assert "orders" in content
        assert "ecom_graph" in content
        assert "arangosh" in content


class TestCsvDirectRefusesUnfaithfulMappings:
    """CSV-direct loads every CSV column as it stands, so a mapping feature it
    cannot reproduce must refuse, never load the column unmasked or unfiltered."""

    def _refusal(self, config, schema) -> str:
        gen = CsvImportGenerator(config, schema)
        with pytest.raises(CsvDirectUnsupportedError) as plan_err:
            gen.build_plan()
        with pytest.raises(CsvDirectUnsupportedError) as script_err:
            gen.render_script()
        assert str(plan_err.value) == str(script_err.value)
        return str(plan_err.value)

    @pytest.mark.parametrize("kind", ["redact", "nullify", "hash", "tokenize"])
    def test_masked_column_is_refused(self, ecommerce_config, ecommerce_schema, kind):
        ecommerce_config.collections["customers"].field_expressions = [
            make_mask_expression("email", kind)
        ]
        msg = self._refusal(ecommerce_config, ecommerce_schema)
        assert "customers: field expressions (email)" in msg
        assert "JSONL mode" in msg

    def test_excluded_column_is_refused(self, ecommerce_config, ecommerce_schema):
        ecommerce_config.collections["customers"].exclude_fields = ["email", "balance"]
        msg = self._refusal(ecommerce_config, ecommerce_schema)
        assert "customers: excluded fields (balance, email)" in msg

    def test_include_filter_is_refused(self, ecommerce_config, ecommerce_schema):
        ecommerce_config.collections["customers"].include_fields = ["id", "name"]
        msg = self._refusal(ecommerce_config, ecommerce_schema)
        assert "customers: an include-field filter" in msg

    def test_computed_field_is_refused(self, ecommerce_config, ecommerce_schema):
        ecommerce_config.collections["customers"].field_expressions = [
            FieldExpression(
                target="label", sources=["name", "email"],
                expression='CONCAT(@name, " <", @email, ">")',
            )
        ]
        msg = self._refusal(ecommerce_config, ecommerce_schema)
        assert "customers: field expressions (label)" in msg

    def test_type_overrides_are_refused(self, ecommerce_config, ecommerce_schema):
        ecommerce_config.type_overrides = {"numeric": "string"}
        msg = self._refusal(ecommerce_config, ecommerce_schema)
        assert "mapping-level type overrides (numeric)" in msg

    def test_every_problem_is_named_at_once(self, ecommerce_config, ecommerce_schema):
        ecommerce_config.collections["customers"].exclude_fields = ["email"]
        ecommerce_config.collections["orders"].field_expressions = [
            make_mask_expression("notes", "redact")
        ]
        msg = self._refusal(ecommerce_config, ecommerce_schema)
        assert "customers: excluded fields (email)" in msg
        assert "orders: field expressions (notes)" in msg

    def test_identity_expression_that_changes_nothing_is_allowed(
        self, ecommerce_config, ecommerce_schema
    ):
        ecommerce_config.collections["customers"].field_expressions = [
            FieldExpression(target="name", sources=["name"]),
            FieldExpression(target="email"),
        ]
        plan = CsvImportGenerator(ecommerce_config, ecommerce_schema).build_plan()
        # The only warning is the nullable-text one, which the fixture's
        # customers.email and orders.notes always produce.
        assert all("nullable text columns" in w for w in plan.warnings)

    def test_identity_rename_expression_is_refused(
        self, ecommerce_config, ecommerce_schema
    ):
        # A rename belongs in field_mappings (reproduced with --translate); as a
        # field expression CSV-direct would load it under the source name.
        ecommerce_config.collections["customers"].field_expressions = [
            FieldExpression(target="display_name", sources=["name"])
        ]
        msg = self._refusal(ecommerce_config, ecommerce_schema)
        assert "customers: field expressions (display_name)" in msg


class TestTextColumns:
    """Text columns: typed when NOT NULL, named when nullable.

    Measured on arangoimport 3.12.11. Untyped, a text value that looks like a
    number is converted (00789 is stored as 789). Typed ``string``, an empty
    field becomes "" rather than a missing value, which is right only when the
    column cannot hold NULL.
    """

    def test_required_text_columns_are_typed_string(self, generator):
        # Flatten the backslash-continued commands to one line each.
        commands = {
            spec.source_table: " ".join(spec.command.replace("\\\n", " ").split())
            for spec in generator.build_document_specs()
        }

        assert "--datatype name=string" in commands["customers"]
        assert "--datatype name=string" in commands["products"]
        # Nullable text stays untyped: typed, a NULL would load as "".
        assert "email=string" not in commands["customers"]
        assert "notes=string" not in commands["orders"]
        # The primary key is still typed once, as itself.
        assert commands["customers"].count("--datatype id=string") == 1

    def test_nullable_text_columns_are_named_in_script_and_plan(self, generator):
        assert generator.nullable_text_columns() == ("customers.email", "orders.notes")

        plan = generator.build_plan()
        (warning,) = plan.warnings
        assert "customers.email, orders.notes" in warning
        assert "00789 is stored as 789" in warning
        assert f"# WARNING: {warning}" in plan.script

    def test_no_warning_when_every_text_column_is_required(self, ecommerce_config, ecommerce_schema):
        for table in ecommerce_schema.tables.values():
            for col in table.columns:
                if col.data_type == "text":
                    col.is_nullable = False

        plan = CsvImportGenerator(ecommerce_config, ecommerce_schema).build_plan()

        assert plan.warnings == ()
        assert "# WARNING" not in plan.script
