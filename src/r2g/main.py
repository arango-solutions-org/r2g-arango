from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, List, Optional

import typer
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, SpinnerColumn, TextColumn
from rich.syntax import Syntax
from rich.table import Table as RichTable

from r2g.config import ConfigManager
from r2g.generators.arangoimport import ArangoImportGenerator, CsvImportGenerator
from r2g.generators.visualizer import MappingVisualizer
from r2g.input.dump_reader import DumpReader
from r2g.log import get_logger, setup_logging
from r2g.transformers.edge_transformer import EdgeTransformer
from r2g.transformers.node_transformer import NodeTransformer
from r2g.types import EdgeDefinition, Schema

app = typer.Typer(help="R2G-ETL: Relational to Graph Pipeline")
source_app = typer.Typer(help="Manage data sources")
project_app = typer.Typer(help="Manage projects")
catalog_app = typer.Typer(help="Connect to external data catalogs (discovery)")
entitlements_app = typer.Typer(help="Governance: classification entitlement reports (Phase 9)")
ontology_app = typer.Typer(help="LLM-assisted ontology derivation (Phase 10)")
shared_keys_app = typer.Typer(help="Cross-source shared-key (join key) inference (P6.7)")
forge_app = typer.Typer(
    help="Federation Forge: reverse generation — ontology -> schema + data (ADR-0006)"
)
app.add_typer(source_app, name="source")
app.add_typer(project_app, name="project")
app.add_typer(catalog_app, name="catalog")
app.add_typer(entitlements_app, name="entitlements")
app.add_typer(ontology_app, name="ontology")
app.add_typer(shared_keys_app, name="shared-keys")
app.add_typer(forge_app, name="forge")
console = Console()
log = get_logger(__name__)


def _version_callback(value: bool) -> None:
    if value:
        from r2g import __version__

        console.print(f"r2g {__version__}")
        raise typer.Exit()


def _bearer_guard(asgi_app: Any, token: str) -> Any:
    """Wrap an ASGI app so every HTTP request must carry ``Bearer <token>``.

    A pure-ASGI guard (not Starlette's BaseHTTPMiddleware) so it does not
    buffer the SSE response stream. Used to protect the network-exposed MCP
    SSE transport.
    """
    expected = f"Bearer {token}".encode()

    async def guarded(scope: dict, receive: Any, send: Any) -> None:
        if scope.get("type") == "http":
            headers = dict(scope.get("headers") or [])
            if headers.get(b"authorization", b"") != expected:
                from starlette.responses import JSONResponse

                await JSONResponse({"detail": "Unauthorized"}, status_code=401)(scope, receive, send)
                return
        await asgi_app(scope, receive, send)

    return guarded


@app.callback()
def main(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
    json_log: bool = typer.Option(False, "--json-log", help="Output logs as JSON"),
    env_file: Optional[str] = typer.Option(
        None, "--env-file", help="Path to .env file (default: auto-detect .env in cwd)"
    ),
    version: bool = typer.Option(
        False, "--version", help="Show the r2g version and exit", callback=_version_callback, is_eager=True
    ),
) -> None:
    from dotenv import load_dotenv

    load_dotenv(env_file or ".env", override=False)
    setup_logging(level="DEBUG" if verbose else "INFO", json_output=json_log)


@app.command("validate-data")
def validate_data_cmd(
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    config_path: str = typer.Option(..., "--config", "-c", help="Mapping config YAML"),
    dumps_dir: str = typer.Option(..., "--data-dir", help="Directory containing CSV dump files"),
    file_pattern: str = typer.Option("*.csv", "--file-pattern", help="Glob pattern for dump files"),
    max_issues: int = typer.Option(100, "--max-issues", help="Max issues to report per FK relationship"),
) -> None:
    """Validate referential integrity of dump data before import.

    Reads CSV dump files, builds PK lookup sets per table, then checks
    every FK column value to ensure the referenced PK exists in the
    target table's dump. Reports orphaned references that would produce
    broken edges in ArangoDB.
    """
    from r2g.data_validator import validate_data

    try:
        schema = Schema.load_from_file(schema_file)
        mapping = ConfigManager.load_config(config_path)
        report = validate_data(schema, mapping, dumps_dir, file_pattern, max_issues)

        console.print(
            f"[dim]Scanned {report.rows_scanned:,} rows across "
            f"{report.tables_checked} tables, {report.fk_checks:,} FK checks, "
            f"{report.pk_sets_built} PK sets built[/dim]\n"
        )

        if report.is_clean:
            console.print("[green]Data integrity check passed — no orphaned FK references found.[/green]")
        else:
            summary = report.summary_by_fk()
            table = RichTable(title=f"Orphaned FK References ({len(report.issues)} issues)")
            table.add_column("FK Relationship", style="cyan")
            table.add_column("Orphans", justify="right", style="red")
            for fk_label, count in sorted(summary.items(), key=lambda x: -x[1]):
                table.add_row(fk_label, f"{count:,}")
            console.print(table)

            console.print(f"\n[yellow]Sample orphaned values (first {min(10, len(report.issues))}):[/yellow]")
            for issue in report.issues[:10]:
                console.print(
                    f"  [red]![/red] {issue.source_table}.{issue.fk_column} "
                    f"row {issue.row_number}: value '{issue.orphan_value}' "
                    f"not in {issue.target_table} PKs"
                )
            if len(report.issues) > 10:
                console.print(f"  [dim]... and {len(report.issues) - 10} more[/dim]")

            raise typer.Exit(code=1)

    except typer.Exit:
        raise
    except Exception as e:
        log.exception("validate_data_failed")
        console.print(f"[red]Data validation failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("validate-config")
def validate_config_cmd(
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    config_path: str = typer.Option(..., "--config", "-c", help="Mapping config YAML"),
) -> None:
    """Validate a mapping config against a schema file.

    Checks that every collection references a known table, every edge
    references valid collections and columns, and field lists only name
    columns that exist in the source table.
    """
    from r2g.config import validate_config

    try:
        schema = Schema.load_from_file(schema_file)
        mapping = ConfigManager.load_config(config_path)
        issues = validate_config(schema, mapping)
        if issues:
            console.print(f"[red]Found {len(issues)} issue(s):[/red]")
            for issue in issues:
                console.print(f"  [yellow]•[/yellow] {issue}")
            raise typer.Exit(code=1)
        n_coll = len(mapping.collections)
        n_edge = len(mapping.edges)
        console.print(
            f"[green]Config valid![/green] "
            f"{n_coll} collections, {n_edge} edges — all references resolve."
        )
    except typer.Exit:
        raise
    except Exception as e:
        log.exception("validate_config_failed")
        console.print(f"[red]Validation failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("generate-config")
def generate_config(
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    output: str = typer.Option("mapping.yaml", "--output", "-o", help="Output YAML path"),
) -> None:
    """Generate a default mapping config from a schema file."""
    try:
        schema = Schema.load_from_file(schema_file)
        config = ConfigManager.generate_default_config(schema)
        ConfigManager.save_config(config, output)
        n_collections = len(config.collections)
        n_edges = len(config.edges)
        console.print(
            f"[green]Wrote mapping config to[/green] [bold]{output}[/bold] "
            f"([bold]{n_collections}[/bold] collections, [bold]{n_edges}[/bold] edges)."
        )
    except Exception as e:
        log.exception("generate_config_failed", output=output)
        console.print(f"[red]Failed to generate config:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("visualize-mapping")
def visualize_mapping(
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    config_path: str = typer.Option(..., "--config", "-c", help="Mapping config YAML"),
    output: str = typer.Option("mapping_viz.html", "--output", "-o", help="Output HTML file"),
    no_open: bool = typer.Option(False, "--no-open", help="Do not open in default browser"),
) -> None:
    """Generate an interactive HTML visualization of the PG-to-graph mapping."""
    try:
        schema = Schema.load_from_file(schema_file)
        mapping = ConfigManager.load_config(config_path)
        viz = MappingVisualizer(schema, mapping)
        viz.generate(output)
        console.print(f"[green]Wrote mapping visualization:[/green] [bold]{output}[/bold]")
        console.print(
            f"  [dim]{len(schema.tables)} tables → "
            f"{len(mapping.collections)} collections, "
            f"{len(mapping.edges)} edges[/dim]"
        )
        if not no_open:
            import webbrowser
            webbrowser.open(f"file://{Path(output).resolve()}")
    except Exception as e:
        log.exception("visualize_mapping_failed", output=output)
        console.print(f"[red]Failed to generate visualization:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("transform-edges")
def transform_edges(
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    config_path: str = typer.Option(..., "--config", "-c", help="Mapping config YAML"),
    table_name: str = typer.Option(..., "--table", "-t", help="Source table name"),
    dump_file: str = typer.Option(..., "--input", "-i", help="Input dump file"),
    output_file: str = typer.Option(..., "--output", "-o", help="Output JSONL file (base name if multiple edges)"),
    limit: Optional[int] = typer.Option(None, help="Max rows to process"),
) -> None:
    """Transform a table dump into ArangoDB edge documents (JSONL)."""
    try:
        schema = Schema.load_from_file(schema_file)
        if table_name not in schema.tables:
            raise ValueError(f"Table '{table_name}' not found in schema.")
        table_def = schema.tables[table_name]

        mapping = ConfigManager.load_config(config_path)
        matching: list[EdgeDefinition] = [e for e in mapping.edges if e.from_collection == table_name]
        if not matching:
            raise ValueError(
                f"No edge definitions with from_collection='{table_name}' in mapping config."
            )

        reader = DumpReader(dump_file)
        out_path = Path(output_file)
        target_by_source = ConfigManager.target_by_source_table(mapping)
        transformers = [
            (
                e,
                EdgeTransformer(
                    e,
                    table_def,
                    key_separator=mapping.key_separator,
                    from_name=target_by_source.get(e.from_collection),
                    to_name=target_by_source.get(e.to_collection),
                ),
            )
            for e in matching
        ]

        if len(matching) == 1:
            out_paths = [(matching[0], out_path)]
        else:
            base = out_path.stem
            parent = out_path.parent
            suffix = out_path.suffix if out_path.suffix else ".jsonl"
            out_paths = [
                (e, parent / f"{base}_{e.edge_collection}{suffix}") for e in matching
            ]

        handles: list[tuple[EdgeDefinition, Any]] = []
        try:
            for e, path in out_paths:
                path.parent.mkdir(parents=True, exist_ok=True)
                handles.append((e, path.open("w", encoding="utf-8")))

            handle_map = {e.edge_collection: h for e, h in handles}
            counts: dict[str, int] = {e.edge_collection: 0 for e in matching}
            row_num = 0
            for row in reader.read_rows():
                for edge_def, transformer in transformers:
                    doc = transformer.transform_row(row)
                    if doc is not None:
                        handle_map[edge_def.edge_collection].write(json.dumps(doc) + "\n")
                        counts[edge_def.edge_collection] += 1
                row_num += 1
                if limit is not None and row_num >= limit:
                    break
        finally:
            for _, h in handles:
                h.close()

        for e, path in out_paths:
            console.print(
                f"[green]Wrote[/green] [bold]{counts[e.edge_collection]}[/bold] edges "
                f"to [bold]{path}[/bold] ([cyan]{e.edge_collection}[/cyan])."
            )
    except Exception as e:
        log.exception("transform_edges_failed", table=table_name)
        console.print(f"[red]Edge transformation failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("transform-all")
def transform_all(
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    config_path: str = typer.Option(..., "--config", "-c", help="Mapping config YAML"),
    input_dir: str = typer.Option(..., "--input-dir", help="Directory of dump files (one per table)"),
    output_dir: str = typer.Option("./output", "--output-dir", help="Output directory for JSONL files"),
    file_pattern: str = typer.Option("*.csv", "--file-pattern", help="Glob pattern for input files"),
) -> None:
    """Transform an entire dataset: node JSONL per table, then edge JSONL per edge definition."""
    summary: list[tuple[str, str, int]] = []

    try:
        schema = Schema.load_from_file(schema_file)
        mapping = ConfigManager.load_config(config_path)
        in_dir = Path(input_dir)
        out_root = Path(output_dir)
        out_root.mkdir(parents=True, exist_ok=True)

        if not in_dir.is_dir():
            raise ValueError(f"Input directory does not exist or is not a directory: {input_dir}")

        dump_by_table: dict[str, Path] = {}
        for fpath in sorted(in_dir.glob(file_pattern)):
            if fpath.is_file():
                dump_by_table[fpath.stem] = fpath

        doc_jobs: list[tuple[str, str, Path, Any, Any]] = []
        for _key, cm in mapping.collections.items():
            if cm.collection_type != "document":
                continue
            st = cm.source_table
            if st not in schema.tables:
                log.warning("transform_all_skip_unknown_table", source_table=st)
                continue
            if st not in dump_by_table:
                log.warning("transform_all_skip_no_dump", source_table=st)
                continue
            doc_jobs.append((st, cm.target_collection, dump_by_table[st], schema.tables[st], cm))

        edge_jobs: list[tuple[EdgeDefinition, Path, Any]] = []
        for edge in mapping.edges:
            src = edge.from_collection
            if src not in schema.tables:
                log.warning("transform_all_edge_unknown_table", from_collection=src)
                continue
            if src not in dump_by_table:
                log.warning("transform_all_edge_no_dump", from_collection=src)
                continue
            edge_jobs.append((edge, dump_by_table[src], schema.tables[src]))

        total_steps = len(doc_jobs) + len(edge_jobs)
        if total_steps == 0:
            console.print(
                "[yellow]No document or edge transforms to run "
                "(check mapping config, schema, and input files).[/yellow]"
            )
            return

        progress_cols = (
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
        )

        with Progress(*progress_cols, console=console) as progress:
            task_id = progress.add_task("Transforming dataset", total=total_steps)

            for table_name, target_coll, dump_path, table_def, col_mapping in doc_jobs:
                progress.update(task_id, description=f"Nodes: {target_coll}")
                out_file = out_root / f"{target_coll}.jsonl"
                transformer = NodeTransformer(
                    table_def,
                    collection_mapping=col_mapping,
                    key_separator=mapping.key_separator,
                    type_overrides=mapping.type_overrides,
                )
                reader = DumpReader(str(dump_path))
                n = 0
                with out_file.open("w", encoding="utf-8") as f_out:
                    for row in reader.read_rows():
                        doc = transformer.transform_row(row)
                        f_out.write(json.dumps(doc) + "\n")
                        n += 1
                summary.append((target_coll, "document", n))
                progress.advance(task_id)

            target_by_source = ConfigManager.target_by_source_table(mapping)
            for edge, dump_path, table_def in edge_jobs:
                progress.update(task_id, description=f"Edges: {edge.edge_collection}")
                out_file = out_root / f"{edge.edge_collection}.jsonl"
                et = EdgeTransformer(
                    edge,
                    table_def,
                    key_separator=mapping.key_separator,
                    from_name=target_by_source.get(edge.from_collection),
                    to_name=target_by_source.get(edge.to_collection),
                )
                reader = DumpReader(str(dump_path))
                n = 0
                with out_file.open("w", encoding="utf-8") as f_out:
                    for row in reader.read_rows():
                        edge_doc = et.transform_row(row)
                        if edge_doc is not None:
                            f_out.write(json.dumps(edge_doc) + "\n")
                            n += 1
                summary.append((edge.edge_collection, "edge", n))
                progress.advance(task_id)

        table = RichTable(title="Transform summary")
        table.add_column("Collection", style="cyan")
        table.add_column("Type", style="magenta")
        table.add_column("Rows", justify="right")
        for name, kind, rows in summary:
            table.add_row(name, kind, str(rows))
        console.print(table)
        console.print("[green]Transform complete.[/green]")
    except Exception as e:
        log.exception("transform_all_failed")
        console.print(f"[red]Full transform failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("generate-import")
def generate_import(
    config_path: str = typer.Option(..., "--config", "-c", help="Mapping config YAML"),
    data_dir: str = typer.Option(..., "--data-dir", help="Directory containing JSONL files"),
    output: str = typer.Option("import.sh", "--output", "-o", help="Output shell script path"),
    endpoint: str = typer.Option(
        "http://127.0.0.1:8529", "--endpoint", help="ArangoDB endpoint URL", envvar="ARANGO_ENDPOINT"
    ),
    database: str = typer.Option("_system", "--database", "-d", help="Database name", envvar="ARANGO_DB"),
    username: str = typer.Option("root", "--username", "-u", help="ArangoDB username", envvar="ARANGO_USER"),
    password: str = typer.Option("", "--password", "-p", help="ArangoDB password", envvar="ARANGO_PASSWORD"),
    on_duplicate: str = typer.Option("ignore", "--on-duplicate", help="arangoimport --on-duplicate value"),
    graph_name: Optional[str] = typer.Option(None, "--graph-name", help="If set, also write a graph creation AQL file"),
) -> None:
    """Generate an arangoimport shell script (and optional arangosh graph creation script)."""
    try:
        mapping = ConfigManager.load_config(config_path)
        gen = ArangoImportGenerator(
            mapping,
            endpoint,
            database,
            username,
            password,
            data_dir,
            on_duplicate,
        )
        gen.generate_script(output, overwrite_on_initial=True)
        console.print(f"[green]Wrote import script:[/green] [bold]{output}[/bold]")
        if graph_name:
            graph_script = gen.generate_create_graph_aql(graph_name)
            out_p = Path(output)
            graph_path = out_p.with_name(f"{out_p.stem}_graph_{graph_name}.js")
            graph_path.write_text(graph_script, encoding="utf-8")
            console.print(
                f"[green]Wrote arangosh graph creation script:[/green] [bold]{graph_path}[/bold]"
            )
    except Exception as e:
        log.exception("generate_import_failed", output=output)
        console.print(f"[red]Failed to generate import script:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("generate-csv-import")
def generate_csv_import(
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    config_path: str = typer.Option(..., "--config", "-c", help="Mapping config YAML"),
    data_dir: str = typer.Option(..., "--data-dir", help="Directory containing PG CSV dump files"),
    output: str = typer.Option("import_csv.sh", "--output", "-o", help="Output shell script path"),
    endpoint: str = typer.Option(
        "http://127.0.0.1:8529", "--endpoint", help="ArangoDB endpoint URL", envvar="ARANGO_ENDPOINT"
    ),
    database: str = typer.Option("_system", "--database", "-d", help="Database name", envvar="ARANGO_DB"),
    username: str = typer.Option("root", "--username", "-u", help="ArangoDB username", envvar="ARANGO_USER"),
    password: str = typer.Option("", "--password", "-p", help="ArangoDB password", envvar="ARANGO_PASSWORD"),
    on_duplicate: str = typer.Option("replace", "--on-duplicate", help="arangoimport --on-duplicate value"),
    graph_name: Optional[str] = typer.Option(
        None, "--graph-name", help="Create a named graph after import via Gharial API"
    ),
) -> None:
    """Generate an arangoimport script that imports PG CSV dumps directly.

    Uses arangoimport --type csv with --translate for key remapping,
    --datatype for type coercion, --from-collection-prefix / --to-collection-prefix
    for edge generation, and --remove-attribute for column projection.
    No intermediate JSONL transformation required.
    """
    try:
        schema = Schema.load_from_file(schema_file)
        mapping = ConfigManager.load_config(config_path)
        gen = CsvImportGenerator(
            mapping,
            schema,
            endpoint,
            database,
            username,
            password,
            data_dir,
            on_duplicate,
        )
        gen.generate_csv_script(output, overwrite_on_initial=True, graph_name=graph_name)
        console.print(f"[green]Wrote CSV import script:[/green] [bold]{output}[/bold]")
        console.print(
            f"  [dim]{len(mapping.collections)} document collections, "
            f"{len(mapping.edges)} edge collections[/dim]"
        )
        if graph_name:
            console.print(f"  [dim]Named graph '{graph_name}' will be created after import[/dim]")
        console.print(
            "  [dim]Imports PG CSV dumps directly — no JSONL transformation needed[/dim]"
        )
    except Exception as e:
        log.exception("generate_csv_import_failed", output=output)
        console.print(f"[red]Failed to generate CSV import script:[/red] {e}")
        raise typer.Exit(code=1)


_CURATED_LABELS_HELP = (
    "Curated-labels JSON (e.g. exported from arango-ontoextract's lexicon): the "
    "curator's names for entities, properties and relationships, keyed by the "
    "names r2g generates. Applied identically by export-csi and export-r2rml."
)
_ALLOW_UNAPPLIED_HELP = (
    "Write the output even when a curated label is refused or matches nothing. "
    "Without it the export fails, so a curator's decision is never silently missing."
)


def _report_curated_plan(plan) -> bool:
    """Print what the curated labels did; True when every entry was applied."""
    applied = [r for r in plan.records if r["resolution"] == "applied"]
    if applied:
        console.print(f"[green]Applied {len(applied)} curated label(s).[/green]")
    for r in plan.problems:
        where = f"{r['entity']}.{r['name']}" if r["kind"] == "property" else r["name"]
        console.print(
            f"[red]Curated {r['kind']} {where} -> {r['label']!r} {r['resolution']}:[/red] "
            f"[dim]{r.get('reason', '')}[/dim]"
        )
    return not plan.problems


@app.command("export-csi")
def export_csi(
    config_path: str = typer.Option(..., "--config", "-c", help="Mapping config YAML"),
    schema_file: Optional[str] = typer.Option(
        None, "--schema", "-s", help="Path to schema.json (enriches entity properties)"
    ),
    output: str = typer.Option("mapping.csi.json", "--output", "-o", help="Output CSI v1 JSON path"),
    source_type: str = typer.Option(
        "relational", "--source-type", help="provenance.source.kind (e.g. postgresql, mysql, mssql)"
    ),
    source_ref: str = typer.Option(
        "", "--source-ref", help="provenance.source.ref (database/schema pointer; defaults to config source_schema)"
    ),
    validate: bool = typer.Option(True, "--validate/--no-validate", help="Validate output against the CSI v1 schema"),
    label_policy: str = typer.Option(
        "qualify",
        "--label-policy",
        help=(
            "Attribute labels shared by two entities: 'qualify' renames every "
            "occurrence to <entity><Label>; 'roles' classifies each collision by "
            "the role of its column (semantic -> qualify, primary key -> emit as "
            "identity, foreign key -> the owning entity keeps the bare label); "
            "'warn' reports them untouched; 'off' skips the check. Collisions are "
            "always recorded in provenance.labelCollisions."
        ),
    ),
    curated_labels_file: Optional[str] = typer.Option(
        None, "--curated-labels", help=_CURATED_LABELS_HELP
    ),
    allow_unapplied_labels: bool = typer.Option(
        False, "--allow-unapplied-labels", help=_ALLOW_UNAPPLIED_HELP
    ),
) -> None:
    """Emit a forward CSI v1 interchange document from an r2g mapping.

    CSI v1 pairs the conceptual model (entities/relationships) with its ArangoDB
    physical mapping so downstream tools (Ontop R2RML, arango-sparql-py) can
    partition a conceptual query by source. r2g is the forward producer.

    Two entities emitting the same attribute label (``Contract.renewalDate`` and
    ``Opportunity.renewalDate``) make that word ambiguous for every consumer that
    derives a flat vocabulary from the document, and no consumer can recover the
    distinction afterwards. ``--label-policy`` decides what happens; the default
    qualifies them.

    ``--curated-labels`` applies a curator's names (P12.12) before that check.
    """
    from datetime import datetime, timezone

    from r2g.csi import mapping_to_csi, validate_csi
    from r2g.curated_labels import CuratedPlan, load_curated_labels

    # Validated before the try: typer.Exit is an Exception, so raising it inside
    # would be swallowed by the generic handler below and reported as an empty
    # "Failed to export CSI:".
    if label_policy not in {"qualify", "roles", "warn", "off"}:
        console.print(
            f"[red]--label-policy must be one of qualify|roles|warn|off "
            f"(got '{label_policy}').[/red]"
        )
        raise typer.Exit(code=2)

    try:
        mapping = ConfigManager.load_config(config_path)
        schema = Schema.load_from_file(schema_file) if schema_file else None
        if schema is None:
            # Class names read the schema's foreign keys (P12.13); export-r2rml
            # always has them, so without --schema the two can name a class apart.
            console.print(
                "[yellow]No --schema: class names use the singularizer only, without the "
                "schema's foreign-key evidence, and can differ from export-r2rml's.[/yellow]"
            )
        curated = load_curated_labels(curated_labels_file) if curated_labels_file else None
        generated_at = datetime.now(timezone.utc).isoformat()
        # Forward the bitemporal stamps RSA >= 0.8.0 records on the physical schema
        # (DESIGN-ADDENDUM-bitemporal): r2g neither mints nor reshapes them.
        valid_from = getattr(schema, "valid_from", None) if schema is not None else None
        doc = mapping_to_csi(
            mapping,
            schema,
            source_type=source_type,
            source_ref=source_ref,
            generated_at=generated_at,
            label_policy=label_policy,
            transaction_time=getattr(schema, "transaction_time", None) if schema is not None else None,
            valid_time={"from": valid_from} if valid_from else None,
            valid_time_source=getattr(schema, "valid_time_source", None) if schema is not None else None,
            predecessor_fingerprint=(
                getattr(schema, "predecessor_fingerprint", None) if schema is not None else None
            ),
            curated_labels=curated,
        )
        if validate:
            validate_csi(doc)
        if curated is not None:
            plan = CuratedPlan(records=doc["provenance"]["curatedLabels"])
            if not _report_curated_plan(plan) and not allow_unapplied_labels:
                console.print(
                    f"[red]Not written: {len(plan.problems)} curated label(s) not applied "
                    f"(--allow-unapplied-labels to write anyway).[/red]"
                )
                raise typer.Exit(code=1)
        Path(output).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
        console.print(f"[green]Wrote CSI v1 document:[/green] [bold]{output}[/bold]")
        console.print(
            f"  [dim]{len(doc['conceptualModel']['entities'])} entities, "
            f"{len(doc['conceptualModel']['relationships'])} relationships"
            f"{' (schema-validated)' if validate else ''}[/dim]"
        )

        # Shared attribute labels are silent corruption downstream, so they are
        # always reported — never left for a consumer to trip over.
        collisions = doc["provenance"].get("labelCollisions", [])
        qualified = [c for c in collisions if c["resolution"] == "qualified"]
        unresolved = [c for c in collisions if c["resolution"] == "unresolved"]
        reported = [c for c in collisions if c["resolution"] == "reported"]
        identity = [c for c in collisions if c["resolution"] == "identity"]
        if qualified:
            console.print(
                f"[yellow]Qualified {len(qualified)} colliding attribute label(s):[/yellow]"
            )
            for c in qualified:
                pairs = ", ".join(f"{e}.{n}" for e, n in sorted(c["renamedTo"].items()))
                keeps = f" [{c['owner']} keeps '{c['label']}']" if c.get("owner") else ""
                console.print(f"  [dim]{c['label']} -> {pairs}{keeps}[/dim]")
        if identity:
            console.print(
                f"[yellow]{len(identity)} key label(s) emitted as identity rather than "
                f"as an attribute:[/yellow]"
            )
            for c in identity:
                console.print(
                    f"  [dim]{c['label']} on {', '.join(c['entities'])} "
                    f"— carried by _key in the physical mapping[/dim]"
                )
        if reported:
            console.print(
                f"[yellow]{len(reported)} colliding attribute label(s) left as-is "
                f"(--label-policy warn):[/yellow]"
            )
            for c in reported:
                console.print(f"  [dim]{c['label']} on {', '.join(c['entities'])}[/dim]")
        if unresolved:
            console.print(
                f"[red]{len(unresolved)} collision(s) could NOT be auto-qualified "
                f"— a curator must rename these:[/red]"
            )
            for c in unresolved:
                console.print(
                    f"  [dim]{c['label']} on {', '.join(c['entities'])} — {c['reason']}[/dim]"
                )
    except typer.Exit:
        raise
    except Exception as e:
        log.exception("export_csi_failed", output=output)
        console.print(f"[red]Failed to export CSI:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("export-r2rml")
def export_r2rml(
    config_path: str = typer.Option(..., "--config", "-c", help="Mapping config YAML"),
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    output: str = typer.Option("mapping.r2rml.ttl", "--output", "-o", help="Output R2RML Turtle path"),
    concept_base: str = typer.Option(
        "urn:arango-sparql:concept#", "--concept-base", help="IRI namespace for classes/properties"
    ),
    resource_base: str = typer.Option(
        "http://r2g.example/resource/", "--resource-base", help="IRI namespace for row subject IRIs"
    ),
    source_type: str = typer.Option(
        "relational", "--source-type", help="Source system kind (recorded in the header)"
    ),
    curated_labels_file: Optional[str] = typer.Option(
        None, "--curated-labels", help=_CURATED_LABELS_HELP
    ),
    allow_unapplied_labels: bool = typer.Option(
        False, "--allow-unapplied-labels", help=_ALLOW_UNAPPLIED_HELP
    ),
) -> None:
    """Emit an R2RML mapping (relational -> RDF) for SPARQL->SQL pushdown via Ontop.

    R2RML lets a Virtual Knowledge Graph engine answer SPARQL over the live
    relational database with no data movement. Concept IRIs default to the same
    namespace the AQL leg uses, so both federation legs share one vocabulary.
    ``--curated-labels`` must be the same file given to ``export-csi``, or the
    two legs will name concepts differently.
    """
    from r2g.csi import curated_plan_for_mapping
    from r2g.curated_labels import load_curated_labels
    from r2g.r2rml import mapping_to_r2rml

    try:
        mapping = ConfigManager.load_config(config_path)
        schema = Schema.load_from_file(schema_file)
        curated = load_curated_labels(curated_labels_file) if curated_labels_file else None
        if curated is not None:
            plan = curated_plan_for_mapping(mapping, schema, curated)
            if not _report_curated_plan(plan) and not allow_unapplied_labels:
                console.print(
                    f"[red]Not written: {len(plan.problems)} curated label(s) not applied "
                    f"(--allow-unapplied-labels to write anyway).[/red]"
                )
                raise typer.Exit(code=1)
        ttl = mapping_to_r2rml(
            mapping,
            schema,
            concept_base=concept_base,
            resource_base=resource_base,
            source_type=source_type,
            curated_labels=curated,
        )
        Path(output).write_text(ttl, encoding="utf-8")
        n_entities = sum(
            1
            for cm in mapping.collections.values()
            if cm.collection_type != "edge" and not cm.is_join_table
        )
        console.print(f"[green]Wrote R2RML mapping:[/green] [bold]{output}[/bold]")
        console.print(
            f"  [dim]{n_entities} TriplesMaps, {len(mapping.edges)} FK relationships[/dim]"
        )
    except typer.Exit:
        raise
    except Exception as e:
        log.exception("export_r2rml_failed", output=output)
        console.print(f"[red]Failed to export R2RML:[/red] {e}")
        raise typer.Exit(code=1)


@app.command()
def transform_nodes(
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    table_name: str = typer.Option(..., "--table", "-t", help="Table name in the schema"),
    dump_file: str = typer.Option(..., "--input", "-i", help="Input dump file"),
    output_file: str = typer.Option(..., "--output", "-o", help="Output JSONL file"),
    config_path: Optional[str] = typer.Option(
        None, "--config", "-c", help="Mapping config YAML (enables type coercion)"
    ),
    limit: Optional[int] = typer.Option(None, help="Max rows to process"),
) -> None:
    """Transform a table dump into ArangoDB node documents (JSONL)."""
    try:
        schema = Schema.load_from_file(schema_file)
        if table_name not in schema.tables:
            raise ValueError(f"Table '{table_name}' not found in schema.")

        table_def = schema.tables[table_name]

        col_mapping = None
        key_sep = "_"
        type_ovr: dict[str, str] = {}
        if config_path:
            mapping = ConfigManager.load_config(config_path)
            col_mapping = mapping.collections.get(table_name)
            key_sep = mapping.key_separator
            type_ovr = mapping.type_overrides

        transformer = NodeTransformer(
            table_def,
            collection_mapping=col_mapping,
            key_separator=key_sep,
            type_overrides=type_ovr,
        )
        reader = DumpReader(dump_file)

        console.print(f"[green]Transforming nodes for table '{table_name}'...[/green]")

        count = 0
        with open(output_file, "w", encoding="utf-8") as f_out:
            for row in reader.read_rows():
                doc = transformer.transform_row(row)

                f_out.write(json.dumps(doc) + "\n")

                count += 1
                if limit is not None and count >= limit:
                    break

        console.print(f"Successfully wrote {count} documents to [bold]{output_file}[/bold]")
    except Exception as e:
        log.exception("transform_nodes_failed", table=table_name)
        console.print(f"[red]Transformation failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command()
def inspect_dump(
    file_path: str = typer.Argument(..., help="Path to the dump file (CSV/TSV/GZ)"),
    delimiter: str = typer.Option(",", help="Delimiter character"),
    limit: int = typer.Option(5, help="Number of rows to preview"),
) -> None:
    """Preview the contents of a dump file to verify parsing."""
    console.print(f"[green]Inspecting {file_path}...[/green]")

    try:
        reader = DumpReader(file_path, delimiter=delimiter)
        count = 0
        for row in reader.read_rows():
            if count == 0:
                console.print(f"[bold]Columns detected:[/bold] {list(row.keys())}")

            console.print(row)
            count += 1
            if count >= limit:
                break

    except Exception as e:
        log.exception("inspect_dump_failed", file_path=file_path)
        console.print(f"[red]Failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command()
def ingest_schema(
    connection_string: str = typer.Option(..., "--conn", "-c", help="PostgreSQL connection string", envvar="PG_CONN"),
    output: str = typer.Option("./schema.json", "--output", "-o", help="Path to save the schema metadata"),
    pg_schema: str = typer.Option("public", "--pg-schema", help="PostgreSQL schema name to introspect"),
) -> None:
    """Connect to PostgreSQL and extract schema metadata."""
    from r2g.connectors.base import create_source_connector

    console.print("[green]Connecting to PostgreSQL...[/green]")

    try:
        connector = create_source_connector(
            "postgresql", connection_string, schema_name=pg_schema
        )
        schema = connector.get_schema()

        console.print(f"[green]Successfully extracted schema with {len(schema.tables)} tables.[/green]")

        schema.save_to_file(output)
        console.print(f"Schema metadata saved to [bold]{output}[/bold]")

    except Exception as e:
        log.exception("ingest_schema_failed")
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(code=1)


@app.command()
def validate_schema(
    schema_file: str = typer.Argument(..., help="Path to the schema JSON file"),
) -> None:
    """Validate a schema file against the internal model."""
    try:
        schema = Schema.load_from_file(schema_file)
        console.print("[green]Schema valid![/green]")
        console.print(f"Contains {len(schema.tables)} tables.")

        json_str = schema.model_dump_json(indent=2)
        syntax = Syntax(json_str, "json", theme="monokai", line_numbers=True)
        console.print(syntax)

    except Exception as e:
        log.exception("validate_schema_failed", schema_file=schema_file)
        console.print(f"[red]Invalid schema file:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("stream")
def stream(
    pg_conn: Optional[str] = typer.Option(
        None,
        "--pg-conn",
        help="PostgreSQL connection string (legacy; prefer --source)",
        envvar="PG_CONN",
    ),
    source_name: Optional[str] = typer.Option(
        None,
        "--source",
        help=(
            "Catalog source name; supports PostgreSQL, MySQL/MariaDB, "
            "SQL Server, Snowflake, and ClickHouse"
        ),
    ),
    schema_file: Optional[str] = typer.Option(
        None, "--schema", "-s",
        help="Path to schema.json (with --source, default: the source's latest snapshot)",
    ),
    config_path: str = typer.Option(..., "--config", "-c", help="Mapping config YAML"),
    endpoint: str = typer.Option(
        "http://127.0.0.1:8529", "--endpoint", help="ArangoDB endpoint URL", envvar="ARANGO_ENDPOINT"
    ),
    database: str = typer.Option("_system", "--database", "-d", help="ArangoDB database name", envvar="ARANGO_DB"),
    username: str = typer.Option("root", "--username", "-u", help="ArangoDB username", envvar="ARANGO_USER"),
    password: str = typer.Option("", "--password", "-p", help="ArangoDB password", envvar="ARANGO_PASSWORD"),
    batch_size: int = typer.Option(10000, "--batch-size", "-b", help="Rows per batch"),
    on_duplicate: str = typer.Option("replace", "--on-duplicate", help="ArangoDB on-duplicate strategy"),
    graph_name: Optional[str] = typer.Option(None, "--graph-name", help="Create a named graph after import"),
    pg_schema: str = typer.Option(
        "public",
        "--pg-schema",
        help="Source namespace/schema to stream (meaning depends on source type)",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Validate connections and preview data without writing to ArangoDB"
    ),
    drop_collections: bool = typer.Option(
        False, "--drop-collections", help="Drop and recreate target collections before import"
    ),
    workers: int = typer.Option(
        1,
        "--workers",
        "-w",
        help="Parallel workers (each gets its own source + ArangoDB connection)",
    ),
    include_tables: Optional[str] = typer.Option(
        None, "--include-tables", help="Comma-separated list of tables to include (default: all)"
    ),
    exclude_tables: Optional[str] = typer.Option(
        None, "--exclude-tables", help="Comma-separated list of tables to exclude"
    ),
    skip_existing: bool = typer.Option(
        False, "--skip-existing", help="Skip collections that already contain data (for resuming partial runs)"
    ),
    since: Optional[str] = typer.Option(
        None, "--since",
        help="Only stream rows where the timestamp column >= this value (ISO 8601, e.g. 2026-04-01T00:00:00)"
    ),
    since_column: Optional[str] = typer.Option(
        None, "--since-column",
        help="Column to use for --since filtering (default: auto-detect updated_at/created_at)"
    ),
) -> None:
    """Stream data directly from a relational source to ArangoDB (no intermediate files).

    Opens a consistent-snapshot read session on the source (PG
    ``REPEATABLE READ`` / Snowflake ``BEGIN``), reads tables in
    configurable batches, transforms rows on the fly, and bulk-imports
    into ArangoDB via the HTTP API.

    Use ``--source <name>`` to dispatch by catalog ``source_type``
    (PostgreSQL, MySQL/MariaDB, SQL Server, Snowflake, or ClickHouse).
    The legacy ``--pg-conn`` flag still works and routes through the
    PostgreSQL connector.

    Use --dry-run to preview row counts and sample documents without writing.
    Use --since with --on-duplicate=replace for basic incremental updates.
    """
    from r2g.connectors.arango_writer import ArangoWriter
    from r2g.connectors.base import create_source_connector
    from r2g.streaming.pipeline import StreamingPipeline

    if not pg_conn and not source_name:
        console.print("[red]Specify either --source <name> or --pg-conn <url>.[/red]")
        raise typer.Exit(code=2)
    snapshot_schema: Schema | None = None
    if not schema_file:
        if not source_name:
            console.print("[red]--schema is needed with --pg-conn.[/red]")
            raise typer.Exit(code=2)
        snap = _get_catalog().get_latest_snapshot(source_name)
        if snap is None:
            console.print(f"[red]Source '{source_name}' has no snapshot.[/red] "
                          f"Take one with `r2g source snapshot {source_name}`, or pass --schema.")
            raise typer.Exit(code=1)
        snapshot_schema = snap.schema_data

    try:
        schema = snapshot_schema or Schema.load_from_file(str(schema_file))
        mapping = ConfigManager.load_config(config_path)

        writer = ArangoWriter(
            endpoint=endpoint,
            database=database,
            username=username,
            password=password,
        )

        inc = {t.strip() for t in include_tables.split(",")} if include_tables else None
        exc = {t.strip() for t in exclude_tables.split(",")} if exclude_tables else None

        if source_name:
            mgr = _get_catalog()
            source = mgr.get_source(source_name)
            if source is None:
                console.print(f"[red]Source '{source_name}' not found in catalog.[/red]")
                raise typer.Exit(code=1)
            source_connector = create_source_connector(
                source.source_type or "postgresql",
                source.connection_string,
                schema_name=pg_schema,
                source_params=source.source_params,
            )
            source_label = f"{source.source_type} ({source_name})"
        else:
            from r2g.connectors.base import create_source_connector

            assert pg_conn is not None  # guarded above: --source or --pg-conn required
            source_connector = create_source_connector(
                "postgresql", pg_conn, schema_name=pg_schema
            )
            source_label = pg_conn.split("@")[-1] if "@" in pg_conn else pg_conn

        pipeline = StreamingPipeline(
            source_connector=source_connector,
            arango_writer=writer,
            schema=schema,
            config=mapping,
            batch_size=batch_size,
            on_duplicate=on_duplicate,
            pg_schema=pg_schema,
            dry_run=dry_run,
            drop_collections=drop_collections,
            workers=workers,
            include_tables=inc,
            exclude_tables=exc,
            skip_existing=skip_existing,
            since=since,
            since_column=since_column,
        )

        mode_label = "[yellow]DRY RUN[/yellow] — " if dry_run else ""
        console.print(
            f"{mode_label}[green]Streaming from source → ArangoDB[/green]\n"
            f"  Source: {source_label}\n"
            f"  ArangoDB: {endpoint}/{database}\n"
            f"  Batch size: {batch_size:,}"
        )

        from rich.progress import (
            BarColumn as PBarColumn,
        )
        from rich.progress import (
            MofNCompleteColumn as PMofN,
        )
        from rich.progress import (
            SpinnerColumn as PSpinner,
        )
        from rich.progress import (
            TextColumn as PText,
        )
        from rich.progress import (
            TimeElapsedColumn as PTime,
        )

        progress_tasks: dict[str, Any] = {}
        progress_ctx = Progress(
            PSpinner(),
            PText("[progress.description]{task.description}"),
            PBarColumn(bar_width=30),
            PMofN(),
            PText("rows"),
            PTime(),
            console=console,
            transient=True,
        )

        def on_progress(event: str, name: str, current: int, total: int | None) -> None:
            if event == "start":
                progress_tasks[name] = progress_ctx.add_task(name, total=total or 0)
            elif event == "progress":
                progress_ctx.update(progress_tasks[name], completed=current)
            elif event == "done":
                progress_ctx.update(progress_tasks[name], completed=total or current)

        with progress_ctx:
            results = pipeline.run(graph_name=graph_name, on_progress=on_progress)

        title = "Dry Run Preview" if dry_run else "Streaming Import Summary"
        table = RichTable(title=title)
        table.add_column("Collection", style="cyan")
        table.add_column("Type", style="magenta")
        table.add_column("Rows", justify="right")
        for name, count in results["documents"]:
            table.add_row(name, "document", f"{count:,}")
        for name, count in results["edges"]:
            table.add_row(name, "edge", f"{count:,}")
        console.print(table)

        total_docs = sum(c for _, c in results["documents"])
        total_edges = sum(c for _, c in results["edges"])
        total_rows = total_docs + total_edges
        elapsed = results.get("elapsed_seconds", 0)
        throughput = total_rows / elapsed if elapsed > 0 else 0

        skipped = results.get("skipped", [])
        if skipped:
            console.print(
                f"[yellow]Skipped {len(skipped)} existing collection(s):[/yellow] "
                + ", ".join(skipped)
            )

        import_errors = results.get("errors", {})
        if import_errors:
            err_table = RichTable(title="Import Errors", style="red")
            err_table.add_column("Collection", style="cyan")
            err_table.add_column("Errors", justify="right", style="red")
            err_table.add_column("Sample Details")
            for coll_name, details in import_errors.items():
                sample = details[0] if details else ""
                err_table.add_row(coll_name, str(len(details)), sample)
            console.print(err_table)

        if dry_run:
            console.print(
                f"[yellow]Dry run complete:[/yellow] {total_docs:,} documents, "
                f"{total_edges:,} edges [bold]would be[/bold] imported "
                f"[dim]({elapsed:.1f}s, {throughput:,.0f} rows/s)[/dim]"
            )
            if pipeline.previews:
                console.print("\n[bold]Sample documents:[/bold]")
                for coll_name, samples in pipeline.previews.items():
                    if not samples:
                        continue
                    console.print(f"\n  [cyan]{coll_name}[/cyan] (first {len(samples)}):")
                    for doc in samples:
                        console.print(f"    {json.dumps(doc, default=str)}")
            if graph_name:
                console.print(f"\n[yellow]Named graph '{graph_name}' would be created.[/yellow]")
        else:
            console.print(
                f"[green]Stream complete:[/green] {total_docs:,} documents, "
                f"{total_edges:,} edges imported "
                f"[dim]({elapsed:.1f}s, {throughput:,.0f} rows/s)[/dim]"
            )
            if graph_name:
                console.print(f"[green]Named graph '{graph_name}' created.[/green]")

    except Exception as e:
        log.exception("stream_failed")
        console.print(f"[red]Streaming pipeline failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("diff-schema")
def diff_schema(
    old_schema: str = typer.Option(..., "--old", help="Path to the old schema.json"),
    new_schema: str = typer.Option(..., "--new", help="Path to the new schema.json"),
    json_output: bool = typer.Option(False, "--json", help="Output diff as JSON"),
) -> None:
    """Compare two schema files and report structural changes.

    Detects added/removed tables, added/removed/changed columns,
    primary key changes, and foreign key changes.
    """
    from r2g.schema_diff import diff_schemas

    try:
        old = Schema.load_from_file(old_schema)
        new = Schema.load_from_file(new_schema)
        result = diff_schemas(old, new)

        if json_output:
            console.print(json.dumps(result, indent=2))
            return

        has_changes = False

        if result["added_tables"]:
            has_changes = True
            console.print("[green]Added tables:[/green]")
            for t in result["added_tables"]:
                console.print(f"  [green]+[/green] {t}")

        if result["removed_tables"]:
            has_changes = True
            console.print("[red]Removed tables:[/red]")
            for t in result["removed_tables"]:
                console.print(f"  [red]-[/red] {t}")

        if result["modified_tables"]:
            has_changes = True
            for table_name, changes in result["modified_tables"].items():
                console.print(f"\n[yellow]Modified table:[/yellow] [bold]{table_name}[/bold]")

                for col in changes.get("added_columns", []):
                    console.print(f"  [green]+[/green] column [cyan]{col['name']}[/cyan] ({col['type']})")

                for col in changes.get("removed_columns", []):
                    console.print(f"  [red]-[/red] column [cyan]{col}[/cyan]")

                for change in changes.get("type_changes", []):
                    console.print(
                        f"  [yellow]~[/yellow] column [cyan]{change['column']}[/cyan]: "
                        f"{change['old_type']} → {change['new_type']}"
                    )

                for change in changes.get("nullable_changes", []):
                    label = "nullable" if change["new_nullable"] else "NOT NULL"
                    console.print(
                        f"  [yellow]~[/yellow] column [cyan]{change['column']}[/cyan]: → {label}"
                    )

                if changes.get("pk_changed"):
                    console.print(
                        f"  [yellow]~[/yellow] primary key: "
                        f"{changes['old_pk']} → {changes['new_pk']}"
                    )

                for fk in changes.get("added_fks", []):
                    console.print(
                        f"  [green]+[/green] FK {fk['columns']} → "
                        f"{fk['foreign_table']}.{fk['foreign_columns']}"
                    )

                for fk in changes.get("removed_fks", []):
                    console.print(
                        f"  [red]-[/red] FK {fk['columns']} → "
                        f"{fk['foreign_table']}.{fk['foreign_columns']}"
                    )

        if not has_changes:
            console.print("[green]Schemas are identical — no changes detected.[/green]")

    except Exception as e:
        log.exception("diff_schema_failed")
        console.print(f"[red]Schema diff failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("migrate-config")
def migrate_config_cmd(
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to the NEW schema.json"),
    config_path: str = typer.Option(..., "--config", "-c", help="Existing mapping config YAML"),
    output: Optional[str] = typer.Option(
        None, "--output", "-o", help="Output path for updated config (default: overwrite input)"
    ),
    source_schema: Optional[str] = typer.Option(None, "--source-schema", help="Override source_schema in config"),
    json_report: bool = typer.Option(False, "--json-report", help="Output migration report as JSON"),
) -> None:
    """Migrate a mapping config to match an updated PostgreSQL schema.

    Preserves user customizations (collection renames, field mappings,
    include/exclude lists, type overrides) while adding mappings for new
    tables and edges, removing edges for dropped FKs, and flagging
    orphaned collections whose source table no longer exists.
    """
    from r2g.config_migrate import migrate_config

    try:
        new_schema = Schema.load_from_file(schema_file)
        old_config = ConfigManager.load_config(config_path)
        updated, report = migrate_config(old_config, new_schema, source_schema=source_schema)

        out_path = output or config_path
        ConfigManager.save_config(updated, out_path)

        if json_report:
            typer.echo(json.dumps({
                "added_collections": report.added_collections,
                "orphaned_collections": report.orphaned_collections,
                "added_edges": report.added_edges,
                "removed_edges": report.removed_edges,
                "cleaned_fields": report.cleaned_fields,
                "output": out_path,
            }, indent=2))
            return

        if not report.has_changes:
            console.print("[green]Config is already up to date — no migration needed.[/green]")
        else:
            if report.added_collections:
                console.print("[green]Added collections:[/green]")
                for name in report.added_collections:
                    console.print(f"  [green]+[/green] {name}")
            if report.orphaned_collections:
                console.print("[yellow]Orphaned collections (source table removed):[/yellow]")
                for name in report.orphaned_collections:
                    console.print(f"  [yellow]![/yellow] {name}")
            if report.added_edges:
                console.print("[green]Added edges:[/green]")
                for name in report.added_edges:
                    console.print(f"  [green]+[/green] {name}")
            if report.removed_edges:
                console.print("[red]Removed edges (FK dropped):[/red]")
                for name in report.removed_edges:
                    console.print(f"  [red]-[/red] {name}")
            if report.cleaned_fields:
                console.print("[yellow]Cleaned references:[/yellow]")
                for note in report.cleaned_fields:
                    console.print(f"  [yellow]~[/yellow] {note}")

        console.print(
            f"\n[green]Wrote updated config to[/green] [bold]{out_path}[/bold] "
            f"([bold]{len(updated.collections)}[/bold] collections, "
            f"[bold]{len(updated.edges)}[/bold] edges)."
        )

    except Exception as e:
        log.exception("migrate_config_failed")
        console.print(f"[red]Config migration failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("dump-tables")
def dump_tables(
    connection_string: str = typer.Option(..., "--conn", "-c", help="PostgreSQL connection string", envvar="PG_CONN"),
    output_dir: str = typer.Option("./dumps", "--output-dir", "-o", help="Directory to write CSV files"),
    schema_filter: Optional[str] = typer.Option("public", "--schema", help="PostgreSQL schema to dump"),
    tables: Optional[str] = typer.Option(None, "--tables", "-t", help="Comma-separated list of tables (default: all)"),
) -> None:
    """Connect to PostgreSQL and dump each table to a CSV file.

    Runs COPY <table> TO STDOUT WITH CSV HEADER for every table in the schema
    (or a subset if --tables is specified). Output files are named <table>.csv.
    """
    import psycopg

    from r2g.connectors.base import create_source_connector

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    try:
        pg_schema_name = schema_filter or "public"
        connector = create_source_connector(
            "postgresql", connection_string, schema_name=pg_schema_name
        )
        schema = connector.get_schema()
        table_names = sorted(schema.tables.keys())

        if tables:
            requested = {t.strip() for t in tables.split(",")}
            missing = requested - set(table_names)
            if missing:
                console.print(f"[yellow]Warning: tables not found in schema: {', '.join(sorted(missing))}[/yellow]")
            table_names = [t for t in table_names if t in requested]

        if not table_names:
            console.print("[yellow]No tables to dump.[/yellow]")
            return

        console.print(f"[green]Dumping {len(table_names)} tables from PostgreSQL to {output_dir}/[/green]")

        with psycopg.connect(connection_string) as conn:
            for tbl in table_names:
                csv_path = out / f"{tbl}.csv"
                copy_sql = f"COPY {schema_filter}.{tbl} TO STDOUT WITH CSV HEADER"
                with csv_path.open("wb") as f:
                    with conn.cursor().copy(copy_sql) as copy:
                        for chunk in copy:
                            f.write(chunk)
                row_count = sum(1 for _ in csv_path.open("r")) - 1
                console.print(f"  [cyan]{tbl}[/cyan] → {csv_path} ({row_count} rows)")

        console.print(f"[green]Done. {len(table_names)} CSV files written to {output_dir}/[/green]")
    except Exception as e:
        log.exception("dump_tables_failed")
        console.print(f"[red]Failed to dump tables:[/red] {e}")
        raise typer.Exit(code=1)


# ── CDC commands ─────────────────────────────────────────────────────


@app.command("cdc-setup")
def cdc_setup(
    pg_conn: str = typer.Option(
        ..., "--pg-conn", help="PostgreSQL connection string", envvar="PG_CONN"
    ),
    slot_name: str = typer.Option(
        "r2g_slot", "--slot", help="Replication slot name"
    ),
    plugin: str = typer.Option(
        "test_decoding", "--plugin",
        help="Output plugin (test_decoding or wal2json)",
    ),
) -> None:
    """Create a logical replication slot for CDC."""
    from r2g.cdc.pg_listener import PGReplicationListener

    try:
        listener = PGReplicationListener(
            pg_conn_string=pg_conn,
            handler=None,  # type: ignore[arg-type]
            slot_name=slot_name,
            plugin=plugin,
        )
        status = listener.setup()
        console.print("[green]Replication slot ready:[/green]")
        for k, v in status.items():
            console.print(f"  {k}: {v}")
    except Exception as e:
        log.exception("cdc_setup_failed")
        console.print(f"[red]CDC setup failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("cdc-teardown")
def cdc_teardown(
    pg_conn: str = typer.Option(
        ..., "--pg-conn", help="PostgreSQL connection string", envvar="PG_CONN"
    ),
    slot_name: str = typer.Option(
        "r2g_slot", "--slot", help="Replication slot name"
    ),
    plugin: str = typer.Option(
        "test_decoding", "--plugin",
        help="Output plugin (test_decoding or wal2json)",
    ),
) -> None:
    """Drop a logical replication slot."""
    from r2g.cdc.pg_listener import PGReplicationListener

    try:
        listener = PGReplicationListener(
            pg_conn_string=pg_conn,
            handler=None,  # type: ignore[arg-type]
            slot_name=slot_name,
            plugin=plugin,
        )
        dropped = listener.drop_slot()
        if dropped:
            console.print(f"[green]Slot '{slot_name}' dropped.[/green]")
        else:
            console.print(f"[yellow]Slot '{slot_name}' not found.[/yellow]")
    except Exception as e:
        log.exception("cdc_teardown_failed")
        console.print(f"[red]CDC teardown failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("cdc-status")
def cdc_status(
    pg_conn: str = typer.Option(
        ..., "--pg-conn", help="PostgreSQL connection string", envvar="PG_CONN"
    ),
    slot_name: str = typer.Option(
        "r2g_slot", "--slot", help="Replication slot name"
    ),
    plugin: str = typer.Option(
        "test_decoding", "--plugin",
        help="Output plugin (test_decoding or wal2json)",
    ),
) -> None:
    """Show the status of a logical replication slot."""
    from r2g.cdc.pg_listener import PGReplicationListener

    try:
        listener = PGReplicationListener(
            pg_conn_string=pg_conn,
            handler=None,  # type: ignore[arg-type]
            slot_name=slot_name,
            plugin=plugin,
        )
        status = listener.slot_status()
        if status is None:
            console.print(f"[yellow]Slot '{slot_name}' not found.[/yellow]")
            raise typer.Exit(code=1)
        table = RichTable(title=f"Replication Slot: {slot_name}")
        table.add_column("Property")
        table.add_column("Value")
        for k, v in status.items():
            table.add_row(k, str(v))
        console.print(table)
    except typer.Exit:
        raise
    except Exception as e:
        log.exception("cdc_status_failed")
        console.print(f"[red]CDC status failed:[/red] {e}")
        raise typer.Exit(code=1)


def _govern_cdc_mapping(schema, mapping, *, govern: bool, allow_sensitive: bool, threshold: str):
    """Apply the Phase 9 sensitivity gate to a CDC mapping (carry on changed rows).

    When ``govern`` is set, build the entitlement report over the (annotated)
    schema and exclude above-threshold unmasked fields from the mapping so
    subsequent CDC/temporal row changes do not launder newly sensitive columns
    into the graph. No-op (returns ``mapping`` unchanged) when ``govern`` is off
    or the schema carries no classifications. Returns the possibly-gated mapping.
    """
    if not govern:
        return mapping
    from r2g.classification import SENSITIVITY_ORDER
    from r2g.governance import apply_sensitivity_gate, build_entitlement_report

    threshold = (threshold or "confidential").strip().lower()
    if threshold not in SENSITIVITY_ORDER:
        console.print(
            f"[red]Invalid --sensitivity-threshold '{threshold}'.[/red] "
            f"Expected one of: {', '.join(SENSITIVITY_ORDER)}."
        )
        raise typer.Exit(code=1)

    report = build_entitlement_report(mapping, schema, threshold=threshold)
    gated, excluded = apply_sensitivity_gate(mapping, report, allow_sensitive=allow_sensitive)
    if excluded:
        cols = ", ".join(
            f"{f.target_collection}.{f.target_property}" for f in excluded
        )
        console.print(
            f"[yellow]Governance gate:[/yellow] excluding {len(excluded)} "
            f"above-threshold field(s) from CDC writes: [dim]{cols}[/dim] "
            "(pass --allow-sensitive to override, or mask them)."
        )
    elif allow_sensitive and report.above_threshold:
        console.print(
            f"[yellow]Governance gate:[/yellow] --allow-sensitive set; loading "
            f"{len(report.above_threshold)} above-threshold field(s) unmasked."
        )
    else:
        console.print("[green]Governance gate:[/green] no above-threshold fields to exclude.")
    return gated


@app.command("cdc-start")
def cdc_start(
    pg_conn: str = typer.Option(
        ..., "--pg-conn", help="PostgreSQL connection string", envvar="PG_CONN"
    ),
    schema_file: str = typer.Argument(..., help="Path to schema JSON file"),
    config_path: str = typer.Argument(..., help="Path to mapping config YAML/JSON"),
    endpoint: str = typer.Option(
        "http://localhost:8529", "--endpoint",
        help="ArangoDB endpoint", envvar="ARANGO_ENDPOINT",
    ),
    database: str = typer.Option(
        "_system", "--database",
        help="ArangoDB database name", envvar="ARANGO_DB",
    ),
    username: str = typer.Option(
        "root", "--username",
        help="ArangoDB username", envvar="ARANGO_USER",
    ),
    password: str = typer.Option(
        "", "--password",
        help="ArangoDB password", envvar="ARANGO_PASSWORD",
    ),
    slot_name: str = typer.Option(
        "r2g_slot", "--slot", help="Replication slot name"
    ),
    plugin: str = typer.Option(
        "test_decoding", "--plugin",
        help="Output plugin (test_decoding or wal2json)",
    ),
    poll_interval: float = typer.Option(
        1.0, "--poll-interval",
        help="Seconds between polls when no changes are pending",
    ),
    batch_size: int = typer.Option(
        1000, "--batch-size",
        help="Maximum changes to consume per poll cycle",
    ),
    create_slot: bool = typer.Option(
        True, "--create-slot/--no-create-slot",
        help="Automatically create the replication slot if it doesn't exist",
    ),
    conflict_policy: str = typer.Option(
        "source_wins", "--conflict-policy",
        help="Conflict resolution policy: source_wins, last_write_wins, log_and_skip, fail",
    ),
    temporal: bool = typer.Option(
        False, "--temporal/--no-temporal",
        help="Use temporal (immutable-proxy) versioned writes instead of direct replace/delete",
    ),
    ttl_seconds: int = typer.Option(
        30 * 24 * 60 * 60, "--ttl-seconds",
        help="Retention (seconds) for historical versions before TTL GC (temporal mode)",
    ),
    smart_field: str = typer.Option(
        "", "--smart-field",
        help="Shard-key attribute for SmartGraph key prefixes (temporal mode, P5.8)",
    ),
    govern: bool = typer.Option(
        False, "--govern/--no-govern",
        help="Apply the Phase 9 sensitivity gate so changed rows carry classification policy",
    ),
    allow_sensitive: bool = typer.Option(
        False, "--allow-sensitive",
        help="With --govern, load above-threshold fields anyway (explicit opt-out)",
    ),
    sensitivity_threshold: str = typer.Option(
        "confidential", "--sensitivity-threshold",
        help="With --govern, exclude fields at/above this level: public|internal|confidential|restricted",
    ),
) -> None:
    """Start the CDC listener (continuous polling for PostgreSQL changes).

    Connects to PostgreSQL via a logical replication slot and ArangoDB
    via the HTTP API.  Polls for row-level changes, transforms them
    through the mapping config, and applies deltas in near real-time.

    Conflict policies:
      source_wins     - PG is truth; upsert on duplicate, insert on missing (default)
      last_write_wins - compare LSN; reject stale writes
      log_and_skip    - log conflicts, skip writes
      fail            - raise on any conflict

    With ``--temporal`` each change is applied as a versioned write using the
    immutable-proxy pattern (ProxyIn/Entity/ProxyOut + hasVersion edges) so
    full history and point-in-time queries are preserved; deletes become soft
    deletes and historical versions age out after ``--ttl-seconds``.

    Press Ctrl+C to stop gracefully.
    """
    from r2g.cdc.conflict import ConflictPolicy
    from r2g.cdc.handler import CDCHandler
    from r2g.cdc.pg_listener import PGReplicationListener
    from r2g.connectors.arango_writer import ArangoWriter
    from r2g.temporal.models import TemporalConfig

    try:
        policy = ConflictPolicy(conflict_policy)
    except ValueError:
        console.print(
            f"[red]Invalid conflict policy:[/red] '{conflict_policy}'. "
            f"Choose from: source_wins, last_write_wins, log_and_skip, fail"
        )
        raise typer.Exit(code=1)

    temporal_config = (
        TemporalConfig(ttl_retain_seconds=ttl_seconds, smart_field=smart_field or None)
        if temporal else None
    )

    try:
        schema = Schema.load_from_file(str(schema_file))
        mapping = ConfigManager.load_config(config_path)
        mapping = _govern_cdc_mapping(
            schema, mapping,
            govern=govern, allow_sensitive=allow_sensitive,
            threshold=sensitivity_threshold,
        )

        writer = ArangoWriter(
            endpoint=endpoint,
            database=database,
            username=username,
            password=password,
        )
        writer.ensure_database()
        writer.connect()

        handler = CDCHandler(
            writer, schema, mapping, conflict_policy=policy,
            temporal=temporal, temporal_config=temporal_config,
        )
        if temporal:
            console.print(
                f"[cyan]Temporal mode enabled[/cyan] "
                f"(versioned writes, ttl={ttl_seconds}s"
                + (f", smart_field={smart_field}" if smart_field else "") + ")"
            )
        listener = PGReplicationListener(
            pg_conn_string=pg_conn,
            handler=handler,
            slot_name=slot_name,
            plugin=plugin,
            poll_interval=poll_interval,
            batch_size=batch_size,
        )

        if create_slot:
            listener.setup()

        console.print(
            f"[green]CDC listener starting[/green] "
            f"(slot={slot_name}, plugin={plugin}, "
            f"poll={poll_interval}s, batch={batch_size})"
        )
        console.print("[dim]Press Ctrl+C to stop.[/dim]")

        listener.run()

        stats = handler.stats.as_dict()
        console.print("\n[green]CDC listener stopped.[/green]")
        stats_table = RichTable(title="CDC Session Statistics")
        stats_table.add_column("Metric")
        stats_table.add_column("Value", justify="right")
        for k, v in stats.items():
            stats_table.add_row(k, str(v))
        console.print(stats_table)

        conflict_summary = handler.resolver.log.summary()
        if conflict_summary["total_conflicts"] > 0:
            ct = RichTable(title=f"Conflicts (policy: {policy.value})")
            ct.add_column("Type")
            ct.add_column("Count", justify="right")
            for ctype, count in conflict_summary["by_type"].items():
                ct.add_row(ctype, str(count))
            ct.add_row("[bold]Total[/bold]", f"[bold]{conflict_summary['total_conflicts']}[/bold]")
            console.print(ct)

        writer.close()

    except Exception as e:
        log.exception("cdc_start_failed")
        console.print(f"[red]CDC start failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command()
def kafka_start(
    schema_file: Path = typer.Argument(..., help="Path to schema JSON"),
    config_path: Path = typer.Argument(..., help="Path to mapping YAML"),
    brokers: str = typer.Option(
        "localhost:9092", "--brokers",
        help="Kafka bootstrap servers (comma-separated)",
    ),
    topics: str = typer.Option(
        ..., "--topics",
        help="Comma-separated list of Kafka topics to consume",
    ),
    group_id: str = typer.Option(
        "r2g-cdc", "--group-id",
        help="Kafka consumer group ID",
    ),
    message_format: str = typer.Option(
        "debezium", "--format",
        help="Message format: debezium, flat",
    ),
    auto_offset_reset: str = typer.Option(
        "earliest", "--offset-reset",
        help="Where to start consuming: earliest, latest",
    ),
    batch_size: int = typer.Option(
        500, "--batch-size",
        help="Max messages to consume per poll",
    ),
    endpoint: str = typer.Option(
        "http://localhost:8529", "--endpoint",
        help="ArangoDB endpoint URL",
    ),
    database: str = typer.Option(
        "_system", "--database",
        help="ArangoDB database name",
    ),
    username: str = typer.Option("root", "--username", help="ArangoDB username"),
    password: str = typer.Option("", "--password", help="ArangoDB password"),
    conflict_policy: str = typer.Option(
        "source_wins", "--conflict-policy",
        help="Conflict resolution: source_wins, last_write_wins, log_and_skip, fail",
    ),
    temporal: bool = typer.Option(
        False, "--temporal/--no-temporal",
        help="Use temporal (immutable-proxy) versioned writes instead of direct replace/delete",
    ),
    ttl_seconds: int = typer.Option(
        30 * 24 * 60 * 60, "--ttl-seconds",
        help="Retention (seconds) for historical versions before TTL GC (temporal mode)",
    ),
    smart_field: str = typer.Option(
        "", "--smart-field",
        help="Shard-key attribute for SmartGraph key prefixes (temporal mode, P5.8)",
    ),
    govern: bool = typer.Option(
        False, "--govern/--no-govern",
        help="Apply the Phase 9 sensitivity gate so changed rows carry classification policy",
    ),
    allow_sensitive: bool = typer.Option(
        False, "--allow-sensitive",
        help="With --govern, load above-threshold fields anyway (explicit opt-out)",
    ),
    sensitivity_threshold: str = typer.Option(
        "confidential", "--sensitivity-threshold",
        help="With --govern, exclude fields at/above this level: public|internal|confidential|restricted",
    ),
) -> None:
    """Start the Kafka CDC consumer (Debezium or flat JSON messages).

    Connects to Kafka broker(s), consumes change events from the
    specified topics, transforms them through the mapping config,
    and applies deltas to ArangoDB.

    Requires confluent-kafka: pip install 'r2g-arango[kafka]'

    With ``--temporal`` each change is applied as a versioned write using the
    immutable-proxy pattern, preserving full history and point-in-time queries.

    Press Ctrl+C to stop gracefully.
    """
    from r2g.cdc.conflict import ConflictPolicy
    from r2g.cdc.handler import CDCHandler
    from r2g.connectors.arango_writer import ArangoWriter
    from r2g.temporal.models import TemporalConfig

    try:
        policy = ConflictPolicy(conflict_policy)
    except ValueError:
        console.print(
            f"[red]Invalid conflict policy:[/red] '{conflict_policy}'. "
            "Choose from: source_wins, last_write_wins, log_and_skip, fail"
        )
        raise typer.Exit(code=1)

    temporal_config = (
        TemporalConfig(ttl_retain_seconds=ttl_seconds, smart_field=smart_field or None)
        if temporal else None
    )

    try:
        from r2g.cdc.kafka_consumer import KafkaConsumer
    except ImportError:
        console.print(
            "[red]confluent-kafka is not installed.[/red] "
            "Install it with: [bold]pip install 'r2g-arango[kafka]'[/bold]"
        )
        raise typer.Exit(code=1)

    try:
        schema = Schema.load_from_file(str(schema_file))
        mapping = ConfigManager.load_config(config_path)
        mapping = _govern_cdc_mapping(
            schema, mapping,
            govern=govern, allow_sensitive=allow_sensitive,
            threshold=sensitivity_threshold,
        )

        writer = ArangoWriter(
            endpoint=endpoint,
            database=database,
            username=username,
            password=password,
        )
        writer.ensure_database()
        writer.connect()

        handler = CDCHandler(
            writer, schema, mapping, conflict_policy=policy,
            temporal=temporal, temporal_config=temporal_config,
        )
        if temporal:
            console.print(
                f"[cyan]Temporal mode enabled[/cyan] "
                f"(versioned writes, ttl={ttl_seconds}s"
                + (f", smart_field={smart_field}" if smart_field else "") + ")"
            )

        topic_list = [t.strip() for t in topics.split(",") if t.strip()]
        if not topic_list:
            console.print("[red]No topics specified.[/red]")
            raise typer.Exit(code=1)

        consumer = KafkaConsumer(
            handler=handler,
            brokers=brokers,
            topics=topic_list,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            message_format=message_format,
            batch_size=batch_size,
        )

        console.print(
            f"[green]Kafka consumer starting[/green] -- "
            f"brokers={brokers}, topics={topic_list}, "
            f"group={group_id}, format={message_format}"
        )
        consumer.run()

        stats = handler.stats.as_dict()
        stats_table = RichTable(title="Kafka CDC Session Statistics")
        stats_table.add_column("Metric")
        stats_table.add_column("Value", justify="right")
        for k, v in stats.items():
            stats_table.add_row(k, str(v))
        console.print(stats_table)

        conflict_summary = handler.resolver.log.summary()
        if conflict_summary["total_conflicts"] > 0:
            ct = RichTable(title=f"Conflicts (policy: {policy.value})")
            ct.add_column("Type")
            ct.add_column("Count", justify="right")
            for ctype, count in conflict_summary["by_type"].items():
                ct.add_row(ctype, str(count))
            ct.add_row("[bold]Total[/bold]", f"[bold]{conflict_summary['total_conflicts']}[/bold]")
            console.print(ct)

        writer.close()

    except Exception as e:
        log.exception("kafka_start_failed")
        console.print(f"[red]Kafka consumer failed:[/red] {e}")
        raise typer.Exit(code=1)


# ── Mapping diff / selective reload ──────────────────────────────────


@app.command("mapping-diff")
def mapping_diff_cmd(
    old_config: str = typer.Argument(..., help="Path to old mapping config YAML"),
    new_config: str = typer.Argument(..., help="Path to new mapping config YAML"),
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    json_output: bool = typer.Option(False, "--json", help="Output diff as JSON"),
) -> None:
    """Compare two mapping configs and show what ArangoDB changes are needed."""
    from r2g.mapping_diff import diff_mappings

    try:
        old = ConfigManager.load_config(old_config)
        new = ConfigManager.load_config(new_config)
        schema = Schema.load_from_file(schema_file)
        plan = diff_mappings(old, new, schema)

        if not plan.changes:
            console.print("[green]Mappings are identical[/green]")
            return

        if json_output:
            console.print(plan.model_dump_json(indent=2))
            return

        changes_table = RichTable(title="Mapping Changes")
        changes_table.add_column("Type", style="magenta")
        changes_table.add_column("Collection / Edge", style="cyan")
        changes_table.add_column("Details")
        for change in plan.changes:
            target = change.collection or change.edge or ""
            details = ", ".join(f"{k}={v}" for k, v in change.details.items()) if change.details else ""
            changes_table.add_row(change.change_type, target, details)
        console.print(changes_table)

        actions_table = RichTable(title="Reload Actions")
        actions_table.add_column("Action", style="magenta")
        actions_table.add_column("Collection", style="cyan")
        actions_table.add_column("Reason")
        for action in plan.actions:
            actions_table.add_row(action.action_type, action.collection, action.reason)
        console.print(actions_table)

    except Exception as e:
        log.exception("mapping_diff_failed")
        console.print(f"[red]Mapping diff failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("selective-reload")
def selective_reload_cmd(
    old_config: str = typer.Argument(..., help="Path to old mapping config YAML"),
    new_config: str = typer.Argument(..., help="Path to new mapping config YAML"),
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    pg_conn: Optional[str] = typer.Option(
        None, "--pg-conn", help="PostgreSQL connection string", envvar="PG_CONN"
    ),
    endpoint: str = typer.Option(
        "http://localhost:8529", "--endpoint", help="ArangoDB endpoint", envvar="ARANGO_ENDPOINT"
    ),
    database: str = typer.Option("_system", "--database", "-d", help="ArangoDB database", envvar="ARANGO_DB"),
    username: str = typer.Option("root", "--username", "-u", help="ArangoDB username", envvar="ARANGO_USER"),
    password: str = typer.Option("", "--password", "-p", help="ArangoDB password", envvar="ARANGO_PASSWORD"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show plan without executing"),
    batch_size: int = typer.Option(10000, "--batch-size", help="Rows per batch for reload"),
    on_duplicate: str = typer.Option("replace", "--on-duplicate", help="On duplicate strategy"),
) -> None:
    """Compute and execute a selective reload based on mapping changes."""
    from r2g.connectors.arango_writer import ArangoWriter
    from r2g.mapping_diff import diff_mappings
    from r2g.selective_reload import SelectiveReloader

    try:
        old = ConfigManager.load_config(old_config)
        new = ConfigManager.load_config(new_config)
        schema = Schema.load_from_file(schema_file)
        plan = diff_mappings(old, new, schema)

        if not plan.changes:
            console.print("[green]No changes detected[/green]")
            return

        changes_table = RichTable(title="Mapping Changes")
        changes_table.add_column("Type", style="magenta")
        changes_table.add_column("Collection / Edge", style="cyan")
        changes_table.add_column("Details")
        for change in plan.changes:
            target = change.collection or change.edge or ""
            details = ", ".join(f"{k}={v}" for k, v in change.details.items()) if change.details else ""
            changes_table.add_row(change.change_type, target, details)
        console.print(changes_table)

        writer = ArangoWriter(
            endpoint=endpoint,
            database=database,
            username=username,
            password=password,
        )

        reloader = SelectiveReloader(
            writer=writer,
            plan=plan,
            pg_conn_string=pg_conn,
            schema=schema,
            config=new,
            batch_size=batch_size,
            on_duplicate=on_duplicate,
        )
        report = reloader.execute(dry_run=dry_run)

        report_table = RichTable(title="Reload Report" + (" (dry run)" if dry_run else ""))
        report_table.add_column("Action", style="magenta")
        report_table.add_column("Collection", style="cyan")
        report_table.add_column("Status")
        report_table.add_column("Detail")
        for entry in report.actions_executed:
            report_table.add_row(
                entry.get("action", ""),
                entry.get("collection", ""),
                "[green]executed[/green]",
                entry.get("reason", ""),
            )
        for entry in report.actions_skipped:
            report_table.add_row(
                entry.get("action", ""),
                entry.get("collection", ""),
                "[yellow]skipped[/yellow]",
                entry.get("reason", ""),
            )
        for entry in report.errors:
            report_table.add_row(
                entry.get("action", ""),
                entry.get("collection", ""),
                "[red]error[/red]",
                entry.get("error", ""),
            )
        console.print(report_table)

        if report.rows_reloaded:
            console.print(f"[green]Rows reloaded:[/green] {report.rows_reloaded:,}")

    except Exception as e:
        log.exception("selective_reload_failed")
        console.print(f"[red]Selective reload failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("ui")
def ui_cmd(
    host: str = typer.Option("127.0.0.1", "--host", help="Bind address"),
    port: int = typer.Option(8501, "--port", help="Port to serve on"),
    project: Optional[str] = typer.Option(None, "--project", help="Open directly to a project"),
    catalog_dir: Optional[str] = typer.Option(None, "--catalog-dir", help="Catalog directory"),
) -> None:
    """Start the Relational-to-Graph Studio web UI."""
    try:
        import uvicorn

        from r2g.ui.server import create_app
    except ImportError:
        console.print(
            "[red]FastAPI/Uvicorn not installed.[/red] "
            "Install with: [bold]pip install 'r2g-arango[ui]'[/bold]"
        )
        raise typer.Exit(code=1)

    console.print(f"[green]Relational-to-Graph Studio[/green] starting at http://{host}:{port}")
    if project:
        console.print(f"  Default project: {project}")

    app_instance = create_app(catalog_dir=catalog_dir, host=host)
    if getattr(app_instance.state, "api_auth_required", False):
        token = app_instance.state.api_token
        if os.environ.get("R2G_API_TOKEN"):
            console.print("  [yellow]API auth enabled[/yellow] (using R2G_API_TOKEN).")
        else:
            console.print(
                "  [yellow]API auth enabled[/yellow] (non-loopback bind). "
                "Paste this token into the UI when prompted:"
            )
            console.print(f"    [bold]{token}[/bold]")
    uvicorn.run(app_instance, host=host, port=port, log_level="info")


@app.command("mcp")
def mcp_cmd(
    transport: str = typer.Option("stdio", "--transport", "-t", help="Transport: stdio or sse"),
    host: str = typer.Option("127.0.0.1", "--host", help="Bind address (SSE only)"),
    port: int = typer.Option(8502, "--port", help="Port (SSE only)"),
) -> None:
    """Start the R2G MCP server for AI agent integration.

    The MCP server exposes R2G capabilities (schema introspection, mapping
    generation, data loading, validation) as tools that AI agents can call.

    Use stdio transport for Cursor / Claude Desktop integration.
    Use SSE transport for remote or multi-client access.

    SSE is network-exposed, so it requires a Bearer token: set R2G_API_TOKEN,
    or one is generated and printed at startup when binding to a non-loopback
    address. stdio is local-only and needs no token.

    It does not run under a control plane (CONTROL_PLANE_URL set).
    """
    from r2g import control_plane

    if control_plane.mode():
        console.print(f"[red]{control_plane.MCP_REFUSED}[/red]")
        raise typer.Exit(code=1)
    try:
        from r2g.mcp_server import mcp as mcp_app
    except ImportError:
        console.print(
            "[red]MCP SDK not installed.[/red] Install with: [bold]pip install 'r2g-arango[mcp]'[/bold]"
        )
        raise typer.Exit(code=1)

    if transport != "sse":
        mcp_app.run(transport="stdio")
        return

    import secrets

    import uvicorn

    _LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1", ""}
    token = (os.environ.get("R2G_API_TOKEN") or "").strip()
    require_auth = bool(token) or host not in _LOOPBACK_HOSTS
    if require_auth and not token:
        token = secrets.token_urlsafe(32)

    mcp_app.settings.host = host
    mcp_app.settings.port = port
    sse_app = mcp_app.sse_app()
    if require_auth:
        sse_app = _bearer_guard(sse_app, token)

    console.print(f"[green]R2G MCP Server[/green] (SSE) starting at http://{host}:{port}/sse")
    if require_auth:
        if os.environ.get("R2G_API_TOKEN"):
            console.print("  [yellow]Bearer auth enabled[/yellow] (using R2G_API_TOKEN).")
        else:
            console.print(
                "  [yellow]Bearer auth enabled[/yellow] (non-loopback bind). "
                "Send this token as 'Authorization: Bearer <token>':"
            )
            console.print(f"    [bold]{token}[/bold]")
    uvicorn.run(sse_app, host=host, port=port, log_level="info")


# ── Source commands ───────────────────────────────────────────────────


def _get_catalog():
    from r2g.catalog import CatalogManager
    return CatalogManager()


@source_app.command("add")
def source_add(
    name: str = typer.Option(..., "--name", help="Source name"),
    source_type: str = typer.Option(
        "postgresql",
        "--type",
        help=(
            "Source type: postgresql, mysql, sqlserver, snowflake, "
            "clickhouse, csv, or kafka"
        ),
    ),
    conn: str = typer.Option(..., "--conn", help="Connection string"),
    description: str = typer.Option("", "--description", help="Description"),
    owner: str = typer.Option("", "--owner", help="Owner"),
    key_overlay: Optional[str] = typer.Option(
        None,
        "--key-overlay",
        help="Reviewed PK/FK overlay JSON or YAML (parsed and stored with the source)",
    ),
) -> None:
    """Register a new data source."""
    try:
        mgr = _get_catalog()
        overlay_data = None
        overlay_source = None
        if key_overlay:
            from relational_schema_analyzer import load_key_overlay

            overlay_data = load_key_overlay(key_overlay)
            overlay_source = str(Path(key_overlay).expanduser().resolve())
        source = mgr.add_source(
            name,
            source_type,
            conn,
            description=description,
            owner=owner,
            key_overlay=overlay_data,
            key_overlay_source=overlay_source,
        )
        console.print(f"[green]Source '{source.name}' added.[/green]")
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(code=1)
    except Exception as e:
        log.exception("source_add_failed")
        console.print(f"[red]Failed to add source:[/red] {e}")
        raise typer.Exit(code=1)


@source_app.command("list")
def source_list() -> None:
    """List all registered data sources."""
    mgr = _get_catalog()
    sources = mgr.list_sources()
    if not sources:
        console.print("[dim]No sources registered.[/dim]")
        return
    table = RichTable(title="Data Sources")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="magenta")
    table.add_column("Description")
    table.add_column("Owner")
    table.add_column("Updated")
    for s in sources:
        table.add_row(s.name, s.source_type, s.description, s.owner, s.updated_at.isoformat())
    console.print(table)


@source_app.command("remove")
def source_remove(
    name: str = typer.Argument(..., help="Source name to remove"),
) -> None:
    """Remove a registered data source."""
    mgr = _get_catalog()
    if mgr.remove_source(name):
        console.print(f"[green]Source '{name}' removed.[/green]")
    else:
        console.print(f"[yellow]Source '{name}' not found.[/yellow]")
        raise typer.Exit(code=1)


@source_app.command("snapshot")
def source_snapshot(
    name: str = typer.Argument(..., help="Source name to snapshot"),
    pg_schema: str = typer.Option(
        "public",
        "--pg-schema",
        help=(
            "Source namespace/schema to introspect (for example public, dbo, "
            "or a Snowflake schema)"
        ),
    ),
    compare_last: bool = typer.Option(False, "--compare-last", help="Diff against previous snapshot"),
) -> None:
    """Introspect the schema from the source and save a snapshot.

    Uses the source's ``source_type`` (``postgresql``, ``mysql``,
    ``sqlserver``, ``snowflake``, ``clickhouse``, or ``csv``) to pick the
    right connector.
    """
    from r2g.snapshot import capture_source_snapshot

    mgr = _get_catalog()
    source = mgr.get_source(name)
    if source is None:
        console.print(f"[red]Source '{name}' not found.[/red]")
        raise typer.Exit(code=1)

    try:
        previous = mgr.get_latest_snapshot(name) if compare_last else None
        snap, result = capture_source_snapshot(
            mgr,
            name,
            schema_name=pg_schema,
        )
        schema = result.schema
        console.print(
            f"[green]Snapshot created:[/green] {snap.id}\n"
            f"  {len(schema.tables)} tables captured at {snap.captured_at.isoformat()}"
        )
        if result.classifications_applied:
            console.print(
                f"  [cyan]Annotated {result.classifications_applied} column(s) "
                "with catalog classifications.[/cyan]"
            )
        if result.key_overlay_summary:
            console.print(
                "  [cyan]Applied reviewed key overlay:[/cyan] "
                f"{result.key_overlay_summary['primaryKeys']} PK(s), "
                f"{result.key_overlay_summary['foreignKeys']} FK(s)."
            )

        if compare_last and previous is not None:
            from r2g.schema_diff import diff_schemas

            diff = diff_schemas(previous.schema_data, schema)
            has_changes = diff["added_tables"] or diff["removed_tables"] or diff["modified_tables"]
            if not has_changes:
                console.print("[green]No schema changes since last snapshot.[/green]")
            else:
                if diff["added_tables"]:
                    console.print("[green]Added tables:[/green] " + ", ".join(diff["added_tables"]))
                if diff["removed_tables"]:
                    console.print("[red]Removed tables:[/red] " + ", ".join(diff["removed_tables"]))
                if diff["modified_tables"]:
                    console.print(f"[yellow]Modified tables:[/yellow] {', '.join(diff['modified_tables'].keys())}")
        elif compare_last and previous is None:
            console.print("[dim]No previous snapshot to compare against.[/dim]")
    except Exception as e:
        log.exception("source_snapshot_failed")
        console.print(f"[red]Snapshot failed:[/red] {e}")
        raise typer.Exit(code=1)


@source_app.command("dump")
def source_dump(
    name: str = typer.Argument(..., help="Catalog source name"),
    output_dir: str = typer.Option(
        "./dumps", "--output-dir", "-o", help="Directory to write CSV files"
    ),
    pg_schema: str = typer.Option(
        "public",
        "--pg-schema",
        help="Source namespace/schema to dump (meaning depends on source type)",
    ),
    tables: Optional[str] = typer.Option(
        None,
        "--tables",
        "-t",
        help="Comma-separated list of tables (default: latest snapshot's tables)",
    ),
) -> None:
    """Dump every table in a cataloged source to CSV files.

    Source-agnostic replacement for the legacy ``r2g dump-tables
    --conn <pg_url>``. Uses :meth:`SourceSession.dump_table_to_csv`,
    so PostgreSQL goes through ``COPY TO STDOUT`` and Snowflake streams
    through the cursor; both produce comma-separated, header-row CSV.
    """
    from r2g.connectors.base import create_source_connector

    mgr = _get_catalog()
    source = mgr.get_source(name)
    if source is None:
        console.print(f"[red]Source '{name}' not found in catalog.[/red]")
        raise typer.Exit(code=1)

    snap = mgr.get_latest_snapshot(name)
    if tables:
        table_names = [t.strip() for t in tables.split(",") if t.strip()]
    elif snap is not None:
        table_names = sorted(snap.schema_data.tables.keys())
    else:
        console.print(
            "[red]No --tables given and no snapshot exists. "
            "Run `r2g source snapshot` first or pass --tables.[/red]"
        )
        raise typer.Exit(code=1)

    if not table_names:
        console.print("[yellow]No tables to dump.[/yellow]")
        return

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    try:
        connector = create_source_connector(
            source.source_type or "postgresql",
            source.connection_string,
            schema_name=pg_schema,
            source_params=source.source_params,
        )
    except ImportError as err:
        console.print(f"[red]{err}[/red]")
        raise typer.Exit(code=1)
    except ValueError as err:
        console.print(f"[red]{err}[/red]")
        raise typer.Exit(code=2)

    console.print(
        f"[green]Dumping {len(table_names)} tables from "
        f"{source.source_type} source '{name}' → {output_dir}/[/green]"
    )

    session = connector.open_session()
    try:
        total_rows = 0
        for tbl in table_names:
            csv_path = out / f"{tbl}.csv"
            try:
                rows = session.dump_table_to_csv(tbl, csv_path)
            except Exception as err:  # noqa: BLE001
                log.exception("source_dump_table_failed", source=name, table=tbl)
                console.print(f"  [red]{tbl}[/red] → failed: {err}")
                continue
            total_rows += rows
            console.print(f"  [cyan]{tbl}[/cyan] → {csv_path} ({rows} rows)")
        console.print(
            f"[green]Done. {len(table_names)} CSV files written "
            f"({total_rows:,} rows total).[/green]"
        )
    finally:
        session.close()


#: Source types suggest-keys can profile, with the schema used when --schema is
#: absent. Each needs a sampler that implements key profiling AND can be governed
#: (budget, per-query timeout, query tag, stats) -- relational-schema-analyzer's
#: sampler contract requires both together. Snowflake's default defers to the
#: schema in the connection URL, which RSA and r2g both let win over "PUBLIC".
_KEY_PROBE_SOURCES: dict[str, str] = {"snowflake": "PUBLIC", "postgresql": "public"}


def _key_probe_sampler(kind: str, connection_string: str, **governed: Any) -> Any:
    """A governed key-profiling sampler for ``kind`` (one of ``_KEY_PROBE_SOURCES``)."""
    from r2g.connectors.base import expand_env_vars

    url = expand_env_vars(connection_string)
    if kind == "snowflake":
        from relational_schema_analyzer.fk_inference import SnowflakeValueSampler

        return SnowflakeValueSampler(url, **governed)
    from r2g.fk_inference import PostgresValueSampler

    # Governance on the Postgres sampler arrived in relational-schema-analyzer 0.9.5.
    return PostgresValueSampler(url, **governed)


@source_app.command("suggest-keys")
def source_suggest_keys(
    name: str = typer.Argument(..., help="Source name"),
    output: Optional[str] = typer.Option(
        None, "--output", "-o", help="Where to write the draft (default: <name>.keys.draft.json)"
    ),
    force: bool = typer.Option(False, "--force", help="Overwrite an existing draft file"),
    schema_name: Optional[str] = typer.Option(
        None,
        "--schema",
        help="Schema to profile (default: the connection string's for Snowflake, public for PostgreSQL)",
    ),
    max_queries: int = typer.Option(200, "--max-queries", min=1, help="Hard cap on database queries for this run"),
    timeout_s: int = typer.Option(
        60, "--timeout", min=1, help="Per-query timeout in seconds, enforced by the database"
    ),
    sample_limit: int = typer.Option(
        10_000, "--sample-limit", min=100, help="Rows/values read per column when screening"
    ),
    min_pk_score: float = typer.Option(0.3, "--min-pk-score", help="Withhold weaker key proposals"),
    min_fk_confidence: float = typer.Option(0.6, "--min-fk-confidence", help="Withhold weaker reference proposals"),
) -> None:
    """Propose primary and foreign keys from data, as a DRAFT overlay for review.

    Profiles the source as it is (no existing overlay applied), proposes a key
    per table and the references between them, and writes a draft key overlay.
    Nothing is applied: review the file, then attach it with
    `r2g source set-key-overlay NAME FILE --reviewed` and re-run `source snapshot`.
    Snowflake and PostgreSQL; every query is counted against --max-queries, and the
    database cancels any single query past --timeout.
    """
    from r2g.connectors.base import create_source_connector, normalize_source_type

    mgr = _get_catalog()
    source = mgr.get_source(name)
    if source is None:
        console.print(f"[red]Source '{name}' not found.[/red]")
        raise typer.Exit(code=1)
    kind = normalize_source_type(source.source_type)
    if kind not in _KEY_PROBE_SOURCES:
        console.print(
            f"[red]suggest-keys supports Snowflake and PostgreSQL sources; '{name}' is "
            f"{source.source_type}.[/red] Use `source infer-fks` for references between declared keys."
        )
        raise typer.Exit(code=2)
    profiled_schema = schema_name or _KEY_PROBE_SOURCES[kind]
    out_path = Path(output or f"{name}.keys.draft.json").expanduser()
    if out_path.exists() and not force:
        console.print(f"[red]{out_path} exists.[/red] Pass --force to overwrite it.")
        raise typer.Exit(code=1)

    from relational_schema_analyzer.key_profiling import draft_key_overlay

    try:
        connector = create_source_connector(
            source.source_type,
            source.connection_string,
            schema_name=profiled_schema,
            source_params=source.source_params,
        )
        schema = connector.get_schema()
        with _key_probe_sampler(
            kind,
            source.connection_string,
            schema_name=profiled_schema,
            limit=sample_limit,
            max_queries=max_queries,
            statement_timeout_s=timeout_s,
            query_tag=f"r2g-suggest-keys:{name}",
        ) as probe:
            draft = draft_key_overlay(
                schema,
                probe,
                sampler=probe,
                min_pk_score=min_pk_score,
                min_fk_confidence=min_fk_confidence,
            )
            stats = probe.stats
    except Exception as e:  # noqa: BLE001
        log.exception("source_suggest_keys_failed")
        console.print(f"[red]Key suggestion failed:[/red] {e}")
        raise typer.Exit(code=1)

    # A sampler that never connected declines every probe, which would otherwise
    # read as a clean run that found no keys (exit 0, "Cost: 0 queries").
    if stats.get("connect_failed"):
        console.print(
            "[red]Key suggestion failed:[/red] the sampler could not connect to Snowflake, "
            "so no table was profiled. No draft written."
        )
        raise typer.Exit(code=1)

    out_path.write_text(json.dumps(draft.overlay, indent=2) + "\n")

    console.print(f"[bold]Proposed keys for '{name}'[/bold] (draft, nothing applied)")
    for table, cand in sorted(draft.primary_keys.items()):
        console.print(f"  [green]key[/green]  {table}: {', '.join(cand.columns)}  (score {cand.score:.2f})")
    for fk in draft.foreign_keys:
        console.print(
            f"  [cyan]ref[/cyan]  {fk.table}.{', '.join(fk.columns)} -> "
            f"{fk.foreign_table}.{', '.join(fk.foreign_columns)}  (confidence {fk.confidence:.2f})"
        )
    for table, why in sorted(draft.no_key_proposed.items()):
        console.print(f"  [yellow]none[/yellow] {table}: {why}")
    spent = f"{stats['queries_run']} of {stats['max_queries']} queries"
    # The snapshot must read the schema the draft was built from.
    snapshot_flag = f" --pg-schema {profiled_schema}" if kind == "postgresql" and profiled_schema != "public" else ""
    if stats["budget_exhausted"]:
        console.print(f"[yellow]Query budget used up ({spent}); some tables were not evaluated.[/yellow]")
    else:
        console.print(f"Cost: {spent}.")
    console.print(
        f"\nDraft written to [bold]{out_path}[/bold]. Review it -- delete what is wrong, add what "
        "is missing -- then:\n"
        f"  r2g source set-key-overlay {name} {out_path} --reviewed\n"
        f"  r2g source snapshot {name}{snapshot_flag}"
    )


def _check_overlay_structure(overlay: dict) -> None:
    """Run RSA's strict overlay validation now, not at the next snapshot.

    `load_key_overlay` only parses; unknown keys and malformed entries are
    rejected when the overlay is applied. Applying it to a stand-in schema made
    of exactly the names it mentions surfaces those mistakes -- a hand-edited
    `primarykey` -- immediately, without querying the source. Whether those
    tables and columns exist is still checked, loudly, at snapshot time.
    """
    from relational_schema_analyzer import apply_key_overlay
    from relational_schema_analyzer.types import PhysicalSchema

    columns: dict[str, set[str]] = {}

    def note(table: object, cols: object) -> None:
        if isinstance(table, str):
            names = columns.setdefault(table, set())
            if isinstance(cols, list):
                names.update(c for c in cols if isinstance(c, str))

    for table, spec in (overlay.get("tables") or {}).items():
        note(table, None)
        if not isinstance(spec, dict):
            continue
        note(table, spec.get("primaryKey"))
        for cols in spec.get("uniqueConstraints") or []:
            note(table, cols)
        for fk in spec.get("foreignKeys") or []:
            if isinstance(fk, dict):
                note(table, fk.get("columns"))
                ref = fk.get("references")
                if isinstance(ref, dict):
                    note(ref.get("table"), ref.get("columns"))
    stand_in = PhysicalSchema.model_validate(
        {
            "tables": {
                t: {"name": t, "columns": [{"name": c, "data_type": "text"} for c in sorted(cols)]}
                for t, cols in columns.items()
            }
        }
    )
    apply_key_overlay(stand_in, overlay)


@source_app.command("set-key-overlay")
def source_set_key_overlay(
    name: str = typer.Argument(..., help="Source name"),
    path: Optional[str] = typer.Argument(None, help="Reviewed key overlay (JSON or YAML)"),
    reviewed: bool = typer.Option(False, "--reviewed", help="Confirm a file still marked DRAFT has been reviewed"),
    clear: bool = typer.Option(False, "--clear", help="Remove the source's key overlay"),
) -> None:
    """Attach a reviewed key overlay to an existing source (applied at the next snapshot).

    A file whose description still starts with DRAFT -- as `suggest-keys` writes
    it -- is refused unless --reviewed is given: proposals become keys only by
    someone's decision.
    """
    mgr = _get_catalog()
    if mgr.get_source(name) is None:
        console.print(f"[red]Source '{name}' not found.[/red]")
        raise typer.Exit(code=1)
    if clear:
        if path:
            console.print("[red]Give a file or --clear, not both.[/red]")
            raise typer.Exit(code=2)
        mgr.update_source(name, key_overlay=None, key_overlay_source=None)
        console.print(f"Key overlay removed from '{name}'. Re-run `r2g source snapshot {name}`.")
        return
    if not path:
        console.print("[red]Give an overlay file, or --clear.[/red]")
        raise typer.Exit(code=2)

    from relational_schema_analyzer import load_key_overlay

    try:
        overlay = load_key_overlay(path)
        _check_overlay_structure(overlay)
    except Exception as e:  # noqa: BLE001
        console.print(f"[red]Not a valid key overlay:[/red] {e}")
        raise typer.Exit(code=1)
    description = str(overlay.get("description") or "")
    if description.lstrip().upper().startswith("DRAFT") and not reviewed:
        console.print(
            "[red]This overlay is still marked DRAFT.[/red] Review it, then either edit its "
            "description or pass --reviewed."
        )
        raise typer.Exit(code=1)
    mgr.update_source(name, key_overlay=overlay, key_overlay_source=str(Path(path).expanduser().resolve()))
    tables = overlay.get("tables") or {}
    pks = sum(1 for t in tables.values() if t.get("primaryKey"))
    fks = sum(len(t.get("foreignKeys") or []) for t in tables.values())
    console.print(
        f"Key overlay attached to '{name}': {pks} key(s), {fks} reference(s). "
        f"Run `r2g source snapshot {name}` to apply it."
    )


@source_app.command("infer-fks")
def source_infer_fks(
    name: str = typer.Argument(..., help="Source name"),
    sample: bool = typer.Option(
        False,
        "--sample",
        help=(
            "Run bounded value-overlap checks (PostgreSQL, MySQL/MariaDB, "
            "SQL Server, and CSV; other sources use name-only evidence)"
        ),
    ),
    sample_limit: int = typer.Option(
        10_000,
        "--sample-limit",
        help="Row cap per side for --sample queries",
    ),
    min_confidence: float = typer.Option(
        0.4,
        "--min-confidence",
        help="Drop candidates below this confidence (0..1)",
    ),
    allow_sensitive: bool = typer.Option(
        False, "--allow-sensitive",
        help="Value-sample above-threshold classified columns anyway (explicit opt-out)",
    ),
    sensitivity_threshold: str = typer.Option(
        "confidential", "--sensitivity-threshold",
        help="Classification level at/above which columns are excluded from sampling",
    ),
    accept: bool = typer.Option(
        False,
        "--accept",
        help=(
            "Write the candidates into a new snapshot as accepted keys: "
            "enforced=false, named inferred:<table>_<cols>_fk, with method and "
            "confidence recorded on the snapshot. Skips anything already declared."
        ),
    ),
) -> None:
    """Propose foreign keys for a source's latest schema snapshot.

    The name-based heuristic works for every stored ``source_type``.
    ``--sample`` additionally scores value overlap for PostgreSQL,
    MySQL/MariaDB, SQL Server, and CSV. Snowflake and ClickHouse sampling
    are not yet supported and fall back to name-only evidence.
    """
    from rich.table import Table as RichTable

    from r2g.connectors.base import normalize_source_type
    from r2g.fk_inference import (
        InferenceOptions,
        create_value_sampler,
        infer_foreign_keys,
    )

    mgr = _get_catalog()
    source = mgr.get_source(name)
    if source is None:
        console.print(f"[red]Source '{name}' not found.[/red]")
        raise typer.Exit(code=1)
    snap = mgr.get_latest_snapshot(name)
    if snap is None:
        console.print(
            f"[red]No snapshot for '{name}'. Run `r2g source snapshot {name}` first.[/red]"
        )
        raise typer.Exit(code=1)

    sampler = None
    if sample:
        sampler = create_value_sampler(
            source.source_type,
            source.connection_string,
            pg_schema=snap.pg_schema,
            source_params=source.source_params,
            limit=sample_limit,
        )
        if sampler is None:
            console.print(
                f"[yellow]--sample is only supported for PostgreSQL, MySQL, SQL Server, and CSV sources (got "
                f"'{normalize_source_type(source.source_type)}'); falling back to name-only inference.[/yellow]"
            )

    # Phase-9 gate: wrap the sampler so classified columns are never probed.
    # The overlap statistic never leaves the process, but reading a Restricted /
    # PII column is a policy decision r2g already answers one way at load time.
    if sampler is not None and not allow_sensitive:
        from r2g.fk_inference import gated_sampler

        gated = gated_sampler(
            sampler, snap.schema_data, threshold=sensitivity_threshold
        )
        if gated is not sampler:
            console.print(
                "[dim]Phase 9: classified columns excluded from value-overlap "
                "sampling (use --allow-sensitive to include).[/dim]"
            )
        sampler = gated

    opts = InferenceOptions(min_confidence=min_confidence, sample_overlap=bool(sampler))
    try:
        candidates = infer_foreign_keys(snap.schema_data, options=opts, sampler=sampler)
    finally:
        if sampler is not None:
            sampler.close()

    if not candidates:
        console.print("[dim]No FK candidates met the confidence threshold.[/dim]")
        return

    tbl = RichTable(title=f"Inferred FK candidates for '{name}'")
    tbl.add_column("Table")
    tbl.add_column("Columns")
    tbl.add_column("→ Foreign")
    tbl.add_column("Conf", justify="right")
    tbl.add_column("Method")
    for c in candidates:
        tbl.add_row(
            c.table,
            ", ".join(c.columns),
            f"{c.foreign_table}({', '.join(c.foreign_columns)})",
            f"{c.confidence:.2f}",
            c.method,
        )
    console.print(tbl)

    if accept:
        # An accepted key is a person's decision about a guess, not a constraint the
        # source declared: saved enforced=False and named inferred:<table>_<cols>_fk,
        # with its method and confidence on the snapshot. Unlabelled, RSA's baseline
        # would treat the table as authoritative and stop inferring there, and every
        # consumer would read the key as source-declared.
        accepted = 0
        schema = snap.schema_data
        records = list(snap.inferred_keys)
        for c in candidates:
            tbl_def = schema.tables.get(c.table)
            if tbl_def is None:
                continue
            existing = {tuple(sorted(fk.columns)) for fk in tbl_def.foreign_keys}
            if tuple(sorted(c.columns)) in existing:
                continue
            tbl_def.foreign_keys.append(c.to_accepted_foreign_key())
            records.append(c.accepted_record())
            accepted += 1
        if accepted:
            # Carry the overlay record forward: this snapshot is the previous one
            # plus accepted keys, so it was still built on that reviewed overlay.
            mgr.create_snapshot(
                name,
                schema,
                pg_schema=snap.pg_schema,
                key_overlay_summary=snap.key_overlay_summary,
                key_overlay_source=snap.key_overlay_source,
                key_overlay_fingerprint=snap.key_overlay_fingerprint,
                inferred_keys=records,
            )
            console.print(
                f"[green]Accepted {accepted} candidate(s); wrote a new snapshot "
                f"with merged FKs.[/green]"
            )
        else:
            console.print("[dim]No new FKs to accept.[/dim]")


@shared_keys_app.command("infer")
def shared_keys_infer(
    sources: Optional[List[str]] = typer.Option(
        None,
        "--source",
        "-s",
        help="Source to include (repeatable). Default: every snapshotted source.",
    ),
    sample: bool = typer.Option(
        False,
        "--sample",
        help=(
            "Run bounded value probes: hub-column uniqueness and cross-source "
            "value overlap. Sources whose type has no sampler still participate "
            "on name/type evidence."
        ),
    ),
    sample_limit: int = typer.Option(
        1_000, "--sample-limit", help="Distinct values pulled per side when sampling"
    ),
    min_confidence: float = typer.Option(
        0.4, "--min-confidence", help="Drop candidates below this confidence (0..1)"
    ),
    show_suppressed: bool = typer.Option(
        False,
        "--show-suppressed",
        help="Also list every grouped column that did NOT become a candidate, with the reason",
    ),
) -> None:
    """Propose cross-source shared keys (join keys) across catalog sources.

    Single-schema FK inference (``r2g source infer-fks``) is name-anchored to a
    parent table *in the same schema*, so a key shared across sources — the
    ClickHouse ``account_id`` whose ``Account`` lives in the PostgreSQL CRM, say
    — is invisible to it. This command groups key-shaped columns that co-occur
    across two or more sources, gates them on JSON-level type compatibility, and
    proposes a hub entity (or a virtual hub concept when no source owns the key).

    Proposals only: nothing is written to any mapping. Accept them in the Studio
    (``Actions -> Suggest shared keys``), which records them as declared join
    keys in the project mapping and the CSI / R2RML export.
    """
    from rich.table import Table as RichTable

    from r2g.xsk import SharedKeyOptions, infer_shared_keys

    mgr = _get_catalog()
    wanted = list(sources) if sources else [s.name for s in mgr.list_sources()]

    schemas = {}
    for source_name in wanted:
        source = mgr.get_source(source_name)
        if source is None:
            console.print(f"[yellow]Source '{source_name}' not found; skipping.[/yellow]")
            continue
        snap = mgr.get_latest_snapshot(source_name)
        if snap is None:
            console.print(
                f"[yellow]No snapshot for '{source_name}'; skipping "
                f"(run `r2g source snapshot {source_name}`).[/yellow]"
            )
            continue
        schemas[source_name] = snap.schema_data

    if len(schemas) < 2:
        console.print(
            f"[red]Cross-source inference needs at least two snapshotted sources; "
            f"got {len(schemas)}.[/red]"
        )
        raise typer.Exit(code=1)

    samplers = {}
    if sample:
        from r2g.fk_inference import create_value_sampler

        for source_name in schemas:
            source = mgr.get_source(source_name)
            snap = mgr.get_latest_snapshot(source_name)
            try:
                sampler = create_value_sampler(
                    source.source_type,
                    source.connection_string,
                    pg_schema=snap.pg_schema,
                    source_params=source.source_params,
                    limit=sample_limit,
                )
            except Exception as err:  # noqa: BLE001
                console.print(f"[yellow]Sampler init failed for '{source_name}': {err}[/yellow]")
                sampler = None
            if sampler is None:
                console.print(
                    f"[yellow]No value sampler for '{source_name}' "
                    f"({source.source_type}); using name/type evidence only.[/yellow]"
                )
            samplers[source_name] = sampler

    opts = SharedKeyOptions(
        min_confidence=min_confidence, sample_overlap=sample, sample_limit=sample_limit
    )
    try:
        result = infer_shared_keys(schemas, options=opts, samplers=samplers)
    finally:
        for sampler in samplers.values():
            if sampler is not None:
                try:
                    sampler.close()
                except Exception:  # noqa: BLE001
                    pass

    console.print(
        f"[dim]Considered {len(result.sources_considered)} sources: "
        f"{', '.join(result.sources_considered)}[/dim]"
    )

    if not result.candidates:
        console.print("[dim]No shared-key candidates met the confidence threshold.[/dim]")
    for cand in result.candidates:
        hub = cand.hub
        if hub.kind == "entity":
            where = f"{hub.source}.{hub.table}.{hub.column}"
            head = f"hub [bold]{hub.concept}[/bold] = {where}"
        else:
            head = f"virtual hub [bold]{hub.concept}[/bold] (no source owns this key)"
        console.print(
            f"\n[green]{cand.confidence:.2f}[/green] [cyan]{cand.key}[/cyan] — {head} "
            f"[dim]({cand.method})[/dim]"
        )
        tbl = RichTable(show_header=True, header_style="dim")
        tbl.add_column("Referencing source")
        tbl.add_column("Table")
        tbl.add_column("Column")
        tbl.add_column("JSON type")
        tbl.add_column("Overlap", justify="right")
        for ref in cand.references:
            tbl.add_row(
                ref.source,
                ref.table,
                ref.column,
                ref.json_type,
                "-" if ref.overlap is None else f"{ref.overlap:.0%}",
            )
        console.print(tbl)
        for line in cand.evidence:
            console.print(f"  [dim]· {line}[/dim]")

    if show_suppressed and result.suppressed:
        console.print(f"\n[dim]Suppressed ({len(result.suppressed)}):[/dim]")
        stbl = RichTable(show_header=True, header_style="dim")
        stbl.add_column("Key")
        stbl.add_column("Where")
        stbl.add_column("Reason")
        stbl.add_column("Detail")
        for s in result.suppressed:
            stbl.add_row(s.key, f"{s.source}.{s.table}.{s.column}", s.reason, s.detail)
        console.print(stbl)
    elif result.suppressed:
        console.print(
            f"\n[dim]{len(result.suppressed)} grouped column(s) suppressed; "
            f"re-run with --show-suppressed to see why.[/dim]"
        )


@source_app.command("analyze-denorm")
def source_analyze_denorm(
    name: str = typer.Argument(..., help="Source name"),
    sample: bool = typer.Option(
        False,
        "--sample",
        help=(
            "Run bounded data probes to detect embedded lookups / functional "
            "dependencies (PostgreSQL, MySQL, SQL Server, CSV). Structural "
            "detectors (repeating groups) run regardless."
        ),
    ),
    sample_limit: int = typer.Option(
        10_000,
        "--sample-limit",
        help="Row cap per probe for --sample queries",
    ),
    min_confidence: float = typer.Option(
        0.4,
        "--min-confidence",
        help="Drop findings below this confidence (0..1)",
    ),
    no_sample_columns: str = typer.Option(
        "",
        "--no-sample-columns",
        help=(
            "Comma-separated columns to never value-sample (bare 'col' or "
            "'table.col'). Additional to the automatic Phase-9 gate below."
        ),
    ),
    allow_sensitive: bool = typer.Option(
        False, "--allow-sensitive",
        help="Value-sample above-threshold classified columns anyway (explicit opt-out)",
    ),
    sensitivity_threshold: str = typer.Option(
        "confidential", "--sensitivity-threshold",
        help="Classification level at/above which columns are excluded from sampling",
    ),
    as_json: bool = typer.Option(False, "--json", help="Emit findings as JSON"),
) -> None:
    """Detect denormalization smells in a source's latest snapshot.

    Surfaces *advisory*, evidence-backed findings (a repeating column family, or
    a non-key column that functionally determines other columns = an embedded
    lookup) with a recommended graph remedy. Read-only: it changes nothing.
    """
    import json as _json

    from rich.table import Table as RichTable

    from r2g.connectors.base import normalize_source_type
    from r2g.denorm import AnalyzeOptions, analyze_denormalization, remediation_hint, with_hints
    from r2g.fk_inference import create_value_sampler

    mgr = _get_catalog()
    source = mgr.get_source(name)
    if source is None:
        console.print(f"[red]Source '{name}' not found.[/red]")
        raise typer.Exit(code=1)
    snap = mgr.get_latest_snapshot(name)
    if snap is None:
        console.print(
            f"[red]No snapshot for '{name}'. Run `r2g source snapshot {name}` first.[/red]"
        )
        raise typer.Exit(code=1)

    sampler = None
    if sample:
        sampler = create_value_sampler(
            source.source_type,
            source.connection_string,
            pg_schema=snap.pg_schema,
            source_params=source.source_params,
            limit=sample_limit,
        )
        if sampler is None:
            console.print(
                f"[yellow]--sample is only supported for PostgreSQL, MySQL, SQL Server, and CSV "
                f"sources (got '{normalize_source_type(source.source_type)}'); running structural "
                f"detectors only.[/yellow]"
            )

    excluded = frozenset(c.strip() for c in no_sample_columns.split(",") if c.strip())
    # Phase-9 gate: classified columns are excluded from value sampling by
    # default, matching the loader's rule (excluded unless --allow-sensitive).
    # Previously `no_sample_columns` was populated ONLY from the flag above, so
    # the gate existed but nothing derived it from the classification map.
    if not allow_sensitive:
        from r2g.classification import sensitive_columns

        auto = sensitive_columns(snap.schema_data, threshold=sensitivity_threshold)
        if auto:
            console.print(
                f"[dim]Phase 9: {len(auto) // 2} classified column(s) excluded from "
                f"sampling (use --allow-sensitive to include).[/dim]"
            )
        excluded = excluded | auto
    opts = AnalyzeOptions(
        sample=bool(sampler),
        sample_limit=sample_limit,
        min_confidence=min_confidence,
        no_sample_columns=excluded,
    )
    try:
        findings = analyze_denormalization(snap.schema_data, options=opts, sampler=sampler)
    finally:
        if sampler is not None and hasattr(sampler, "close"):
            sampler.close()

    if as_json:
        console.print_json(_json.dumps(with_hints(findings)))
        return

    if not findings:
        console.print("[dim]No denormalization findings met the confidence threshold.[/dim]")
        return

    tbl = RichTable(title=f"Denormalization findings for '{name}'")
    tbl.add_column("Kind")
    tbl.add_column("Table")
    tbl.add_column("Columns")
    tbl.add_column("Conf", justify="right")
    tbl.add_column("Suggested remediation")
    for f in findings:
        tbl.add_row(
            f.kind,
            f.table,
            ", ".join(f.columns),
            f"{f.confidence:.2f}",
            remediation_hint(f),
        )
    console.print(tbl)
    console.print(
        "[dim]Advisory only — r2g does not modify your schema, data, or mapping.[/dim]"
    )


# ── External data catalog commands (Phase 8) ─────────────────────────


def _resolve_env_ref(value: str) -> str:
    """Resolve a ``$ENV_VAR`` reference to its environment value.

    A leading ``$`` means "read this from the environment" (same convention as
    source connection strings), so secrets can stay out of the catalog. A plain
    value is returned unchanged.
    """
    if value and value.startswith("$"):
        return os.environ.get(value[1:], value)
    return value


def _get_catalog_provider(mgr, name: str):
    """Build a live catalog provider from a registered catalog config, or exit."""
    from r2g.catalogs.base import create_catalog_provider

    cfg = mgr.get_catalog(name)
    if cfg is None:
        console.print(f"[red]Catalog '{name}' not found.[/red] Register it with `r2g catalog add`.")
        raise typer.Exit(code=1)
    token = _resolve_env_ref(cfg.token) if cfg.token else None
    return create_catalog_provider(
        cfg.provider_type,
        cfg.endpoint,
        name=cfg.name,
        token=token,
        params=cfg.params,
    )


@catalog_app.command("add")
def catalog_add(
    name: str = typer.Option(..., "--name", help="Local name for this catalog connection"),
    provider_type: str = typer.Option("openmetadata", "--type", help="Catalog type: openmetadata"),
    endpoint: str = typer.Option(..., "--endpoint", help="Catalog base URL, e.g. http://localhost:8585"),
    token: str = typer.Option(
        "", "--token", help="API token (or $ENV_VAR reference); stored encrypted"
    ),
    description: str = typer.Option("", "--description", help="Description"),
) -> None:
    """Register an external data catalog for source discovery."""
    try:
        mgr = _get_catalog()
        # Token stored as-is (may be a $ENV_VAR reference, resolved at use time).
        provider = mgr.add_catalog(
            name, provider_type, endpoint, token=token, description=description
        )
        console.print(f"[green]Catalog '{provider.name}' added[/green] ({provider.provider_type}).")
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(code=1)
    except Exception as e:
        log.exception("catalog_add_failed")
        console.print(f"[red]Failed to add catalog:[/red] {e}")
        raise typer.Exit(code=1)


@catalog_app.command("list")
def catalog_list() -> None:
    """List registered external data catalogs."""
    mgr = _get_catalog()
    catalogs = mgr.list_catalogs()
    if not catalogs:
        console.print("[dim]No catalogs registered.[/dim] Add one with `r2g catalog add`.")
        return
    table = RichTable(title="External Data Catalogs")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="magenta")
    table.add_column("Endpoint")
    table.add_column("Description")
    for c in catalogs:
        table.add_row(c.name, c.provider_type, c.endpoint, c.description)
    console.print(table)


@catalog_app.command("browse")
def catalog_browse(
    name: str = typer.Argument(..., help="Registered catalog name"),
    path: Optional[str] = typer.Option(
        None, "--path", help="Asset FQN to descend into (e.g. service.database)"
    ),
    search: Optional[str] = typer.Option(None, "--search", help="Search tables by text"),
) -> None:
    """Browse a catalog: top-level sources, the children of --path, or --search results."""
    mgr = _get_catalog()
    provider = _get_catalog_provider(mgr, name)
    try:
        if search:
            assets = provider.search(search)
        elif path:
            asset = provider.get_asset(path)
            if asset is None:
                console.print(f"[red]Asset '{path}' not found in catalog '{name}'.[/red]")
                raise typer.Exit(code=1)
            assets = provider.list_children(asset)
        else:
            assets = provider.list_data_sources()
    except Exception as e:
        log.exception("catalog_browse_failed")
        console.print(f"[red]Catalog browse failed:[/red] {e}")
        raise typer.Exit(code=1)

    if not assets:
        console.print("[dim]No assets found.[/dim]")
        return
    table = RichTable(title=f"Catalog: {name}")
    table.add_column("Kind", style="magenta")
    table.add_column("Name", style="cyan")
    table.add_column("Source type")
    table.add_column("FQN")
    for a in assets:
        table.add_row(a.kind, a.name, a.source_type or "—", a.fqn)
    console.print(table)


@catalog_app.command("import-source")
def catalog_import_source(
    name: str = typer.Argument(..., help="Registered catalog name"),
    asset_fqn: str = typer.Argument(..., help="Asset FQN to import (database / schema / topic)"),
    source_name: str = typer.Option(..., "--as", help="Name for the new r2g source"),
    description: str = typer.Option("", "--description", help="Description for the new source"),
) -> None:
    """Resolve a catalog asset into an r2g source (discover-then-connect).

    Credentials are NOT taken from the catalog: the generated connection string
    uses ``$R2G_DB_USER`` / ``$R2G_DB_PASSWORD`` placeholders that r2g resolves
    from the environment at connect time.
    """
    mgr = _get_catalog()
    provider = _get_catalog_provider(mgr, name)
    try:
        asset = provider.get_asset(asset_fqn)
        if asset is None:
            console.print(f"[red]Asset '{asset_fqn}' not found in catalog '{name}'.[/red]")
            raise typer.Exit(code=1)
        resolved = provider.resolve_source(asset)
        mgr.add_source(
            source_name,
            resolved.source_type,
            resolved.connection_string,
            description=description or f"Imported from catalog '{name}' ({asset_fqn})",
            source_params=resolved.source_params,
            classifications=resolved.column_classifications,
            data_owners=resolved.owners,
            data_tier=resolved.tier,
            catalog_name=name,
            catalog_asset_fqn=asset_fqn,
        )
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(code=1)
    except Exception as e:
        log.exception("catalog_import_failed")
        console.print(f"[red]Import failed:[/red] {e}")
        raise typer.Exit(code=1)

    console.print(
        f"[green]Source '{source_name}' imported[/green] "
        f"({resolved.source_type}) from catalog '{name}'."
    )
    console.print(f"  Connection: [dim]{resolved.connection_string}[/dim]")
    classified_cols = sum(len(cols) for cols in resolved.column_classifications.values())
    if classified_cols:
        console.print(
            f"  [cyan]Captured classifications for {classified_cols} column(s) "
            f"across {len(resolved.column_classifications)} table(s).[/cyan]"
        )
    if resolved.tier:
        console.print(f"  Tier: [cyan]{resolved.tier}[/cyan]")
    if resolved.notes:
        console.print(f"  [yellow]{resolved.notes}[/yellow]")
    if resolved.schema_name:
        console.print(
            f"  Schema: [cyan]{resolved.schema_name}[/cyan] "
            f"(pass [bold]--pg-schema {resolved.schema_name}[/bold] to `source snapshot`)."
        )


@catalog_app.command("remove")
def catalog_remove(
    name: str = typer.Argument(..., help="Catalog name to remove"),
) -> None:
    """Remove a registered external data catalog."""
    mgr = _get_catalog()
    if mgr.remove_catalog(name):
        console.print(f"[green]Catalog '{name}' removed.[/green]")
    else:
        console.print(f"[yellow]Catalog '{name}' not found.[/yellow]")
        raise typer.Exit(code=1)


@catalog_app.command("resync-classifications")
def catalog_resync_classifications(
    source_name: str = typer.Argument(..., help="r2g source name (imported from a catalog)"),
) -> None:
    """Re-pull classifications from the bound catalog and refresh the source.

    A one-time copy snapshots policy at migration time; source policy drifts.
    This re-queries the catalog/asset the source was imported from, updates the
    stored classifications/owners/tier (+ a ``classifications_synced_at``
    timestamp), and re-merges them onto the latest snapshot's columns so the
    next entitlement report / load reflects current policy.
    """
    from datetime import datetime, timezone

    from r2g.classification import annotate_schema, diff_classifications

    mgr = _get_catalog()
    source = mgr.get_source(source_name)
    if source is None:
        console.print(f"[red]Source '{source_name}' not found.[/red]")
        raise typer.Exit(code=1)
    if not source.catalog_name or not source.catalog_asset_fqn:
        console.print(
            f"[red]Source '{source_name}' was not imported from a catalog.[/red] "
            "Re-sync needs a catalog binding (use `r2g catalog import-source`)."
        )
        raise typer.Exit(code=1)

    provider = _get_catalog_provider(mgr, source.catalog_name)
    try:
        asset = provider.get_asset(source.catalog_asset_fqn)
        if asset is None:
            console.print(
                f"[red]Asset '{source.catalog_asset_fqn}' not found in catalog "
                f"'{source.catalog_name}'.[/red]"
            )
            raise typer.Exit(code=1)
        resolved = provider.resolve_source(asset)
    except typer.Exit:
        raise
    except Exception as e:
        log.exception("catalog_resync_failed")
        console.print(f"[red]Re-sync failed:[/red] {e}")
        raise typer.Exit(code=1)

    # Diff against the previously stored classifications before overwriting, so
    # tier drift (especially escalations) is surfaced to the operator.
    deltas = diff_classifications(source.classifications, resolved.column_classifications)

    now = datetime.now(timezone.utc)
    mgr.update_source(
        source_name,
        classifications=resolved.column_classifications,
        data_owners=resolved.owners,
        data_tier=resolved.tier,
        classifications_synced_at=now,
    )

    classified_cols = sum(len(cols) for cols in resolved.column_classifications.values())
    console.print(
        f"[green]Re-synced classifications for source '{source_name}'[/green] "
        f"from catalog '{source.catalog_name}' ({classified_cols} column(s) across "
        f"{len(resolved.column_classifications)} table(s))."
    )

    if deltas:
        escalations = [d for d in deltas if d.escalated]
        dtable = RichTable(title="Classification changes since last sync")
        dtable.add_column("Column", style="cyan")
        dtable.add_column("Was")
        dtable.add_column("Now")
        dtable.add_column("Direction")
        for d in deltas:
            arrow = "[red]\u2191 escalated[/red]" if d.escalated else "[dim]\u2193 de-escalated[/dim]"
            dtable.add_row(f"{d.table}.{d.column}", d.old_level, d.new_level, arrow)
        console.print(dtable)
        if escalations:
            console.print(
                f"[yellow]Advisory:[/yellow] {len(escalations)} column(s) escalated. "
                "Already-loaded data is not retro-gated; re-run the load (the gate "
                "excludes them by default) or mask them. In CDC/temporal mode, pass "
                "[bold]--govern[/bold] so changed rows carry the new policy."
            )
    else:
        console.print("  [dim]No lattice-level changes since last sync.[/dim]")

    snap = mgr.get_latest_snapshot(source_name)
    if snap is not None and resolved.column_classifications:
        annotated = annotate_schema(snap.schema_data, resolved.column_classifications)
        mgr.update_snapshot_schema(snap.id, snap.schema_data)
        console.print(
            f"  [cyan]Re-merged onto latest snapshot: {annotated} column(s) "
            f"annotated.[/cyan]"
        )
    elif snap is None:
        console.print(
            "  [yellow]No snapshot yet; run `r2g source snapshot` to apply.[/yellow]"
        )
    console.print(f"  Synced at: [dim]{now.isoformat()}[/dim]")


# ── Project commands ─────────────────────────────────────────────────


@project_app.command("create")
def project_create(
    name: str = typer.Option(..., "--name", help="Project name"),
    source: str = typer.Option(..., "--source", help="Source name"),
    mapping: str = typer.Option(..., "--mapping", help="Path to mapping config"),
    endpoint: str = typer.Option("http://localhost:8529", "--endpoint", help="ArangoDB endpoint URL"),
    database: str = typer.Option("_system", "--database", help="ArangoDB database name"),
) -> None:
    """Create a new project."""
    try:
        mgr = _get_catalog()
        project = mgr.create_project(name, source, mapping, arango_endpoint=endpoint, arango_database=database)
        console.print(f"[green]Project '{project.name}' created.[/green]")
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(code=1)
    except Exception as e:
        log.exception("project_create_failed")
        console.print(f"[red]Failed to create project:[/red] {e}")
        raise typer.Exit(code=1)


@project_app.command("list")
def project_list() -> None:
    """List all projects."""
    mgr = _get_catalog()
    projects = mgr.list_projects()
    if not projects:
        console.print("[dim]No projects registered.[/dim]")
        return
    table = RichTable(title="Projects")
    table.add_column("Name", style="cyan")
    table.add_column("Source", style="magenta")
    table.add_column("Mapping Config")
    table.add_column("ArangoDB Endpoint")
    table.add_column("Database")
    for p in projects:
        table.add_row(p.name, p.source_name, p.mapping_config_path, p.arango_endpoint, p.arango_database)
    console.print(table)


@project_app.command("status")
def project_status(
    name: str = typer.Argument(..., help="Project name"),
) -> None:
    """Show the status of a project (last load, snapshot age, mapping path)."""
    mgr = _get_catalog()
    project = mgr.get_project(name)
    if project is None:
        console.print(f"[red]Project '{name}' not found.[/red]")
        raise typer.Exit(code=1)

    table = RichTable(title=f"Project: {name}")
    table.add_column("Property")
    table.add_column("Value")
    table.add_row("Source", project.source_name)
    table.add_row("Mapping Config", project.mapping_config_path)
    table.add_row("ArangoDB Endpoint", project.arango_endpoint)
    table.add_row("ArangoDB Database", project.arango_database)
    table.add_row("Schema Snapshot ID", project.schema_snapshot_id or "[dim]none[/dim]")

    history = mgr.get_history(project_name=name, limit=1)
    if history:
        last = history[0]
        table.add_row("Last Load Status", last.status)
        table.add_row("Last Load Type", last.load_type)
        table.add_row("Last Load Rows", str(last.rows_loaded))
        table.add_row("Last Load Started", last.started_at.isoformat())
    else:
        table.add_row("Last Load", "[dim]none[/dim]")

    console.print(table)


# ── Entitlements (governance) commands ────────────────────────────────


@entitlements_app.command("report")
def entitlements_report(
    project: str = typer.Argument(..., help="Project name"),
    threshold: str = typer.Option(
        "confidential",
        "--threshold",
        help="Lattice level to flag at/above: public|internal|confidential|restricted",
    ),
    as_json: bool = typer.Option(False, "--json", help="Emit the full report as JSON"),
) -> None:
    """Report classified fields at/above a sensitivity threshold for a project.

    Walks the project's mapping over its latest (classification-annotated)
    snapshot, recomputes entity sensitivity (the mosaic = max-of-contributors
    rule), and lists every mapped field at/above ``--threshold`` with its source
    lineage. Advisory only — r2g carries governance metadata and advises; it does
    not enforce access at query time.
    """
    from r2g.classification import SENSITIVITY_ORDER
    from r2g.governance import build_entitlement_report

    threshold = threshold.strip().lower()
    if threshold not in SENSITIVITY_ORDER:
        console.print(
            f"[red]Invalid --threshold '{threshold}'.[/red] "
            f"Expected one of: {', '.join(SENSITIVITY_ORDER)}."
        )
        raise typer.Exit(code=1)

    mgr = _get_catalog()
    proj = mgr.get_project(project)
    if proj is None:
        console.print(f"[red]Project '{project}' not found.[/red]")
        raise typer.Exit(code=1)

    snap = mgr.get_latest_snapshot(proj.source_name)
    if snap is None:
        console.print(
            f"[red]No schema snapshot for source '{proj.source_name}'.[/red] "
            "Run `r2g source snapshot` first."
        )
        raise typer.Exit(code=1)

    try:
        config = ConfigManager.load_config(proj.mapping_config_path)
    except Exception as e:
        console.print(f"[red]Failed to load mapping config:[/red] {e}")
        raise typer.Exit(code=1)

    report = build_entitlement_report(
        config, snap.schema_data, threshold=threshold, project=project
    )

    if as_json:
        payload = report.model_dump()
        payload["summary"] = report.summary()
        console.print_json(json.dumps(payload, default=str))
        return

    above = report.above_threshold
    masked = report.masked_fields
    if not above:
        console.print(
            f"[green]No mapped fields at/above '{threshold}' for project "
            f"'{project}'.[/green]"
        )
    else:
        table = RichTable(title=f"Entitlement report for '{project}' (\u2265 {threshold})")
        table.add_column("Target", style="cyan")
        table.add_column("Level")
        table.add_column("Source columns", style="dim")
        table.add_column("Tags", style="dim")
        for f in above:
            table.add_row(
                f"{f.target_collection}.{f.target_property}",
                f.level,
                ", ".join(f"{f.source_table}.{c}" for c in f.source_columns),
                ", ".join(f.tags) or "[dim]\u2014[/dim]",
            )
        console.print(table)

    s = report.summary()
    console.print(
        f"[dim]{s['total_fields']} mapped fields; {s['above_threshold']} at/above "
        f"threshold; {s['masked']} masked.[/dim]"
    )
    if masked:
        console.print(
            "[dim]Masked: "
            + ", ".join(
                f"{f.target_collection}.{f.target_property} ({f.mask_kind})" for f in masked
            )
            + "[/dim]"
        )
    if above:
        console.print(
            "[yellow]Advisory:[/yellow] at load, above-threshold fields are excluded "
            "by default (pass --allow-sensitive to override, or mask them). r2g advises; "
            "the serving layer enforces."
        )


@entitlements_app.command("emit")
def entitlements_emit(
    project: str = typer.Argument(..., help="Project name"),
    threshold: str = typer.Option(
        "confidential",
        "--threshold",
        help="Lattice level to flag at/above: public|internal|confidential|restricted",
    ),
    out: Optional[str] = typer.Option(
        None, "--out", help="Output directory (default: the project's mapping dir)"
    ),
    tier_layout: bool = typer.Option(
        False, "--tier-layout", help="Also emit a per-tier physical-layout recommendation"
    ),
    no_rego: bool = typer.Option(
        False, "--no-rego", help="Skip the OPA/Rego policy stub"
    ),
) -> None:
    """Emit the Phase 9c governance artifacts for a project (advise, don't enforce).

    Writes, under ``<out>/governance/``: the lineage manifest, a canonical
    ``classification-manifest.json``, a ``suggested-rbac.json`` collection-grant
    table (by clearance), a ``policy.rego`` OPA stub, and (with --tier-layout) a
    ``tier-layout.json`` recommendation. r2g emits this metadata; the serving
    layer (ArangoDB RBAC / OPA / IdP) enforces it.
    """
    from r2g.classification import SENSITIVITY_ORDER
    from r2g.governance import build_entitlement_report, write_governance_artifacts

    threshold = threshold.strip().lower()
    if threshold not in SENSITIVITY_ORDER:
        console.print(
            f"[red]Invalid --threshold '{threshold}'.[/red] "
            f"Expected one of: {', '.join(SENSITIVITY_ORDER)}."
        )
        raise typer.Exit(code=1)

    mgr = _get_catalog()
    proj = mgr.get_project(project)
    if proj is None:
        console.print(f"[red]Project '{project}' not found.[/red]")
        raise typer.Exit(code=1)

    snap = mgr.get_latest_snapshot(proj.source_name)
    if snap is None:
        console.print(
            f"[red]No schema snapshot for source '{proj.source_name}'.[/red] "
            "Run `r2g source snapshot` first."
        )
        raise typer.Exit(code=1)

    try:
        config = ConfigManager.load_config(proj.mapping_config_path)
    except Exception as e:
        console.print(f"[red]Failed to load mapping config:[/red] {e}")
        raise typer.Exit(code=1)

    source = mgr.get_source(proj.source_name)
    owners = source.data_owners if source else []
    synced_at = (
        source.classifications_synced_at.isoformat()
        if source and source.classifications_synced_at
        else None
    )

    report = build_entitlement_report(
        config, snap.schema_data, threshold=threshold, project=project
    )

    out_dir = out or str(Path(proj.mapping_config_path).parent)
    try:
        written = write_governance_artifacts(
            report,
            out_dir,
            owners=owners,
            database=proj.arango_database,
            tier_layout=tier_layout,
            emit_rego=not no_rego,
            synced_at=synced_at,
        )
    except OSError as e:
        console.print(f"[red]Failed to write governance artifacts:[/red] {e}")
        raise typer.Exit(code=1)

    console.print(
        f"[green]Emitted {len(written)} governance artifact(s)[/green] for "
        f"project '{project}':"
    )
    for name_, path in written.items():
        console.print(f"  [cyan]{name_}[/cyan] [dim]{path}[/dim]")
    console.print(
        "[dim]Advisory: r2g emits this governance metadata; the serving layer "
        "(ArangoDB RBAC / OPA / IdP) enforces it.[/dim]"
    )


# ── Ontology suggestion (LLM-assisted, Phase 10) ─────────────────────


@ontology_app.command("suggest")
def ontology_suggest(
    project: str = typer.Argument(..., help="Project name"),
    domain: str = typer.Option(
        "", "--domain", help="Optional domain hint to ground the proposal"
    ),
    engine: str = typer.Option(
        "llm",
        "--engine",
        help=(
            "Derivation engine: 'llm' (model-proposed) or 'rsa' "
            "(deterministic conceptual model via relational-schema-analyzer, offline by default)"
        ),
    ),
    provider: str = typer.Option(
        "openai",
        "--provider",
        help="LLM provider: openai, anthropic, or openai-compatible (local/hosted)",
    ),
    model: Optional[str] = typer.Option(None, "--model", help="Model name (provider default if omitted)"),
    api_key: Optional[str] = typer.Option(
        None,
        "--api-key",
        help="API key or $ENV_VAR reference (defaults to the provider's env key, e.g. $OPENAI_API_KEY)",
    ),
    base_url: Optional[str] = typer.Option(
        None,
        "--base-url",
        help="Endpoint base URL (required for openai-compatible, e.g. http://localhost:11434/v1)",
    ),
    sample: bool = typer.Option(
        False,
        "--sample",
        help="Include a few example values per column (non-sensitive columns only) to ground the model",
    ),
    samples_per_column: int = typer.Option(
        5, "--samples-per-column", help="Max example values per column when --sample is set"
    ),
    ground: bool = typer.Option(
        False,
        "--ground",
        help="Add deterministic denormalization findings (Phase 11) as advisory evidence for the model",
    ),
    refine: bool = typer.Option(
        False,
        "--refine",
        help="With --engine rsa: additively LLM-refine the deterministic model using --provider (optional)",
    ),
    apply: bool = typer.Option(False, "--apply", help="Write the proposed mapping to the project"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt when applying"),
    as_json: bool = typer.Option(False, "--json", help="Emit proposal, diff and provenance as JSON"),
) -> None:
    """Propose a richer target ontology for a project (Phase 10).

    Two engines are available:

    - ``--engine llm`` (default): a model *proposes* an ontology. Metadata-only —
      no row data is sent; Phase-9 Restricted columns are redacted.
    - ``--engine rsa``: the shared ``relational-schema-analyzer`` derives a
      conceptual model **deterministically and offline** (no network by default);
      add ``--refine`` to additively LLM-refine it.

    Either way the output is only a *proposal*: it is converted into a candidate
    ``MappingConfig``, validated and repaired against the real schema
    (hallucinated tables/columns are dropped and reported), and shown as a diff
    against the current mapping. Nothing is saved unless you pass ``--apply``.
    """
    import datetime as _dt

    from r2g.llm import proposal_to_mapping
    from r2g.mapping_diff import diff_mappings

    mgr = _get_catalog()
    proj = mgr.get_project(project)
    if proj is None:
        console.print(f"[red]Project '{project}' not found.[/red]")
        raise typer.Exit(code=1)

    snap = mgr.get_latest_snapshot(proj.source_name)
    if snap is None:
        console.print(
            f"[red]No schema snapshot for source '{proj.source_name}'.[/red] "
            "Run `r2g source snapshot` first."
        )
        raise typer.Exit(code=1)
    schema = snap.schema_data

    try:
        current = ConfigManager.load_config(proj.mapping_config_path)
    except Exception as e:
        console.print(f"[red]Failed to load mapping config:[/red] {e}")
        raise typer.Exit(code=1)

    engine_norm = engine.strip().lower()
    if engine_norm not in ("llm", "rsa"):
        console.print(f"[red]Unknown --engine '{engine}'. Expected 'llm' or 'rsa'.[/red]")
        raise typer.Exit(code=1)

    resolved_key = _resolve_env_ref(api_key) if api_key else None

    if engine_norm == "rsa":
        from r2g.rsa_ontology import propose_ontology_from_schema

        rsa_provider = provider if refine else None
        if not as_json:
            mode = f"refining with {provider}" if refine else "deterministic, offline"
            console.print(
                f"[dim]Deriving ontology via relational-schema-analyzer ({mode})…[/dim]"
            )
        try:
            proposal, rsa_meta = propose_ontology_from_schema(
                schema, provider=rsa_provider, model=model, api_key=resolved_key
            )
        except ImportError as e:
            console.print(f"[red]{e}[/red]")
            raise typer.Exit(code=1)
        except Exception as e:  # noqa: BLE001 - surface analyzer failures cleanly
            console.print(f"[red]Ontology analysis failed:[/red] {e}")
            raise typer.Exit(code=1)

        llm_info = rsa_meta.get("llm") if isinstance(rsa_meta, dict) else None
        provenance = {
            "engine": "relational-schema-analyzer",
            "refined": bool(refine),
            "provider": provider if refine else None,
            "model": (model or "(provider default)") if refine else None,
            "domain_hint": domain,
            "table_count": len(schema.tables),
            "analyzer_confidence": rsa_meta.get("confidence") if isinstance(rsa_meta, dict) else None,
            "detected_patterns": rsa_meta.get("detectedPatterns") if isinstance(rsa_meta, dict) else None,
            "review_required": rsa_meta.get("reviewRequired") if isinstance(rsa_meta, dict) else None,
            "physical_schema_fingerprint": (
                rsa_meta.get("physicalSchemaFingerprint") if isinstance(rsa_meta, dict) else None
            ),
            "llm_refinement": llm_info,
            "proposed_collections": len(proposal.collections),
            "proposed_edges": len(proposal.edges),
            "proposed_renames": len(proposal.renames),
            "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        }
    else:
        from r2g.llm import create_llm_provider
        from r2g.llm.base import OntologyRequest
        from r2g.llm.prompt import build_schema_digest

        samples: dict = {}
        sampled_columns = 0
        grounding = ""
        if sample or ground:
            from r2g.llm.grounding import build_grounding
            from r2g.llm.sampling import build_sampler_for_source, collect_samples

            source = mgr.get_source(proj.source_name)
            sampler = (
                build_sampler_for_source(source, pg_schema=snap.pg_schema)
                if source is not None
                else None
            )
            if sample and sampler is None and not as_json:
                console.print(
                    "[yellow]Value sampling unavailable for this source; "
                    "proceeding metadata-only.[/yellow]"
                )
            try:
                if sample and sampler is not None:
                    samples = collect_samples(
                        sampler, schema, per_column=max(1, samples_per_column)
                    )
                    sampled_columns = sum(len(cols) for cols in samples.values())
                if ground:
                    grounding = build_grounding(schema, sampler=sampler)
            finally:
                if sampler is not None:
                    close = getattr(sampler, "close", None)
                    if callable(close):
                        close()

        try:
            digest = build_schema_digest(
                schema,
                domain_hint=domain,
                include_samples=bool(samples),
                samples=samples,
                samples_per_column=max(1, samples_per_column),
            )
        except ValueError as e:
            console.print(f"[red]{e}[/red]")
            raise typer.Exit(code=1)

        params = {"base_url": base_url} if base_url else None
        try:
            llm = create_llm_provider(provider, model=model, api_key=resolved_key, params=params)
        except (ValueError, ImportError) as e:
            console.print(f"[red]{e}[/red]")
            raise typer.Exit(code=1)

        request = OntologyRequest(
            schema_digest=digest,
            domain_hint=domain,
            grounding=grounding,
            table_count=len(schema.tables),
        )
        if not as_json:
            console.print(f"[dim]Requesting ontology proposal from {provider}…[/dim]")
        try:
            proposal = llm.propose_ontology(request)
        except Exception as e:
            console.print(f"[red]LLM proposal failed:[/red] {e}")
            raise typer.Exit(code=1)

        provenance = {
            "engine": "llm",
            "provider": provider,
            "model": model or "(provider default)",
            "domain_hint": domain,
            "table_count": len(schema.tables),
            "sampled": bool(samples),
            "sampled_columns": sampled_columns,
            "grounded": bool(grounding),
            "proposed_collections": len(proposal.collections),
            "proposed_edges": len(proposal.edges),
            "proposed_renames": len(proposal.renames),
            "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        }

    new_config, notes = proposal_to_mapping(
        proposal, schema, source_schema=current.source_schema
    )
    plan = diff_mappings(current, new_config, schema)

    if as_json:
        console.print_json(
            json.dumps(
                {
                    "proposal": proposal.model_dump(),
                    "notes": notes,
                    "changes": [c.model_dump() for c in plan.changes],
                    "provenance": provenance,
                },
                default=str,
            )
        )
    else:
        _print_ontology_proposal(proposal, notes, plan, provenance)

    if not apply:
        if not as_json:
            console.print(
                "[dim]Preview only. Re-run with --apply to write the accepted mapping "
                "(it still flows through validation and the mapper review).[/dim]"
            )
        return

    if not plan.changes:
        console.print("[green]Proposal matches the current mapping; nothing to apply.[/green]")
        return

    if not yes and not typer.confirm(f"Apply {len(plan.changes)} change(s) to '{project}'?"):
        console.print("[yellow]Aborted; no changes written.[/yellow]")
        raise typer.Exit(code=0)

    try:
        ConfigManager.save_config(new_config, proj.mapping_config_path)
        prov_path = Path(proj.mapping_config_path).parent / "llm-ontology-provenance.json"
        prov_path.write_text(json.dumps(provenance, indent=2, default=str), encoding="utf-8")
    except Exception as e:
        console.print(f"[red]Failed to write mapping:[/red] {e}")
        raise typer.Exit(code=1)

    console.print(
        f"[green]Applied proposal to '{project}'.[/green] Review it in the mapper "
        f"before loading. Provenance: [dim]{prov_path}[/dim]"
    )


def _print_ontology_proposal(proposal, notes, plan, provenance) -> None:
    """Render an ontology proposal, validation notes, and the resulting diff."""
    if provenance.get("engine") == "relational-schema-analyzer":
        conf = provenance.get("analyzer_confidence")
        source = "relational-schema-analyzer"
        if provenance.get("refined"):
            source += f" + {provenance.get('provider')} refinement"
        if conf is not None:
            source += f", confidence {conf}"
        console.print(f"[bold]Ontology proposal[/bold] [dim]({source})[/dim]")
    else:
        console.print(
            f"[bold]Ontology proposal[/bold] [dim]({provenance['provider']} / "
            f"{provenance['model']})[/dim]"
        )
    console.print(
        f"[dim]{provenance['proposed_collections']} collection hint(s), "
        f"{provenance['proposed_edges']} relationship(s), "
        f"{provenance['proposed_renames']} rename(s) proposed over "
        f"{provenance['table_count']} table(s).[/dim]"
    )

    if proposal.edges:
        et = RichTable(title="Proposed relationships")
        et.add_column("Edge", style="cyan")
        et.add_column("From \u2192 To")
        et.add_column("Conf.", justify="right")
        et.add_column("Rationale", style="dim")
        for e in proposal.edges:
            et.add_row(
                e.edge_collection,
                f"{e.from_collection} \u2192 {e.to_collection}",
                f"{e.confidence:.2f}",
                e.rationale or "[dim]\u2014[/dim]",
            )
        console.print(et)

    if plan.changes:
        ct = RichTable(title="Changes vs current mapping")
        ct.add_column("Type", style="magenta")
        ct.add_column("Collection / Edge", style="cyan")
        ct.add_column("Details")
        for change in plan.changes:
            target = change.collection or change.edge or ""
            details = (
                ", ".join(f"{k}={v}" for k, v in change.details.items())
                if change.details
                else ""
            )
            ct.add_row(change.change_type, target, details)
        console.print(ct)
    else:
        console.print("[green]No changes vs the current mapping.[/green]")

    if notes:
        console.print("[bold]Validation & provenance notes[/bold]")
        for n in notes:
            console.print(f"  [dim]\u2022[/dim] {n}")


# ── History command ──────────────────────────────────────────────────


@app.command("history")
def history_cmd(
    project: Optional[str] = typer.Option(None, "--project", help="Filter by project name"),
    limit: int = typer.Option(20, "--limit", help="Max records to show"),
) -> None:
    """Show load history."""
    mgr = _get_catalog()
    records = mgr.get_history(project_name=project, limit=limit)
    if not records:
        console.print("[dim]No load history found.[/dim]")
        return
    table = RichTable(title="Load History")
    table.add_column("ID", style="dim", max_width=8)
    table.add_column("Project", style="cyan")
    table.add_column("Type", style="magenta")
    table.add_column("Status")
    table.add_column("Rows", justify="right")
    table.add_column("Errors", justify="right")
    table.add_column("Started")
    for r in records:
        status_style = {"completed": "green", "failed": "red", "running": "yellow"}.get(r.status, "")
        table.add_row(
            r.id[:8],
            r.project_name,
            r.load_type,
            f"[{status_style}]{r.status}[/{status_style}]" if status_style else r.status,
            str(r.rows_loaded),
            str(r.errors),
            r.started_at.isoformat(),
        )
    console.print(table)


secrets_app = typer.Typer(help="Manage the R2G catalog secret key.")
app.add_typer(secrets_app, name="secrets")


@secrets_app.command("init")
def secrets_init(
    force: bool = typer.Option(False, "--force", help="Overwrite any existing key file"),
) -> None:
    """Initialize (or replace) the on-disk catalog secret key.

    Respects ``R2G_SECRET_KEY`` when set: if the env var is present the
    on-disk key file is not touched and this command is a no-op.
    """
    import os
    from pathlib import Path

    from cryptography.fernet import Fernet

    from r2g.security import SECRET_ENV, SECRET_FILENAME

    if os.environ.get(SECRET_ENV):
        console.print(f"[yellow]{SECRET_ENV} is set; on-disk key is ignored while it is present.[/yellow]")
        return

    path = Path.home() / ".r2g" / SECRET_FILENAME
    if path.exists() and not force:
        console.print(f"[yellow]Secret key already exists at {path} (use --force to replace).[/yellow]")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.write(fd, Fernet.generate_key())
    finally:
        os.close(fd)
    os.chmod(path, 0o600)
    console.print(f"[green]New secret key written to {path} (0600).[/green]")
    console.print(
        "[dim]Tip: back up this file. Losing it makes the encrypted catalog values unrecoverable.[/dim]"
    )


@secrets_app.command("migrate")
def secrets_migrate() -> None:
    """Force-encrypt every secret in the catalog with the active key.

    Reads the catalog, re-writes it with the active key. Any already-encrypted
    values are left alone; plaintext values are encrypted in place. Useful
    after upgrading from a version that predates at-rest encryption.
    """
    mgr = _get_catalog()
    catalog = mgr._load()
    plaintext_sources = [s.name for s in catalog.sources.values() if s.connection_string]
    plaintext_targets = [t.name for t in catalog.targets.values() if t.password]
    mgr._save(catalog)
    console.print(
        f"[green]Re-encrypted {len(plaintext_sources)} sources and {len(plaintext_targets)} targets.[/green]"
    )


@secrets_app.command("status")
def secrets_status() -> None:
    """Show where the active secret key is coming from."""
    import os
    from pathlib import Path

    from r2g.security import SECRET_ENV, SECRET_FILENAME

    if os.environ.get(SECRET_ENV):
        console.print(f"[green]Using {SECRET_ENV} environment variable.[/green]")
        return
    path = Path.home() / ".r2g" / SECRET_FILENAME
    if path.exists():
        console.print(f"[green]Using key file {path}.[/green]")
    else:
        console.print(
            f"[yellow]No key file at {path}. It will be created the next time the catalog is opened.[/yellow]"
        )


@forge_app.command("generate")
def forge_generate(
    ontology_path: str = typer.Option(
        ...,
        "--ontology",
        "-O",
        help="Conceptual ontology JSON: a bare conceptualModel or a full CSI v1 document",
    ),
    dialect: str = typer.Option(
        "postgres",
        "--dialect",
        help="Target dialect: postgres | snowflake | clickhouse (SQL DDL + INSERT loader) "
        "| arango (collection manifest + python-arango loader script)",
    ),
    seed: int = typer.Option(
        0, "--seed", help="RNG seed; the same seed reproduces byte-identical artifacts"
    ),
    rows_per_entity: int = typer.Option(
        10, "--rows-per-entity", help="Synthesized rows per entity"
    ),
    out_dir: str = typer.Option(
        "forge-out",
        "--out-dir",
        "-o",
        help="Output directory: forge.sql + forge.load.sql (SQL dialects) or "
        "forge.collections.json + forge.load.py (arango), plus forge.rows.json",
    ),
) -> None:
    """Generate a physical schema + seeded synthetic data from an ontology.

    The reverse of r2g's forward pipeline (ADR-0006, the Federation Forge):
    the generated schema, loaded and re-introspected through ``ingest-schema``
    -> Auto-Map -> ``export-csi`` (or ASA for ``arango``), must reproduce the
    input ontology. Rows are synthesized once and only projected per dialect,
    so ``forge.rows.json`` is identical across dialects for one seed. Refused
    ontologies (naming that cannot roundtrip, colliding labels) fail here at
    generate time with an explanation — never later at compare time. See
    ``docs/internal/PLAN-federation-forge.md``.
    """
    from r2g.forge import ForgeError, load_ontology_file
    from r2g.forge import generate as forge_generate_artifacts

    # Validated before the try: typer.Exit is an Exception, so raising it
    # inside would be swallowed by the generic handler below (see export-csi).
    if rows_per_entity < 1:
        console.print(f"[red]--rows-per-entity must be >= 1 (got {rows_per_entity}).[/red]")
        raise typer.Exit(code=2)

    try:
        ontology = load_ontology_file(ontology_path)
        artifacts = forge_generate_artifacts(
            ontology, dialect=dialect, seed=seed, rows_per_entity=rows_per_entity
        )
    except ForgeError as e:
        # A refused ontology or unsupported request is a caller problem, kin
        # to bad arguments — report the reason and exit 2.
        console.print(f"[red]Forge refused:[/red] {e}")
        raise typer.Exit(code=2)
    except Exception as e:
        log.exception("forge_generate_failed", ontology=ontology_path)
        console.print(f"[red]Failed to generate:[/red] {e}")
        raise typer.Exit(code=1)

    try:
        paths = artifacts.write_to(out_dir)
        total_rows = sum(len(r) for r in artifacts.rows.values())
        console.print(
            f"[green]Forged[/green] {len(ontology.entities)} entities, "
            f"{len(ontology.relationships)} relationships, {total_rows} rows "
            f"[dim](dialect {dialect}, seed {seed})[/dim]"
        )
        for path in paths:
            console.print(f"  [dim]{path}[/dim]")
    except Exception as e:
        log.exception("forge_write_failed", out_dir=out_dir)
        console.print(f"[red]Failed to write artifacts:[/red] {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
