# r2g — Relational-to-Graph for ArangoDB


[![CI](https://github.com/ArthurKeen/r2g-arango/actions/workflows/ci.yml/badge.svg)](https://github.com/ArthurKeen/r2g-arango/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python: 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)

> **Purpose.** This repository is a working reference application for
> relational-to-graph mapping with ArangoDB: it implements the patterns,
> trade-offs, and techniques for projecting relational and structured data
> into a graph model, and serves as the reference consumer of the
> `relational-schema-analyzer` core. That core (RSA) is actively adopted by
> other projects in ArangoDB Solutions (e.g. the contextual-data-fabric
> building blocks) — so it is an active dependency, not a throwaway demo.

> Project relational and structured data sources as a graph in ArangoDB —
> materialize via batch ETL, sync via CDC/Kafka, or query interactively
> through the mapping studio. **Or federate**: emit CSI v1 + R2RML mappings so a
> query engine reaches your data where it lives — across PostgreSQL, ClickHouse,
> and ArangoDB, joined on declared cross-source keys — with no data movement.

![R2G Mapping Studio showing relational source fields mapped to a target graph model](docs/assets/r2g-mapping-studio.png)

`r2g` turns relational schemas into ArangoDB graph schemas mechanically:
tables become document collections, foreign keys become edges, join tables
become edges, and types are coerced from PostgreSQL representations into
proper JSON types. PostgreSQL, MySQL / MariaDB, SQL Server, Snowflake, and
ClickHouse are supported as relational sources today (plus CSV directories and
Kafka topics); the connector layer is designed for additional structured and
semi-structured sources over time. Beyond loading data *into* ArangoDB, r2g can
also emit **federation mappings** (CSI v1 + R2RML) so a query engine reaches your
data where it lives — see [Federation](#federation-query-your-data-where-it-lives-csi-v1--r2rml).

> **Status — actively used; the production core is RSA.** r2g is the
> reference *application* for relational-to-graph migration with ArangoDB, and
> its reusable introspection/analysis core has been extracted into
> [`relational-schema-analyzer`](https://github.com/ArthurKeen/relational-schema-analyzer)
> (RSA). **This core is actively adopted by other projects in ArangoDB
> Solutions** — the contextual-data-fabric building blocks depend on it — so
> RSA is the production-grade dependency and carries the production bar. **Downstream
> systems depend on RSA (and named, tested r2g modules), not on r2g as a
> whole.** r2g itself is well-tested (CI: ruff + mypy + a large unit suite +
> Dockerized integration); its operational envelope is still single-node with
> no scale/HA guarantees — tracked in [Known limitations](#known-limitations).

See [`docs/PRD.md`](docs/PRD.md) for the full product requirements document
and roadmap.

## Concepts

Relational databases model relationships implicitly through foreign keys and resolve them at query time via joins. Graph databases model relationships explicitly as first-class edges, enabling direct traversal without joins.

The R2G pipeline applies a mechanical mapping:

- Each **table** becomes an ArangoDB **document collection** (vertices). The table's primary key becomes the document `_key`.
- Each **foreign key** becomes an **edge collection**. For every row in the source table, an edge is created from the source vertex to the target vertex, using the FK value to resolve the `_to` endpoint.
- **Join tables** (many-to-many) become **edges** rather than vertices -- the two FK columns point to the two vertex collections the edge connects.
- **Data types** are coerced from PostgreSQL representations to proper JSON types: integers, floats, booleans, nested JSON for `jsonb` columns, and arrays.

```mermaid
flowchart LR
    PG[(PostgreSQL)] -->|ingest-schema| SJ[schema.json]
    SJ -->|generate-config| MY[mapping.yaml]
    PG -->|"dump-tables"| CSV[".csv per table"]
    CSV --> CSVPATH["generate-csv-import<br/>(file-based)"]
    MY --> CSVPATH
    SJ --> CSVPATH
    CSVPATH --> SH1["import_csv.sh"]
    SH1 -->|"arangoimport --type csv"| ADB[(ArangoDB)]
    PG -->|"stream (direct)"| STREAM["streaming pipeline<br/>server-side cursors"]
    MY --> STREAM
    SJ --> STREAM
    STREAM -->|"HTTP API bulk import"| ADB
```

## Federation: query your data where it lives (CSI v1 + R2RML)

Beyond *moving* data into ArangoDB, r2g can emit the mappings that let a query
engine reach your data **where it already lives** — unmoved, across several
engines at once. This is r2g's role as the forward mapping producer for the
Contextual Data Fabric federated-query engine.

Two exports, both load-independent (no migration required):

- **`export-csi`** emits a **CSI v1** (Conceptual Schema Interchange) document
  pairing the conceptual model — CC-12 OWL naming: singular PascalCase entities,
  lowerCamel properties — with its ArangoDB physical mapping. It drives the
  fabric's Arango leg (CSI → MappingBundle → `arango-sparql-py`).
- **`export-r2rml`** emits **R2RML** for the relational leg (R2RML → Ontop), so a
  Virtual Knowledge Graph engine can answer SPARQL over the live relational
  database with no data movement. Both legs default to the same concept
  namespace, so they share one vocabulary.

**Cross-source join keys (P6.7).** Two sources often share a business key — say
`account_id` in a ClickHouse analytics table and in a PostgreSQL CRM `Account`.
r2g proposes these as *shared keys* (confirm-to-accept, never invented) and emits
them as `conceptualModel.joinKeys` in the CSI export: declared cross-source join
keys the federated executor **bind-joins** on, with no materialized edge.

The payoff is a single conceptual query spanning **PostgreSQL (via Ontop) ⋈
ClickHouse (via a native executor) ⋈ ArangoDB (via `arango-sparql-py`)**, joined
on a declared key and grounded with citations — three engines, no data movement.

```bash
r2g export-csi   --config mapping.yaml --schema schema.json --output analytics.csi.json
r2g export-r2rml --config mapping.yaml --schema schema.json --output analytics.r2rml.ttl
```

> Federation is read-only by construction and reuses r2g's mapping model, so the
> same introspect → shape → validate workflow yields both a migration and a
> federation mapping from one source.

## Prerequisites

- **Python 3.10+**
- **PostgreSQL** with data you want to migrate (any version with `pg_catalog` support)
- **ArangoDB** instance (tested with 3.11+) with `arangoimport` on your PATH
- **psql** or another tool to export CSV dumps from PostgreSQL

## Features

- **Schema introspection** -- connects to PostgreSQL and extracts tables, columns, primary keys, and foreign keys
- **Mechanical mapping** -- tables become document collections, foreign keys become edge collections, join tables become edges
- **Type coercion** -- PostgreSQL types (integer, boolean, jsonb, arrays, etc.) are converted to proper JSON types
- **YAML-driven configuration** -- auto-generate a default mapping or hand-tune collection names, field renames, include/exclude lists
- **Polars-powered file processing** -- CSV/TSV/GZ dump files processed via Polars for high throughput
- **`arangoimport` script generation** -- produces executable bash scripts that load documents first, then edges, with configurable connection parameters
- **Studio batch-script viewer** -- previews the current draft mapping as a secret-safe JSONL or CSV-direct `arangoimport` bundle, or as one focused vertex/edge command, with copy and client-side download; this is an alternative artifact, while Studio **Load** continues to stream directly over HTTP
- **Named graph creation** -- generates arangosh JavaScript to create ArangoDB named graph definitions from edge mappings
- **Structured logging** -- human-readable dev output or JSON for production via structlog
- **CSV-direct import** -- generate `arangoimport --type csv` scripts that import PG CSV dumps directly with `--translate` for key remapping, `--datatype` for type coercion, and collection prefixes for edge `_from`/`_to` construction; no intermediate JSONL needed
- **Mapping visualizer** -- interactive HTML visualization (D3.js force-directed graph) of the relational-to-graph mapping with four views: graph schema, relational schema cards, edge mapping detail, and a mapping editor with YAML export
- **Direct PG streaming** -- stream data directly from PostgreSQL to ArangoDB via the HTTP API with server-side cursors, configurable batch sizes, and REPEATABLE READ snapshot isolation -- no intermediate files; supports parallel streaming with `--workers`
- **Dry-run mode** -- `stream --dry-run` validates connectivity to both PostgreSQL and ArangoDB, reads and transforms all data, but skips writes and graph creation -- reports row counts and sample documents per collection for pre-flight validation
- **Progress bars and throughput** -- Rich progress bars during streaming with real-time row counts; elapsed time and rows/second throughput displayed on completion
- **Retry with backoff** -- transient ArangoDB write failures (connection errors, server overload) are retried with exponential backoff
- **Collection management** -- `--drop-collections` flag drops and recreates target collections before import for idempotent re-runs
- **Table filtering** -- `--include-tables` and `--exclude-tables` on the `stream` command for selective import of large schemas
- **Import error reporting** -- document-level errors from ArangoDB bulk imports are captured, logged, and displayed in the summary table instead of silently dropped
- **Comprehensive type mapping** -- 50+ PostgreSQL types explicitly mapped to JSON types: integer variants, float variants, boolean, JSON/JSONB, UUID, timestamps, intervals, network types, geometric types, and text search types
- **Schema diff** -- `diff-schema` command compares two schema snapshots and reports added/removed tables, column type changes, nullable changes, primary key changes, and foreign key changes; supports `--json` output for scripting
- **Config migration** -- `migrate-config` command auto-updates a mapping YAML when the PostgreSQL schema evolves: adds collections for new tables, adds edges for new FKs, removes edges for dropped FKs, flags orphaned collections, and cleans stale field references and type overrides -- all while preserving user customizations (renames, field mappings, include/exclude lists)
- **Data validation** -- `validate-data` command checks referential integrity of dump files before import: builds PK lookup sets per table and verifies every FK value references an existing PK; reports orphaned references that would produce broken edges
- **Topological import ordering** -- document collections are imported in dependency order (FK targets before FK sources) so that referenced vertices exist before edges are created; circular FK dependencies are detected and warned
- **Environment variable support** -- connection parameters (`PG_CONN`, `ARANGO_ENDPOINT`, `ARANGO_DB`, `ARANGO_USER`, `ARANGO_PASSWORD`) can be set via environment variables or a `.env` file; CLI flags override env vars when both are provided
- **Skip existing** -- `stream --skip-existing` skips collections that already contain data, enabling resumption of partial streaming runs without re-importing completed collections
- **Incremental streaming** -- `stream --since 2026-04-01T00:00:00` filters rows by a timestamp column (auto-detects `updated_at`/`created_at` or use `--since-column`); combine with `--on-duplicate=replace` for basic incremental updates
- **PK-less table safety** -- tables without a primary key are warned during validation and streaming; documents receive auto-generated keys and edges referencing such tables are flagged
- **CDC (Change Data Capture)** -- near real-time PostgreSQL→ArangoDB sync via logical replication. `PGReplicationListener` manages replication slots, polls `pg_logical_slot_get_changes`, parses output via `test_decoding` (built-in) or `wal2json` plugins. `DeltaTransformer` converts row-level changes to graph mutations (document upserts/deletes + edge recalculation). `CDCHandler` orchestrates event processing with transaction grouping and stats tracking. Configurable conflict resolution policies: `source_wins` (default), `last_write_wins`, `log_and_skip`, `fail`. CLI commands: `cdc-setup`, `cdc-teardown`, `cdc-status`, `cdc-start`
- **Composite foreign key support** -- multi-column foreign keys are correctly introspected from `pg_catalog`, represented in mappings, and transformed into composite `_key` / `_from` / `_to` values using a configurable separator
- **Multi-schema support** -- `--pg-schema` option on `ingest-schema`, `dump-tables`, and `stream` commands allows introspection and import from any PostgreSQL schema, not just `public`
- **Automated table dumping** -- `dump-tables` command connects to PostgreSQL and exports each table as a CSV file in one pass
- **Join table auto-detection** -- `generate-config` heuristically identifies junction tables (exactly 2 FKs, no non-structural data columns) and flags them as join tables

## Project structure

```
src/r2g/
├── main.py                     # Typer CLI (top-level commands + `source`/`project`/`secrets` groups)
├── config_migrate.py           # Config migration when schema evolves
├── data_validator.py           # Referential integrity checker for dump data
├── schema_diff.py              # Schema comparison / structural diff
├── topo_sort.py                # Topological sort for import ordering, cycle detection
├── cdc/                        # Change Data Capture (Phase 3)
│   ├── conflict.py             # ConflictPolicy, ConflictResolver, ConflictLog
│   ├── models.py               # ChangeEvent, ArangoDelta, TransactionBatch
│   ├── parsers.py              # Output plugin parsers (test_decoding, wal2json)
│   ├── pg_listener.py          # PGReplicationListener: slot mgmt, polling loop
│   ├── delta_transformer.py    # Convert CDC events → ArangoDB mutations
│   ├── handler.py              # CDCHandler: orchestrate event processing with stats
│   ├── kafka_parser.py         # Debezium and flat JSON message parsers
│   └── kafka_consumer.py        # Kafka consumer with confluent-kafka
├── types.py                    # Pydantic models (Schema, Table, MappingConfig, EdgeDefinition, ...)
├── config.py                   # ConfigManager, YAML load/save, PG→JSON type map, join detection
├── snapshot.py                 # Shared introspection + reviewed-overlay snapshot pipeline
├── log.py                      # structlog setup
├── connectors/
│   ├── base.py                 # SourceConnector registry/factory
│   ├── session.py              # SourceSession bulk-read protocol
│   ├── postgres.py             # PostgreSQL connector/session via psycopg
│   ├── mysql.py                # MySQL/MariaDB connector/session
│   ├── mssql.py                # SQL Server connector/session
│   ├── snowflake.py            # Snowflake connector/session + key-pair auth
│   ├── clickhouse.py           # ClickHouse connector/session
│   └── arango_writer.py        # ArangoDB HTTP API writer via python-arango
├── input/
│   └── dump_reader.py          # Polars-based CSV/TSV/GZ reader
├── transformers/
│   ├── node_transformer.py     # Row → ArangoDB document (with type coercion)
│   ├── edge_transformer.py     # Row → ArangoDB edge (FK and join-table modes)
│   └── converter.py            # Re-exports NodeTransformer, EdgeTransformer
├── generators/
│   ├── arangoimport.py         # Bash script generator (JSONL and CSV-direct)
│   └── visualizer.py           # Interactive HTML mapping visualizer + editor
└── streaming/
    └── pipeline.py             # SourceSession → ArangoDB direct streaming pipeline
```

## Installation

`r2g` is a CLI plus an optional FastAPI mapping studio. It ships from PyPI as
`r2g-arango` and is designed around opt-in extras so you only pull in the
connectors / UI you actually need.

### End users — install with `pipx`

We recommend [`pipx`](https://pipx.pypa.io/) so the CLI lives in its own
isolated venv and does not pollute your project environments:

```bash
pipx install 'r2g-arango[postgres,ui]'   # PG source + interactive mapping studio
r2g --help
r2g ui                                   # opens http://localhost:8501
```

Plain `pip install` works too; just be aware that the connector
dependency trees (psycopg, snowflake-connector-python, confluent-kafka) are
substantial and you probably don't want them in an unrelated app venv.

### Extras matrix

Pick the extras that match your use case. Combine with commas.

| Extra        | What it pulls in                                        | When you need it                              |
| ------------ | ------------------------------------------------------- | --------------------------------------------- |
| `postgres`   | `psycopg[binary]`                                       | Any PostgreSQL source — schema introspection, dumps, streaming, CDC via logical replication |
| `mysql`      | `pymysql`                                               | MySQL / MariaDB source — introspection, dumps, streaming |
| `sqlserver`  | `pymssql`                                               | SQL Server source — introspection, dumps, streaming |
| `snowflake`  | `snowflake-connector-python`                            | Snowflake source                              |
| `clickhouse` | `clickhouse-connect`                                    | ClickHouse source — introspection, dumps, streaming, and federation (CSI/R2RML) |
| `kafka`      | `confluent-kafka`                                       | Kafka-fed CDC consumer (Debezium or flat JSON) |
| `ui`         | `fastapi`, `uvicorn[standard]`, `httpx`                 | Local mapping studio (`r2g ui`)               |
| `mcp`        | `mcp[cli]`                                              | Run r2g as an MCP server for AI assistants    |
| `openmetadata` | `httpx`                                               | Discover & import sources from an OpenMetadata data catalog (`r2g catalog`) |
| `all`        | everything above                                        | Don't think about it; want every feature      |

Common recipes:

```bash
pipx install 'r2g-arango[postgres,ui]'              # mapping studio against PG
pipx install 'r2g-arango[mysql,ui]'                 # mapping studio against MySQL/MariaDB
pipx install 'r2g-arango[sqlserver,ui]'             # mapping studio against SQL Server
pipx install 'r2g-arango[postgres,kafka]'           # batch load + Kafka CDC worker
pipx install 'r2g-arango[snowflake,ui]'             # Snowflake source via the studio
pipx install 'r2g-arango[clickhouse,ui]'            # ClickHouse source via the studio
pipx install 'r2g-arango[postgres,llm]'             # PG + AI ontology suggestions (Phase 10)
pipx install 'r2g-arango[postgres,ontology]'        # PG + deterministic ontology engine (Phase 10)
pip   install 'r2g-arango[all]'                     # kitchen sink
```

### Contributors — editable install

```bash
git clone https://github.com/ArthurKeen/r2g-arango
cd r2g-arango
pip install -e ".[all,test,dev]"   # all extras + pytest + ruff
ruff check src/ tests/
pytest tests/ -m "not integration"
```

## Quick start

### 0. Configure credentials (optional)

Instead of passing connection strings on the command line, create a `.env` file (see `.env.example`):

```bash
cp .env.example .env
# Edit .env with your PostgreSQL and ArangoDB credentials
```

The CLI auto-loads `.env` from the working directory. All connection flags (`--conn`, `--pg-conn`, `--endpoint`, `--database`, `--username`, `--password`) can be set via environment variables (`PG_CONN`, `ARANGO_ENDPOINT`, `ARANGO_DB`, `ARANGO_USER`, `ARANGO_PASSWORD`). CLI flags override env vars.

Snowflake accepts password URLs and Contextual Data Fabric-style key-pair URLs:

```text
snowflake://user:password@account/DATABASE/SCHEMA?warehouse=WH&role=R
snowflake://$SNOWFLAKE_USER:@$SNOWFLAKE_ACCOUNT/DATABASE/SCHEMA?warehouse=$SNOWFLAKE_WAREHOUSE&role=$SNOWFLAKE_ROLE&private_key_file=$SNOWFLAKE_PRIVATE_KEY_FILE&private_key_file_pwd=$SNOWFLAKE_PRIVATE_KEY_FILE_PWD
```

Password and key-pair credentials are mutually exclusive. Key-pair connections
use `SNOWFLAKE_JWT`; missing/unreadable key files fail before the driver is
called, and credentials are scrubbed from API errors. The private key must exist
on the machine or container running r2g at the path resolved from
`SNOWFLAKE_PRIVATE_KEY_FILE`; an environment reference does not upload or copy
the key.

A snapshot lists base tables only (dynamic tables included). Add
`&include_views=true` to also list the schema's views and materialized views,
for a schema whose joinable objects are views; a view declares no keys, so
give it a reviewed key overlay or run FK inference.

### Snowflake quick start

Install the connector, register the source, and capture its schema:

```bash
pip install 'r2g-arango[snowflake,ui]'
r2g source add \
  --name customer360 \
  --type snowflake \
  --conn 'snowflake://$SNOWFLAKE_USER:@$SNOWFLAKE_ACCOUNT/DATABASE/SCHEMA?warehouse=$SNOWFLAKE_WAREHOUSE&role=$SNOWFLAKE_ROLE&private_key_file=$SNOWFLAKE_PRIVATE_KEY_FILE' \
  --key-overlay ./keys.overlay.json
r2g source snapshot customer360 --pg-schema SCHEMA
```

`--key-overlay` is optional, but recommended for Snowflake schemas whose PK/FK
constraints are absent or informational. It accepts a reviewed RSA JSON/YAML
overlay; r2g stores the parsed overlay and records its source and fingerprint on
each snapshot. r2g never invents those keys. Name-based FK suggestions work for
Snowflake, but value-overlap sampling is not yet implemented for Snowflake and
falls back to name-only evidence.

Export CSV files or stream directly from the registered source:

```bash
r2g source dump customer360 --pg-schema SCHEMA --output-dir ./dumps
r2g stream --source customer360 --pg-schema SCHEMA \
  --schema schema.json --config mapping.yaml \
  --database customer360_graph
```

In Studio, **Load** uses the direct streaming path from Snowflake to ArangoDB.
**View arangoimport script bundle** only renders an alternative, secret-safe
batch artifact; it does not execute the script or create its input files. The
bundled live demo and role/setup instructions are in
`examples/snowflake_customer_360/README.md`.

### 1. Extract schema from PostgreSQL

```bash
r2g ingest-schema --conn "postgresql://user:pass@localhost/mydb" --output schema.json
```

### 2. Generate a default mapping config

```bash
r2g generate-config --schema schema.json --output mapping.yaml
```

This creates a YAML file with one document collection per table and one edge collection per foreign key. Edit it to rename collections, exclude fields, or mark join tables.

### 3. Dump tables to CSV

Use the built-in `dump-tables` command to export all tables at once:

```bash
r2g dump-tables --conn "postgresql://user:pass@localhost/mydb" --output-dir ./dumps
```

Or use `psql` manually (one file per table, filename must match the table name):

```bash
for table in users orders products; do
  psql -d mydb -c "COPY ${table} TO STDOUT WITH CSV HEADER" > dumps/${table}.csv
done
```

### 3a. Visualize the mapping (optional)

```bash
r2g visualize-mapping --schema schema.json --config mapping.yaml --output mapping.html
```

Opens an interactive HTML report in your browser showing the PG-to-graph mapping: draggable graph schema, table cards with PK/FK badges, and edge mapping details.

### 4. Generate CSV-direct import script (preferred)

CSV-direct loads each CSV column as it stands. If your mapping masks or excludes columns,
filters them with `include_fields`, computes fields with expressions, or sets type
overrides, `generate-csv-import` refuses it and names what it cannot reproduce; use the
JSONL path (4a) instead. Renames in `field_mappings` work on both paths.

```bash
r2g generate-csv-import \
  --schema schema.json \
  --config mapping.yaml \
  --data-dir ./dumps \
  --output import_csv.sh \
  --endpoint http://localhost:8529 \
  --database mydb \
  --graph-name my_graph
```

### 4a. Alternative: JSONL transform path

Transform an entire directory of CSV dumps in one pass:

```bash
r2g transform-all \
  --schema schema.json \
  --config mapping.yaml \
  --input-dir ./dumps \
  --output-dir ./output \
  --file-pattern "*.csv"
```

Or transform a single table's nodes or edges:

```bash
r2g transform-nodes --schema schema.json --config mapping.yaml --table users --input dumps/users.csv --output output/users.jsonl
r2g transform-edges --schema schema.json --config mapping.yaml --table orders --input dumps/orders.csv --output output/orders_edges.jsonl
```

Then generate the arangoimport script:

```bash
r2g generate-import \
  --config mapping.yaml \
  --data-dir ./output \
  --output import.sh \
  --endpoint http://localhost:8529 \
  --database mydb \
  --graph-name my_graph
```

This produces an executable `import.sh` (documents first, then edges) and an arangosh graph creation script.

The Studio exposes the same renderer under **Actions → View arangoimport script
bundle** and on target collection/edge context menus. The preview defaults to
`./output/*.jsonl` or `./dumps/*.csv`; the artifact directory is editable and
shell-quoted in every generated command. It reads `ARANGO_PASSWORD` only at
runtime and never executes or writes the displayed script on the server.

### 5. Load into ArangoDB

**CSV-direct path (step 4):**

```bash
./import_csv.sh
```

**JSONL path (step 4a):**

```bash
./import.sh
```

Override connection details via environment variables (works with the generated scripts):

```bash
ARANGO_ENDPOINT=http://prod:8529 ARANGO_DB=prod_db ARANGO_PASSWORD=secret ./import_csv.sh
```

### Pre-flight: validate data integrity (optional)

```bash
r2g validate-data --schema schema.json --config mapping.yaml --data-dir ./dumps
```

Checks that every FK value in your dump files references an existing PK in the target table. Reports orphaned references that would produce broken edges in ArangoDB.

### Schema evolution: migrating the mapping config

When your PostgreSQL schema changes (new tables, dropped columns, added/removed FKs), update the mapping config automatically:

```bash
# Re-extract the updated schema
r2g ingest-schema --conn "postgresql://user:pass@localhost/mydb" --output schema_v2.json

# Migrate the existing config to match
r2g migrate-config --schema schema_v2.json --config mapping.yaml
```

This preserves all your customizations (collection renames, field mappings, include/exclude lists, type overrides) while adapting to schema changes. Use `--output new_mapping.yaml` to write to a different file, or `--json-report` for machine-readable output.

### Alternative: Direct streaming (no intermediate files)

Skip steps 3-5 entirely and stream data directly from PostgreSQL to ArangoDB:

```bash
r2g stream \
  --pg-conn "postgresql://user:pass@localhost/mydb" \
  --schema schema.json \
  --config mapping.yaml \
  --endpoint http://localhost:8529 \
  --database mydb \
  --batch-size 10000 \
  --graph-name my_graph
```

This uses server-side cursors with REPEATABLE READ isolation for consistent snapshots and bulk-imports via the ArangoDB HTTP API.

Options:

- `--workers 4` -- parallel streaming with per-worker PG + ArangoDB connections. **Note:** each worker opens its own `REPEATABLE READ` transaction; this provides per-worker consistency but not a single global snapshot across all workers. For strict point-in-time consistency, use `--workers 1` (default).
- `--on-duplicate replace` -- ArangoDB duplicate handling strategy (`replace`, `update`, `ignore`, `error`)
- `--include-tables users,orders` -- only stream specified tables (and their edges)
- `--exclude-tables audit_log` -- skip specified tables
- `--drop-collections` -- drop and recreate target collections before import
- `--skip-existing` -- skip collections that already have data (for resuming partial runs)

Add `--dry-run` to preview row counts and sample documents without writing to ArangoDB:

```bash
r2g stream --dry-run \
  --pg-conn "postgresql://user:pass@localhost/mydb" \
  --schema schema.json \
  --config mapping.yaml \
  --endpoint http://localhost:8529 \
  --database mydb
```

## CLI reference

| Command | Description |
|---|---|
| `ingest-schema` | Connect to PostgreSQL and extract schema metadata to JSON |
| `validate-schema` | Validate a schema JSON file against the internal model |
| `inspect-dump` | Preview rows from a CSV/TSV/GZ dump file |
| `generate-config` | Auto-generate a YAML mapping config from a schema file |
| `transform-nodes` | Transform a single table dump into ArangoDB document JSONL |
| `transform-edges` | Transform a single table dump into ArangoDB edge JSONL |
| `transform-all` | Transform all tables and edges in one pass with progress bar |
| `generate-import` | Generate arangoimport bash script and optional graph creation JS |
| `generate-csv-import` | Generate arangoimport script for direct CSV import (no JSONL intermediate) |
| `visualize-mapping` | Generate interactive HTML visualization of the PG-to-graph mapping |
| `dump-tables` | Connect to PostgreSQL and dump each table to a CSV file |
| `validate-config` | Validate mapping config against schema (checks table references, column names, edge definitions) |
| `validate-data` | Check referential integrity of dump files (FK values vs target PKs); reports orphaned references before import |
| `diff-schema` | Compare two schema.json files and report structural changes (tables, columns, types, PKs, FKs); supports `--json` for machine-readable output |
| `migrate-config` | Auto-update a mapping config YAML to match an evolved schema: adds new tables/edges, removes stale edges, flags orphaned collections, cleans dropped-column references; `--json-report` for machine-readable output |
| `cdc-setup` | Create a PostgreSQL logical replication slot for CDC (`--slot`, `--plugin test_decoding\|wal2json`) |
| `cdc-teardown` | Drop a logical replication slot |
| `cdc-status` | Show replication slot metadata (active, LSN positions) |
| `cdc-start` | Start the CDC listener: polls for changes, transforms via mapping config, applies deltas to ArangoDB in near real-time (`--poll-interval`, `--batch-size`, `--create-slot/--no-create-slot`, `--conflict-policy`, `--temporal`; `--govern` applies the Phase 9 sensitivity gate so changed rows carry classification policy — `--allow-sensitive`, `--sensitivity-threshold`) |
| `kafka-start` | Start Kafka CDC consumer: consumes Debezium or flat JSON messages from Kafka topics, transforms via mapping config, applies deltas to ArangoDB (`--brokers`, `--topics`, `--group-id`, `--format`, `--offset-reset`, `--conflict-policy`, `--temporal`, `--govern`) |
| `stream` | Stream data directly from PostgreSQL to ArangoDB via HTTP API (no intermediate files); supports `--dry-run`, `--pg-schema`, `--drop-collections`, `--workers`, `--include-tables`, `--exclude-tables`, `--skip-existing`, `--on-duplicate`, `--since`, and `--since-column` |
| `mapping-diff` | Compare two mapping configs and show what ArangoDB changes are needed |
| `selective-reload` | Compute and execute a selective reload based on mapping changes |
| `history` | Show load history (`--project`, `--limit`) |
| `export-csi` | Emit a **CSI v1** conceptual-schema-interchange document (conceptual model + ArangoDB physical mapping) for federation; drives the fabric's Arango leg (`--config`, `--schema`, `--output`, `--source-type`, `--source-ref`, `--label-policy`, `--curated-labels`, `--allow-unapplied-labels`, `--validate/--no-validate`) |
| `export-r2rml` | Emit an **R2RML** mapping (relational → RDF) for SPARQL→SQL pushdown via Ontop — the federation relational leg (`--config`, `--schema`, `--output`, `--concept-base`, `--resource-base`, `--source-type`, `--curated-labels`, `--allow-unapplied-labels`) |
| `ui` | Start the Relational-to-Graph Studio web UI (requires the `ui` extra) |
| `mcp` | Start the R2G MCP server for AI agent integration (requires the `mcp` extra) |

### `r2g source` — catalog source management

| Command | Description |
|---|---|
| `source add` | Register a new data source (PostgreSQL, MySQL / MariaDB, SQL Server, Snowflake, ClickHouse, CSV directory, or Kafka topic) |
| `source list` | List all registered data sources |
| `source remove` | Remove a registered data source |
| `source snapshot` | Introspect the schema from a source and save a snapshot |
| `source dump` | Dump every table in a cataloged source to CSV files (source-agnostic replacement for `dump-tables`) |
| `source infer-fks` | Propose foreign keys for a source's latest schema snapshot (`--sample` for value-overlap scoring, `--accept` to write back) |
| `source analyze-denorm` | Detect denormalization smells (repeating column groups; embedded lookups via `--sample`) and recommend graph remedies — advisory, read-only (`--min-confidence`, `--no-sample-columns`, `--json`) |

### `r2g project` — project management

| Command | Description |
|---|---|
| `project create` | Create a new project |
| `project list` | List all projects |
| `project status` | Show the status of a project (last load, snapshot age, mapping path) |

### `r2g entitlements` — governance (Phase 9)

| Command | Description |
|---|---|
| `entitlements report` | List a project's mapped fields at/above a sensitivity threshold with source lineage (mosaic = max-of-contributors). Advisory: at load, above-threshold fields are excluded by default unless `--allow-sensitive` or masked (`--threshold`, `--json`) |
| `entitlements emit` | Emit the Phase 9c governance artifacts under `<project>/governance/`: `classification-manifest.json`, `suggested-rbac.json` (collection grants by clearance), `policy.rego` (OPA stub), `lineage.json`, and (with `--tier-layout`) `tier-layout.json`. r2g emits; the serving layer enforces (`--threshold`, `--out`, `--tier-layout`, `--no-rego`) |
| `catalog resync-classifications` | Re-pull classifications/owners/tier from the catalog a source was imported from, refresh the stored map + `classifications_synced_at`, and re-merge onto the latest snapshot's columns (counters source-policy drift) |
| `ontology suggest` | **(Phase 10)** Propose a richer target ontology (vertex-vs-edge, implicit relationships, clearer names) from the snapshot; prints the proposal + a diff vs the current mapping + validation/provenance notes. Two engines via `--engine`: **`llm`** (default) asks a model to propose (metadata-only; Phase-9 Restricted/PII columns redacted, never sampled; providers OpenAI/Anthropic/OpenAI-compatible+local via `--provider`/`--base-url`; `--sample` grounds with non-sensitive example values, `--ground` adds Phase-11 denormalization findings; needs `r2g-arango[llm]` + the provider key), and **`rsa`** which derives a conceptual model **deterministically and offline** via `relational-schema-analyzer` (semantic names, join-table detection, FK relationships, confidence/provenance; add `--refine` to LLM-improve it; needs `r2g-arango[ontology]`). Either way every proposal is validated/repaired against the real schema so it always yields a loadable mapping, and nothing is written without `--apply` (`--domain`, `--model`, `--api-key`, `--samples-per-column`, `--yes`, `--json`) |

The load API also accepts `emit_governance` / `tier_layout` to drop the artifact set on a governed load.

### `r2g secrets` — catalog credential encryption

| Command | Description |
|---|---|
| `secrets init` | Initialize (or replace) the on-disk catalog secret key; respects `R2G_SECRET_KEY` |
| `secrets migrate` | Force-encrypt every secret in the catalog with the active key |
| `secrets status` | Show where the active secret key is coming from |

### `r2g catalog` — external data catalog discovery (Phase 8a)

Connect to an external enterprise data catalog (OpenMetadata today) and use it
to discover and import migration sources ("discover-then-connect"). Credentials
are **not** read from the catalog — imported sources use `$ENV_VAR` placeholders
you resolve at connect time.

| Command | Description |
|---|---|
| `catalog add` | Register an external catalog (`--type openmetadata --endpoint <url> [--token $OM_TOKEN]`); the token is stored encrypted |
| `catalog list` | List registered catalogs |
| `catalog browse` | Browse a catalog: top-level sources, the children of `--path <fqn>`, or `--search <q>` results |
| `catalog import-source` | Resolve a catalog asset (database / schema / Kafka topic) into a new r2g source (`--as <source-name>`) |
| `catalog remove` | Remove a registered catalog |

All commands support `--verbose` / `-v` for debug logging and `--json-log` for structured JSON output.
Run `r2g --version` to print the installed version, and `r2g --install-completion` to set up shell tab-completion (bash/zsh/fish).

## Mapping configuration

The YAML mapping config controls how PostgreSQL tables map to ArangoDB collections. See [`examples/sample_mapping.yaml`](examples/sample_mapping.yaml) for a commented example.

Key sections:

- **`collections`** -- per-table settings: target collection name, field renames (`field_mappings`), `exclude_fields`, `include_fields`, `is_join_table`
- **`edges`** -- foreign key relationships: edge collection name, from/to vertex collections, from/to fields
- **`type_overrides`** -- force a specific JSON type for a column when auto-detection is wrong
- **`key_separator`** -- character used to join composite primary key values (default: `_`)

## CDC (Change Data Capture)

After an initial full load, use CDC to keep ArangoDB in sync with PostgreSQL changes in near real-time:

```bash
# 1. Create a logical replication slot on PostgreSQL
r2g cdc-setup --pg-conn "postgresql://user:pass@localhost/mydb"

# 2. Start the CDC listener (blocks, Ctrl+C to stop)
r2g cdc-start --pg-conn "postgresql://user:pass@localhost/mydb" \
  schema.json mapping.yaml \
  --endpoint http://localhost:8529 \
  --database mydb \
  --username root --password secret \
  --poll-interval 1.0 --batch-size 1000

# 3. Check slot status
r2g cdc-status --pg-conn "postgresql://user:pass@localhost/mydb"

# 4. Clean up when done
r2g cdc-teardown --pg-conn "postgresql://user:pass@localhost/mydb"
```

The listener supports two PostgreSQL output plugins:
- **`test_decoding`** (default) -- built-in to PostgreSQL, no extensions needed
- **`wal2json`** -- requires the [wal2json](https://github.com/eulerto/wal2json) extension; provides cleaner JSON output

### Conflict resolution

Use `--conflict-policy` to control how write conflicts are handled:

```bash
r2g cdc-start --pg-conn "..." schema.json mapping.yaml \
  --conflict-policy last_write_wins
```

| Policy | Behaviour |
| :--- | :--- |
| `source_wins` (default) | PostgreSQL is the source of truth. INSERT duplicates become upserts; REPLACE on missing documents falls back to insert; DELETE is idempotent. |
| `last_write_wins` | Compares event LSN against a per-document `_r2g_lsn` field. Stale events are rejected. |
| `log_and_skip` | Logs every conflict as a warning, skips the write, and continues processing. Useful for monitoring conflict frequency. |
| `fail` | Raises an error on the first conflict. Use when conflicts indicate a bug. |

A conflict summary table is printed at the end of each CDC session showing counts by conflict type.

For UPDATE/DELETE events, set `REPLICA IDENTITY FULL` on source tables to capture old row values:

```sql
ALTER TABLE users REPLICA IDENTITY FULL;
```

### Kafka CDC

Consume change events from Kafka instead of PostgreSQL logical replication. Requires the optional dependency:

```bash
pip install 'r2g-arango[kafka]'
```

**Debezium** messages (default `--format debezium`):

```bash
r2g kafka-start schema.json mapping.yaml \
  --brokers localhost:9092 \
  --topics dbserver1.public.users,dbserver1.public.orders \
  --group-id r2g-cdc \
  --format debezium \
  --offset-reset earliest \
  --endpoint http://localhost:8529 \
  --database mydb \
  --username root --password secret
```

**Flat JSON** from custom producers (`--format flat`):

```bash
r2g kafka-start schema.json mapping.yaml \
  --brokers kafka.example.com:9092 \
  --topics my_app.changes \
  --format flat \
  --group-id r2g-flat \
  --conflict-policy source_wins \
  --endpoint http://localhost:8529 \
  --database mydb
```

Use `--offset-reset earliest` or `latest` to control where a new consumer group starts. The same conflict policies as PG CDC apply via `--conflict-policy`. Press Ctrl+C for graceful shutdown (offsets committed for processed batches). Optional flags include `--batch-size` for poll batching.

## Known limitations

r2g is a reference application (see the Status note near the top): well-tested, but not held to a production operational bar. The following constraints apply:

- **Supported sources: PostgreSQL, MySQL / MariaDB, SQL Server, Snowflake, ClickHouse, and CSV directories** (Kafka is supported for streaming sync via `kafka-start` and introspection in the catalog). Schema introspection, FK inference (with value-overlap sampling on PostgreSQL, MySQL, SQL Server, and CSV), dump export (`r2g source dump`), and streaming into ArangoDB (`r2g stream --source …`) work across these backends through a common `SourceConnector` / `SourceSession` abstraction. MySQL is gated on the optional `r2g-arango[mysql]` extra (pure-Python `pymysql`, also covers MariaDB); SQL Server on `r2g-arango[sqlserver]` (pure-Python `pymssql`); Snowflake on `r2g-arango[snowflake]`; ClickHouse on `r2g-arango[clickhouse]` (pure-Python `clickhouse-connect`). PostgreSQL, MySQL, and SQL Server are verified end-to-end against live servers in the integration suite; Snowflake password/key-pair snapshot, reviewed-key overlay, mapping, dump, and streaming paths are field-validated against the live Customer 360 demo. No SQLite or Oracle support yet.
- **External data catalog discovery (Phase 8a):** connect to an [OpenMetadata](https://open-metadata.org) catalog (`r2g-arango[openmetadata]`) to browse its database/schema/Kafka assets and import a selection as an r2g source — see `r2g catalog`. Distinct from r2g's internal catalog; read-only; credentials stay with the user (the catalog supplies host/db, not secrets). AWS Glue and Atlan are planned next (see [docs/PRD.md](docs/PRD.md) Phase 8).
- **Ontology derivation (Phase 10):** an *optional* way to **propose** a richer target graph from the introspected schema — the engine proposes, the deterministic pipeline disposes. Two engines: an **LLM engine** (`r2g-arango[llm]`; OpenAI, Anthropic/Claude, or any OpenAI-compatible/local endpoint like Ollama, vLLM, LM Studio), and a **deterministic engine** (`r2g-arango[ontology]`) that runs the shared [`relational-schema-analyzer`](https://github.com/ArthurKeen/relational-schema-analyzer) — the introspection core originally extracted from r2g — to derive a conceptual model (semantic collection names, join-table detection, foreign-key relationships, confidence/provenance) **offline, from structure alone** (no rows, no network); add `--refine` to LLM-improve it. The LLM path is metadata-only (no row data; Phase-9 Restricted/PII columns redacted to name-only and never sampled), prompt-injection-hardened; opt-in `--sample`/`--ground` add non-sensitive example values and Phase-11 denormalization findings. Both engines' proposals are validated/repaired against the real schema so a proposal can never load a hallucinated table or column, flowing through the **same** `validate_config` → mapper-review → loader path as Auto-Map (still the default); nothing is applied without explicit confirmation. Use it from the CLI (`r2g ontology suggest --engine {llm,rsa}`) or the **Mapping Studio**: a "Suggest model (AI)" action (Actions menu / canvas right-click / `m`) opens a floating review panel — pick the engine, accept or reject each suggestion per item, then apply the selection as an editable draft to review and Save.
- **Data validation is opt-in** -- orphaned foreign key references (FK values pointing to non-existent PKs) will produce edges to vertices that don't exist in ArangoDB. Use `validate-data` before import to catch these, but it is not enforced automatically.
- **Incremental + change capture** -- `stream --since` filters rows by a timestamp column for basic incremental loads; full change capture is available via the CDC (`cdc-start`) and Kafka (`kafka-start`) pipelines with configurable conflict resolution, and `mapping-diff` / `selective-reload` reconcile mapping changes against an already-loaded target.
- **Self-referential FKs** -- these work but produce edges within the same collection (e.g., `orders.referrer_id -> customers.id` creates `orders_to_customers_referrer_id`). This is correct but may be unexpected.
- **ArangoDB write path** -- the `stream` command connects directly to ArangoDB via python-arango, but the file-based paths (`generate-csv-import`, `generate-import`) still require `arangoimport` installed separately.
- **Credential handling** -- catalog secrets (source connection strings, target passwords) are encrypted at rest with Fernet using a key from `R2G_SECRET_KEY` or `~/.r2g/secret.key` (managed via `r2g secrets init|status|migrate`); API responses redact them. Generated import scripts still reference connection defaults via environment variables, and there is no integrated remote secrets manager (e.g. Vault).

## Testing

```bash
pytest tests/ -v
```

Over 1,000 tests (6 integration tests skipped without Docker) covering CLI commands (via typer.testing.CliRunner, including CDC and Kafka commands), types (including composite FK serialization), schema diff, config migration, data validation (referential integrity with orphan detection), topological sort (dependency ordering, circular FK detection), config validation (including self-referential FKs, duplicate edge naming, PK-less table warnings, and reserved-attribute checks), dump reader, node/edge transformers, import generators (JSONL and CSV-direct), visualizer, ArangoDB writer (retry logic, error surfacing, single-document CDC ops), the full CDC stack (models, output-plugin parsers, delta transformer, handler, conflict resolution, replication listener), Kafka parsers and consumer, the streaming pipeline (sequential, parallel, filtering, skip-existing, topological/since handling), the expression evaluator (`expressions.py`), the MySQL, SQL Server, Snowflake, ClickHouse, and CSV connectors, the CSI v1 and R2RML federation emitters, FK inference (name heuristics + PostgreSQL/MySQL/SQL Server/CSV value-overlap samplers), naming conventions, mapping diff + selective reload, temporal graph mode (applier, models, queries), credential encryption (`security.py`), the Mapping Studio API (`ui/server.py`) and MCP server, plus end-to-end integration tests against live PostgreSQL / MySQL / SQL Server + ArangoDB.

To run unit tests only (no Docker required):

```bash
pytest tests/ -m "not integration"
```

To run all tests including integration (requires Docker PG + ArangoDB):

```bash
pytest tests/ -v
```

## Roadmap

Phases 1 through 4 are implemented. See [`docs/PRD.md`](docs/PRD.md) for the full phased roadmap:

- **Phase 1** -- Table dump file processing (MVP): schema ingestion, JSONL transforms, CSV-direct import, visualizer -- **complete**
- **Phase 2** -- Direct PostgreSQL streaming (server-side cursors, HTTP API bulk import, REPEATABLE READ snapshots) -- **complete**
- **Phase 3** -- CDC integration: event models, delta transformer, handler, replication listener, output plugin parsers (`test_decoding`/`wal2json`), continuous polling loop, conflict resolution (`source_wins`/`last_write_wins`/`log_and_skip`/`fail`), CLI commands -- **complete**
- **Phase 4** -- Kafka integration: Debezium parser, flat JSON parser, confluent-kafka consumer, kafka-start CLI command -- **complete**
- **Phase 5** -- Temporal graph mode: immutable-proxy time travel pattern (ProxyIn/Entity/ProxyOut), soft deletes with `created`/`expired` versioning, TTL aging, MDI-prefixed temporal indexes, point-in-time query templates, SmartGraph compatibility -- **implemented** (`r2g.temporal`, `--temporal`/`--ttl-seconds`/`--smart-field` on `cdc-start`/`kafka-start`; live-warehouse field validation pending)
- **Phase 5f** -- Naming conventions (PascalCase collections / camelCase properties + edges / snake_case) and rename change-management for already-loaded targets: `r2g.naming`, identity-based diff in `mapping-diff`, in-place `selective-reload`, `migration-plan` / `migrate` API endpoints, reserved-attribute protection -- **implemented**
- **Phase 6** -- Snowflake integration -- **done**. Includes password/key-pair authentication, schema introspection, reviewed RSA key overlays with provenance/fingerprints, type mapping, source-agnostic CSV dumps and direct streaming, and the bundled Customer 360 rehearsal. Deterministic FK inference is available, but Snowflake value-overlap sampling remains name-only. `r2g stream --source <name>` and `r2g source dump <name>` dispatch through the registered source connector; legacy PostgreSQL-only flags remain available for compatibility.
- **Phase 12** -- Federation: forward **CSI v1** and **R2RML** emitters (`export-csi` / `export-r2rml`) so r2g mappings drive the Contextual Data Fabric federated-query engine; **ClickHouse** source connector (federation mapping + ETL); **P6.7** cross-source shared keys emitted as `conceptualModel.joinKeys` -- **shipped (producer side)**; the fabric-side E1 routing consumer is tracked separately
- **Phase 7+** -- Additional sources, LLM-driven ontology derivation, ArangoRDF, bi-directional sync -- **exploratory**

## Contributing

Contributions are welcome. Before opening a pull request please read
[CONTRIBUTING.md](CONTRIBUTING.md) and the
[Code of Conduct](CODE_OF_CONDUCT.md).

Bug reports and feature requests go to the
[issue tracker](https://github.com/ArthurKeen/r2g-arango/issues). Use the
provided templates so reports include the context needed to reproduce.

## Security

Please do **not** report security vulnerabilities through public issues.
See [SECURITY.md](SECURITY.md) for the responsible disclosure process.

### Hardening the Studio UI and MCP server

The Studio UI and MCP servers are built for frictionless local use: bound to
loopback they need no token. When exposing either on a shared host, set these
environment variables (see [.env.example](.env.example)):

| Variable | Effect |
|---|---|
| `R2G_API_TOKEN` | Require `Authorization: Bearer <token>` on the UI API and the MCP SSE transport. A token is auto-generated and printed when binding to a non-loopback address. |
| `R2G_CORS_ORIGINS` | Comma-separated allow-list of CORS origins for the UI (default: same-origin only). |
| `R2G_CSV_BASE_DIR` | Confine CSV sources to a trusted directory tree; paths outside it (including via symlinks/`..`) are rejected. |
| `R2G_SECRET_KEY` | Fernet key for encrypting catalog credentials at rest (else `~/.r2g/secret.key`). |

The MCP `stdio` transport (Cursor / Claude Desktop) is local-only and needs no
token; only the network-exposed SSE transport is gated.

## License

`r2g` is licensed under the [Apache License, Version 2.0](LICENSE).
Third-party dependency notices are in [NOTICE](NOTICE).

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for release notes.
