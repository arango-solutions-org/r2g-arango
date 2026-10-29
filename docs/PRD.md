# R2G-ETL Pipeline

*Product Requirements Document (PRD) -- Reference Application (production core: `relational-schema-analyzer`)*

| Field | Value |
| :--- | :--- |
| **Product name** | R2G-ETL Pipeline (Relational to Graph -- Extract, Transform, Load) |
| **Version** | 0.1.0 (experimental) |
| **Date** | Originally drafted December 2025, consolidated April 2026 |
| **Status** | Phases 1--4 implemented and hardened; Phase 5 (Temporal graph mode) implemented and end-to-end validated against live ArangoDB; Phase 5b (Visual Mapper), Phase 5c (Expression / Graph-of-Graphs UI), Phase 5e (UI Architecture Upgrade), Phase 5f (Naming conventions & rename change-management), and Phase 5g (Post-demo UX refinements) implemented; Phase 6 (Snowflake) done; MySQL/MariaDB and SQL Server sources added; Phase 5d (ArangoDB-backed catalog), Phase 8 (external data catalog integration; 8a–8b implemented), Phase 9 (classification propagation & entitlement-aware loading; 9a + 9b + 9c implemented), Phase 10 (ontology derivation; 10a + 10b + 10c + the deterministic `relational-schema-analyzer` engine, Stage 1, implemented), Phase 11 (denormalization & normal-form analysis; 11a–11c implemented), and Phase 7 are planned or exploratory |
| **Target users** | Database architects, data engineers, and developers evaluating relational-to-graph migration with ArangoDB |

---

## 1. Goals and objectives

The primary goal of the R2G-ETL Pipeline is a configurable tool for transforming and loading data from relational schemas into ArangoDB graph schemas. r2g is the **reference application** for these patterns; its reusable introspection/analysis core is factored into [`relational-schema-analyzer`](https://github.com/ArthurKeen/relational-schema-analyzer) (RSA). **RSA is actively adopted by other projects in ArangoDB Solutions** as their foundation (the contextual-data-fabric building blocks depend on it), so RSA is the production-grade dependency and carries the production bar. r2g composes RSA with loading, CDC/Kafka sync, classification/governance, and the mapping studio; it is well-tested (CI: ruff + mypy + a large unit suite + Dockerized integration) but its operational envelope is still single-node with no scale/HA guarantees. Downstream systems depend on **RSA and named, tested r2g modules**, not on r2g as a whole. PostgreSQL, MySQL/MariaDB, SQL Server, Snowflake, and ClickHouse are supported relational sources; the architecture accommodates further sources via the `SourceConnector` abstraction.

### Key objectives

| Objective | Detail |
| :--- | :--- |
| **Automation** | Eliminate manual spreadsheet-based mapping and script generation for initial data migration. |
| **Flexibility** | Support multiple ingestion paths: flat files, direct connection, CDC (PostgreSQL logical replication), and Kafka (Debezium / custom producers). All implemented. |
| **Schema management** | Ingest schemas from supported relational sources and maintain metadata for mapping to target ArangoDB graph topologies (property graph, labeled property graph). |
| **Scalability** | Use `arangoimport` for efficient, high-volume bulk loading. |
| **Synchronization** | Synchronize the relational system through live stream processing of delta changes via CDC (Phase 3) and Kafka (Phase 4), with configurable conflict resolution. |

---

## 2. Solution overview

The product is a multi-phased pipeline that reads relational schema, applies a configurable mapping, and loads data into ArangoDB via multiple paths: `arangoimport` scripts (file-based), HTTP API bulk streaming (direct connection), CDC logical replication (near real-time), and Kafka consumption (Debezium / custom producers).

### Core components

| Component | Function |
| :--- | :--- |
| **Schema reader** | Dispatches through `SourceConnector` to PostgreSQL, MySQL/MariaDB, SQL Server, Snowflake, ClickHouse, or CSV and reads tables, columns, primary keys, and foreign keys (including composite FKs where the source exposes them). |
| **Metadata store** | Persists source schema snapshots and the user-defined target ArangoDB ontology/schema as JSON and YAML files. |
| **Mapping engine** | Applies transformation logic: tables to document collections; foreign keys to edge collections (PK/FK values to `_from` / `_to` with collection prefixes). |
| **Data egress / import generator** | Generates executable bash import scripts. Supports two modes: JSONL-based (transforms CSV to intermediate JSONL) and CSV-direct (uses `arangoimport --type csv` with `--translate` and `--datatype` flags to import PG dumps without intermediate files). |
| **Mapping visualizer** | Generates self-contained HTML reports with an interactive D3.js force-directed graph showing the source-to-ArangoDB mapping, relational schema cards, edge mapping details, and a mapping editor with YAML export. |
| **Streaming engine** | Opens a source-specific consistent read session and writes directly to ArangoDB via the python-arango HTTP bulk import API, with configurable batch sizes. Supports `--dry-run`, `--drop-collections`, parallel workers, retry with exponential backoff, topological import ordering, and incremental filtering where supported. No intermediate files. |
| **Table dumper** | `r2g source dump` opens the registered source session and exports one CSV file per table; PostgreSQL uses `COPY`, while other relational connectors stream rows through their drivers. |
| **CDC engine** | Near real-time PostgreSQL-to-ArangoDB sync via logical replication. `PGReplicationListener` manages replication slots and polls `pg_logical_slot_get_changes`. Parsers for `test_decoding` (built-in) and `wal2json` output plugins. `DeltaTransformer` converts row-level changes to graph mutations. `CDCHandler` orchestrates event processing with transaction grouping, stats tracking, and configurable conflict resolution (`source_wins`, `last_write_wins`, `log_and_skip`, `fail`). |
| **Kafka consumer** | Consumes CDC events from Kafka topics via `confluent-kafka`. `DebeziumParser` handles Debezium JSON envelopes (including Kafka Connect wrappers and snapshot reads). `FlatJsonParser` for custom producers. At-least-once delivery with post-write offset commits. Reuses the CDC engine's handler and conflict resolution. Optional dependency (`pip install r2g-arango[kafka]`). |
| **Schema diff / config migration** | `diff-schema` compares two schema snapshots (added/removed tables, column changes, FK changes). `migrate-config` auto-updates mapping YAML when the source schema evolves, preserving user customizations. |
| **Data validator** | `validate-data` checks FK referential integrity of dump files before import, detecting orphaned references that would create dangling edges. |

### Relational-to-graph mapping logic

The transformation is largely mechanical and can be described in three layers (these are **mapping** concerns, not the same as project Phases 1--4 in Section 3):

1. **Transliteration (structural mapping)**
   - Each relational **table** maps to an ArangoDB **document collection**.
   - The table **primary key** feeds document **`_key`** (or another agreed unique identifier).
   - Each **foreign key** relationship maps to an **edge collection**: traverse the dependent table and map PK/FK values to ArangoDB **`_from`** and **`_to`**, with correct collection prefixes.
   - Table columns become document properties, with appropriate JSON type conversion.

2. **Join tables**
   Join tables that implement many-to-many relationships in the relational model are modeled as **edges** in the graph. The two FK columns become the `_from` and `_to` endpoints.

3. **Normalization**
   Categorical attributes (e.g., country codes) may be normalized into dedicated **vertex collections** with connecting edges when richer category data or reuse across entities is required.

### Edge cases in mapping

The mechanical mapping handles several non-trivial patterns:

- **Self-referential FKs** (e.g., `employees.manager_id -> employees.id`): produces edges within the same vertex collection. This is correct graph modeling but may surprise users expecting separate collections.
- **Multiple FKs to the same table** (e.g., `orders.customer_id` and `orders.referrer_id` both referencing `customers`): each FK produces a separate edge collection, named `{source}_to_{target}` with a `_{fk_column}` suffix to disambiguate.
- **Nullable FKs**: rows where the FK value is NULL are silently skipped (no edge is created). This is intentional -- a NULL FK means "no relationship."
- **Tables with no primary key**: these cannot produce meaningful `_key` values. The tool warns during config validation and streaming; documents receive auto-generated `_key` values and edges referencing such tables are flagged.

The following patterns are **handled with caveats**:

- **Circular FK dependencies** (table A references B, B references A): detected by topological sort (Kahn's algorithm). The tool warns about cycles and proceeds with a best-effort ordering.
- **Inheritance patterns** (single-table inheritance, table-per-type): no special handling; each table is mapped independently.
- **Polymorphic associations**: not supported.

---

## 3. Project phases and requirements

The roadmap is organized into phases: Phases 1–4 (MVP through Kafka), Phase 5 (temporal graph mode), Phase 5f (naming conventions & rename change-management), Phase 5g (post-demo UX refinements), and Phase 6 (Snowflake + multi-source) are implemented; Phase 5d (ArangoDB catalog backend) is planned; Phase 7+ (additional sources and advanced features) is exploratory.

### Phase 1: Table dump file processing (MVP) -- Implemented

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P1.1** | **Schema ingestion** | Connect to PostgreSQL (credentials/URL) and read table, column, PK, and FK definitions. | Done |
| **P1.2** | **Metadata storage** | Store ingested PostgreSQL schema and user-defined target ArangoDB schema (ontology) as metadata. | Done |
| **P1.3** | **Dump file input** | Accept flat-file dumps (e.g., CSV, TSV, GZ) of individual PostgreSQL tables. | Done |
| **P1.4** | **Node transformation** | Transform dump rows into ArangoDB document form with type coercion for `arangoimport`. | Done |
| **P1.5** | **Edge transformation** | Build edge collections by cross-referencing PKs and FKs; map to `_from` and `_to` including collection prefixes. | Done |
| **P1.6** | **`arangoimport` script generation** | Emit executable shell scripts to run `arangoimport` for all generated document and edge files. | Done |
| **P1.7** | **CSV-direct import** | Generate `arangoimport --type csv` scripts that import PG CSV dumps directly using `--translate` for key remapping, `--datatype` for type coercion, and `--from-collection-prefix` / `--to-collection-prefix` for edge `_from`/`_to`. No intermediate JSONL step required. **CSV-direct is faithful or it refuses.** `arangoimport` loads every CSV column as it stands, so a mapping that uses a feature this path cannot reproduce (a field expression, masks included; an include or exclude filter; a mapping-level type override) is refused, by the CLI and by Studio's CSV-direct preview, with an error naming each collection and feature and pointing to JSONL mode. A column the mapping masks or excludes therefore never loads in clear, and a computed field is never silently missing (P5c.1.6). Renames in `field_mappings` are reproduced with `--translate`. | Done (`src/r2g/generators/arangoimport.py:1009`, `tests/test_csv_import_generator.py::TestCsvDirectRefusesUnfaithfulMappings`) |
| **P1.8** | **Mapping visualizer** | Interactive HTML visualization of the relational-to-graph mapping using D3.js force-directed graph layout, with relational schema cards and edge mapping detail views. | Done |
| **P1.9** | **Import script safety** | Generated `arangoimport` / `arangosh` scripts (P1.6, P1.7) never put the ArangoDB password in the process list (it is passed as `@ARANGO_PASSWORD@` and read by the client tool from its exported environment), and they fail instead of reporting success when `arangoimport` rejects rows, which it does while exiting 0. The one tolerated rejection is a CSV-direct edge whose nullable foreign key is NULL; it is skipped and counted. | Done |

### Phase 2: Direct PostgreSQL connection and streaming -- Implemented

| ID | Requirement | Description | Pre-requisite | Status |
| :--- | :--- | :--- | :--- | :--- |
| **P2.1** | **Direct read interface** | Establish direct, persistent connections to the live PostgreSQL database via psycopg server-side cursors. | P1.1 | Done |
| **P2.2** | **Batched data extraction** | Read data in controlled batches (configurable `--batch-size`, default 10,000) using named server-side cursors to bound memory use. | P2.1 | Done |
| **P2.3** | **Streaming import** | Stream transformed data to ArangoDB via the python-arango HTTP bulk import API (`import_bulk`) without intermediate files. | P2.2, P1.4, P1.5 | Done |
| **P2.4** | **Snapshotting logic** | Full initial load with REPEATABLE READ transaction isolation for consistent snapshot semantics. | P2.3 | Done |

### Phase 3: Change Data Capture (CDC) integration -- Complete

| ID | Requirement | Description | Pre-requisite | Status |
| :--- | :--- | :--- | :--- | :--- |
| **P3.0** | **CDC event model** | Pydantic models for `ChangeEvent` (INSERT/UPDATE/DELETE with old/new row, LSN, timestamp, transaction ID), `ArangoDelta` (target mutation), and `TransactionBatch` (grouped deltas). | P1.4 | Done |
| **P3.0b** | **Delta transformer** | `DeltaTransformer` converts `ChangeEvent`s into `ArangoDelta`s using existing `NodeTransformer` and `EdgeTransformer`. Handles INSERT→insert, UPDATE→replace (document + edges), DELETE→delete (document + edge cleanup). | P3.0, P1.4, P1.5 | Done |
| **P3.0c** | **CDC handler** | `CDCHandler` orchestrates event consumption, transformation, and application. Supports single events, event streams, and transaction-grouped batches. Tracks stats (events, deltas, failures, LSN). | P3.0b | Done |
| **P3.0d** | **Single-document writer ops** | `ArangoWriter.insert_document`, `replace_document`, `delete_document`, and `apply_delta` methods with retry logic for CDC use. | P2.3 | Done |
| **P3.1** | **CDC listener** | `PGReplicationListener` manages logical replication slots via `pg_create_logical_replication_slot` / `pg_drop_replication_slot`. Polls changes via `pg_logical_slot_get_changes`. Supports `test_decoding` (built-in) and `wal2json` output plugins with dedicated parsers. Feeds parsed `ChangeEvent`s into `CDCHandler` grouped by transaction. CLI commands: `cdc-setup`, `cdc-teardown`, `cdc-status`, `cdc-start`. | P2.1, P3.0c | Done |
| **P3.2** | **Delta transformation** | Map captured changes to ArangoDB replace/insert/delete operations. | P1.4, P1.5 | Done (P3.0b) |
| **P3.3** | **Live stream processing** | `cdc-start` command runs a continuous polling loop with configurable `--poll-interval` and `--batch-size`. Graceful shutdown via SIGINT/SIGTERM. Session statistics displayed on exit. | P3.1, P2.3 | Done |
| **P3.4** | **Conflict resolution** | Configurable conflict policies for CDC delta application: `source_wins` (default, PG is truth — upsert on duplicate, insert on missing), `last_write_wins` (LSN comparison, reject stale writes via `_r2g_lsn` field), `log_and_skip` (log conflicts, skip writes), `fail` (raise on any conflict). `ConflictResolver` wraps write operations, detects conflict types (INSERT_DUPLICATE, REPLACE_MISSING, DELETE_MISSING, STALE_OVERWRITE, ORPHAN_EDGE), and resolves per policy. `ConflictLog` accumulates conflict events with per-type counts and session summary. Integrated into `CDCHandler._apply_delta` and `cdc-start --conflict-policy` CLI option. | P3.3 | Done |

### Phase 4: Kafka integration -- Complete

| ID | Requirement | Description | Pre-requisite | Status |
| :--- | :--- | :--- | :--- | :--- |
| **P4.1** | **Kafka producer/connector** | Connect to an external CDC pipeline (e.g., Debezium) that streams PostgreSQL changes to Kafka topics. R2G consumes from the Kafka side; Debezium connector setup is external. | P3.1 | Done (external) |
| **P4.2** | **Kafka consumer** | `KafkaConsumer` wraps `confluent-kafka`, subscribes to topics, polls in batches, commits offsets after successful processing (at-least-once semantics). Graceful shutdown via SIGINT/SIGTERM. Optional dependency via `pip install r2g-arango[kafka]`. | P4.1 | Done |
| **P4.3** | **Kafka message transformation** | `DebeziumParser` parses Debezium JSON envelope (`before`/`after`/`op`/`source`) including Kafka Connect `payload` wrapper, snapshot reads (`op: r`). `FlatJsonParser` for custom producers. Both produce `ChangeEvent` objects fed into existing `CDCHandler`. | P4.2, P3.2 | Done |
| **P4.4** | **Transactional ordering** | Messages consumed in Kafka partition order. Events grouped by `transaction_id` (from Debezium `source.txId`) and applied through `CDCHandler.handle_transaction` for ordered delta application. Conflict resolution policies apply. | P4.3 | Done |

### Phase 5: Temporal graph mode -- Implemented

CDC and Kafka pipelines apply changes as direct replaces/deletes by default. Temporal graph mode adds an alternative write strategy using the **immutable-proxy time travel pattern** (ProxyIn / Entity / ProxyOut), enabling full version history, point-in-time queries, and soft deletes with automatic TTL-based garbage collection.

All of P5.1--P5.8 are implemented (`src/r2g/temporal/` — `TemporalConfig` / `TemporalNaming` models, `TemporalApplier`, and AQL query templates in `queries.py`; CLI `--temporal`, `--ttl-seconds`, and `--smart-field` flags on `cdc-start` and `kafka-start`; covered by `tests/test_temporal_applier.py`, `test_temporal_models.py`, `test_temporal_queries.py`). End-to-end verification against a live temporal workload is covered by `tests/integration/test_temporal_e2e.py` — a skip-when-unavailable integration test that drives INSERT → UPDATE → DELETE through the applier against a real ArangoDB and asserts the proxies, `hasVersion` edges, TTL stamping, interval/TTL indexes, replay-safety, and every point-in-time / version-history / interval query template.

#### Architecture

Stable identity (proxies) is separated from mutable state (versioned entities). Topology edges attach to proxies and are never rewritten when entities change.

```
Topology edges --> EntityProxyIn --hasVersion--> Entity v0 (current: expired=NEVER)
                   (stable _key)  --hasVersion--> Entity v1 (historical: expired=T)
                   EntityProxyOut <-- Entity     (outbound version link)
```

| ID | Requirement | Description | Pre-requisite |
| :--- | :--- | :--- | :--- |
| **P5.1** | **Temporal write strategy** | `--temporal` flag on `cdc-start` and `kafka-start` that switches the delta application from direct replace/delete to versioned writes. CDC INSERT creates ProxyIn + ProxyOut + Entity v0 (with `created=now`, `expired=NEVER_EXPIRES`). CDC UPDATE expires the current Entity (`expired=now`) and inserts a new version (`created=now`, `expired=NEVER_EXPIRES`). CDC DELETE soft-deletes by setting `expired=now` on the current Entity; proxies and topology edges are preserved. | P3.4, P4.4 |
| **P5.2** | **Proxy collection management** | Auto-create `{Collection}ProxyIn` and `{Collection}ProxyOut` document collections alongside each mapped entity collection. Proxies carry only the shard key attribute (for SmartGraph compatibility) and a stable `_key`. | P5.1 |
| **P5.3** | **hasVersion edge collection** | Auto-create `hasVersion` edge collection with bidirectional edges: `ProxyIn -> Entity` (inbound) and `Entity -> ProxyOut` (outbound). Edges carry `created` and `expired` timestamps matching their entity version. | P5.2 |
| **P5.4** | **Interval semantics** | Every versioned entity and version edge carries `created` (float, unix timestamp) and `expired` (float, unix timestamp or sentinel `sys.maxsize = 9223372036854775807` for current). Current entities: `expired == NEVER_EXPIRES`. Historical entities: `expired` is a finite timestamp. | P5.1 |
| **P5.5** | **TTL aging** | Automatic garbage collection of historical versions via TTL indexes. Only documents with `expired != NEVER_EXPIRES` receive a `ttlExpireAt` field (`expired + ttl_retain_seconds`). TTL index is `sparse: true` to skip current documents. Configurable retention period via `--ttl-seconds` (default: 30 days). Static reference data and proxy collections are excluded from TTL. | P5.4 |
| **P5.6** | **MDI-prefixed temporal indexes** | Create `mdi-prefixed` indexes on `[created, expired]` for all versioned entity and hasVersion edge collections. Accelerates point-in-time snapshot queries and interval intersection queries. Verify usage via `zkd` index type in query execution plans. | P5.4 |
| **P5.7** | **Point-in-time query templates** | Emit AQL query templates for common temporal operations: snapshot at time T (`created <= @t AND expired > @t`), version history traversal (ProxyIn -> hasVersion -> Entity, sorted by `created DESC`), temporal overlap/interval intersection (`created <= @end AND expired >= @start`), and "what changed between T1 and T2". | P5.6 |
| **P5.8** | **SmartGraph compatibility** | Key structure supports SmartGraph shard key prefixes (`{shardKey}:{entityType}{index}` for proxies, `{shardKey}:{entityType}{index}-{version}` for entities). Optional `--smart-field` parameter for multi-tenant isolation. Satellite collections for shared taxonomy/classification data. | P5.2 |

#### Temporal-specific considerations

- **Write amplification.** Each source UPDATE produces 3-5 ArangoDB writes (expire old entity + insert new entity + 2 hasVersion edges + optional classification edges). Size RocksDB write buffers and monitor compaction accordingly.
- **Storage growth.** Without TTL, historical versions accumulate indefinitely. Monitor collection document counts; unbounded growth indicates TTL misconfiguration.
- **Conflict resolution interaction.** `last_write_wins` is recommended for temporal mode to prevent out-of-order events from creating phantom versions. A replayed INSERT should not create a duplicate entity version.
- **Topology edge stability.** Topology edges (connections, associations, locations) attach to proxies, NOT to versioned entities. This is the key invariant -- relationships survive entity versioning without being rewritten.
- **DELETE semantics.** CDC DELETEs become soft deletes (set `expired=now`). The entity remains queryable at any historical point in time. Physical removal is handled exclusively by TTL.

---

## 4. Technical requirements

| Category | Requirement | Details |
| :--- | :--- | :--- |
| **Architecture** | Modularity | Data sources are swappable via the `SourceConnector` / `SourceSession` abstraction (`connectors/base.py`). Implemented relational sources: PostgreSQL, MySQL/MariaDB, SQL Server, Snowflake, and ClickHouse; CSV directories and Kafka are also supported. |
| **Target DB** | ArangoDB | Load via `arangoimport` (file-based, Phase 1) and the ArangoDB HTTP API (streaming/CDC/Kafka, Phases 2--4). |
| **Transformation** | Schema mapping | Configurable prefix mapping for `_from` and `_to` (e.g., `user_1` to `Users/1`). |
| **Data integrity** | Key generation | Correct document `_key` values derived from source primary keys, including composite keys joined by a configurable separator. |
| **Technology stack** | Python | Chosen for ecosystem support (psycopg, python-arango, Polars, Pydantic, structlog, confluent-kafka, python-dotenv). |

### Known constraints

- **Referential integrity is opt-in**: the `validate-data` command checks FK values against PK sets from dump files, but this check is not enforced automatically during import. Orphaned references will still produce edges pointing to non-existent vertices if validation is skipped.
- **Bulk load idempotency**: re-running the streaming pipeline with `--drop-collections` replaces all data. For incremental updates, CDC and Kafka pipelines provide configurable conflict resolution (`source_wins`, `last_write_wins`, `log_and_skip`, `fail`) with at-least-once delivery semantics.
- **System database protection**: loads into the ArangoDB `_system` database are refused by the UI; new targets default to a non-`_system` database to prevent accidental writes into the management database.
- **Reserved ArangoDB attributes**: `_id`, `_key`, `_rev` (documents) and `_from`, `_to` (edges) are reserved system attribute names. They are never produced or renamed by naming conventions or rename migrations, and `validate_config` flags any `field_mapping` / `field_expression` that targets one (see Phase 5f).
- **Credential handling**: connection parameters can be loaded from `.env` files or environment variables (`PG_CONN`, `ARANGO_ENDPOINT`, etc.). Credentials stored in the filesystem catalog (`~/.r2g/catalog.json`) are encrypted at rest via Fernet using a key loaded from `R2G_SECRET_KEY` or `~/.r2g/secret.key` (0600, auto-generated). Generated import scripts still contain connection defaults, and no integrated remote secrets manager (e.g., HashiCorp Vault) is provided.

### Phase 6: Snowflake integration -- **Done**

Snowflake is a common data warehouse among R2G users. This phase adds Snowflake as a source alongside PostgreSQL, reusing the existing mapping, transformation, and loading infrastructure.

Delivery was sliced: slice 1 (P6.1 introspect-only + P6.2 type mapping + P6.5 source abstraction), slice 2 (P6.6 FK inference), and slice 3 (P6.3 dump export + P6.4 streaming) are all **Done**. The streaming pipeline, `r2g stream` CLI, and UI `POST /api/projects/{name}/load` endpoint are now source-agnostic and dispatch through `create_source_connector` based on the cataloged `source_type`; every supported source implements `open_session()` returning a consistent-snapshot `SourceSession` that the pipeline and the new `r2g source dump` CLI consume. Password and key-pair authentication, schema snapshotting, reviewed-key overlay application, mapping generation, CSV export, and streaming have been field-validated against the live Customer 360 Snowflake demo.

| ID | Requirement | Description | Pre-requisite | Status |
| :--- | :--- | :--- | :--- | :--- |
| **P6.1** | **Snowflake schema reader** | Connect to Snowflake via the Snowflake Connector for Python (`snowflake-connector-python`, installed as the optional `r2g-arango[snowflake]` extra) and introspect `INFORMATION_SCHEMA.TABLES` / `INFORMATION_SCHEMA.COLUMNS` plus `SHOW PRIMARY KEYS` / `SHOW IMPORTED KEYS` to populate the same `Schema` model used by PostgreSQL. Delivered as `r2g.connectors.snowflake.SnowflakeConnector`; imports are lazy so the UI degrades gracefully with a 501 + pip-install hint when the extra is not installed. Connection strings use the Snowflake SQLAlchemy URL shape and support either password auth (`snowflake://user:pass@account/DATABASE[/SCHEMA]?warehouse=WH&role=R`) or mutually exclusive key-pair auth (`private_key_file=...`, optional `private_key_file_pwd=...`, `authenticator=SNOWFLAKE_JWT`); `$ENV_VAR` references are expanded only at connection time and secrets are redacted from errors and API output. | P1.1 | **Done** (introspection, dump, and streaming all wired via `SnowflakeSession`; see P6.3–P6.5) |
| **P6.2** | **Snowflake type mapping** | Map Snowflake data types (`NUMBER`, `VARCHAR`, `BOOLEAN`, `TIMESTAMP_*`, `VARIANT`, `ARRAY`, `OBJECT`, `GEOGRAPHY`, `GEOMETRY`, `BINARY`, `VECTOR`, etc.) to JSON types. `VARIANT`/`OBJECT` map to JSON objects; `ARRAY` maps to JSON arrays. `DEFAULT_TYPE_MAP` in `r2g.config` has been extended with Snowflake-specific entries and is shared with PostgreSQL via `pg_type_to_json_type`. | P1.4 | **Done** |
| **P6.3** | **Snowflake dump export** | `r2g source dump <name>` is the source-agnostic replacement for the legacy `r2g dump-tables --conn`. It reads the catalog entry, dispatches through `create_source_connector`, opens a `SourceSession`, and calls `SourceSession.dump_table_to_csv()` for every table in the latest snapshot (or a `--tables` subset). PostgreSQL goes through the server-side `COPY … TO STDOUT WITH CSV HEADER` fast path; Snowflake streams the cursor and writes CSV via Python's `csv` module with empty-string NULLs and `"` quoting. Output is one `<table>.csv` per table under `--output-dir`. The UI will gain a "Dump tables" button in a later polish pass. | P6.1 | **Done** |
| **P6.4** | **Snowflake streaming** | `StreamingPipeline` now consumes any `SourceConnector`; PG and Snowflake each implement `open_session()` returning a `SourceSession` with `count_rows`, `stream_rows`, `dump_table_to_csv`, and `close`. PG sessions set `REPEATABLE READ`; Snowflake sessions open a `BEGIN`/`COMMIT` transaction for implicit snapshot isolation and stream via `cursor.fetchmany(batch_size)`. `r2g stream --source <name>` resolves the catalog source and drives the pipeline through the abstraction; the legacy `--pg-conn` flag still works by wrapping the string in a `PostgresConnector`. `POST /api/projects/{name}/load` dispatches the same way. Value-overlap FK sampling (P6.6) against Snowflake becomes available as a follow-up once a `SnowflakeValueSampler` is wired on top of the session. | P6.1, P2.3 | **Done** |
| **P6.5** | **Source abstraction layer** | `r2g.connectors.base` exposes a structural `SourceConnector` Protocol (attributes `connection_string`, `schema_name`; methods `get_schema() -> Schema` and `open_session() -> SourceSession`), the `SUPPORTED_SOURCE_TYPES` registry, and a `create_source_connector(source_type, connection_string, schema_name)` factory. `r2g.connectors.session.SourceSession` is the matching bulk-read Protocol (`count_rows`, `stream_rows`, `dump_table_to_csv`, `close`). `PostgresConnector` / `PostgresSession`, `SnowflakeConnector` / `SnowflakeSession`, and `ClickHouseConnector` / `ClickHouseSession` all satisfy the Protocols. The UI, MCP server, streaming pipeline, and every `source` CLI subcommand now dispatch through the factory based on the stored `source.source_type`. | P6.1, P6.4 | **Done** |
| **P6.6** | **FK inference** | Many schemas (Snowflake especially, but also legacy relational dumps) ship without declared FKs. Delivered as `r2g.fk_inference`: a pure-Python name-heuristic engine with pluggable value-overlap sampling. Scans for `{prefix}_id`, `{prefix}id`, `{prefix}_{pkcol}`, and non-generic PK-name matches, filters by JSON-level type compatibility, and emits a composite-FK pass that grabs local tables covering every column of a multi-column PK. `POST /api/sources/{name}/infer-fks` returns scored candidates; the Mapping Studio exposes them via a "Suggest FKs" action with per-row Accept / Accept all / Dismiss buttons that write real `EdgeDefinition`s to the project mapping. `r2g source infer-fks <name> [--sample] [--accept]` mirrors the surface on the CLI; `--sample` activates a value-overlap sampler (veto-on-zero by default, confidence boost on ≥50% / ≥90% overlap). Sampling is implemented for PostgreSQL, MySQL/MariaDB, SQL Server, and CSV; Snowflake and ClickHouse fall back to name-only evidence. Name-heuristic inference works against any source type. **An accepted candidate is labelled, not laundered:** `--accept` writes each key with `enforced: false`, named `inferred:<table>_<cols>_fk` (the overlay's naming, so every key has its own name), records its method and confidence on the snapshot (`inferred_keys`), and carries the snapshot's key-overlay record forward. An unlabelled guess would read as a constraint the source declared and enforces, which also stops relationship inference on that table downstream. Visualizer and Studio show such keys as `inferred`. Keys accepted before 2026-10-14 keep their `inferred_<method>` names and read as enforced until the source is re-snapshotted. | P6.1 | **Done** — accept labelling: `src/r2g/main.py:2798` (`to_accepted_foreign_key`), overlay record and `inferred_keys` carried forward `src/r2g/main.py:2811`; `src/r2g/fk_inference.py:97` (`to_accepted_foreign_key`), `src/r2g/fk_inference.py:65` (`INFERRED_CONSTRAINT_PREFIX`), `src/r2g/fk_inference.py:68` (`key_provenance`); `src/r2g/catalog.py:136` (`SchemaSnapshot.inferred_keys`). |
| **P6.7** | **Cross-source shared-key inference** | Extend FK inference beyond a single introspected schema. Given two or more source schemas / CSI bundles, propose **shared-key relationships**: columns co-occurring across sources with compatible names (identical, or `{entity}_id` on both sides), JSON-level type compatibility (reusing `pg_type_to_json_type`), and value-overlap where a sampler is available. When one source owns the key as a table PK (e.g. `crm.account.id` / `account_id`), propose that table as a **hub entity** and emit edges from each referencing source (`query_events → Account`, `usage_metrics → Account`); when no source owns it as a PK, propose a **virtual hub concept** (the federation join key) rather than inventing a table. Candidates are **confirm-to-accept** (same discipline as P6.6 — never silently materialized), surfaced in the Studio and as CSI/mapping suggestions. An accepted shared-key relationship is recorded as a declared **join key** in the CSI/R2RML export and feeds the Phase-12 E1 partitioner's class-binding routing, so the federated executor bind-joins on it without hand-authored config. Deterministic-first; the Phase-10 LLM seam may *rank* ambiguous hubs but never invents them. Motivated by the ClickHouse connector (WP-CH2): `account_id` shared by `clickhouse.query_events` / `usage_metrics` is an FK to an `Account` entity that lives in the Postgres CRM source — a cross-source join key that single-schema, name-anchored P6.6 inference structurally cannot see. **Value-overlap note:** a cross-*database* `LEFT JOIN` is impossible, so the overlap probe is an intersection of bounded per-side value samples (`sample_values` on each side), which makes it **asymmetric evidence** — a high overlap is real support, but a low overlap over truncated samples is *not* evidence of absence and never vetoes a candidate. | P6.6, P12.1 | **Done** — engine `src/r2g/xsk.py` (`infer_shared_keys`, hub selection incl. the virtual-hub case), persistence as a declared join key on `MappingConfig.shared_keys` (`src/r2g/types.py`), CSI export as `conceptualModel.joinKeys` (`src/r2g/csi.py`), `POST /api/shared-keys/infer`, `r2g shared-keys infer`, and a Studio card (shortcut `k`) with a Federated Join Keys section. **Open:** the Phase-12 E1 partitioner *consumer* side and R2RML join-key emission (drift alert `P6.7-E1-ROUTING`). |
| **P6.8** | **Reviewed key overlays** | A source may carry a reviewed RSA JSON/YAML PK/FK overlay (`r2g source add --key-overlay`). Snapshot capture applies it before classifications, persists non-secret source/summary metadata plus a canonical content fingerprint, and invalidates cached demo snapshots when source, schema, or overlay content changes. RSA marks resulting overlay FKs `enforced=false`; the overlay document itself need not contain that field. **UNIQUE keys survive persistence:** an overlay's `uniqueConstraints` (and any UNIQUE key in the captured schema) are written with the snapshot and survive a catalog reload, so FKs inferred through a natural key are still found afterwards; tables without one serialize exactly as before. Snapshots carry `schema_format_version`, which versions the *storage*: absent/1 means the snapshot could not hold UNIQUE keys (written before 2026-10-02), 2 means it keeps whatever the capture produced. It does not say the capture looked for them -- r2g's connectors read no declared UNIQUE yet, so today they arrive only through an overlay. Implemented in `src/r2g/snapshot.py`, `src/r2g/catalog.py`, `src/r2g/types.py`, and the shared CLI/UI snapshot path. | P6.1 | **Done** |
| **P6.9** | **Bundled Customer 360 rehearsal** | Ship deterministic, constraint-free Snowflake sample data, a reviewed five-PK/six-FK overlay, role-separated bootstrap/validation, and an idempotent Studio preset that creates/reuses the source, snapshot, editable five-vertex/six-edge mapping, and a selected non-`_system` target project without persisting key material. Implemented under `src/r2g/demo/snowflake_customer_360`, `examples/snowflake_customer_360`, and `POST /api/demo-presets/snowflake-customer-360/install`. | P6.8 | **Done** |
| **P6.10** | **Key suggestion from data** | For a source that declares no keys, or only surrogate ones (Snowflake, PostgreSQL), `r2g source suggest-keys NAME` proposes primary keys, natural keys beside a declared surrogate key, and the references between them from the data, using relational-schema-analyzer's key profiler and a cost-governed sampler for the source (query budget `--max-queries`, a per-query `--timeout` the database enforces, a query tag per run, cost reported). Other source types are refused: their samplers cannot profile keys under a budget. The output is a **draft** key overlay -- every entry carries its evidence, alternatives are listed, tables without a proposal say why, and a natural key beside a surrogate one is emitted as a UNIQUE key, never as the primary key -- and **nothing is applied**. `r2g source set-key-overlay NAME FILE` attaches a reviewed overlay to an existing source (applied at the next `source snapshot`; for a PostgreSQL schema other than `public` the command prints the `--pg-schema` the snapshot needs), refuses a file still marked DRAFT unless `--reviewed` is given, validates the overlay's structure immediately, and `--clear` removes it. Requires relational-schema-analyzer `>=0.9.5` (governed PostgreSQL sampler). A sampler that cannot connect fails the run without writing a draft, rather than reporting every table as unprofiled. Verified live 2026-10-02 on the constraint-free Customer 360 Snowflake data (the reviewed snapshot matched the hand-written overlay exactly, 5 PKs and 6 FKs, in 13 queries) and 2026-10-13 on PostgreSQL (keyless and surrogate-key schemas, `tests/integration/test_suggest_keys_postgres.py`). | P6.8 | **Done** — `src/r2g/main.py:2423` (`_KEY_PROBE_SOURCES`), `src/r2g/main.py:2426` (`_key_probe_sampler`), `src/r2g/main.py:2442` (`source_suggest_keys`; source-type gate `:2480`, query budget `:2507`, timeout `:2508`, query tag `:2509`, connect-failure guard `:2526`, `--pg-schema` hint `:2547`), `src/r2g/main.py:2605` (`source_set_key_overlay`; DRAFT gate `:2641`), `src/r2g/main.py:2560` (`_check_overlay_structure`); tests `tests/test_cli_suggest_keys.py`, `tests/integration/test_suggest_keys_postgres.py`. |

#### Snowflake-specific considerations

- **FK constraints are not enforced in Snowflake.** They can be declared but are informational only. Many Snowflake schemas have no FK metadata at all. The FK inference feature (P6.6) addresses this gap.
- **Semi-structured data.** Snowflake `VARIANT`, `OBJECT`, and `ARRAY` columns can contain nested JSON. These should be preserved as nested structures in ArangoDB documents rather than flattened.
- **Future large-table optimization (not implemented).** Snowflake sessions currently use bounded `fetchmany()` batches. Warehouse result-cache integration and Arrow-based fetching (`fetch_arrow_all()`) remain future work.
- **Authentication.** Password and key-pair authentication are supported. Key-pair URLs use `private_key_file`, optional `private_key_file_pwd`, and `authenticator=SNOWFLAKE_JWT`; password and key-pair credentials are mutually exclusive. Parameters may use `$ENV_VAR` references, including the Contextual Data Fabric variables `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY_FILE`, `SNOWFLAKE_PRIVATE_KEY_FILE_PWD`, `SNOWFLAKE_WAREHOUSE`, and `SNOWFLAKE_ROLE`. Missing/unreadable keys fail before driver invocation, and private-key paths, passphrases, passwords, and source URLs must not appear in client errors or generated artifacts. OAuth, SSO, and external-browser flows remain out of scope.
- **Future cost reporting (not implemented).** Every query against Snowflake consumes warehouse credits. Explicit query-cost estimates in `--dry-run` remain future work; operators must currently use Snowflake warehouse/query monitoring.

### Phase 5b: Visual Graph Data Mapper & Ingestion Engine -- Implemented

The existing Mapping Studio UI (`r2g ui`) provides basic project selection, graph visualization, and YAML export. Phase 5b evolves it into a full-featured visual mapping and ingestion tool, inspired by TigerGraph GraphStudio. This phase is defined by three epics: Data Catalog, Visual Mapping Interface, and Ingestion & Execution Engine.

#### Target personas

- **Data Engineer:** Needs to set up reliable, high-throughput pipelines from relational/flat sources to the graph database.
- **Graph Architect / Data Modeler:** Needs to ensure that the source data correctly maps to the target ontology (vertices, edges, properties) without writing complex scripts.

#### Product principles

- **Object-centricity:** Users interact with visual representations of data structures (tables, streams, vertices, edges) rather than text lists or code.
- **Intelligent automation:** The system does the heavy lifting where possible -- introspecting schemas upon connection and generating intelligent default mappings.
- **Referential integrity:** The system maintains strict internal consistency. Deleting a foundational object (like a source connection) cleanly cascades to dependent objects (mappings, projects, load history).

#### Epic 1: Data Catalog Interface

The Data Catalog is the central repository for all incoming data sources and target database connections.

| ID | Requirement | Description | Pre-requisite | Status |
| :--- | :--- | :--- | :--- | :--- |
| **P5b.1.1** | **Data source CRUD** | Users can Create, Read, Update, and Delete data sources. Supported types are PostgreSQL, MySQL/MariaDB, SQL Server, Snowflake, ClickHouse, CSV directories, and Kafka topics. | -- | Done (`SUPPORTED_SOURCE_TYPES` in `connectors/base.py`; CLI, API, in-UI "+ New source" form, and right-click remove, see P5e.8.1) |
| **P5b.1.2** | **Automated schema introspection** | Upon saving a new data source, the system automatically introspects the source. CSV: parse headers and infer types. RDBMS: extract tables, columns, PKs, FKs where available. Kafka: extract schema from Schema Registry or parse sample payload. Display as hierarchical tree or entity cards. | P5b.1.1 | Done (`POST /api/sources/{name}/snapshot`, auto-triggered after in-UI source creation and exposed as right-click "Re-introspect") for PostgreSQL, MySQL/MariaDB, SQL Server, Snowflake, ClickHouse, and CSV; Kafka exposes introspection-only in the studio (batch sync via `kafka-start`) |
| **P5b.1.3** | **Target graph definition** | Users define connections to target graph databases. System introspects the target graph to fetch existing vertex types, edge types, and their properties. | -- | Done (`TargetConfig` in catalog, `src/r2g/connectors/arango_reader.py`, `GET/POST/DELETE /api/targets`, `POST /api/targets/{name}/introspect`, and in-UI "+ New target" form + right-click actions, see P5e.8.2) |
| **P5b.1.4** | **Referential integrity & cascading deletes** | Deleting a data source warns the user of dependent mappings/projects. Upon confirmation, all associated mappings and load history are deleted. | P5b.1.1 | Done (`CatalogManager.remove_source(cascade=True)` + `DependencyError`; `DELETE /api/sources/{name}?cascade=true`; in-UI right-click "Remove source…" shows a confirmation dialog before cascading) |

#### Epic 2: Visual Mapping Interface

The core workspace where users define how source data populates the graph.

| ID | Requirement | Description | Pre-requisite | Status |
| :--- | :--- | :--- | :--- | :--- |
| **P5b.2.1** | **Split-screen object-centric UI** | Left pane: introspected source schema (table cards with columns). Right pane: visual graph schema of the target database (vertices and edges). | P5b.1.2 | Done (persistent three-zone layout: sources sidebar, center canvas with source / mapping / target graphs, floating and overlay property surfaces; see Phase 5e) |
| **P5b.2.2** | **Mapping management (CRUD)** | Users can create, read, update, and delete "Map Objects" that save the state of source-to-target connections. Maps have metadata: name, description, source ID, target ID, last modified. | P5b.2.1 | Partial (save / load via `PUT /api/projects/{name}/mapping`; per-map metadata and multi-map management pending, see Phase 5d) |
| **P5b.2.3** | **Parallel connection configuration** | Within the mapping interface, users can specify ingestion parallelism: number of parallel connections/threads for reading from the source (`fetch_size`, partition strategies for RDBMS, consumer group concurrency for Kafka). | P5b.2.1 | Done (Settings modal exposes `workers`, `batch_size`, `on_duplicate`, `drop_collections`; wired into the Load payload) |
| **P5b.2.4** | **Automated default mapping** | When source and target are selected, generate a default mapping via heuristics (column name matching to vertex/edge property names, PK → Vertex ID). Render as visual connectors. | P5b.1.2 | Done (`POST /api/projects/{name}/auto-map` + toolbar "Auto-Map" action, shortcut `a`) |
| **P5b.2.5** | **Mapping customization** | Users can drag-and-drop to draw new connections between source columns and target properties. Select existing mapping lines to delete or edit. Map a single source table to multiple vertex types or edge types. | P5b.2.4 | Done (drag from source column dot to target property dot; Shift / Alt drag between target cards to add edges; right-click context menus on every connector, column, property, FK line, and edge; promote join table to explicit edge) |

#### Epic 3: Ingestion & Execution Engine

The mechanics of moving data based on the accepted map.

| ID | Requirement | Description | Pre-requisite | Status |
| :--- | :--- | :--- | :--- | :--- |
| **P5b.3.1** | **Execution trigger ("Load" button)** | Once a mapping is saved, a "Load" / "Run" button provisions the ingestion job based on parallel connection settings and the active map. | P5b.2.2 | Done (`POST /api/projects/{name}/load` runs the streaming pipeline in a background thread with load id tracking; toolbar "Load" action + confirmation modal; shortcut Ctrl/Cmd+Enter) |
| **P5b.3.2** | **Job monitoring** | Status indicator (Pending, Running, Success, Failed). Metrics: rows processed, vertices created, edges created, error count. | P5b.3.1 | Done (`GET /api/projects/{name}/load/{load_id}/status` + Server-Sent Events stream at `GET /api/projects/{name}/load/{load_id}/stream`; `progress_callback` in `StreamingPipeline`; floating progress card in the UI with live per-table counters, error count, elapsed time, minimize-to-tray; bottom timeline strip records completed runs) |
| **P5b.3.3** | **Error handling** | Records that fail mapping constraints (missing Vertex IDs, type mismatch) are routed to a dead-letter queue or error log without stopping the ingestion job. | P5b.3.1 | Done (`src/r2g/dlq.py` `DeadLetterQueue` writes `~/.r2g/dlq/{load_id}.jsonl`; `GET /api/projects/{name}/load/{load_id}/errors` exposes entries; streaming pipeline continues past per-record failures and records them to the DLQ) |

#### Non-functional requirements (Phase 5b)

| Category | Requirement |
| :--- | :--- |
| **Performance** | UI must render schemas with up to 500 tables/vertices without noticeable lag. Ingestion must support parallel data streams for TB-scale data. |
| **Scalability** | Backend ingestion engine should be decoupled from the UI, ideally supporting distributed workers (e.g., Kubernetes). |
| **Security** | Passwords and tokens for data sources and target graphs must be encrypted at rest (AES-256 or OS keychain). |

#### Out of scope (V1 of Phase 5b)

- **Bi-directional sync:** Strictly source-to-graph ingestion, not graph-to-relational export.
- **Scheduling:** Cron jobs for recurring loads are out of scope; all ingestion is manually triggered via the "Load" button.

### Phase 5c: Expression Mapping & Graph-of-Graphs UI -- Implemented

Phase 5b delivers a card-based split-screen mapper with 1:1 pass-through mappings. Phase 5c evolves the mapper into a **three-graph workspace** (source graph, mapping graph, target graph) and introduces a first-class **expression engine** so users can transform values during ingestion, not just rename them.

#### Conceptual model

Every target property is produced by a **mapping function** that takes one or more source properties as inputs and emits a single value. The function body is an expression string in a supported engine (AQL inline expressions for bulk load; KSQL or equivalent streaming SQL for streaming loads). The default function is identity (pass-through of a single source property). Fan-in is supported natively (multiple source columns flowing into one function).

```
source.first_name ─┐
                   ├─► function: CONCAT(@first_name, " ", @last_name) ─► Person.fullName
source.last_name  ─┘
```

#### Visual workspace

Three vertically-aligned graphs, left to right:

- **Source graph (left):** Force-laid-out entity-relationship diagram of the source tables. Tables are rectangular nodes; foreign-key relationships are directed edges between tables. Each table expands on click to reveal its columns with connector ports.
- **Mapping graph (center):** Mapping function nodes rendered as circles on the connector lines. Each function node shows its target-property name; clicking it opens an expression editor.
- **Target graph (right):** Force-laid-out graph model of the ArangoDB target. Vertex collections are nodes; edge collections are labeled, directed edges between vertex nodes. Each vertex collection expands to show its property list with connector ports.

#### Expression engines

| Load path | Engine | Execution | Notes |
| :--- | :--- | :--- | :--- |
| `arangoimport` bulk | **AQL-flavoured expressions, Python evaluator** | Pre-transformed in-memory before writing JSONL (P5c.1.6). An expression outside the evaluator's subset must be evaluated server-side before emission or rejected. | Expression references source columns as `@col_name`. Supports the P5c.1.4 function set, whose semantics are not AQL's (see P5c.1.4). |
| Streaming pipeline | **AQL via transform step** | Python-side evaluation using the P5c.1.4 subset, or delegation to ArangoDB via a per-batch `RETURN` query (P5c.1.5). | Same expression language surface as bulk. The two routes can return different values for the same row (P5c.1.4). |
| Kafka / CDC streaming | **KSQL (or ksqlDB-compatible SQL)** | Applied in the Kafka pipeline before write, supporting time-windowed joins and stateful transforms. | Deferred (P5c.1.7). Until it ships, `ksql` expressions are rejected (P5c.1.4). |

#### Epic 1: Expression-aware data model

| ID | Requirement | Description | Pre-requisite | Status |
| :--- | :--- | :--- | :--- | :--- |
| **P5c.1.1** | **FieldExpression model** | New Pydantic `FieldExpression` type with fields: `target` (str), `sources` (list[str], fan-in), `expression` (str; empty = identity on `sources[0]`), `engine` ("aql" \| "ksql" \| "python"), `description` (str). | -- | Done |
| **P5c.1.2** | **CollectionMapping.field_expressions** | `CollectionMapping` gains an optional `field_expressions: list[FieldExpression]`. When non-empty it takes precedence over the legacy `field_mappings` dict for any target property it owns; otherwise `field_mappings` fallback applies. | P5c.1.1 | Done |
| **P5c.1.3** | **Serialization** | `FieldExpression` round-trips through YAML and JSON using standard Pydantic v2 serializers. Legacy mapping configs (no `field_expressions`) load without change. | P5c.1.2 | Done |
| **P5c.1.4** | **Expression evaluator (AQL subset)** | A Python-side evaluator for an AQL-flavoured expression language: the functions `CONCAT`, `CONCAT_SEPARATOR`, `UPPER`, `LOWER`, `SUBSTRING`, `LENGTH`, `LTRIM`, `RTRIM`, `TRIM`, `TO_STRING`, `TO_NUMBER`, `TO_BOOL`, `CONTAINS`, `COALESCE`, with arithmetic, comparison, boolean logic, ternary `? :` and null-coalescing `??`. **The evaluator's semantics are not AQL's.** Measured against ArangoDB 3.12.9 through r2g's own delegation query, it differs on: `null` in arithmetic (r2g: `null`; AQL: treated as `0`); ordered comparison with `null` (r2g: `null`, falsy; AQL: `null` sorts below every number); `\|\|` and `&&` (r2g: a boolean; AQL: one of the operands); `+` on text (r2g: concatenation; AQL: numeric conversion); comparison across types (`"10" < 9`, `true == 1`); and the truthiness of an empty array. It also accepts `COALESCE` and `??`, which AQL rejects. The local evaluator and AQL delegation (P5c.1.5) can therefore return different values for the same row. Which semantics r2g adopts is an open decision (`docs/research/transformation-language-evaluation.md` §5.2–5.3). `NodeTransformer` applies `field_expressions` per row. An AQL expression the evaluator cannot compile is delegated to ArangoDB on the streaming path (P5c.1.5); bulk and CDC loads must evaluate or reject it (P5c.1.6). An expression whose `engine` has no backend (today `ksql` and `python`) is rejected before any document is written — when a load (`transform-all`, `transform-nodes`, `stream`, CDC) builds its transformers — with an error naming the collection, the target field and the engine; it is never loaded as an identity pass-through. `validate_config` reports the same error, so the UI and `validate-config` catch it before a load. The rejection lasts until the engine has a backend: P5c.1.7 for `ksql`; `python` has no planned backend. `validate_config` compile-checks every AQL expression and flags unresolved `@bindings`. The UI exposes `/api/expressions/functions` (list supported functions) and `/api/expressions/compile` (parse-check) so the modal editor shows live syntax status and the referenced columns. | P5c.1.1 | Partial — evaluator done (`src/r2g/expressions.py`, `tests/test_expressions.py`); rejection of engines without a backend not implemented: `validate_config` checks only `aql` (`src/r2g/config.py:253`) and is not called by any load command, and `NodeTransformer` passes other engines through with a warning (`src/r2g/transformers/node_transformer.py:64`). |
| **P5c.1.5** | **AQL delegation for complex expressions** | For expressions outside the Python evaluator's subset, the streaming pipeline submits a per-batch AQL `FOR doc IN @@batch LET ... RETURN doc` query to ArangoDB and uses the rewritten result as the ingestion payload. | P5c.1.4 | Done for AQL (streaming) — `NodeTransformer` flags delegated targets and `StreamingPipeline._apply_delegation` runs a per-batch AQL rewrite via `build_delegation_query` (see `tests/test_streaming_pipeline.py`). KSQL remains TODO (P5c.1.7); `python` has no planned backend (P5c.1.4). |
| **P5c.1.6** | **arangoimport pre-transformation (and CDC)** | For bulk load, the JSONL generator applies expressions in-memory with the Python evaluator, so `arangoimport` only sees the final document shape. An expression the evaluator cannot compile (one P5c.1.5 delegates on the streaming path) must either be evaluated server-side before emission or rejected with an error naming the collection and target field. A bulk-loaded document must never silently omit an expression's target field. The same rule applies to CDC: an insert or update must evaluate or reject a delegated expression, and an update must never remove a target field that an earlier load set. No changes to `arangoimport` invocation required. | P5c.1.4, P5c.1.5 | Partial — Python-subset expressions are applied by `NodeTransformer.transform_row` before JSONL emission. Delegated targets are left unset (`src/r2g/transformers/node_transformer.py:213`) and no path except streaming calls the server: `transform-all` (`src/r2g/main.py:400`) and `transform-nodes` (`src/r2g/main.py:834`) emit documents without those fields, and CDC (`src/r2g/cdc/delta_transformer.py:171`) builds them the same way, then replaces the whole document (`src/r2g/cdc/conflict.py:184`), erasing a field the initial streaming load had filled. |
| **P5c.1.7** | **KSQL translation layer** | For Kafka/streaming loads, a translator that rewrites the canonical AQL-flavoured expressions into KSQL (ksqlDB-compatible) `SELECT` projections. Initial scope: arithmetic, string concat, CASE. | P5c.1.3 | Not started |

#### Epic 2: Graph-of-Graphs UI

| ID | Requirement | Description | Pre-requisite | Status |
| :--- | :--- | :--- | :--- | :--- |
| **P5c.2.1** | **Source ER-graph visualization** | Left pane renders source tables as graph nodes with FK relationships drawn as directed edges between tables. Click to expand a table inline, exposing its columns with connector ports on the right edge. | P5b.2.1 | Partial (split-screen exists; inter-table FK edges in-pane added) |
| **P5c.2.2** | **Target graph-model visualization** | Right pane renders target vertex collections as graph nodes and edge collections as labeled directed edges between them. Click to expand a vertex to show its properties with connector ports on the left edge. | P5b.2.1 | Partial (split-screen exists; inter-collection edges in-pane added) |
| **P5c.2.3** | **Mapping function nodes** | Each connector line carries a circular function node in the center canvas. The node labels its target property; hovering shows the expression preview; clicking opens an expression editor modal. Default (identity) functions render as small hollow circles; non-identity as filled circles in a distinct colour. | P5c.1.1 | Done |
| **P5c.2.4** | **Fan-in via drag-and-drop** | Users drag a source column connector dot onto an existing function circle to add that column as another input (multi-input fan-in). The function's `sources` list is updated in the mapping config. | P5c.2.3 | Done |
| **P5c.2.5** | **Expression editor** | Modal editor with engine selector, syntax-highlighted textarea, available-sources picker (columns already bound to this function), optional description field, and a live preview button that evaluates the expression against one source row. The engine selector offers only engines with a backend (today AQL); KSQL appears once P5c.1.7 ships (P5c.1.4). | P5c.2.3 | Partial — the UI still offers KSQL and Python (`src/r2g/ui/static/index.html:1219`, `:6218`). Otherwise done (editor modal + engine selector + sources picker + textarea + reset-to-identity; syntax highlighting and live server-side preview implemented via `highlightExpr()` and `POST /api/expressions/preview`; autocomplete / UDFs deferred) |
| **P5c.2.6** | **Graph-of-graphs consistency** | All three graphs (source, mapping, target) stay aligned during scroll, window resize, and layout simulation ticks. Connectors reroute when either source or target nodes move. | P5c.2.1, P5c.2.2, P5c.2.3 | Done |
| **P5c.2.7** | **Expression validation feedback** | When a user closes the editor, the mapping is validated: source references must exist, the expression must parse under the selected engine, and the target property must not conflict with another mapping. Errors shown inline in the properties panel. | P5c.2.5 | Done (inline validation via `POST /api/projects/{name}/validate-draft` on a debounce; per-entity validation badges and a validation lens shared with Phase 5e) |

#### Out of scope (V1 of Phase 5c)

- **User-defined functions (UDFs):** Custom AQL functions registered on the server are not exposed to the expression editor in V1.
- **Expression autocomplete:** The editor is a plain textarea in V1; autocomplete with function signatures is deferred.
- **KSQL:** deferred with P5c.1.7. Until the translation layer ships, `ksql` expressions are rejected (P5c.1.4). Its initial scope is string, numeric, date and CASE; windowed joins and stateful aggregations are deferred beyond that.

### Phase 5d: ArangoDB-backed data catalog -- Planned

The catalog currently persists to `~/.r2g/catalog.json` and mapping configs to YAML files. Phase 5d migrates all catalog state (sources, snapshots, targets, projects, mapping configs, mapping expressions, load history) into an ArangoDB database so the catalog itself is a graph and can be queried, versioned, and shared across users.

| ID | Requirement | Description | Pre-requisite |
| :--- | :--- | :--- | :--- |
| **P5d.1** | **Catalog schema** | ArangoDB collections: `r2g_sources`, `r2g_targets`, `r2g_snapshots`, `r2g_projects`, `r2g_mappings`, `r2g_loads`. Edge collections: `r2g_snapshot_of` (snapshot -> source), `r2g_project_uses` (project -> source/target/snapshot/mapping), `r2g_load_of` (load record -> project). A named graph `r2g_catalog` ties it all together. | P5b.1.1 |
| **P5d.2** | **CatalogManager backend swap** | `CatalogManager` gains a pluggable persistence layer: `FileCatalogBackend` (current behaviour) and `ArangoCatalogBackend` (new). Selectable via `R2G_CATALOG_BACKEND=arango` env var or `r2g catalog use arango --endpoint ... --database r2g_meta`. Identical Python API so the UI and CLI are unchanged. | P5d.1 |
| **P5d.3** | **Catalog initialization** | `r2g catalog init` creates the catalog database, collections, indexes, and graph. Idempotent. Supports a migration path from `~/.r2g/catalog.json`: `r2g catalog migrate --from-file ~/.r2g/catalog.json`. | P5d.2 |
| **P5d.4** | **Catalog introspection UI** | Mapping Studio gains a "Catalog" view (read-only) rendering the catalog graph itself (sources, projects, mappings) using the same graph visualization primitives as the source and target panes. | P5d.2 |
| **P5d.5** | **Multi-user concurrency** | Optimistic concurrency via `_rev`: mapping save compares the cached revision and rejects on mismatch with a merge prompt in the UI. | P5d.2 |

#### Phase 5d non-functional notes

- The catalog database is a separate ArangoDB instance (or database within an instance) from the data target; they must not be conflated.
- **Done (April 2026) — credential encryption at rest.** The filesystem catalog (`~/.r2g/catalog.json`) encrypts every `SourceConfig.connection_string` and `TargetConfig.password` with a Fernet (AES-128-CBC + HMAC-SHA256) symmetric key that is read in order: (1) `R2G_SECRET_KEY` env var, (2) `~/.r2g/secret.key` (auto-generated on first use, 0600). Ciphertexts are tagged with `enc:v1:` so legacy plaintext catalogs continue to open read-only; any write-through re-emits them encrypted. API responses and logs are redacted via `redact_connection_string` / `redact_for_display`. The CLI ships `r2g secrets init|status|migrate` to bootstrap, inspect, and force-upgrade existing catalogs. OS-keychain integration (via `keyring`) remains a future enhancement.
- A lightweight "zero-config" mode continues to use the filesystem backend for single-user local development.

### Phase 5e: Mapping UI architecture upgrade -- Implemented

Phases 5b and 5c delivered the functional split-screen visual mapper and expression nodes. Phase 5e realigns the workspace with the object-centric UI contract (`/.cursor/rules/ui-architecture.mdc`): one persistent stage, context-menu-primary interactions, lenses that only repaint, floating non-blocking work surfaces, first-class edges, legible legend, and inline validation. It is additive on top of 5b / 5c.

#### Product principles (reaffirmed)

- **Single stage, no new routes.** The Mapping Studio (`r2g ui`) is the only workspace; every feature integrates into it.
- **Context-over-navigation.** Every entity (source table, column, target collection, property, function circle, connector, edge, FK line, canvas blank space, sources sidebar item, history row) exposes its actions through right-click context menus; side-panel buttons are secondary paths.
- **Overlay, not replacement.** Detail editors (expression editor, edge editor, progress view, validation and help modals) float over the canvas with drag / minimize / dismiss.
- **Lenses repaint, never relayout.** Topology, Coverage, Validation, and Diff lenses change only colors / badges / opacities on the stable graph.
- **Legible encoding.** A persistent legend documents every node, edge, connector, and function visual encoding per lens.

#### Epic 1: Shared primitives

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5e.1.1** | **Context-menu component** | Vanilla-JS `openContextMenu` / `closeContextMenu` primitive with submenu support, click-outside / Esc dismissal, `role="menu"` / `role="menuitem"` ARIA semantics, and per-entity menu builders. | Done |
| **P5e.1.2** | **Keyboard shortcut registry** | `registerShortcut(combo, description, scope, handler)` with modifier normalization, scoped dispatch (global / canvas / modal), and an introspectable registry that feeds the help overlay. | Done |
| **P5e.1.3** | **Help overlay** | `?` opens a modal listing every registered shortcut and the right-click contract. Toolbar `?` button and shortcut `?` / `Shift+?`. | Done |
| **P5e.1.4** | **Floating card primitive** | `openFloatingCard` / `minimizeFloatingCard` / `restoreFloatingCard` / `closeFloatingCard` with draggable headers, minimized tray, and restore-on-reopen. Used by progress, edge editor, expression editor, diff, validation detail. | Done |

#### Epic 2: Right-click everywhere

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5e.2.1** | **Entity-specific context menus** | Menus bound to source tables, source columns, target collections, target properties, mapping function circles, connectors, target edges, source FK lines, canvas blank space, sources sidebar items, and history rows. Each menu surfaces only the legal operations for that entity (edit, delete, reverse, promote, include / exclude, copy id, etc.). | Done |
| **P5e.2.2** | **View-as submenu on canvas** | Canvas right-click exposes a "View as" submenu that switches lens (Topology, Coverage, Validation, Diff); same action is available via keyboard accelerators `1`–`4`. | Done |

#### Epic 3: First-class edges

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5e.3.1** | **Floating edge editor** | Dedicated floating card to edit an edge's name, direction, from / to collections, from / to fields (including composite keys), and delete. Same surface as node editors. | Done |
| **P5e.3.2** | **Drag-to-add edges** | Shift / Alt drag between two target-vertex cards creates a new edge. Visual feedback during drag; drop target highlights. | Done |
| **P5e.3.3** | **Promote join table** | Right-click a join table offers "Promote to edge" which converts the join-table mapping into an explicit edge definition and removes the vertex mapping. | Done |
| **P5e.3.4** | **Composite keys** | `EdgeDefinition` round-trips both singular (`from_field` / `to_field`) and plural (`from_fields` / `to_fields`) forms; the UI emits plural when any side has more than one column, and the backend normalizes comma-separated strings into lists. | Done |

#### Epic 4: Non-blocking execution

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5e.4.1** | **Floating progress card** | Replaces the canvas-blocking overlay. Live SSE counters stream into a minimizable floating card so the user can keep mapping while loads run. Click a minimized card in the tray to restore. | Done |
| **P5e.4.2** | **Bottom timeline strip** | Collapsible bottom strip listing recent load runs as pills with summary stats; right-click a pill for actions (view report, copy run id, open visualization, re-load). Toggle with shortcut `h`. | Done |

#### Epic 5: Legend and lenses

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5e.5.1** | **Complete legend** | Collapsible legend documenting every node, edge, connector, and mapping-function visual encoding (pass-through vs expression, engine badges, edge-collection arrows, FK lines, dirty / validation badges, active-lens cues). Anchored near the canvas; visible at all times unless explicitly collapsed. | Done |
| **P5e.5.2** | **Lens infrastructure** | `currentLens` state + `applyLens()` paint-only step. Lens-change never reruns force layout. Header status chip (`#lens-chip`) shows the active lens. | Done |
| **P5e.5.3** | **Lens implementations** | Topology (default), Coverage (colors nodes / connectors by mapping completeness), Validation (colors by issue severity and pins tooltip lists), Diff (highlights changed entities vs the saved mapping). | Done |

#### Epic 6: Inline validation

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5e.6.1** | **Silent draft validation endpoint** | `POST /api/projects/{name}/validate-draft` accepts a draft `MappingConfig` (unsaved), returns bucketed issues, and tolerates parse errors (reported as validation issues rather than 500s). | Done |
| **P5e.6.2** | **Debounced revalidation** | Mapping edits mark the state dirty; a debounced request revalidates silently; responses update per-entity badges and tooltip issue lists. No user action required. | Done |
| **P5e.6.3** | **Validation badges** | Per-entity badges rendered on source tables, target collections, edges, and connectors; colored by severity; clicking a badge opens a floating validation detail card. | Done |

#### Epic 7: A11y + hygiene

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5e.7.1** | **ARIA roles** | Context menu uses `role="menu"` and `role="menuitem"`; bottom strip uses `role="region"`; floating-card tray uses `role="complementary"`; key buttons carry `aria-label`s. | Done |
| **P5e.7.2** | **Esc closes topmost overlay** | Global Esc handler pops the top of the overlay stack (context menu, floating card, modal) in order. | Done |
| **P5e.7.3** | **Discoverability copy** | Empty-state copy on the canvas and properties panel points the user at right-click and the `?` help overlay; status strings on the strip describe the right-click contract. | Done |
| **P5e.7.4** | **Configurable hints (menus + buttons)** | A single styled tooltip surface (`#ctx-hint`, `role="tooltip"`, `aria-hidden` toggling, viewport-clamped). Context-menu items carry optional `hint` text rendered by `openContextMenu` (items with a hint show an ⓘ marker; tooltip appears to the right on hover / keyboard focus). Toolbar and panel buttons (New project, project menu, Validate, Save, Auto-Map, Suggest FKs, Diff, Export YAML, History, Settings, Help, Load, New source, New target) carry `data-hint` text shown via a delegated `mouseover` / `focusin` handler (tooltip placed below the button); these `data-hint`s replace the old native `title` tooltips while `aria-label`s are retained for screen readers. A single "Show menu hints" preference (default on, persisted in `localStorage` as `r2g.menuHints`) toggles both menus and buttons from the `?` help overlay — no new route. Menu hints are seeded across every entity menu (canvas, project, source table / column, target collection / property, function circle, connector, edge, FK, sidebar items / panels, target cluster, history rows). | Done |

#### Epic 8: In-UI catalog management

New-user bootstrapping. Before Phase 5e.8, sources / targets / projects could only be created via CLI (`r2g source add`, `r2g project create`) or direct HTTP API calls; the Mapping Studio only listed and selected existing ones. Phase 5e.8 adds in-UI creation surfaces so a fresh user can reach a working mapping without leaving the workspace.

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5e.8.1** | **Source CRUD in UI** | Sources panel header exposes a "+ New source" button opening a floating-card form (name, type, connection string or env-var ref, description). On save: `POST /api/sources` → auto-trigger snapshot. Right-click a source → "Re-introspect", "Copy name", "Remove source…" with a cascade-delete confirmation that lists dependent projects / snapshots / loads. Toasts surface success / failure. | Done |
| **P5e.8.2** | **Target CRUD in UI** | New Targets panel in the left sidebar mirrors the Sources panel. "+ New target" floating-card form (name, endpoint, database, username, password, description). On save: `POST /api/targets` → optionally `POST /api/targets/{name}/introspect`. Right-click a target → "Introspect", "Copy endpoint", "Remove target". | Done |
| **P5e.8.3** | **Project CRUD in UI** | Toolbar gains a "+ New project" button next to the project selector, opening a floating-card form (name, source selector populated from the catalog, Arango endpoint, database, mapping config path with a sensible default). On save: `POST /api/projects` → auto-select. Right-click the project chip → "Edit project…" (rename / change description / target endpoint / database via `PATCH /api/projects/{name}`), "Delete project…" (confirmation dialog, `DELETE /api/projects/{name}`), "Copy project name". | Done (full create / edit / delete in-UI; `PATCH`/`DELETE /api/projects/{name}`) |
| **P5e.8.4** | **Discoverability copy** | Empty states on the Sources panel, Targets panel, and project selector include a direct CTA to the "+ New …" action instead of a dead-end "None configured" label. | Done |

#### Non-functional notes (Phase 5e)

- No new routes were introduced; all interactions are overlays, lenses, or context menus on the existing workspace.
- Lens changes are paint-only; topology changes (new / removed / filtered entities) are the only triggers for relayout.
- The floating-card stack survives page interaction: restore preserves prior coordinates and contents.
- The UI consumes `progress_callback` events from `StreamingPipeline` via SSE; the backend keeps at-least-once semantics unchanged.

### Phase 5f: Naming conventions & rename change-management -- Implemented

Phases 5b/5c/5e let users rename individual collections, properties, and edges by hand. Phase 5f adds two related capabilities: (a) **bulk naming conventions** that re-case every generated name at once, and (b) safe **change-management for renames when the target database has already been loaded**, so renaming a collection, edge, or property reconciles the live graph in place instead of forcing a full drop-and-reload. Reserved ArangoDB system attributes are protected throughout.

#### Conceptual model

A `MappingConfig` carries an optional `naming_convention` describing the desired casing for collections (default PascalCase), properties, and edges (default camelCase). Applying a convention rewrites the *target* names while preserving any names the user set by hand and never touching reserved attributes. Separately, each successful load snapshots the live mapping (`Project.loaded_mapping`); when the user later edits names and saves, R2G diffs the live snapshot against the new mapping and offers an in-place migration.

Renames are reconciled with three primitives:

- **Document-collection rename** → rename the collection in place; affected edges are reloaded from the source so `_from`/`_to` point at the new collection name.
- **Edge-collection rename** → rename the edge collection in place (matched by relationship identity, not by name).
- **Property rename** → an AQL `UNSET`/`MERGE` rewrite on the owning collection.

After any rename the named graph is rebuilt so its edge definitions reference current collection names.

#### Epic 1: Naming conventions

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5f.1.1** | **NamingConvention model** | `NamingConvention` (each of `collections` / `properties` / `edges` is one of `preserve` \| `snake` \| `camel` \| `pascal`) added to `MappingConfig.naming_convention`; round-trips through YAML / JSON. | Done |
| **P5f.1.2** | **Identifier conversion engine** | `r2g.naming.split_identifier` / `convert_identifier` split mixed snake / camel / Pascal / kebab identifiers (including digit and acronym handling) and recase them to the requested style. | Done |
| **P5f.1.3** | **Apply convention** | `apply_naming_convention(config, convention, schema)` materializes the convention into `target_collection`, `field_mappings` targets, `field_expressions` targets, and `edge_collection` names, preserving user-set manual renames and skipping reserved attributes. Exposed as `POST /api/projects/{name}/apply-naming`, which returns the transformed (unsaved) config as an editable draft. | Done |
| **P5f.1.4** | **In-UI action** | Canvas right-click → "Apply naming convention…" opens a floating form (style pickers defaulting to PascalCase collections / camelCase properties and edges); the result loads into the editor as a draft for review before save. | Done |

#### Epic 2: Rename change-management (loaded target)

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5f.2.1** | **Loaded-state tracking** | `Project.loaded_mapping` / `loaded_at` snapshot the mapping that is live in the target; `CatalogManager.set_loaded_mapping` records it after every successful full or selective load (UI and CLI), giving migrations an accurate baseline to diff against. | Done |
| **P5f.2.2** | **Migration plan (smarter diff)** | `diff_mappings` detects document-collection renames by **source-table identity**, edge-collection renames by **relationship identity** (from/to collections + key columns), and property renames, emitting typed `ReloadAction`s with structured `params`. `GET /api/projects/{name}/migration-plan` returns the plan. | Done |
| **P5f.2.3** | **In-place executor** | `SelectiveReloader` renames collections in place, reloads affected edges from the source (rebuilding `_from`/`_to`), renames properties via parameter-bound AQL, and rebuilds the named graph. Idempotent (`has_collection` guards) and refuses to write into `_system`. `POST /api/projects/{name}/migrate` runs the plan (with `dry_run` support) and updates `loaded_mapping` on success. | Done |
| **P5f.2.4** | **Save-time migration prompt** | After a save where names changed and the project has load history, the UI surfaces an overlay summarizing the detected renames and offers "Migrate in place", "Full reload", or "Save mapping only". | Done |

#### Epic 3: System-attribute safety

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5f.3.1** | **Reserved-attribute guards** | Shared `RESERVED_ATTRIBUTES = {_id, _key, _rev, _from, _to}` in `r2g.types`. Naming conventions never remap a source column named like a system attribute and never emit a reserved target name; the diff engine skips any rename touching a reserved name; the executor refuses reserved renames (reported as skipped); and `validate_config` flags any `field_mapping` / `field_expression` target that is reserved. | Done |

#### Latent bug fixes folded into Phase 5f

- **Edge endpoint resolution.** `EdgeTransformer` and the named-graph builder previously built `_from`/`_to` and edge definitions from *source-table* names, which broke whenever a `target_collection` differed (e.g. after a rename). `EdgeTransformer` now accepts resolved `from_name` / `to_name`, and `ConfigManager.graph_edge_definitions` centralizes source→target resolution used by the streaming pipeline, CLI `transform-*` commands, the CDC delta transformer, and the graph rebuild step.

#### Out of scope (V1 of Phase 5f)

- **History rewriting of historical data values** when a property rename collides with existing data of a different shape — the AQL rewrite assumes a straight key rename.
- **Cross-collection moves** (re-pointing an edge to a different vertex collection) beyond the reload-from-source path.

---

### Phase 5g: Post-demo UX refinements -- Implemented

Captured from the first stakeholder demo (June 2026). The studio works but the
layout is heavy: too much permanent chrome, too many always-visible toolbar
buttons, and the relationship between a source element and its mapped target is
not visually obvious. This phase makes the canvas the focus, moves supporting
surfaces on-demand, and adds progressive disclosure so the mapping reads at the
level of detail the user wants. All work stays within the single `/workspace`
shell (no new routes), uses overlay / slide-out panels and context menus, and
preserves graph layout on lens-only changes per the UI architecture rules.

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5g.1** | **Light mode default + theme toggle** | Ship a light theme as the default and a one-click toggle to dark mode. Drive both from the existing CSS custom properties via a `[data-theme]` attribute on `<html>`; persist the choice in `localStorage` (`r2g.theme`) and honor `prefers-color-scheme` on first run. A toggle control lives in the top bar (or the consolidated menu, P5g.2) with an accessible label and keyboard operability. | Done |
| **P5g.2** | **Simplified top bar (action overflow menu)** | The toolbar action row (Validate, Save, Auto-Map, Suggest FKs, Diff, Export YAML, History, Settings, Help, Load) is too dense. Keep only the highest-frequency primary actions inline (e.g. Save, Load, and the active-lens chip) and collapse the rest into an overflow / "Actions" menu (kebab) that reuses the existing context-menu primitive and hint system. Keyboard shortcuts and `aria-label`s are retained. | Done |
| **P5g.3** | **On-demand Sources & Targets** | The left Sources/Targets sidebar is always consuming width. Replace the permanent panel with an on-demand surface: a slide-out drawer (or popover) opened from a toolbar/explorer affordance, dismissible with Esc and click-outside. Source/Target CRUD and right-click actions (P5e.8.1/8.2) move into the drawer unchanged. (Note: the drawer currently defaults **open** on first run rather than collapsed.) | Done (drawer defaults open) |
| **P5g.4** | **On-demand detail (Properties) panel** | The right-hand Properties / collection-detail panel likewise becomes on-demand: it appears as a slide-in or floating overlay when an entity is selected (reusing the floating-card primitive, P5e.1.4) and is otherwise hidden, returning that real estate to the canvas. Minimize / dismiss supported. | Done |
| **P5g.5** | **Bidirectional mapping highlight** | Selecting a source table or column highlights the full mapping path to its target collection / property (connector + opposite-side card/row), and selecting a target collection or property highlights the path back to the source table / attribute. Hover gives a transient highlight; click pins it. Built on the existing `data-table` / `data-col` / `data-collection` / `data-prop` attributes and the `.connector.selected` styling — paint-only, no relayout. | Done |
| **P5g.6** | **Project explorer (IDE-style)** | Replace the awkward project dropdown + kebab menu with an optional left explorer panel listing available projects (grouped by source/target as useful), with the active project highlighted. Selecting opens it on the canvas; right-click exposes the existing project actions (edit / delete / copy name, P5e.8.3). Collapsible so it does not compete with the canvas. May share the slide-out shell with Sources/Targets (P5g.3) as an "explorer" with sections. | Done |
| **P5g.7** | **Progressive disclosure / mapping detail filter** | Control the level of mapping detail shown. Default: source tables and target collections render **collapsed**, showing only table→collection mappings. Expanding a source table reveals its attributes and, when the matching collection is also expanded, the attribute→property connectors; collapsing hides them again. Each source table and each target collection has an independent expand/collapse affordance. Connectors are drawn per visible level (table-conn always; col-conn only when both ends are expanded). Optionally complemented by explicit filters (e.g. show only unmapped, only edges). State persisted per project. Refinements: edge collections are themselves collapsible; a coarse table→edge "spine" keeps relationships visible while an edge is collapsed and is replaced by the detailed attribute→`_from`/`_to` connectors once it is expanded (no duplicate). Expanding a source table opens its whole relationship neighbourhood — its 1:1 document collection, every edge that references it, and the far-endpoint tables (+ their collections) — so the relationship renders entirely at the attribute→property level. Collapsing tidies edges with no remaining open endpoint back to the spine. The coarse spine connects **both** endpoint tables of an edge (deduped for self-edges). (Note: the optional "show only unmapped / only edges" detail filters are not yet built.) | Done (explicit filters pending) |
| **P5g.8** | **Open target database** | A per-project "Open database" action (project context menu + Actions menu) opens that project's ArangoDB target (`<endpoint>/_db/<database>/_admin/aardvark/index.html`) in a new browser tab. Resolves the endpoint/database from the linked target, falling back to the project's inline `arango_endpoint` / `arango_database`. | Done |
| **P5g.9** | **Function-node discoverability** | Make the mapping circles legibly read as editable transformation functions (Adria feedback). Identity (pass-through) circles carry a small italic `\u0192` glyph and a stronger hover/grow + pointer affordance; tooltips state "click to add/edit a transformation"; the legend's "Function nodes" section is titled "click a circle to edit" and its identity swatch shows the `\u0192` mark. | Done |
| **P5g.10** | **Composite / multi-input function nodes** | When several source attributes map to one target property, render a **single** function node with one input connector per source attribute fanning into the circle and one output to the target property, instead of several separate circles. The composite primary key (e.g. `order_id` + `product_id` -> `_key`) is the canonical case. `_key` is **read-only / informational**: the loader always derives it from the primary key joined by `key_separator` (`NodeTransformer._generate_key`), and edge `_from`/`_to` are built from the same join, so a user-defined `_key` expression would be ignored at load and would desync edges. The `_key` node is therefore muted/dashed (default cursor, legible `--text-muted` in both themes), and clicking it (from the node, detail panel, or context menu) shows an explanation rather than the expression editor. Non-`_key` properties remain fully editable, and their editor defaults inputs to all contributing source columns. | Done |
| **P5g.11** | **Generated batch-import viewer** | From project/canvas actions and target collection/edge context menus, render the current validated draft mapping as a secret-safe `arangoimport` artifact without executing commands or writing server files. Support full-bundle and focused table-to-vertex / FK-to-edge views in JSONL (default `./output/*.jsonl`) and CSV-direct (default `./dumps/*.csv`) modes, with an operator-editable shell-quoted artifact directory, runnable graph-creation shell, warnings, copy, and client-side download. Reject unknown mapping identities, unsafe artifact paths, and unsupported LPG previews. The UI must state that normal Studio Load streams over HTTP and does not execute this script. | Done |

#### Sequencing & dependencies

- P5g.1 (theme) and P5g.2 (top-bar overflow) are independent, low-risk, and unblock a cleaner shell first.
- P5g.7 (progressive disclosure) changes the connector-drawing logic (`renderSourcePane` / `renderTargetPane` / `drawConnectors`) and is a prerequisite for P5g.5 (highlight) reading cleanly, since highlight targets must be visible.
- P5g.3 / P5g.4 / P5g.6 share a slide-out/overlay shell; build the shell once and host Sources, Targets, and the project explorer as sections.

#### Out of scope (V1 of Phase 5g)

- A full dockable / rearrangeable panel system (drag panels between zones).
- Multi-project tabs or split canvases.
- Server-side persistence of layout preferences (V1 uses `localStorage`).

---

### Phase 8: External data catalog integration -- 8a–8b implemented; 8c–8d planned

> **Distinct from Phase 5d.** Phase 5d is r2g's *own internal* catalog (its
> sources / projects / mappings, optionally ArangoDB-backed). Phase 8 connects
> r2g to *external enterprise data catalogs* (e.g. OpenMetadata, AWS Glue Data
> Catalog, Atlan) and uses them as an upstream **discovery** layer: browse the
> catalog, pick a database/schema/table (or file collection / Kafka topic) the
> user has access to, and register it as an r2g migration source. r2g still
> connects to the underlying data store itself to perform the migration.

**Motivation.** Enterprises increasingly treat a data catalog as the system of
record for "what data exists and where." Customers asked whether r2g can read
those catalogs so a user can select a database from the catalog instead of
hand-typing connection details. This lowers onboarding friction and slots
r2g into governed data estates.

#### Research summary (2026-06; see `docs/internal/PLAN-external-data-catalogs.md` for the cited landscape)

A cited research pass evaluated the catalog landscape against r2g's
"discover-then-connect" need. Key findings (with confidence):

- **OpenMetadata (OSS)** — *high confidence, recommended first integration.*
  Official type-safe Python SDK over a REST API; a canonical
  `DatabaseService → Database → Schema → Table → Column` hierarchy covering
  exactly r2g's sources (PostgreSQL, MySQL, SQL Server, Snowflake, ClickHouse) plus Kafka
  via messaging services; service entities carry real connection metadata
  (host/port, db name, SSL). Self-hostable via Docker → end-to-end testable.
- **AWS Glue Data Catalog** — *high confidence.* `Connection` objects store
  genuine connection metadata (credentials, URI, VPC), reusable across
  sources/targets. AWS-centric; needs an AWS account (no local e2e).
- **Atlan** — *high confidence on API.* Mature official `pyatlan` SDK,
  API-token auth, 400+ asset types (SQL + object-store categories). Commercial;
  needs a tenant/sandbox.
- **DataHub (OSS)** — *partial.* Pull-based ingestion + entity-aspect model
  confirmed, but several read/discovery API claims were **refuted in
  verification** and must be re-validated before relying on its read API.
- **Collibra** — *medium / likely governance-only.* GraphQL "Knowledge Graph"
  API (BETA); no confirmed exposure of underlying-source *connection* metadata,
  which would limit discover-then-connect.
- **Common metadata model** — every catalog converges on
  `Source/Service → Database/Asset → Schema → Table → Column` plus
  tags/classification and lineage, so a single catalog-agnostic abstraction is
  feasible.

**Important caveats (do not treat as settled):** (1) Market-share / adoption
evidence (Gartner/Forrester position, revenue, customers, GitHub stars) was
**not** established by the research and remains an open question — the ordering
below rests on *API suitability and testability*, not verified penetration.
(2) A recurring constraint is that catalogs **encrypt/mask credentials on API
read**: r2g can read host / db-type / db-name for discovery, but the user must
still supply credentials at connect time. The design therefore reuses r2g's
existing `$ENV_VAR` connection-string convention and `r2g secrets` store rather
than expecting catalogs to hand over secrets.

#### Requirements

| ID | Requirement | Description | Pre-requisite |
| :--- | :--- | :--- | :--- |
| **P8.1** | **Catalog provider abstraction** | A `CatalogProvider` Protocol (mirroring `SourceConnector`): `list_data_sources()`, `list_databases()/list_schemas()/list_tables()`, `search(query)`, and `resolve_source(asset_ref) → ResolvedSource`. A normalized `CatalogAsset` model (provider, fqn/id, kind, name, `source_type`, connection hint without secrets, tags, parent refs). A `create_catalog_provider(provider_type, …)` factory with lazy imports, matching `create_source_connector`. | P6.5 |
| **P8.2** | **Catalog registry in the catalog** | New `CatalogProviderConfig` (provider type, endpoint, auth token) persisted alongside `SourceConfig`/`TargetConfig`, with the auth token encrypted at rest via the existing Fernet layer and redacted in API/log output. CLI: `r2g catalog add/list/remove`. | P8.1, P5d-D4 |
| **P8.3** | **OpenMetadata provider** | First concrete provider (`r2g-arango[openmetadata]`, official SDK): browse the `DatabaseService → Database → Schema → Table` hierarchy + messaging services (Kafka), map a selected asset to a `source_type` + connection hint. | P8.1 |
| **P8.4** | **Discover-then-connect bridge** | `r2g catalog browse <name> [--search …]` lists/searches assets; `r2g catalog import-source <catalog> <asset-fqn> --as <source-name>` resolves the asset to a `SourceConfig` and registers it as a normal r2g source (credentials supplied via `$ENV_VAR` / `r2g secrets`, never assumed from the catalog). Everything downstream (`source snapshot`, mapping, `stream`) is unchanged. | P8.3 |
| **P8.5** | **UI catalog browser** | "Import from catalog" path in the **+ New source** flow: pick a registered catalog → searchable tree (service → database → schema → table) → selection pre-fills the new-source form (host/type/db), leaving credentials to the user. | P8.4 |
| **P8.6** | **MCP catalog tools** | `list_catalogs`, `catalog_browse`, `catalog_import_source` MCP tools so agents can discover and register sources from a catalog. | P8.4 |
| **P8.7** | **AWS Glue provider** | Second provider via `boto3`: list Glue databases/tables and resolve `Connection` objects (which carry real connection metadata). | P8.1 |
| **P8.8** | **Atlan / DataHub provider** | Third provider — Atlan via `pyatlan` (commercial breadth) and/or DataHub (OSS), the latter pending re-validation of its read API. | P8.1 |

#### Phasing

- **8a (foundation + OpenMetadata):** P8.1–P8.4. OSS + dockerizable → unit + end-to-end integration tests in CI.
- **8b (UI + MCP):** P8.5, P8.6 — **implemented**.
- **8c (Glue):** P8.7. Unit-tested with mocked `boto3`; live verification is a field exercise (needs AWS).
- **8d (Atlan / DataHub):** P8.8.
- **Later / out of scope for V1:** *publishing back* to the catalog — registering the resulting ArangoDB graph + column-level lineage as a downstream governed asset. Valuable for governance but a separate write-path effort.

#### Non-functional notes

- **Credentials.** Providers expose host/type/db for discovery; credentials are supplied by the user (env var or `r2g secrets`), consistent with the masked-on-read reality of catalog APIs and r2g's existing security model.
- **Testability gradient.** OpenMetadata (Docker) is end-to-end testable in CI; Glue/Atlan need cloud accounts and are unit-tested with mocked SDKs plus manual field validation — mirroring how Snowflake is handled today.
- **No catalog writes in V1.** Phase 8 is read-only against external catalogs.

---

### Phase 9: Classification propagation & entitlement-aware loading -- COMPLETE (9a + 9b + 9c)

> **Built on the Phase 8 catalog backbone.** The moment r2g copies data out of a
> relational source into ArangoDB it creates a new system of record with *none*
> of the source's access controls. The source enforced who-sees-what via
> GRANTs, row-level security, column masking, and catalog policy; the graph copy
> has none of that by default. Worse, graph denormalization creates *new*
> sensitivity — joining `customers` + `orders` + `payments` into one
> neighborhood can expose a combined picture no single source table revealed
> (the **mosaic effect**). Phase 9 is therefore a governance problem the
> migration itself creates, not a feature bolted on.

**Lane discipline (non-negotiable).** r2g is a *migration tool, not a runtime
authorization engine*. It must not pretend to enforce access at query time.
The defensible posture is three tiers, only the first two of which r2g owns:

1. **Capture & propagate** (squarely r2g's job, cheap given Phase 8). The
   portable unit of entitlement is **not** engine-specific GRANTs (which do not
   map across systems or to graph consumers) — it is the
   **classifications / tags / owners / tiers** that live in the catalog
   (OpenMetadata already holds PII/PHI tags, confidentiality tiers, ownership,
   glossary terms). Pull those during discovery and stamp them onto the target
   graph as collection- and field-level annotations (`_classification`,
   `_sensitivity`, `_source_owner`) plus column-level lineage
   (`source table.column → graph property`). This makes the graph *governable*
   even though r2g does not enforce.
2. **Advise & gate** (also r2g's job). A pre-load **entitlement report** —
   "these 7 mapped fields are classified Restricted/PII; confirm before
   exfiltrating into ArangoDB" — with *exclude-above-a-threshold by default*,
   plus **transform-at-load** for sensitive fields (the existing field
   expression engine makes `HASH(@ssn)` / tokenize / redact a natural fit). The
   right answer to "how do we protect it?" is often *don't copy it in the clear*.
3. **Enable enforcement** (**not** r2g — the graph platform / app). ArangoDB has
   no native row/column security (collection-level RBAC in Enterprise,
   app-level otherwise), so r2g emits the *inputs* for whatever enforces:
   per-collection classification + suggested ArangoDB RBAC, an OPA/Rego policy
   artifact, and/or a **tier-based physical layout** (separate
   collections/databases/graphs per sensitivity tier) so coarse collection-RBAC
   can actually bite. r2g produces metadata + recommendations; the serving layer
   enforces.

**Motivation.** Customers operating in governed data estates need the
relational→graph boundary to preserve — and visibly account for — data
sensitivity. The high-leverage, uniquely-r2g value is carrying governance
metadata across that boundary and *refusing to silently launder sensitive
data*, without overreaching into being an authz engine it cannot be.

#### Data-model reality (sizes the work honestly)

Today `CatalogAsset` carries `tags: list[str]` **only at asset (table/database)
level**, and `ResolvedSource` drops tags entirely — so nothing flows past
discovery. Phase 9's real first task is not "stamp tags"; it is a **data-flow
thread**: column-level classification capture → a carrier through
`resolve_source` → `SourceConfig` → snapshot (`Column`) → mapping
(`CollectionMapping` / `FieldExpression`) → target collection/field annotations
+ lineage. Tiers 2 and 3 hang off that backbone.

#### Requirements

| ID | Requirement | Description | Pre-requisite |
| :--- | :--- | :--- | :--- |
| **P9.1** | **Column-level classification capture** | Extend the catalog provider + `CatalogAsset` to read column-level classifications (tags, glossary terms), owners, and confidentiality tier — not just asset-level tags. OpenMetadata exposes these via `tables?fields=columns,tags,owners` and the classification/glossary APIs. A normalized `Classification` model (tag FQN, source, confidence) keyed by column. | P8.3 |
| **P9.2** | **Classification carrier through the pipeline** | Thread classifications from `resolve_source` → `ResolvedSource` → `SourceConfig` (persisted) → snapshot, annotating `Column` with an optional classification, with a **sensitivity lattice** (e.g. `public < internal < confidential < restricted`) that orders tiers for comparison and rollup. | P9.1 |
| **P9.3** | **Target annotation & column-level lineage** | Stamp classification onto the target: collection- and field-level annotations (`_classification`, `_sensitivity`, `_source_owner`) emitted by the transformers, plus a **lineage manifest** mapping each `source table.column` to its graph property/edge. Stored as governance metadata (sidecar manifest + optional per-collection metadata document), never as silent data loss. | P9.2 |
| **P9.4** | **Mosaic recomputation** | A vertex/edge assembled from multiple source columns/tables takes the **maximum** sensitivity of its contributors (never blindly inherits one column's tier). Recomputed at mapping-build time over fan-in (`FieldExpression.sources`), edge endpoints, and denormalized neighborhoods; surfaced in the report and annotations. | P9.2 |
| **P9.5** | **Pre-load entitlement report + threshold gate** | `r2g entitlements report <project>` (and a UI panel) lists every mapped field at/above a configurable sensitivity threshold with its source lineage; `r2g load` **excludes above-threshold fields by default** and requires explicit confirmation / override to copy them. | P9.3, P9.4 |
| **P9.6** | **Transform-at-load masking** | First-class masking for classified fields via the existing expression engine: `HASH` / `TOKENIZE` / `REDACT` / `NULLIFY` helper expressions and a one-click "mask this field" in the mapper. Masking choice is recorded in the lineage manifest. | P9.3 |
| **P9.7** | **Enforcement artifact (for the serving layer)** | Emit, on load, a **classification manifest** (machine-readable) plus optional artifacts the serving layer can enforce: suggested ArangoDB collection-RBAC grants per tier, an OPA/Rego policy stub, and/or a **tier-based physical layout** recommendation (per-tier collections/databases/graphs). r2g never enforces these itself. | P9.3, P9.4 |
| **P9.8** | **Classification re-sync (staleness)** | A one-time copy snapshots policy at migration time; source policy drift will not propagate. Provide `r2g catalog resync-classifications <project>` to refresh annotations from the catalog, and carry classification *changes* through CDC/temporal mode (policy itself is not carried). Staleness (last-synced timestamp) is surfaced. | P9.3 |
| **P9.9** | **Sensitivity lens + entitlement panel (UI)** | A workspace **classification lens** (paint-only, per the UI architecture contract) coloring collections/fields by sensitivity tier with a legend, and a floating entitlement-report panel feeding the pre-load gate — no new routes, context-menu-primary. | P9.5 |

#### Phasing

- **9a (capture & propagate):** P9.1–P9.4. The data-model backbone, lineage, and
  mosaic rule. Fully unit-testable with the mocked OpenMetadata HTTP seam + an
  OpenMetadata e2e that seeds column tags; no enforcement surface yet.
  **Implemented (June 2026):** the `Classification` carrier (P9.1–P9.2) —
  OpenMetadata column-tag/owner/tier capture, `ResolvedSource` →
  `SourceConfig.classifications` persistence → `Column.classification` merge at
  `source snapshot` — plus the sensitivity lattice + mosaic recompute
  (`src/r2g/classification.py`, the P9.4 max-sensitivity rule over fan-in,
  collection, and edge entities). Target-document governance stamping + the
  on-disk lineage manifest (the rest of P9.3) are folded into 9b/9c, where they
  are consumed by the gate and the emitted artifacts.
- **9b (advise & gate):** P9.5, P9.6, P9.9. The entitlement report, threshold
  gate, transform-at-load masking, and the sensitivity lens.
  **Implemented (June 2026):** `src/r2g/governance.py` (entitlement
  report over mapping + annotated schema, the default-exclude threshold gate, and
  the `governance/lineage.json` manifest) and `src/r2g/masking.py`
  (`hash`/`tokenize`/`redact`/`nullify` field-expression helpers, sentinel-tagged
  so the gate treats masked fields as safe). Surfaced via `r2g entitlements
  report <project> [--threshold] [--json]`, `GET /api/projects/{name}/entitlements`,
  and the load gate on `POST /api/projects/{name}/load` (`allow_sensitive`,
  `sensitivity_threshold`; excluded fields reported, lineage written). The Studio
  ships the full UI: an **entitlement-report panel** (Actions menu / canvas context
  menu / `g` shortcut) listing above-threshold fields with lineage; a paint-only
  **sensitivity lens** (View as → Sensitivity / press `5`) that tints source columns
  and target properties by mosaic-recomputed tier (source-column color = the highest
  level it feeds), with a legend; and a one-click **"mask this field"** target-property
  context action that writes a masking `FieldExpression` so the field clears the gate.
- **9c (enable enforcement):** P9.7, P9.8. The classification manifest + RBAC/OPA
  /tier-layout artifacts and classification re-sync.
  **Implemented (June 2026):** `src/r2g/governance.py` adds `classification_manifest`
  (canonical per-collection/edge/field classification + lineage + owners + sync
  timestamp), `suggested_rbac` (per-clearance cumulative ArangoDB collection
  read-grants), `policy_rego` (default-deny OPA/Rego stub keyed on collection level
  vs. principal clearance; no `opa` dependency), and `tier_layout_recommendation`
  (collections grouped by tier with a suggested database/prefix). All keyed by
  **target** (ArangoDB) collection names. `write_governance_artifacts` emits them
  under `<project>/governance/`. Surfaced via `r2g entitlements emit` (`--threshold`,
  `--out`, `--tier-layout`, `--no-rego`), `POST /api/projects/{name}/governance/emit`,
  and `emit_governance`/`tier_layout` on the load endpoint. `r2g catalog
  resync-classifications` re-pulls from the bound catalog (provenance stored on
  `SourceConfig`: `catalog_name` / `catalog_asset_fqn` / `classifications_synced_at`),
  re-merges onto the latest snapshot, and reports lattice-level drift via
  `diff_classifications` (escalations called out). The CDC/temporal path carries
  the policy onto changed rows: `cdc-start` / `kafka-start` gain `--govern`
  (+ `--allow-sensitive`, `--sensitivity-threshold`), which applies the same
  sensitivity gate to the mapping so subsequent row changes do not launder newly
  sensitive columns. **Out of scope (by design, lane discipline):** retroactively
  re-gating *already-loaded* data when a tier rises — that is a serving-layer /
  backfill concern; r2g advises (escalation report) and gates *future* writes.
  The gated OpenMetadata classification e2e test ships (skip-when-unavailable).

#### Non-functional notes

- **Lane discipline.** r2g emits governance *metadata and recommendations*; it is
  never in the query-time authorization path. Documentation and artifact names
  must make this boundary explicit.
- **Mosaic = max.** Combined-entity sensitivity is recomputed (max of
  contributors), not inherited from a single column — a first-class rule, not a
  footnote.
- **Identity is catalog-anchored, not GRANT-anchored.** Source DB roles ≠
  ArangoDB users ≠ end-user SSO identities; entitlements are expressed against
  the catalog's identity-agnostic tag/tier/owner layer (ultimately org IdP
  groups), which is exactly why GRANTs are the wrong carrier.
- **Safe by default.** Above-threshold fields are excluded unless explicitly
  confirmed; the migration refuses to silently launder sensitive data.
- **Reuse, don't reinvent.** Token/owner metadata rides the existing Fernet +
  redaction layer; masking rides the existing `FieldExpression` engine; the lens
  rides the existing paint-only lens infrastructure (Phase 5e).
- **Out of scope (V1).** Runtime enforcement; pulling engine GRANTs/RLS; mapping
  catalog policies to ArangoDB users automatically; writing classifications
  *back* to the catalog (a Phase 8 write-path concern).

---

### Phase 10: Ontology derivation -- 10a + 10b + 10c + RSA engine (Stage 1) implemented

> **The LLM proposes; the deterministic pipeline disposes.** Today a target
> graph is derived mechanically by `ConfigManager.generate_default_config`
> (tables → document collections, join tables → edges, FKs → edges) and refined
> by hand in the mapper. Phase 10 adds an *optional* path where a large language
> model analyzes the introspected schema and **proposes** a richer target
> ontology — which tables are really vertices vs. edges, implicit/undeclared
> relationships, denormalization/embedding opportunities, and clearer
> collection/property names. Crucially, the LLM never writes to the graph: its
> output is a candidate `MappingConfig` that flows through the **same**
> `validate_config` → mapper review → loader path as every other mapping. The
> deterministic Auto-Map remains the default and the safety net.

**Deterministic engine via `relational-schema-analyzer` (Stage 1, implemented).**
Alongside the LLM engine, `r2g ontology suggest --engine rsa` (UI: **Engine**
selector) derives a conceptual model **deterministically and offline** using the
shared [`relational-schema-analyzer`](https://github.com/ArthurKeen/relational-schema-analyzer)
library — the introspection core that was extracted *from* r2g plus a
conceptual/OWL layer. RSA's `PhysicalSchema`/`Table`/`Column`/`ForeignKey` are a
field-compatible superset of r2g's `Schema`, so an r2g snapshot round-trips into
the analyzer verbatim. Its tool-contract bundle
(`{conceptualSchema, physicalMapping, metadata}`) is converted (in
`src/r2g/rsa_ontology.py`) into an `OntologyProposal` — semantic (PascalCase)
collection names, authoritative join-table detection, foreign-key relationships,
and provenance (confidence, detected patterns, fingerprint, review flags) — and
flows through the **same** `proposal_to_mapping` hallucination gate as the LLM
path, guaranteeing a schema-valid, loadable `MappingConfig`. It needs no rows and
no network by default; `--refine` additively LLM-improves the deterministic
model. Many-to-many join tables are surfaced as advisory review notes (loaded as a
vertex + FK edges), consistent with embed hints. Installed via the
`r2g-arango[ontology]` extra. **Stage 2** (dependency reversal) is now **complete
(descoped)**: the drift-prone shared-semantics core is unified with RSA while the
introspection connectors stay in r2g by design. r2g's `Schema`/`Table`/`Column`/
`ForeignKey` subclass RSA's types with legacy-preserving serializers (byte-stable
snapshots, no migration — `Column` keeps Phase-9 `classification` as a first-class
field); the FK-inference heuristic engine and the FK/denorm value samplers are
imported from RSA (r2g keeps thin subclasses for `to_edge_definition` and
`sample_values`); and the `SourceSession` protocol plus the source-type helpers are
re-exported from RSA. RSA is a **core dependency**. The introspection connectors and
bulk-read sessions remain local because RSA's have diverged (enum sampling,
provenance, `ordinal`/`is_unique`) and drive r2g's data-migration path, and the
connector sets differ (r2g has Arango/Kafka, RSA has DuckDB/Databricks). A live-DB
introspection parity audit (`tests/integration/test_rsa_introspection_parity.py`)
over an edge-case corpus (Postgres northwind/chinook/pagila, MySQL/SQL-Server shop,
CSV) confirms r2g and RSA agree exactly on every shared base table (0 column/PK/FK
diffs, including pagila's ENUM/domain/array/`tsvector`/composite-PK/partition cases);
the only divergence is that RSA's Postgres introspector also returns views + a
materialized view that r2g omits, so reuse remains a wrapper-plus-view-filter effort
rather than a semantic reconciliation. Full history and rationale are in
[`docs/internal/PLAN-rsa-dependency-reversal.md`](internal/PLAN-rsa-dependency-reversal.md)
and [`docs/internal/DESIGN-rsa-compat-layer.md`](internal/DESIGN-rsa-compat-layer.md).

**Motivation.** `generate_default_config` is correct but naive: it mirrors the
relational structure 1:1 and cannot recognise that, say, an `order_items` table
is better modelled as an edge with properties, that two tables form an
inheritance hierarchy, or that a lookup table should be embedded rather than
linked. A domain-aware model can propose a graph that a human would otherwise
hand-craft, collapsing the modeling effort from hours to a review. Customers
have asked for exactly this "describe my domain, suggest the graph" capability.

**Design principles (non-negotiable).**
- **Human-in-the-loop, never auto-applied.** A proposal is rendered as a *diff*
  against the current (or Auto-Map) mapping; the user accepts / edits / rejects
  per item. Nothing is saved or loaded without explicit confirmation.
- **Schema-grounded, not data-dumping.** The model is fed the *introspected
  metadata* (tables, columns, types, PKs, FK-inference results) — never bulk row
  data. Optional value sampling is opt-in and **classification-aware**: columns
  tagged Restricted/PII by Phase 9 are never sent to an external model.
- **Validated, hallucination-resistant.** Every proposed collection/edge/field
  is checked against the real schema (`validate_config`); references to
  non-existent tables/columns are dropped or flagged, never silently loaded.
- **Reproducible & auditable.** The model, prompt, parameters (temperature 0 by
  default), and raw response are stored as provenance on the project.
- **Provider-agnostic & optional.** An `LLMProvider` abstraction (mirroring
  `CatalogProvider` / `SourceConnector`) with lazy imports; the whole feature is
  an optional extra and absent-by-default — r2g never requires an LLM.

#### Requirements

| ID | Requirement | Description | Pre-requisite |
| :--- | :--- | :--- | :--- |
| **P10.1** | **LLM provider abstraction** | An `LLMProvider` Protocol + `create_llm_provider(provider_type, …)` factory with lazy imports (OpenAI first; Anthropic / local/OpenAI-compatible endpoints to follow), mirroring `create_source_connector` / `create_catalog_provider`. API keys supplied via env / `r2g secrets` (`$ENV_VAR` convention), never persisted in plaintext. | P6.5 |
| **P10.2** | **Schema-grounded prompt builder** | Serialize the introspected `Schema` (tables, columns, types, nullability, PKs) + accepted/inferred FKs into a compact, model-friendly description, with an optional user **domain hint**. Redaction-aware: excludes (or masks) columns classified Restricted/PII per Phase 9; optional, opt-in value samples are bounded and classification-filtered. | P10.1, P9.2 (soft) |
| **P10.3** | **Ontology proposal → `MappingConfig`** | The model returns a **structured** ontology proposal (JSON-schema-constrained): vertex vs. edge designation, new/implicit relationships, embed-vs-link recommendations, and name suggestions. r2g maps this to a candidate `MappingConfig`, then runs `validate_config`; invalid items are repaired or dropped with reasons. | P10.2 |
| **P10.4** | **Human-in-the-loop review (diff)** | The proposal is presented as a `diff_mappings`-style diff against the current/Auto-Map mapping; users accept / modify / reject per collection / edge / property. No write occurs without confirmation. | P10.3 |
| **P10.5** | **CLI + API entry points** | `r2g ontology suggest <project> [--domain "…"] [--sample] [--apply]` and `POST /api/projects/{name}/suggest-ontology`; returns the proposal (+ validation notes), applying only on explicit `--apply` / UI accept. | P10.3 |
| **P10.6** | **Provenance & reproducibility** | Persist the provider, model, prompt, parameters, and raw response on the project (and surface cost/latency). Default `temperature=0` for repeatable proposals. | P10.3 |
| **P10.7** | **UI: "Suggest model (AI)" action + review panel** | A canvas/Actions entry that runs a suggestion and opens a floating review panel showing the proposed diff with per-item accept/reject — context-menu-primary, no new route, consistent with the workspace contract. | P10.4, P10.5 |
| **P10.8** | **Guardrails** | Privacy (no egress of classified columns; explicit opt-in for any value sampling), determinism, the `validate_config` gate, prompt-injection hardening of schema-derived text, and cost/rate limits with a hard token budget. | P10.2, P10.3 |

#### Phasing

- **10a (grounded proposal, structure-only) — IMPLEMENTED (June 2026):**
  P10.1–P10.3, P10.5 (CLI) + P10.6, P10.8. New `src/r2g/llm/` package: an
  `LLMProvider` Protocol + lazy-importing `create_llm_provider` factory
  (`base.py`), a REST-over-`httpx` `OpenAIProvider` in JSON-object/`temperature=0`
  mode (`openai_provider.py`), a metadata-only, classification-redacted,
  injection-hardened, token-budgeted prompt builder (`prompt.py`), and
  `proposal_to_mapping` (`ontology.py`) — the hallucination gate that starts from
  the Auto-Map baseline, applies each proposed collection/edge/rename behind a
  guard mirroring `validate_config`, drops anything referencing a non-existent
  table/column (recorded in notes), and guarantees the result always passes
  `validate_config` (worst case: Auto-Map-equivalent). Embed hints are advisory
  notes only (no mechanical apply in V1). CLI `r2g ontology suggest <project>
  [--domain] [--provider] [--model] [--api-key] [--apply] [--json]` prints the
  proposal + diff vs the current mapping + validation/provenance notes, applies
  only on `--apply` (with confirmation), and writes a provenance sidecar. Shipped
  as the optional `r2g-arango[llm]` extra. Fully unit-tested with a **fake
  `LLMProvider`** seam (no network): `tests/test_llm_base.py`,
  `tests/test_llm_prompt.py`, `tests/test_ontology_proposal.py`,
  `tests/test_cli_ontology.py`.
- **10b (review & apply in the Studio) — IMPLEMENTED (July 2026):** P10.4, P10.7.
  Two endpoints on the UI server: `POST /api/projects/{name}/suggest-ontology`
  (read-only — returns the structured proposal, the resulting validated candidate
  mapping, a `diff_mappings` diff vs the current mapping, and validation/provenance
  notes) and `POST /api/projects/{name}/apply-ontology` (rebuilds an accepted,
  possibly-subset proposal through the same `proposal_to_mapping` gate and returns
  an **editable draft** — nothing is persisted server-side, exactly like
  `apply-naming`). In the Studio, a "Suggest model (AI)" entry (Actions menu +
  canvas context menu + `m` shortcut) opens a floating review panel that lists each
  proposed relationship / collection change / property rename with its rationale
  and confidence and a **per-item checkbox**; "Apply selected" sends only the
  checked items, loads the returned draft into the mapper edit state, and marks the
  project dirty so the user Saves through the normal path (which offers migration
  when the target is already loaded). Context-menu-primary, overlay panel, no new
  route — consistent with the UI architecture contract. Tested in
  `tests/test_ui_api.py::TestSuggestOntologyApi` (fake provider, no network).
- **10c (enrichment) — IMPLEMENTED (July 2026):** P10.1 (breadth), P10.2
  (sampling). **Additional providers behind the same factory:** an Anthropic
  (Claude) provider (`anthropic_provider.py`, Messages API, JSON parsed from the
  response with code-fence stripping) and an **OpenAI-compatible / local**
  provider (Ollama, vLLM, LM Studio, llama.cpp, …) served by `OpenAIProvider`
  with `require_key=False` + a required `base_url` (the Authorization header is
  omitted when no key is set). The factory aliases (`claude`, `local`, `ollama`,
  `vllm`, …) resolve to these; keys read from the provider's env var
  (`$ANTHROPIC_API_KEY` / `$OPENAI_API_KEY`), never persisted. **Opt-in,
  classification-filtered value sampling:** `build_schema_digest` now accepts a
  `table → column → values` sample map and renders a few bounded, injection-
  neutralized example values per column — but **only** for columns below the
  redaction threshold, so Restricted/PII columns are still name-only and are
  never sampled. `r2g.llm.sampling.collect_samples` gathers samples via a new
  bounded `sample_values` probe on the Postgres/MySQL/SQL-Server/CSV value
  samplers (best-effort; skips redacted columns entirely). Threaded through the
  CLI (`--provider`, `--model`, `--api-key`, `--base-url`, `--sample`,
  `--samples-per-column`) and the `suggest-ontology` API / Studio dialog
  (provider select, base URL, sample toggle); provenance records whether/how many
  columns were sampled. A **gated live smoke test** (`tests/test_llm_live.py`,
  run only when `R2G_LLM_LIVE=1`) exercises the real round-trip and asserts a
  valid `MappingConfig`. New unit tests: `tests/test_llm_providers.py`,
  `tests/test_llm_sampling.py`, plus sampling cases in `tests/test_llm_prompt.py`
  and `tests/test_cli_ontology.py`.
  **Deterministic grounding wired in (P11.10):** an opt-in `--ground` / `ground`
  path (`r2g.llm.grounding.build_grounding`) runs the Phase 11 analyzer and feeds
  its findings to the model as advisory evidence in a fence-neutralized prompt
  block — structural detectors always run, FD detectors reuse the `--sample`
  sampler when present, and the classification gate carries over (Restricted/PII
  columns are never value-sampled while grounding). Threaded through CLI, the
  `suggest-ontology` API, and the Studio dialog; provenance records `grounded`.
  Tested in `tests/test_llm_grounding.py`. (Mechanical embed-vs-link application
  remains advisory — the deterministic `MappingConfig` shape has no document-
  embedding representation yet, so embed hints stay review notes.)

#### Non-functional notes

- **Determinism & honesty.** Proposals are suggestions, scored and explained;
  the deterministic Auto-Map is always available and is the default. r2g states
  clearly when output is model-generated.
- **Privacy / governance tie-in.** Phase 10 explicitly respects Phase 9
  classifications: sensitive columns are withheld from (external) models — the
  migration must not leak via the *modeling* step either.
- **Optional & provider-agnostic.** Shipped as an extra (e.g.
  `r2g-arango[llm]`); no LLM dependency or network call unless the user invokes a
  suggestion.
- **Out of scope (V1).** Autonomous mapping without review; fine-tuning;
  LLM-driven data *transformation* at load time (expressions remain
  user-authored); natural-language querying of the resulting graph.

---

### Phase 11: Denormalization & normal-form analysis -- 11a–11c implemented (advisory; mechanical auto-apply deferred)

> **Deterministic, no LLM.** Phase 10 asks a model to *propose* a better
> ontology; Phase 11 is the complementary **rules + sampling** engine that
> *detects* denormalization in the source and advises on it — reusing exactly
> the machinery already proven in `fk_inference.py` (name heuristics + bounded
> value-overlap sampling via a pluggable sampler). It recognises patterns the
> 1:1 `generate_default_config` is blind to: embedded lookups, repeating column
> groups, multi-valued columns, and over-split 1:1 tables — and surfaces them as
> scored, evidence-backed *findings* the user can act on. It advises; it never
> silently rewrites the schema or the data.

**Motivation.** Real relational schemas are rarely clean 3NF. A table often
carries an *embedded lookup* (`zip → city, state`; `product_id → product_name,
category`), **repeating groups** (`phone1/phone2/phone3`), **multi-valued
columns** (`tags = "a,b,c"`), or redundant reference data duplicated across many
rows. Mapped 1:1, these become awkward graph models (redundant properties, no
shared vertices, list-in-a-string). Detecting them lets r2g recommend a better
target — extract a vertex, embed an array, split a multi-valued column — and also
produces high-quality, deterministic *grounding* for the Phase 10 LLM proposal.

**Design principles.**
- **Statistical signal, not proof.** Findings carry a confidence + concrete
  evidence (sampled counts/examples), exactly like FK inference; the user
  decides. Bounded, resilient sampling (sampler errors degrade to name/structure
  signals, never crash).
- **Advise, don't auto-rewrite.** V1 emits findings + a recommended action;
  applying a remediation (scaffolding a collection/edge or an embed expression)
  is explicit and optional.
- **Classification-aware (Phase 9 tie-in).** Value sampling reads data, so
  Restricted/PII columns are excluded from sampling — consistent with the
  governance lane.
- **Reuse, don't reinvent.** Same `create_value_sampler` seam, bounded-`LIMIT`
  queries, and Suggest-FKs-style review card as P6.6.

#### Requirements

| ID | Requirement | Description | Pre-requisite |
| :--- | :--- | :--- | :--- |
| **P11.1** | **Denormalization analyzer engine** | A deterministic `analyze_denormalization(schema, options, sampler=None) -> list[DenormFinding]` (new `src/r2g/denorm.py`), mirroring `infer_foreign_keys`: a pure-Python structural/name core plus an optional, bounded value sampler. `DenormFinding` carries kind, table, columns, recommended action, confidence, and evidence. | P6.6 |
| **P11.2** | **Repeating-group detection** | Recognise numbered/suffixed column families (`phone1/phone2`, `addr_line_1..3`) and repeated typed sets → recommend a child collection or an embedded array. Name + type structural heuristic; no sampling required. | P11.1 |
| **P11.3** | **Functional-/transitive-dependency detection (2NF/3NF)** | The flagship: detect a non-key column that *determines* other non-key columns (embedded lookup) by sampling — group by the candidate determinant and test whether dependents are single-valued per group. Recommend extracting the determinant+dependents into a shared vertex linked by an edge. | P11.1 |
| **P11.4** | **Redundant reference-data detection** | Flag sets of co-varying columns whose distinct combinations are small relative to row count (duplicated reference data) → candidate lookup/vertex extraction. Sampling-based, with distinct-ratio evidence. | P11.1 |
| **P11.5** | **Multi-valued attribute detection** | Detect delimited lists in a text column (`"a,b,c"`) via sampling for consistent delimiters → recommend splitting into an array or child collection. | P11.1 |
| **P11.6** | **1:1 over-normalization detection** | Detect two tables in strict 1:1 on a shared/identical key → recommend merging/embedding rather than two collections + an edge (the inverse modeling smell). | P11.1 |
| **P11.7** | **CLI + API entry points** | `r2g source analyze-denorm <name> [--sample] [--sample-limit N] [--min-confidence 0.4]` and `POST /api/sources/{name}/analyze-denorm` → scored findings with evidence and recommended actions (Rich table / JSON). Read-only by default. | P11.1 |
| **P11.8** | **UI findings panel** | A Suggest-FKs-style floating card listing findings with confidence pill + evidence + recommended action and per-finding Accept / Dismiss, opened from the Actions / canvas context menu — no new route, context-menu-primary. | P11.7 |
| **P11.9** | **Remediation scaffolding (opt-in)** | Accepting a finding can scaffold the mapping: e.g. create the extracted collection + `EdgeDefinition`, add an embed/split `FieldExpression`, or merge a 1:1 pair — written through the normal mapping save + `validate_config`, never applied silently. | P11.8 |
| **P11.10** | **Grounding for Phase 10** | Findings are emitted in a form the Phase 10 prompt builder can consume, so the LLM proposal is grounded in deterministic evidence (and the two can cross-check). Phase 11 itself requires no LLM. | P11.1 |

#### Phasing

- **11a (engine + CLI/API, the deterministic core):** P11.1–P11.3, P11.7 —
  **implemented**. Structural repeating-group detector + the functional-dependency
  sampler (`distinct_ratio` + `group_single_valued` bounded probes on all four
  value samplers) + both the `r2g source analyze-denorm` command and the
  `POST /api/sources/{name}/analyze-denorm` endpoint, fully unit-tested with a
  fake sampler (canned counts), exactly like `test_fk_inference.py`.
- **11b (more detectors + Studio review):** P11.4–P11.6, P11.8 — **implemented**.
  Multi-valued (delimiter-rate probe), redundant-reference (low distinct-ratio,
  overlap-suppressed against embedded lookups), and structural 1:1 over-norm
  (PK == FK) detectors, plus a read-only Studio findings card (Actions menu /
  canvas context menu / `n` shortcut) cloned from Suggest-FKs.
- **11c (remediation + grounding):** P11.9, P11.10 — **implemented (advisory)**.
  Per-finding remediation guidance (`remediation_hint`) is surfaced in the CLI,
  the API (`hint` field via `with_hints`), and the Studio card; the Phase-10
  grounding digest (`summarize_findings_for_prompt`) is ready for the ontology
  prompt builder. **Mechanical auto-apply is deferred**: extracting a vertex from
  a column subset or splitting a column into an array is not representable in the
  current source-table-bound `MappingConfig` + AQL expression engine (no
  synthetic collections, no `SPLIT`/array literals), and silently emitting an
  invalid mapping would violate `validate_config`. Auto-apply will follow a
  mapping/expression-model extension (tracked as a Phase 11 follow-up), keeping
  faith with the phase principle: **advise, never silently rewrite**.

#### Non-functional notes

- **Bounded & resilient.** All sampling uses bounded `LIMIT` queries and degrades
  to name/structure signals on error — the analyzer never blocks or crashes a
  workflow (same contract as `PostgresValueSampler`).
- **Advisory by default.** Findings change nothing until a remediation is
  explicitly accepted and validated.
- **Privacy.** Sampling honours Phase 9 classifications; Restricted/PII columns
  are not sampled.
- **Complements, not replaces.** The deterministic 1:1 Auto-Map remains the
  default starting point; Phase 11 layers advice on top and grounds Phase 10.
- **Out of scope (V1).** Automatic schema rewriting; full functional-dependency
  *mining* (we test *candidate* determinants from keys/heuristics, not all
  column subsets); BCNF/4NF formalism; cross-table deduplication of entities
  (entity resolution).

---

### Phase 12: Federated query & runtime mappings (Contextual Data Fabric) -- P12.1 shipped; relational leg = Ontop (adopted, Apache-2.0 OSS)

Serves the **Contextual Data Fabric** project (fabric modules M4 Mapping Layer and M5 Federated Query Engine; fabric spec `contextual-data-fabric/docs/architecture/_repo-enhancements/r2g-federated-query.md`). The customer constraint is *do not move the data*: instead of running the mapping once to hydrate ArangoDB, r2g must expose the mapping as a runtime artifact and compile it — plus a predicate — into a **pushdown query** executed at the source, per question. Batch/CDC loading remains available; it stops being the only consumption mode.

**Reframed by the fabric's conceptual-query decision (contextual-data-fabric ADR-0001, validated by a 75-finding research pass).** The fabric expresses a question against the ontology in a typed graph-pattern IR (serializing to SPARQL) — the Ontology-Based Data Access / Virtual Knowledge Graph pattern — then hands r2g the relational **partition** (resolved concepts + predicate) to compile. So the durable contract r2g owes the fabric is the **mapping as a versioned, R2RML-exportable runtime artifact (P12.1)**. Whether r2g *generates* the pushdown SQL itself (P12.2) or the fabric runs a VKG engine (**Ontop**) over r2g's R2RML mappings was an explicit **adopt-OSS-vs-build decision in ADR-0001 — resolved (July 2026): adopt Ontop.** Ontop is **Apache-2.0 open source** (no purchase, no license cost — the only cost of adopting is operating the service) and already rewrites SPARQL→SQL with no materialization across every relational source r2g supports (PostgreSQL, MySQL, SQL Server, Snowflake), so **native generation (P12.2) is retired as the target** — kept only as a documented fallback for deployments that decline to run Ontop. P12.1 is the load-bearing deliverable either way, and **is now shipped** (`src/r2g/csi.py`, `src/r2g/r2rml.py`; `export-csi` / `export-r2rml` CLI). The fabric's Arango leg and conceptual-query IR are owned by sibling repos (`arango-sparql-py` / `arango-cypher-py`, built on `arangodb-schema-analyzer` — RSA's Arango counterpart), so r2g Phase 12 stays scoped to the **relational / SQL side**.

**What already exists (why this is a bounded step, not a rewrite):** mappings are produced independently of any load (`ingest-schema → schema.json → generate-config → mapping.yaml`); the field-expression engine (Phase 5c, reused by 9b masking) expresses value transforms source-agnostically; source connectors + consistent-snapshot sessions cover PostgreSQL, MySQL/MariaDB, SQL Server, Snowflake, and ClickHouse; RSA supplies the conceptual↔physical bundle. The genuinely new capability is **query generation**: mapping + concept + predicate → parameterized source-dialect SQL.

#### Requirements

| ID | Requirement | Notes |
| :--- | :--- | :--- |
| **P12.1 ✅ shipped** | **Runtime mapping export (the durable contract).** Expose the existing mapping (concept→table, property→column, field expressions) as a versioned, load-independent artifact — **forward `CSI v1` + R2RML** (ADR-0001). `CSI v1` is the cross-tool interchange `arangodb-schema-analyzer` already defines and **names r2g as the forward producer of**; r2g is uniquely able to emit it because it knows *both* the source tables and the ArangoDB collections it created. R2RML is what VKG engines (Ontop) consume for the SQL leg. Include a content hash so consumers can cite the mapping version used for a query. | An R2RML serializer + a `to_csi(direction="forward")` emitter over `generate-config` output; no loader dependency. This is what the fabric depends on regardless of the P12.2 adopt-vs-build outcome, and it feeds both the SQL-side (R2RML→Ontop) and AQL-side (CSI→MappingBundle→arango-sparql-py/arango-cypher-py) transpilers. **Shipped (July 2026):** `src/r2g/r2rml.py` (`mapping_to_r2rml` + `export-r2rml`) and `src/r2g/csi.py` (`mapping_to_csi` + `export-csi`), load-independent; verified end-to-end against a live Ontop + Postgres stack. **Conceptual label uniqueness (August 2026):** within one emitted CSI document no attribute label may be carried by two entities. A CSI document scopes properties per entity, but a consumer deriving a flat vocabulary from the labels cannot recover that scoping, and the distinction is unrecoverable downstream because it is discarded at extraction time — the only point with the whole document in view. Collisions are resolved under a selectable policy (`export-csi --label-policy`): **`qualify`** (default) renames *every* occurrence to `<entity><Label>`, since the result must not depend on mapping insertion order and leaving one entity holding the bare label preserves exactly the false confidence being fixed; **`roles`** first classifies the collision by the structural role of its column, because a collision on a business attribute and one on a key are not the same defect — a plain column is qualified, a **primary key** is emitted as identity (already carried by `_key`) rather than as N renamed attributes, and a **foreign key** leaves the bare label with the entity that *owns* the key and qualifies only the referencing entities, avoiding names like `accountAccountId` that read as a business attribute when the thing described is a join; **`warn`** records without renaming and **`off`** skips the check. A qualification that would itself collide is refused rather than traded for a fresh ambiguity. Every collision is recorded in `provenance.labelCollisions` with its resolution (`qualified` \| `identity` \| `reported` \| `unresolved`), so the fact that two entities meant the same word is never silently discarded. Renames are conceptual only — `arangoPhysicalMapping` continues to resolve each property to its stored field. **Implementation note:** the structural role must be read from the **source column**, not from the physical mapping `field` — `field_mappings` makes the two diverge, and classifying off `field` silently degrades every renamed key to a plain attribute. **Out of scope:** labels colliding *across* documents from different extractors; no single extraction pass can observe those. |
| P12.2 (fallback — not the target) | **Pushdown query generation.** Given a mapping + a relational query-graph **partition** (resolved concept/properties + predicate) from the fabric's conceptual-query IR (ADR-0001), generate a **parameterized SQL query** in the source dialect (PostgreSQL first) that returns only the requested rows/columns. Filters must push down; no client-side scans. | **Adopt-vs-build (ADR-0001) — RESOLVED: adopt Ontop (Apache-2.0 open source, free; not a purchase).** A VKG engine (**Ontop**) already does SPARQL→SQL pushdown with no materialization across all supported sources, driven by the P12.1 R2RML export — so native generation is **retired as the target**, kept only as a documented fallback for a deployment that declines to run Ontop. If ever built: compile field expressions per dialect; reuse the Phase 5c expression engine. |
| P12.3 | **No-materialization mode.** A documented mode/entry point producing mappings + queries with no ArangoDB writes at all. Batch/CDC load remains an option, not a default. | Read-only; honors Phase 9 classification gates (no generating queries that select Restricted/PII columns above the entitlement threshold unmasked). |
| P12.4 | **Citation contract.** Every generated query is returned *alongside* its results: query text, bind parameters, source objects (tables/columns) read, and an as-of execution timestamp — so the fabric's grounding layer can cite the actual SQL. | Mirrors the fabric M7 requirement; nothing hidden. |
| P12.5 | **Query fragmentation (P2).** When a request spans >1 source, emit per-source fragments + the join keys; the fabric's engine reassembles. | Depends on P12.2 shipping for one source first. |
| P12.6 | **Deterministic + LLM paths (P2).** Query generation is deterministic from the mapping; an optional LLM path (behind the existing `LLMProvider` seam from Phase 10) acts as safety net only, with the generated query always surfaced for inspection. | Consistent with Phase 10 principles: human-reviewable, temperature 0, provider-agnostic. |
| P12.7 | **Snowflake dialect — PULLED FORWARD, due 2026-07-24** (Databricks stays P3). Scope narrowed by the fabric's ADR-0001 Ontop adoption: what P12.7 now means is the **CSI + R2RML export path over a Snowflake-introspected schema** (`schema.json` via the Phase-6 connector / catalog-source path; RSA introspection is the named fallback) — Ontop does the SPARQL→SQL, so no bespoke Snowflake SQL generation is needed. Watch Snowflake's **uppercase identifier folding**: physical names land uppercase in `schema.json` and must flow verbatim into `rr:tableName`/`rr:column` (the CC-12 naming layer owns the conceptual names — `USAGE_METRICS` → `UsageMetric`). | Snowflake connector + sessions already exist (Phase 6); fabric Sprint-2 WP-S3 is the consumer. |
| P12.8 | **Module boundary.** Ship behind a named, tested module (or extract to an `r2g-query`/RSA-adjacent package) so downstream systems pin it the way they pin RSA — not r2g wholesale. | Decide extraction at kickoff; follows the RSA precedent. |
| **P12.9 ✅ shipped (0.4.1, 2026-09-14)** | **Bitemporal provenance passthrough.** `mapping_to_csi` and `r2g export-csi` forward the four bitemporal keys stamped by `relational-schema-analyzer` ≥ 0.8.0 — `transactionTime`, `validTime`, `validTimeSource`, `predecessorFingerprint` — onto the CSI `provenance` envelope unchanged. r2g neither mints nor reshapes them; valid time is capturable only at introspection, so RSA records it and r2g carries it. | Commissioned by the CDF unified-architecture paper (Q-3, step 7); converged with `arangodb-schema-analyzer` 0.13. RSA band raised to `>=0.8.0,<0.9.0` for it. |
| **P12.10 ✅ shipped (0.4.1)** | **CSI schema sourced from the analyzer.** `csi_schema()` loads `schema_analyzer.csi.v1` (the authoritative CSI v1 schema in `arangodb-schema-analyzer`) when importable and falls back to the vendored `src/r2g/schemas/csi_v1.schema.json` otherwise; a drift test fails when the two diverge. The `validTimeSource` enum admits RSA's five values (`catalog`, `event`, `file`, `fingerprint-continuity`, `observed`). | Closes the vendored-copy drift the paper's step 2 flagged (both copies had been hand-edited in lockstep on 2026-09-12). |
| **P12.11 ✅ shipped (0.4.2, 2026-09-21)** | **Federation Forge dialects (ADR-0006 S2).** The reverse generator `generate(ontology, dialect, seed) -> {ddl, loader, rows}` (`src/r2g/forge/`) supports the ADR-0006 D-4 launch set — `postgres`, `snowflake`, `clickhouse`, `arango` — behind one registry (`r2g.forge.dialects.DIALECTS`); a new dialect is one module following `dialects/base.py` (and PLAN F-8: it renders what the plan gives it and derives no cross-table fact or identifier spelling itself) plus one registry line. Data is synthesized **once** from the seeded plan and only *projected* per system, so `rows` are byte-identical across dialects (D-2). Each dialect ships a live roundtrip `introspect(generate(O)) ≡ O` through the **real** analyzers (D-3): Postgres via `PostgresConnector` + Auto-Map + CSI, Snowflake via RSA's `SnowflakeConnector` over a throwaway schema, ClickHouse via r2g's `ClickHouseConnector` + FK inference (RSA has no ClickHouse connector; the constraint-stripped two-branch contract — keys recovered by inference OR reported absent — is asserted both ways), Arango via `arangodb-schema-analyzer`'s deterministic baseline over the generated loader's output. | Snowflake identifiers are unquoted UPPERCASE (the folding reality P12.7 named); its one type gap (`NUMBER(38,0)` introspects as bare `NUMBER` → `float`) is **pinned by name** in the roundtrip so it can neither hide nor silently close — closing it is an RSA (report `NUMERIC_SCALE`) + r2g (scale-aware map) follow-up. Temporal/decimal/uuid conceptual types stay out until the forward map stops collapsing them to `string`. Partitioner, denormalizer, and goldens are S3 (CDF orchestration). Plan: `docs/internal/PLAN-federation-forge.md` §"S2 dialects". |
| **P12.12 ✅ shipped (0.7.0, 2026-10-21; `identifier` 0.8.0, 2026-10-26)** | **Curated labels applied at export.** `export-csi` and `export-r2rml` accept `--curated-labels FILE`, a curated-labels document (`curatedLabelsVersion: "1"`; `entities` / `properties` / `relationships`, each entry keyed by the name r2g *generates* and carrying the curator's `label`, plus optional `description`, `altLabels`, `decidedBy`, `decidedAt`). The curator's words become a CC-12 identifier (`"Customer Name"` → `customerName` / `CustomerName`); the words are kept as `prefLabel` and the generated name joins `altLabels`. An entry may also give `identifier`, the curator's last word on the name (r2g-arango#34): it is used exactly as written, never singularized or re-cased (`Sales` stays `Sales`), so a label in a non-Latin script can keep its words while the identifier names the concept; it must already be CC-12 shaped (PascalCase for an entity, lowerCamel for a property or relationship) and still goes through every refusal below. Without it, accents are folded first (`"Café owner"` → `cafeOwner`); a label with letters that have no ASCII form (`ß`, non-Latin scripts) or with no letters or digits is **refused**, never shortened or written raw into an IRI. One planner (`r2g.curated_labels.plan_curated_labels`, reached through `r2g.csi.curated_plan_for_mapping`) decides every rename for **both** exporters, so the CSI and the R2RML exported from one mapping and one file name every concept identically. Renames are conceptual only — physical fields, `rr:tableName` and `rr:column` are untouched — and run **before** the P12.1 label-collision check, so a curated rename can resolve a collision. A rename is **refused**, keeping the generated name, when it would rename a declared join key (P6.7), give a property or relationship a name another property or relationship carries on any entity, or give an entity another entity's name; refusal repeats until stable. Every entry is recorded in `provenance.curatedLabels` (`applied` \| `unchanged` \| `refused` \| `unmatched`), and the CLI writes nothing while any entry is refused or unmatched unless `--allow-unapplied-labels` is given. A key that matches nothing but equals the name `--label-policy qualify` / `roles` gives a collided label is reported as such. | Commissioned by a customer request to override concept labels in extracted ontologies with a business vocabulary (2026-10-20). The decisions are curated in arango-ontoextract's lexicon (its PRD §6.20), which exports this document; keys match when the CSI imported into AOE was exported with `--label-policy warn` or `off`, as contextual-data-fabric does. Evidence: explicit identifier `src/r2g/curated_labels.py:110` and `:298`, planner `src/r2g/curated_labels.py:253`, identifier rule `src/r2g/curated_labels.py:85`, refusal loop `src/r2g/curated_labels.py:373`, join-key refusal `src/r2g/curated_labels.py:351`; shared entry `src/r2g/csi.py:617`, CSI application `src/r2g/csi.py:404`, provenance `src/r2g/csi.py:941`; R2RML `src/r2g/r2rml.py:134`; CLI refusal to write `src/r2g/main.py:661` / `src/r2g/main.py:763`; tests `tests/test_curated_labels.py`. **A rename is a new name for every consumer:** contextual-data-fabric manifests that list concept or property names (entitlements, `uniqueConstraints`) fail its catalog build until updated to the curated names — loudly, by design. **Out of scope:** R2RML still does not apply the P12.1 collision renames, so under `qualify` / `roles` the two exports already disagree on collided labels — a pre-existing gap this does not change. |
| **P12.13 ✅ shipped (0.7.0, 2026-10-21; dictionary layer and foreign-key evidence 0.8.0, 2026-10-26; evidence narrowed to a column's own singular 0.8.1, 2026-10-26)** | **Conceptual class names follow the shared singularization rules, and the schema's own foreign keys.** CSI and R2RML class names are the singular PascalCase of the collection name (CC-12). The singularizer tries, in order: the named lists (irregulars such as `people` → `Person`, words that do not change such as `news` or `analytics`, singulars that end in `s` such as `status`, and data-modelling decisions such as `demographics` → `demographic`); then a noun dictionary (relational-schema-analyzer's vendored `noun_lexicon.json.gz`, generated from LemmInflect's noun lexicon): its singular for a known plural (`knives` → `knife`, `cacti` → `cactus`, `criteria` → `criterion`), and a word it lists as a noun in its own right stays as it is (`logistics`, `cactus`); then the suffix rules for words it does not know (`skus`). Only the last word of a compound is inflected (`ORDER_STATUS` → `OrderStatus`). `pluralize`, which names the Forge's tables, is its inverse (`knife` → `knives`), so every listed class round-trips through the Forge (PLAN F-2). **The schema's foreign keys come first when they disagree:** a declared or curated single-column key whose column names one row of the table it references with a singular of its own (`plans.basis_id` → `bases`) is the schema designer's word for that row; when every such column agrees on a singular that differs from the singularizer's, that singular names the class (`bases` → `Basis`, where the singularizer gives `Base`) and `provenance.entityNaming` records the columns. A column that only repeats its table's name (`orders.goods_id` → `goods`, `orders.customers_id` → `customers`) is **not** evidence: many schemas name every key `<table>_id`, and nothing in one table tells that habit from a designer calling one row "goods", so a table such as `goods` keeps the singularizer's `Good` unless a curator names it with an explicit `identifier` (P12.12). A table's class therefore depends only on that table and the keys that reference it, never on unrelated tables. Keys r2g inferred are not evidence (they were matched with the singularizer), role-named columns (`ship_via`, `reports_to`) are ignored, a collection the mapping renamed keeps the curator's name, and a collection two tables share keeps the singularizer's. `export-csi` without `--schema` has no foreign keys to read, so it warns that its class names can differ from `export-r2rml`'s. One function decides every collection's class for CSI, R2RML and the curated-labels planner. r2g, relational-schema-analyzer and arangodb-schema-analyzer each keep a copy of the rules; all copies must agree with RSA's published list of known answers (`relational_schema_analyzer.naming_known_answers`), ASA's vendored dictionary must have RSA's SHA-256, and r2g's test suite is where they are checked against each other. | Decided in r2g-arango#30 after the copies had drifted (`warehouses` → `Warehous` in three copies, `buses` → `buse` in the fourth). The dictionary was chosen by benchmark (`tools/singularize_benchmark` in relational-schema-analyzer: 0.7.0 rules 44/72 on held-out words, the dictionary design 68/72; ArangoSearch's Snowball `stem`/`text` analyzers 36/72, because they return stems such as `warehous`). On Chinook, Northwind and Pagila every table-named foreign-key column agreed with the dictionary (28 tables, no change). Evidence: `src/r2g/naming.py:190` (`_singular_word`), `src/r2g/naming.py:178` (RSA's lexicon), `src/r2g/naming.py:241` (`singularize`), `src/r2g/naming.py:221` (`pluralize`), `src/r2g/csi.py:529` (`entity_names_for_mapping`), `src/r2g/csi.py:729` and `src/r2g/r2rml.py:186` (both exporters use it), `src/r2g/csi.py:939` (`entityNaming`), `src/r2g/forge/core.py:351` (`table_name`), `src/r2g/xsk.py:209` (delegates); tests `tests/test_naming_known_answers.py`, `tests/test_entity_naming.py`. A curator corrects anything left over with an explicit identifier (P12.12, r2g-arango#34). |
| **P12.14 ✅ shipped (0.8.2, 2026-10-29)** | **CSI 1.1: the version follows the mappings; property mappings hold only strict keys.** CSI 1.1 (arangodb-schema-analyzer 0.18.0, arango-sparql-py 0.5.0; arango-sparql-py#72) adds `path` to a property mapping — a value inside a nested object, one name per level — and makes a property mapping strict: `field` or `path`, the descriptive keys `type`, `indexed`, `unique`, `required`, `role`, `sentinelValues`, `numericLike`, `sampleValues`, or an `x-` key. `mapping_to_csi` derives `csiVersion` from the mappings (`csi_version_for`): `"1.1"` only when some property mapping uses `path`, `"1"` otherwise, so readers that accept only `"1"` keep reading r2g's flat documents. r2g writes `field` only; if it ever embeds a table's columns as a nested object, those columns are mapped with `path`, never a dotted `field`. The vendored schema follows the analyzer's 0.18.0 copy (P12.10's drift test), and the analyzer floor in `[test]`/`[dev]` is `>=0.18.0`. | An unknown key in a property mapping can change where a reader takes a value from, so the canonical schema refuses it; a document that needs `path` must declare a version old readers refuse. Evidence: `src/r2g/csi.py:43` (`CSI_VERSION_NESTED`), `src/r2g/csi.py:51` (`CSI_PROPERTY_MAPPING_KEYS`), `src/r2g/csi.py:65` (`CSI_PROPERTY_EXTENSION_PREFIX`), `src/r2g/csi.py:648` (`csi_version_for`), `src/r2g/csi.py:955` (`mapping_to_csi` uses it), `pyproject.toml:126` and `pyproject.toml:134` (analyzer floor); tests `tests/test_csi.py` (`test_csi_version_for_*`, `test_emitted_property_mappings_hold_only_strict_keys` over six emission paths). r2g-arango#39. |

#### Phasing

- **12a (fabric Phase 1, ~days):** P12.1 + P12.2 + P12.3 + P12.4 for PostgreSQL — enough for the fabric's 1-week federated-query demo (one relational DB + the Arango unstructured graph).
- **12b (fabric Phase 2):** P12.5 + P12.6 + P12.7 (Snowflake).
- **12c (fabric Phase 3):** P12.7 (Databricks); join-key optimization feedback into fragmentation.

#### Non-functional notes

- **Read-only by construction:** generated queries are `SELECT`-only; execution goes through read-only roles (fabric CC-7).
- **Classification-aware:** P12.2/P12.3 respect Phase 9 sensitivity gates — the federated path must not become a bypass around the entitlement report ("advise and gate" applies to reads leaving the source, not just loads).
- **Inspectable:** the mapping artifact, the generated SQL, and the bind parameters are all first-class outputs; nothing about the pushdown path is a black box.
- **Acceptance (12a):** given a Postgres schema (Chinook/Pagila samples suffice), r2g emits a versioned mapping artifact and, for a seed-question predicate, a parameterized pushdown query returning the right rows with no ArangoDB writes — query text + source objects + as-of timestamp returned for citation.

---

### Phase 13: LPG target layout -- implemented (P13.1–P13.11, P13.13); P13.12 guarded, full support pending

Until now r2g's ArangoDB target has had exactly one physical shape: a document
collection per node type and an edge collection per edge type (call it **PG**).
That shape is a good default — the type *is* the collection, so per-type
indexes, sharding, and permissions come free, and a traversal is scoped by
which edge collection it walks. It is not the only shape people want. The
**LPG** shape puts every node in one collection carrying a `labels` array and
every edge in one collection carrying a `type` property, which is what you
choose when types churn, when a node legitimately has several labels, or when a
downstream tool expects that model.

The design consequence is that everything the collection used to express must
move **into the documents** — and that is not merely a serialization change:

- **Two collapse hazards, both silent.** Distinct in PG, colliding in LPG:
  `customers/1` and `orders/1` both become `nodes/1`, and two edge types joining
  the same pair produce one `_key`. Neither errors; each just overwrites. Keys
  are therefore label-namespaced, and the edge key leads with its type.
- **Filtering must be paid for.** A traversal that was scoped by its edge
  collection now shares one collection with every other type, so the engine
  would load each incident edge and discard the misses. Hence **vertex-centric
  indexes**, and hence the copied endpoint labels that make them possible.

| ID | Requirement | Notes |
|---|---|---|
| **P13.1 ✅** | **Selectable layout.** `MappingConfig.graph_layout` is `pg` (default) or `lpg`, with an `LpgLayout` model naming the collections and fields. The default is omitted from serialization, so every mapping written before P13 is byte-identical. | `src/r2g/types.py:304` (`LpgLayout`), `:350` (`graph_layout`); guarded by the serialization compatibility corpus. |
| **P13.2 ✅** | **Collapse-safe identity.** Node `_key` is label-namespaced (`<Label><sep><pk>`, e.g. `Account_42`); edge `_key` leads with the edge type. Both hazards lose data *silently* in the PG→LPG collapse, so neither may be left to chance. Nodes carry `labels[]`; edges carry `type`. | `src/r2g/transformers/node_transformer.py:176`, `:183`; `src/r2g/transformers/edge_transformer.py`. |
| **P13.3 ✅** | **Vertex-centric indexing.** The edge indexes are `[_from, type]` and `[_to, type]`, and each edge additionally carries copies of its endpoints' labels (`fromLabels`/`toLabels`) as **non-indexed** attributes. The copies are load-bearing but not as index fields: they let a label filter be satisfied from the edge document the traversal has already loaded, without materializing the neighbour vertex — were the labels left only on the neighbour, every candidate would have to be fetched to be rejected. **The label arrays are not indexed on the edge collection by default.** A persistent index over an array-expansion field stores *one entry per element*, and a traversal scans every entry under the `_from`/`type` prefix, so indexing `toLabels[*]` returns each edge **once per label on its target** — the multiplier being introduced by exactly the multi-label support the arrays exist for, and the symptom being inflated counts rather than an error. There is no query-side repair: the label filter does not narrow the prefix scan, `uniqueEdges: 'global'` is rejected (ERR 10), and `RETURN DISTINCT` merely hides the symptom while the traversal still does the work and any edge aggregate stays inflated. Nothing is lost by dropping the field: the flat spec is still selected and still serves the pushed-down `p.edges[*].type ALL == @type` condition. The expansion remains correct on the **node** collection, where an equality lookup matches one entry per document and the access path is an `IndexNode` rather than a traversal, so `labels[*]` continues to serve traversal entry points. **Opt-in escape hatch:** `LpgLayout.index_edge_labels` (default `false`) additionally provisions the array-expanded specs *alongside* the flat ones, under distinct names, for a deployment that runs label-filtered **pattern-match** edge queries (`FOR e IN edges FILTER e._from == …`) — an access path where an equality on the array field seeks one entry per document and the index is correctly selected. This is safe to enable *because the generated templates carry an explicit nested `indexHint` naming the flat index*: with both index sets present each access path takes the right one, so the hint is a **correctness guard, not only a performance one**. It stays off by default because nothing r2g emits would use it (every generated template is a traversal), because r2g still writes exactly one label per node (P13.10) so on r2g's own output the array index degenerates to a three-field index over one-element arrays, and because it costs one index entry per label on every bulk load. Enable it when a consumer actually runs label-filtered edge lookups, or when multi-label production lands. | `LpgLayout.index_edge_labels` (`src/r2g/types.py:348`), `src/r2g/lpg.py:109`. `ArangoWriter.ensure_indexes` (`src/r2g/connectors/arango_writer.py:364`) warns whenever an array-expansion index is created on an edge collection — a warning rather than a refusal, since the pattern-match use is legitimate, and unlike the index itself that check runs on every provision. Verified live with targets carrying 1/2/3 labels: traversal → flat index, counts 1/1/1; pattern-match → array index, counts 1/1/1. Core specs: `src/r2g/lpg.py:92` (`edge_indexes`), `:39` (`vertex_indexes`), `:68` (the rationale). Measured on 3.12.11 with targets carrying 1/2/3/4 labels: the array spec returned them 1/2/3/4 times; the built-in edge index and the flat spec returned each exactly once. Regression-guarded by `TestLpgNoDuplicationFromLabelArrays`, which counts occurrences (never `DISTINCT`s them) and structurally asserts no edge index carries a `[*]` field. |
| **P13.4 ✅** | **Traversals must be hinted.** Generated traversal templates carry the **nested** per-collection/per-direction/per-level `indexHint`; the familiar flat `indexHint: 'name'` form is *silently ignored* by traversals — no error, no effect, not even with `forceIndexHint` — so an unhinted traversal degrades to the built-in `edge` index with identical results and very different cost. Index usage is asserted against a live `explain`, never assumed. | `src/r2g/lpg.py:130` (`traversal_index_hint`). Verified on ArangoDB 3.12.9, both directions, including the array-expanded label field. Templates avoid `arr[*] ALL == x` (trivially true over a one-element array) and always emit `WITH` (absent, it passes on single-server and fails on cluster with error 1521). |
| **P13.5 ✅** | **Provisioning.** One named-graph edge definition (one edge collection, both endpoints the one node collection), with the collections and indexes created idempotently. The edge collection **must** be created with `edge=True`: as a document collection it succeeds quietly, then `create_graph` fails ERR 1944 and traversals return nothing — a failure that surfaces far from its cause. | `src/r2g/connectors/arango_writer.py:336` (`ensure_indexes`); `src/r2g/lpg.py` (`graph_edge_definition`). |
| **P13.6 ✅** | **Pipeline routing.** The streaming load path routes every document into the single node collection and every edge into the single edge collection when `graph_layout: lpg`, so an LPG target loads end-to-end. The per-type name is retained as the *label* and as the reported unit, so progress and per-table result counts are unchanged. **The shared collections are prepared once, not per table** — the pg path prepares a collection per table, which is safe only because each table owns its own; repeating that here would make each table's `--drop-collections` erase the tables loaded before it and make `--skip-existing` skip every table after the first wrote a row. Both fail silently, yielding a partial graph rather than an error. | `src/r2g/streaming/pipeline.py` (`_lpg`, `_prepare_lpg_collections`, routing in `_stream_one_document` / `_stream_one_edge`). Verified end-to-end against live ArangoDB 3.12.9: two tables + one FK load into one node collection (4 documents, label-namespaced keys) and one edge collection (2 edges), and the generated traversal over the loaded graph returns the right row *through the vertex-centric index*. |
| **P13.7 ✅** | **Layout-aware CSI export.** The emitter reports the layout it actually produced: `LABEL` / `GENERIC_WITH_TYPE` for an LPG target instead of the previously unconditional `COLLECTION` / `DEDICATED_COLLECTION`, naming the shared collection plus the carrier field (`typeField`) and this entity's or relationship's value in it (`typeValue`). Emitting the pg styles for an LPG target describes collections that do not exist — a consumer resolves `Account` to a collection named `Account`, finds nothing, and fails far from the cause. `typeValue` is the **stored** label (the physical target-collection name), not the conceptual entity name, because a naming convention makes those diverge (`accounts` → `Account`) and this mapping must describe what the loader wrote. | `src/r2g/csi.py:69-80` (both style pairs), `mapping_to_csi`. No contract change: `src/r2g/schemas/csi_v1.schema.json:60,75` already permit both enums and already define `typeField`/`typeValue`. Both layouts validate against the schema. |
| **P13.8 ✅** | **Studio selector.** The Mapping Studio exposes the target model — a **Target graph model** action and an always-visible header chip (`PG`/`LPG`) that opens the same picker, since the layout governs how every document is written and should not be buried in a menu. Choosing LPG reveals the node/edge collection names (rejecting equal names) and warns that switching model changes the write layout, so the target should be re-loaded rather than left half in each shape. The setting is sent only when non-default, so a mapping that never touched it keeps its historical on-disk shape. | `src/r2g/ui/static/index.html` (`openGraphModelForm`, `applyGraphModel`, `renderGraphModelChip`; carried through `applyConfigToEditState` / `buildMappingPayload`). No API change — `PUT /api/projects/{name}/mapping` already validates the whole `MappingConfig`. Round-trip covered by `tests/test_ui_api.py::TestGraphModelSelection`. |

| **P13.13 ✅** | **Multi-label node production.** A collection may declare `extra_labels`; its nodes then carry `target_collection` **plus** those. The **primary** label (`target_collection`) remains the sole basis for the `_key` namespace and for the CSI physical `typeValue`, so adding a label *widens what a node answers to without relocating it* — a re-load after adding one updates documents in place rather than orphaning the old keys. `extra_labels` is omitted while empty, so mappings written before P13.13 serialize byte-identically. **The load-bearing requirement is the edge copies:** an edge's `fromLabels`/`toLabels` must carry its endpoints' **full** label sets, and must do so from *every* writer. Those copies are what traversals filter against, so an edge carrying only the primary label while its endpoint carries three makes every filter on the other two silently miss; and were only one writer updated, bulk-loaded and CDC-written edges would disagree about the same graph. Nothing in the type system enforces this — a future write path must thread the label sets too. CSI reports the full set as conceptual `labels`, while the physical `typeValue` stays the primary (the schema permits one, and identity is built on it). | `CollectionMapping.extra_labels` (`src/r2g/types.py:256`) and its `.labels` property (`:266`, primary first then extras, deduped); `src/r2g/transformers/edge_transformer.py:54` (the copies), threaded from **both** writers — `src/r2g/streaming/pipeline.py:155` and `src/r2g/cdc/delta_transformer.py`. Studio: a *Labels (LPG)* group on the collection panel, shown only in `lpg` mode. Verified live: a 3-label collection loads, edge copies match the node documents, filters on **non-primary** labels return correctly, and there is no duplication at 3 labels. |
| **P13.12 ✅** (guard) | **Temporal mode is refused in the LPG layout.** Temporal writes build a proxy/version structure **per entity type** (`ProxyIn_<X>` / `ProxyOut_<X>`) — a collection-per-type design with no LPG awareness. Combined with `graph_layout: lpg` that would create per-type proxy collections *beside* the shared node/edge pair, leaving the target half in each shape with **no error at any point**, because each half succeeds on its own terms. `CDCHandler` therefore raises `NotImplementedError` when `--temporal` meets an LPG mapping, naming both ways out (`graph_layout: pg` for temporal CDC, or LPG without `--temporal`). This is the same silent mixed-shape failure P13.9 removed from the delta path; the difference is that the delta path was taught the layout and the temporal path has not been. **Full support remains unimplemented** — it needs a design for how time-travel proxies are represented when there is only one node collection (labels distinguishing proxy kinds is the obvious candidate). Worth noting the horizon: ArangoDB is making time travel a native capability with a dedicated collection type and a query-level time/interval setting, at which point the extra collections stop being visible and this guard's condition stops being reachable — but a guard is still warranted for graphs built against the current blueprint implementation. | `src/r2g/cdc/handler.py` (the refusal, sited where the temporal flag and the mapping config meet). Covered by `TestTemporalLpgGuard`. The natural index for a future LPG temporal design is `mdi-prefixed` with `prefixFields: ["_from","type"]` over the single edge collection — one index covering every edge type — though note MDI is selected only on the pattern-match path, never inside a traversal. |
| **P13.9 ✅** | **CDC / Kafka sync in the LPG layout.** The delta path (`cdc-start`, `kafka-start`) routes documents to the shared node collection and edges to the shared edge collection, passing the layout to both transformers so deltas carry `labels` / `type` / endpoint labels and label-namespaced keys — a CDC stream therefore maintains the graph in exactly the shape the initial load produced, rather than building a second one beside it. **The delete path needed separate handling:** a delete carries only the old row, so its `_key` is built outside `NodeTransformer` and had to be label-namespaced there too. Un-namespaced it would address `nodes/10` instead of `nodes/Order_10` and delete nothing — or another table's document — and because deletes are idempotent the miss would leave no trace. | `src/r2g/cdc/delta_transformer.py` (`_lpg`, `_node_collection`, `_edge_collection`, namespacing in `_document_key_from_row`). Verified live against ArangoDB 3.12.9: CDC inserts create **no** extra collections, the inserted row is reachable through the generated traversal, and a delete genuinely removes the document (the assertion that would fail on an un-namespaced key). Indexes come from the initial load; CDC only ensures the collections exist. |

| **P13.10 ✅** | **Multi-label semantics and their filter contract.** `labels` on a node and `fromLabels`/`toLabels` on an edge are **arrays**, and must stay correct — and index-served — when they hold several labels. Label tests use AQL array-comparison operators, which map 1:1 onto Cypher's label expressions: `@l IN arr` = `:A`; `@ls ALL IN arr` = `:A:B`; `@ls ANY IN arr` = `:A\|B`; `@ls NONE IN arr` = negation. r2g exposes these through `label_predicate()` so a consumer does not have to derive them. **A label filter must never be written against `p.edges[*]`:** for a *list-valued* field that accessor expands to a **nested** array, so both `… ALL == @l` and `@l IN p.edges[*]…` match nothing and return an **empty result with no error**. Crucially the problem is *not* performance — the optimizer pushes `p.edges[*].f ALL == @v` into the traverser as a **global edge condition** served by the vertex-centric index, so on a list field the condition is fast **and wrong**, which is worse than a slow correct one. `FLATTEN()` is correct but expresses only "some hop", since flattening discards which hop each element came from; chained array operators (`ALL ANY ==`) are a syntax error. The correct all-hops form **inverts** the question — select the hops that *lack* the label with an inline `FILTER`, then assert that set is empty: `p.edges[* FILTER @l NOT IN CURRENT.toLabels]._key NONE != null`. This is correct on any 3.12 and, from **v3.12.11**, is pushed into the traverser (inline-`FILTER` pushdown requires `ALL`/`NONE`, no inline `LIMIT` or `RETURN` projection, and no reference to the path variable), so it gets faster on newer servers rather than changing answer. Two traps: the **positive `ALL` shape is vacuously true** — an inline `FILTER` restricts the comparison's *scope*, so on a path where *no* hop carries the label the selected set is empty and `[] ALL != null` is `true`, accepting exactly the paths it should reject (and from 3.12.11 doing so quickly); and the **projection must be an always-present attribute** (`_key`), because a violating edge's label array is not *null*, it merely lacks the label, so `… .toLabels NONE == null` is trivially satisfied. | `src/r2g/lpg.py:117` (`LABEL_MODES`), `:120` (`label_predicate`, which refuses to emit a `[*]` accessor). Verified live on ArangoDB 3.12.9 with 3-label nodes and multi-element edge copies: all four operators correct **and** engaging the VCI; tests pin the two broken `[*]` forms returning `[]`. |
| **P13.11 ✅** | **Depth-ranged traversals must constrain every hop.** In `FOR v, e IN 1..n`, `e` is the **final** edge of the emitted path — not every edge — so a `FILTER` on `e` constrains only the last hop and admits paths that traversed non-matching earlier edges. The depth-ranged templates therefore test the edge type with `p.edges[*].<type> ALL == @type`, which on a **scalar** field is both correct and pushed into the traverser as a global edge condition served by the VCI, and test the label on the **destination vertex** (`@toLabel IN v.<labels>`) — the only form that is correct at any depth, given P13.10. Single-hop templates keep the `e`-based filters, where `e` is the only edge and the VCI serves both. | `src/r2g/lpg.py` (`traversal_templates`: `outbound_by_type` / `inbound_by_type`). Verified live: on a graph where one Account is reachable only through a non-matching first hop, the corrected template returns just the correctly-reached vertex while the previous `e`-based form returned **both** — a regression test pins that leak. |

**Round-tripping.** Nodes and edges also carry `sourceTable`, so an LPG graph
stays self-describing and reversible back to the PG layout rather than becoming
a one-way projection.

**Known limitations of the LPG layout.** Both are recorded because the array
shape invites assumptions the loader does not yet honour:

- **Multi-label nodes are producible** as of P13.13 — a collection declares
  `extra_labels` and its nodes carry the primary label plus those. This entry
  previously recorded the opposite; the constraint was lifted once the `[*]`
  nesting bug, the missing pushed-down all-hops test, and the array-index
  duplication were resolved, so the feature rests on measured ground rather
  than on three silent bugs.
- **An edge's endpoint labels are a denormalized copy.** That copy is what makes
  the vertex-centric indexes possible (labels on the neighbour node are
  unreachable from an index on the edge collection), and it is therefore only as
  current as the load: labels changed on a node afterwards leave existing edges
  stale, and a traversal filtering on them silently misses. Any future
  multi-label mapping has to re-sync the copies, not just widen the arrays.

**Verified end-to-end** against live PostgreSQL 16 (northwind) → ArangoDB 3.12.9:
a real introspection and generated mapping, switched to `lpg`, collapses 13
tables and 11 FK relationships into **two collections** (3,362 nodes / 7,113
edges) with 12 distinct labels, 11 distinct edge types, **zero `_key`
collisions**, one named-graph edge definition, and a traversal that filters
through `r2g_lpg_vci_outbound` and returns only correctly-labelled neighbours.
A re-load with `--drop-collections` preserves the full node count, proving the
shared pair is dropped once per run rather than once per table.

---

### Phase 14: Running under a context-and-governance control plane -- P14.1–P14.7 done; P14.8–P14.13 planned

A context-and-governance control plane can serve several tools behind one web
address, sign the user in once, and hand each tool a short-lived **signed
context**: who the user is, their roles, the tenant, the project, the project's
ontology. It also holds a policy saying who may change which parts of that
ontology, and one history of changes across tools. Running the Studio as one of
those tools means the control plane, not r2g, decides who is working and in
which project, and that mapping edits answer to the same policy as edits made
in the ontology tool.

Everything here applies only when `CONTROL_PLANE_URL` is set. With it unset the
Studio is unchanged. Sources, targets and catalogs stay tenant-wide: every
transformation project in the tenant sees them. Under a control plane the Studio
does not change them (P14.12); an administrator does, with the CLI.

P14.8–P14.13 govern the Studio's changes. Each mapping element names the ontology
class it stands for, and the control plane's policy over that class decides who
may change the element and load its data, as it decides who may change the class
in the ontology tool. Every change and every refusal goes into the project's
history.

| ID | Requirement | Notes |
|---|---|---|
| P14.1 | `CONTROL_PLANE_URL` switches the mode on; with it unset the Studio, CLI and MCP server behave as before, apart from `R2G_CATALOG_DIR` (P14.6), which every entry point honours in any mode. | Done — `src/r2g/control_plane.py:72` (`mode`), `src/r2g/ui/server.py:156`; `src/r2g/catalog.py:246` |
| P14.2 | Every API call carries the control plane's signed context token, verified offline with the control plane's SDK, and nothing else authenticates: `R2G_API_TOKEN` is not used. The load-progress stream takes the token as a query parameter, because an `EventSource` cannot send a header. | Done — `src/r2g/ui/server.py:104` (Bearer), `:106` (`?token=` on `/stream`), `:164` (`R2G_API_TOKEN` off) |
| P14.3 | Every API call about a transformation project — listing, reading, changing, loading, and creating one, including from a demo preset — is limited to the transformation project the control-plane project names for r2g; any other name answers "not found". | Done — `src/r2g/ui/server.py:121` (project routes), `:906` (list), `:227` (`_only_the_one_it_opens`: create `:918`, demo preset `:295`) |
| P14.4 | The Studio shows the project ontology, read from the ontology service at `AOE_URL` with the same context. | Done — `src/r2g/control_plane.py:153` (`ontology_classes`), `src/r2g/ui/server.py:240` |
| P14.5 | The page keeps the token for its own browser tab only (never a cookie or `localStorage`, which every tab shares), removes it from the address bar, renews it before it expires, and works under any path prefix the control plane mounts it at. | Done — `src/r2g/ui/static/index.html:1343` (`sessionStorage`), `:1345` (address bar), `:1350` (renewal), `:1290` (`_basePath`) |
| P14.6 | Storage is per tenant: the Studio, CLI and MCP server keep their catalog in `R2G_CATALOG_DIR` when it is set. In this mode `R2G_TENANT` is required — the Studio refuses to start without it — and a context for any other tenant is refused. | Done — `src/r2g/catalog.py:246`; `src/r2g/ui/server.py:95` (refuses to start), `:112` (other tenant 403) |
| P14.7 | The MCP server refuses to start when `CONTROL_PLANE_URL` is set, since it checks no context; the CLI is an administrator's tool outside the control plane's policy. | Done — `src/r2g/main.py:2127` (`r2g mcp`), `src/r2g/mcp_server.py:24` (any import) |
| P14.8 | A mapping element may name the ontology term it maps to, an IRI chosen from the project ontology: a table (collection) a class, a column (property) a property, a relationship (edge) a property. The terms are kept in the mapping file and are optional. The Studio offers the project ontology's terms, read as in P14.4. Without `CONTROL_PLANE_URL` nothing reads them. | Planned |
| P14.9 | Under a control plane, saving a mapping is checked against the project's policy, element by element, against the saved mapping (the mapping diff), before anything is written; if any element is refused the save is refused whole, saying which element and why. The term an element is checked on: a table its own class; a column its table's class; a relationship the class of the table it starts from. A property term on a column or relationship records meaning and does not change the check. Changing an element's class is checked on the old class and the new one. A table that names no class (with its columns and relationships), and a mapping-wide setting (key separator, naming convention, graph layout, type overrides, shared keys), is checked by the project-wide rules. | Planned |
| P14.10 | Under a control plane, loading data and migrating are checked for every collection they write, on that collection's class (project-wide where it names none), and refused whole if any one is refused. A load is recorded when it starts: who started it, its collections and the decision. Its outcome stays in r2g's load history, since a load may outlast the user's context. | Planned |
| P14.11 | Every governed change and every refusal is recorded in the project's history through the control plane's SDK, attributed by the context. Every saved version of a mapping is kept, named by its content hash, so a record points at the versions before and after. Records go out through a queue in the tenant's catalog directory and are resent under the same id until delivered, or until the context that attributes them expires. | Planned |
| P14.12 | Under a control plane, the Studio's change routes are governed (saving a mapping, loading, migrating, changing the transformation project's details, creating the one the control plane names), served as reads because they store nothing (expression compile and preview, FK and shared-key inference, denormalisation analysis, the batch-artifact preview, validate, validate-draft, diff, suggest-ontology, apply-ontology, apply-naming, auto-map, target introspection), or refused, saying why (adding, removing and snapshotting a source; adding and removing a target or a catalog; importing a source from a catalog; deleting the transformation project; installing a demo preset, which adds a source; writing governance files). Any other change route is refused by default, so a new route is closed until it is governed. | Planned |
| P14.13 | The actions r2g declares to the control plane: `mapping.update`, `data.load` and `data.migrate`, each taking an ontology term optionally (a request without one is decided by the rules with no target), and `transformation.update`, taking none. They need the control plane's SDK with optional terms; an older SDK rejects a policy that uses them. | Planned |

---

## 6. Future considerations (Phase 7+) -- Exploratory

These ideas are exploratory and represent potential directions, not committed work. Each would require significant design effort.

- **Additional source databases:** MySQL/MariaDB, SQL Server, and ClickHouse have been added following the `SourceConnector` pattern established in Phase 6 (ClickHouse introspects `system.tables`/`system.columns`, maps `Int*`/`UInt*` → `xsd:integer`, and streams via server-side block streaming — federation mapping *and* ETL); Oracle, SQLite, and others could follow the same way (source-specific schema reader, type map, streaming adapter).
- **Cross-source shared-key inference (see P6.7, planned):** the inference counterpart to Phase 12's federation join model — single-source FK inference (P6.6) finds *intra*-source edges; P6.7 finds the *cross*-source join keys federated queries actually run on, proposing shared **hub entities** (e.g. an `Account` hub linking the ClickHouse analytics tables to the Postgres CRM) under the same confirm-to-accept discipline. Closes the loop between the Phase-6 introspection layer and the Phase-12 federation layer.
- **Publishing back to external catalogs:** the inverse of Phase 8 — register the migrated ArangoDB graph (and column-level lineage from source to graph) as a governed downstream asset in the connected catalog. A write-path effort, deferred until the Phase 8 read path proves out.
- **Ontology derivation (LLM integration):** *Promoted to a committed phase — see Phase 10: LLM-assisted ontology derivation.* Use a large language model to analyze the source schema and propose an optimized target ArangoDB graph schema for a given domain (vertices vs. edges, implicit relationships, denormalization strategies), reviewed by a human and validated through the existing mapping pipeline.
- **ArangoRDF integration:** Emit data compatible with ArangoRDF so RDF, property graph, and labeled property graph representations can be selected as needed. Requires understanding the target use case (SPARQL queries, knowledge graphs, etc.) to choose the right representation.
- **Bi-directional synchronization:** Propagate changes from ArangoDB back to the source database. This is an extremely complex problem involving conflict resolution, schema evolution, and transactional consistency across two fundamentally different data models. Should be considered only if a concrete use case demands it.

---

## Document history

| Version | Date | Notes |
| :--- | :--- | :--- |
| Draft (Gemini-structured source) | December 2025 | Initial PRD with phased requirements P1.1--P4.4, technical requirements, and Phase 5+ items. |
| Narrative supplement (NotebookLM source) | December 2025 | Overlapping content with expanded relational-to-graph mapping (transliteration, join tables, normalization) and synchronization framing. |
| **Consolidated PRD** | **April 2026** | Single authoritative document. Gemini structure and requirement IDs preserved; NotebookLM mapping logic merged; conversational phrasing removed. Scope clarified as experimental reference implementation. Status columns added to phase tables. Edge cases, known constraints, and security notes added. "Antigravity" branding removed. |
| **Phase 1 extensions** | **April 2026** | CSV-direct import path (P1.7) and interactive mapping visualizer (P1.8) added. README updated to reflect the CSV-direct path as the preferred pipeline. |
| **Phase 2 implemented** | **April 2026** | Direct PG streaming to ArangoDB (P2.1--P2.4) implemented via psycopg server-side cursors and python-arango HTTP bulk import. `dump-tables` command, join table auto-detection, and interactive mapping editor with YAML export added. |
| **Hardening** | **April 2026** | Composite FK support (introspection, transformation, CSV-direct `--merge-attributes`). Multi-schema support (`--pg-schema`). Dry-run mode (`stream --dry-run`). GitHub Actions CI (pytest + ruff). 230 tests. |
| **Robustness & Performance** | **April 2026** | Rich progress bars for streaming. `validate-config` CLI command for static mapping/schema consistency checks. `--drop-collections` flag for idempotent re-import. Retry logic with exponential backoff in ArangoDB bulk writes. `--workers` flag for parallel table streaming (concurrent PG connections + ArangoDB writers). Elapsed time and throughput (rows/s) in stream output. Docker-based integration test suite. 251 tests. |
| **Data Integrity & UX** | **April 2026** | Import error surfacing (document-level errors captured and reported instead of silent failures). `--include-tables` / `--exclude-tables` for selective streaming. `source_schema` now populated from actual PG schema. Extended PG type map (50+ types). Tests for self-referential FKs and duplicate edge naming. 276 tests. |
| **Developer Experience** | **April 2026** | `diff-schema` command for comparing schema snapshots (added/removed tables, column changes, FK changes, with `--json` output). `--skip-existing` flag for resuming partial streaming runs. CLI integration test suite (31 tests via typer CliRunner). Fixed `transform-edges` unhashable EdgeDefinition bug. 314 tests. |
| **Config Migration** | **April 2026** | `migrate-config` command auto-updates mapping YAML when PG schema evolves: adds new tables/edges, removes stale edges, flags orphaned collections, cleans dropped-column references (field_mappings, include/exclude_fields, type_overrides). Preserves all user customizations. `--json-report` for CI pipelines. Typer upgraded from 0.12 to 0.24 for Click 8.3 compatibility. 341 tests. |
| **Usability & Safety** | **April 2026** | `.env` file and environment variable support (`PG_CONN`, `ARANGO_ENDPOINT`, `ARANGO_DB`, `ARANGO_USER`, `ARANGO_PASSWORD`) via python-dotenv -- credentials no longer required in CLI args. `validate-data` command checks FK referential integrity of dump files before import. Topological import ordering ensures FK targets are loaded before sources; circular FK deps detected and warned. `--since` timestamp filtering for basic incremental streaming. PK-less table warnings during validation and streaming. `.env.example` template. Snowflake integration planned as Phase 6. 373 tests. |
| **CDC Foundation** | **April 2026** | Phase 3 foundation: `ChangeEvent` model, `ArangoDelta`, `TransactionBatch`, `DeltaTransformer`, `CDCHandler`, `ArangoWriter` single-document ops with retry logic. 409 tests. |
| **CDC Listener** | **April 2026** | Phase 3 P3.1 + P3.3: `PGReplicationListener` manages logical replication slots, polls via `pg_logical_slot_get_changes`. Output plugin parsers for `test_decoding` and `wal2json`. Continuous polling loop with graceful shutdown. CLI commands: `cdc-setup`, `cdc-teardown`, `cdc-status`, `cdc-start`. 453 tests. |
| **Conflict Resolution** | **April 2026** | Phase 3 complete (P3.4): Configurable conflict policies (`source_wins`, `last_write_wins`, `log_and_skip`, `fail`). `ConflictResolver` wraps writes with error classification and policy-based resolution. `ConflictLog` for session conflict tracking. `--conflict-policy` CLI option on `cdc-start`. LWW uses `_r2g_lsn` field for per-document LSN tracking. 468 tests. |
| **Kafka Integration** | **April 2026** | Phase 4 complete: DebeziumParser for Debezium JSON envelope, FlatJsonParser for custom producers, KafkaConsumer wraps confluent-kafka with batch polling and at-least-once offset commits, graceful shutdown. kafka-start CLI command. Optional dependency. 502 tests. |

| **Visual Mapper PRD** | **April 2026** | Phase 5b added: Visual Graph Data Mapper & Ingestion Engine. Three epics (Data Catalog Interface, Visual Mapping Interface, Ingestion & Execution Engine) incorporating TigerGraph-inspired split-screen mapping UI, automated introspection, parallel ingestion, job monitoring, and cascading referential integrity. Mapped against existing implementation (catalog, mapping diff, selective reload, Mapping Studio UI). |
| **Expression + Graph-of-Graphs** | **April 2026** | Phase 5c added: Expression Mapping & Graph-of-Graphs UI. Introduces `FieldExpression` first-class data model (fan-in of 1..N source properties into a single target property via AQL / KSQL / Python expressions, identity default), mapping function nodes rendered as circles on connector lines with an inline expression editor, fan-in via drag-and-drop, and mini ER/graph-model visualizations within the source and target panes (FK edges in-pane on the left, edge-collection arrows in-pane on the right). Phase 5d added: ArangoDB-backed data catalog (sources, targets, snapshots, projects, mappings, loads as a named graph). |
| **Phase 5b Execution Engine** | **April 2026** | P5b.3.1 Load trigger, P5b.3.2 SSE progress monitoring, and P5b.3.3 dead-letter queue (`src/r2g/dlq.py`) implemented. Target graph introspection (P5b.1.3) landed via `src/r2g/connectors/arango_reader.py`, `TargetConfig` in the catalog, and `/api/targets*` endpoints. Cascading deletes (P5b.1.4) wired through `remove_source(cascade=True)` and `DELETE /api/sources/{name}?cascade=true`. Settings modal + Auto-Map button closed P5b.2.3 / P5b.2.4. MCP server (`src/r2g/mcp_server.py`) exposes catalog and load operations for agent tooling. Composite foreign keys normalized on `EdgeDefinition` via plural `from_fields` / `to_fields` with round-trip serialization. |
| **UI Architecture Upgrade** | **April 2026** | Phase 5e added and implemented: shared primitives (context menu, keyboard registry, `?` help overlay, floating-card stack); right-click menus on every entity; first-class edges with floating edge editor, drag-to-add, and promote-join-table; floating non-blocking progress card replaces the canvas-blocking overlay; bottom timeline strip for recent loads; complete legend; paint-only lens infrastructure (Topology / Coverage / Validation / Diff) with `1`–`4` shortcuts and header status chip; inline silent validation via `POST /api/projects/{name}/validate-draft` with per-entity badges and tooltip issue lists; a11y and hygiene polish (menu / region / complementary roles, `aria-label`s, Esc pops the top overlay, discoverability copy). 673 tests passing. |
| **In-UI catalog management** | **April 2026** | Phase 5e Epic 8 added: in-UI "+ New source", "+ New target", and "+ New project" floating-card forms wired to the existing `POST /api/sources`, `POST /api/targets`, `POST /api/projects` endpoints; right-click "Remove source…" with cascade-delete confirmation listing dependent projects / snapshots / loads; new Targets panel in the left sidebar with introspect + remove actions; empty-state copy now includes direct CTAs. Closes the "how do I even start" gap so first-time users can go from zero to a working mapping without touching the CLI. |
| **Expression evaluator** | **April 2026** | P5c.1.4 closed: new `src/r2g/expressions.py` ships a safe AQL-subset evaluator (literals, `@bind` refs, arithmetic with null propagation, comparisons, `&&`/`||`/`NOT`, `??`, ternary, and the function set `CONCAT`, `CONCAT_SEPARATOR`, `UPPER`, `LOWER`, `SUBSTRING`, `LENGTH`, `LTRIM`, `RTRIM`, `TRIM`, `TO_STRING`, `TO_NUMBER`, `TO_BOOL`, `CONTAINS`, `COALESCE`). `NodeTransformer` applies compiled `field_expressions` to every row, with identity / unsupported-engine / uncompilable fallbacks. `validate_config` parse-checks expressions and reports unresolved `@bindings`. UI gains a live compile indicator (`/api/expressions/compile`) and a functions-list endpoint (`/api/expressions/functions`), with save blocked on syntactically invalid expressions. 732 tests passing. |
| **Credential encryption at rest** | **April 2026** | Phase 5d non-functional D4 closed for the filesystem catalog: new `src/r2g/security.py` encrypts `SourceConfig.connection_string` and `TargetConfig.password` with Fernet (AES-128-CBC + HMAC-SHA256) using a key sourced from `R2G_SECRET_KEY` env var or a 0600 key file (`~/.r2g/secret.key`). Tagged `enc:v1:` envelope keeps legacy plaintext catalogs readable and transparently upgrades them on next save. UI responses (`GET /api/sources`, `GET /api/targets`) are redacted via `redact_connection_string` / `redact_for_display`; new `r2g secrets init|status|migrate` commands manage the key and force-encrypt existing catalogs. Closes the urgent gap created when Phase 5e.8 surfaced credential capture in the browser. 758 tests passing. |
| **Snowflake introspect-only slice** | **April 2026** | Phase 6 opened with the "thin slice": P6.5 source abstraction (new `r2g.connectors.base.SourceConnector` Protocol + `create_source_connector` factory), P6.1 `SnowflakeConnector` reading `INFORMATION_SCHEMA.TABLES/COLUMNS` plus `SHOW PRIMARY KEYS`/`SHOW IMPORTED KEYS`, and P6.2 Snowflake-aware `DEFAULT_TYPE_MAP` entries (`NUMBER`, `VARIANT`, `ARRAY`, `OBJECT`, `TIMESTAMP_*`, `BINARY`, `VECTOR`, geo types). `snowflake-connector-python` is an optional extra (`pip install 'r2g-arango[snowflake]'`); the UI, MCP server, and `r2g source snapshot` CLI dispatch through the factory based on the stored `source.source_type`, with a 501 + install hint when the extra is missing. Snowflake appears in the "+ New source" dropdown with a Snowflake SQLAlchemy-style connection-string hint. Dump (P6.3), streaming (P6.4), and FK inference (P6.6) remain planned. 785 tests passing. |
| **FK inference** | **April 2026** | P6.6 closed: new `src/r2g/fk_inference.py` provides a pure-Python name-heuristic engine with pluggable value-overlap sampling. Candidates are produced from `{prefix}_id`, `{prefix}id`, `{prefix}_{pkcol}`, non-generic PK-name matches, and a composite-PK pass, filtered by JSON-level type compatibility and ranked by confidence (bonuses for identical data types, identical nullability, and sampler-reported overlap ≥0.5 / ≥0.9; veto on zero overlap). `PostgresValueSampler` runs bounded `LEFT JOIN` queries to score value overlap (PG only this slice). New `POST /api/sources/{name}/infer-fks` endpoint + `GET /api/projects/{name}` lookup, new "Suggest FKs" toolbar button / `i` shortcut / canvas context-menu entry opening a floating card with per-row Accept / Accept all / Dismiss actions that write real `EdgeDefinition`s into the mapping (collision-proof naming preserves existing edges). New `r2g source infer-fks <name> [--sample] [--accept]` CLI command prints a Rich table and optionally writes accepted candidates into a fresh snapshot. 819 tests passing, 34 new. |
| **Temporal graph mode (Phase 5)** | **June 2026** | Phase 5 implemented: `src/r2g/temporal/` (`TemporalConfig` / `TemporalNaming`, `TemporalApplier`, point-in-time / version-history / interval AQL templates in `queries.py`). CDC and Kafka pipelines gain `--temporal` versioned writes (ProxyIn / Entity / ProxyOut + `hasVersion` edges, `created`/`expired` intervals, soft-delete), `--ttl-seconds` retention with sparse TTL + `mdi`-prefixed interval indexes (P5.5/P5.6), and `--smart-field` SmartGraph key prefixes (P5.8). Covered by `test_temporal_applier.py`, `test_temporal_models.py`, `test_temporal_queries.py`, and (live-workload) `tests/integration/test_temporal_e2e.py`. |
| **Demo sample databases + UI polish** | **June 2026** | Added Chinook and Pagila PostgreSQL samples plus a CSV library-system sample under `docker/` with a `load-samples.sh` helper and `docker-compose.yml` wiring for Friday-demo readiness. UI polish: R2G application rebrand with custom icon (`src/r2g/ui/static/r2g-icon.png`) and ArangoDB color scheme; targets rendered as a cluster→database tree; draggable / resizable inter-frame boundaries; non-ArangoDB-green PK badge color; project edit/delete in-UI (`PATCH`/`DELETE /api/projects/{name}`, P5e.8.3); `_system` database protected from loads with non-`_system` target defaults. |
| **CSV primary-key heuristic** | **June 2026** | `CsvConnector` PK detection (`src/r2g/connectors/csv_source.py`) now recognizes `id` plus `{table}_id` / `{singular(table)}_id` / `{table}id` / `{singular(table)}id` (e.g. `customers.csv` keyed on `customer_id`), gated by a uniqueness + non-null check on the read sample so a non-unique column is never marked a key; header-only files fall back to a name-only match. This unlocks FK-inference *targets* for CSV tables that lack a bare `id` column. 6 new tests in `tests/test_csv_connector.py`. 1000 passing, 6 skipped. |
| **CSV value-overlap FK sampling** | **June 2026** | Extended P6.6 FK inference with `CsvValueSampler` in `src/r2g/fk_inference.py`: reads the two candidate CSV files with Polars (type inference disabled so values compare as raw text, avoiding int/float token mismatches), bounded by a row limit, and returns the fraction of distinct local values present in the foreign column — the same statistic `PostgresValueSampler` computes via `LEFT JOIN`. Resilient (missing file / unreadable column → `None`). Wired into `r2g source infer-fks --sample` and `POST /api/sources/{name}/infer-fks` for CSV sources; Snowflake still falls back to name-only. 9 new tests in `tests/test_fk_inference.py`. 992 passing, 6 skipped. |
| **Configurable hints — menus + buttons (Phase 5e.7.4)** | **June 2026** | Context-menu items gained optional explanatory `hint` tooltips (ⓘ marker, shown on hover / keyboard focus, `role="tooltip"`, viewport-clamped) rendered centrally in `openContextMenu`. The same styled tooltip surface was extended to toolbar / panel buttons via `data-hint` + a delegated `mouseover` / `focusin` handler (placed below the button, replacing the old native `title` tooltips; `aria-label`s retained). A single "Show menu hints" toggle (default on, `localStorage` `r2g.menuHints`) in the `?` help overlay controls both. Static-asset-only change to `src/r2g/ui/static/index.html`. |
| **Naming conventions & rename change-management (Phase 5f)** | **June 2026** | Phase 5f added and implemented. New `src/r2g/naming.py` (`split_identifier` / `convert_identifier` / `apply_naming_convention`) and `NamingConvention` model on `MappingConfig`; `POST /api/projects/{name}/apply-naming` + canvas "Apply naming convention…" form (PascalCase collections, camelCase properties/edges by default). Rename change-management: `Project.loaded_mapping` tracking via `CatalogManager.set_loaded_mapping`; identity-based rename detection in `diff_mappings` (source-table identity for collections, relationship identity for edges) emitting parameterized `ReloadAction`s; hardened `SelectiveReloader` (in-place collection/edge rename, edge reload-from-source, parameter-bound property-rename AQL, named-graph rebuild, idempotency + `_system` guards); `GET /api/projects/{name}/migration-plan` + `POST /api/projects/{name}/migrate`; save-time migration prompt overlay. Reserved-attribute protection (`RESERVED_ATTRIBUTES`) across naming, diff, executor, and `validate_config`. Fixed latent edge `_from`/`_to` + named-graph resolution to use target collection names (`ConfigManager.graph_edge_definitions`, `EdgeTransformer` `from_name`/`to_name`). New `tests/test_naming.py`; expanded diff / executor / config / UI-API tests. 983 passing, 6 skipped. |
| **Post-demo UX feedback (Phase 5g)** | **June 2026** | Added Phase 5g (Planned) capturing first-demo feedback: light-mode default + dark toggle (P5g.1), simplified top bar via an action overflow menu (P5g.2), on-demand Sources/Targets (P5g.3) and Properties/detail (P5g.4) surfaces instead of permanent sidebars, bidirectional source↔target mapping highlight (P5g.5), an IDE-style project explorer (P5g.6), and progressive-disclosure mapping detail with per-table / per-collection / per-edge expand/collapse and relationship-neighbourhood cascade (P5g.7), an open-target-database action (P5g.8), function-node discoverability (P5g.9), and composite / multi-input function nodes with a read-only `_key` (P5g.10). **Implemented** in `src/r2g/ui/static/index.html`; remaining gaps: the P5g.3 drawer defaults open and the P5g.7 "show only unmapped / only edges" filters are not yet built. |
| **Snowflake dump + streaming (Phase 6 close-out)** | **April 2026** | P6.3 + P6.4 closed, completing Phase 6. New `r2g.connectors.session.SourceSession` Protocol (`count_rows`, `stream_rows`, `dump_table_to_csv`, `close`) plus concrete `PostgresSession` (`REPEATABLE READ` + server-side cursor + `COPY TO STDOUT` fast path) and `SnowflakeSession` (`BEGIN`/`COMMIT` snapshot + `cursor.fetchmany` streaming + `csv`-module dump). `SourceConnector` gains an `open_session()` method; `StreamingPipeline` is now fully source-agnostic and opens one session per worker (same consistent-snapshot semantics on both backends). `r2g stream --source <name>` resolves a catalog source by `source_type`; the legacy `--pg-conn` still works. New `r2g source dump <name>` subcommand replaces `r2g dump-tables` for catalog-aware dumps. UI `POST /api/projects/{name}/load` dispatches through `create_source_connector` with a 501 + install hint when the Snowflake extra is missing. Backward-compat constructor shim (`StreamingPipeline(pg_conn_string=…)`) keeps existing call sites working. 845 tests passing, 26 new. |

| **External data catalog integration (Phase 8) — Planned** | **June 2026** | Added Phase 8: connect r2g to external enterprise data catalogs (OpenMetadata, AWS Glue, Atlan, …) as an upstream discovery layer (browse → select → import as a source), distinct from the internal Phase 5d catalog. Backed by a cited research pass on catalog API suitability for "discover-then-connect" (`docs/internal/PLAN-external-data-catalogs.md`); OpenMetadata recommended as the first integration (OSS, official SDK, connection metadata, dockerizable for e2e testing). Market-share ranking flagged as unverified. Implementation + test plan drafted for review; no code yet. Also recorded MySQL/MariaDB + SQL Server source connectors shipped since the April baseline. |
| **External data catalog integration (Phase 8a–8b) — Implemented** | **June 2026** | Shipped 8a (foundation + OpenMetadata) and 8b (Studio UI): `src/r2g/catalogs/` provider abstraction + OpenMetadata REST provider (`httpx`, `openmetadata` extra), an encrypted `CatalogProviderConfig` registry, the `r2g catalog add/list/browse/import-source/remove` CLI, Studio `/api/catalogs*` browse/import endpoints with a left-rail Catalogs panel, and unit + skip-when-unavailable e2e tests. MySQL + SQL Server source connectors landed alongside. 1189 unit tests passing, 82% coverage. |
| **MCP catalog tools (Phase 8 P8.6) — Implemented** | **June 2026** | Closed the last 8b gap: added `list_catalogs`, `add_catalog`, `remove_catalog`, `catalog_browse`, and `catalog_import_source` MCP tools so an agent can discover and register sources from a connected external catalog, mirroring the CLI/UI (token redaction on read, `$ENV_VAR` token resolution at use time, DSN-scrubbed errors, credentials never taken from the catalog). Hoisted `get_asset` onto the `CatalogProvider` protocol now that all three surfaces (CLI/UI/MCP) depend on it. Unit-tested with a fake provider (browse list/descend/search, import-creates-source, redaction, error paths). |
| **Classification propagation & entitlement-aware loading (Phase 9) — Planned** | **June 2026** | Added Phase 9 on the Phase 8 catalog backbone: a three-tier governance posture — capture & propagate catalog classifications/owners/tiers onto target collections/fields + column-level lineage (P9.1–P9.4, incl. the mosaic = max-sensitivity rule), advise & gate via a pre-load entitlement report + exclude-above-threshold default + transform-at-load masking reusing the field-expression engine (P9.5–P9.6, P9.9), and enable enforcement by emitting a classification manifest + suggested ArangoDB RBAC / OPA / tier-layout artifacts (r2g never enforces) plus classification re-sync (P9.7–P9.8). Lane discipline: r2g carries governance metadata and refuses to silently launder sensitive data, but is not a runtime authz engine. Implementation + test plan drafted (`docs/internal/PLAN-classification-entitlement.md`); no code yet. |
| **LLM-assisted ontology derivation (Phase 10) — 10a + 10b + 10c implemented** | **June–July 2026** | Promoted the exploratory "ontology derivation" idea to committed Phase 10: an optional `LLMProvider` abstraction proposes a richer target ontology (vertex-vs-edge, implicit relationships, embed-vs-link, naming) from the introspected schema, which flows through the **same** `validate_config` → mapper-review (`diff_mappings`) → loader path as Auto-Map. Design principles: human-in-the-loop (never auto-applied), schema-grounded (metadata not bulk rows), classification-aware (no egress of Phase-9 Restricted/PII columns), validated/hallucination-resistant, reproducible (temperature 0 + stored provenance), and optional/provider-agnostic (deterministic `generate_default_config` stays the default). **10a (June 2026):** `src/r2g/llm/` (provider seam + OpenAI REST provider + metadata-only/redacted/injection-hardened prompt builder + `proposal_to_mapping` hallucination gate), the `r2g ontology suggest` CLI, the `r2g-arango[llm]` extra, and a network-free fake-provider test suite. **10b (July 2026):** `POST /suggest-ontology` (read-only proposal + diff) and `POST /apply-ontology` (accepted-subset → editable draft, not persisted) on the UI server, plus a Studio "Suggest model (AI)" action (Actions + canvas menu + `m`) opening a floating review panel with per-item accept/reject that loads the draft into the mapper for Save. **10c (July 2026):** additional providers behind the same factory — Anthropic (Claude) and OpenAI-compatible/local endpoints (Ollama, vLLM, LM Studio, …) via `OpenAIProvider` + `base_url` with an optional key — plus opt-in, classification-filtered value sampling in the prompt builder (a bounded `sample_values` probe on the value samplers; Restricted/PII columns never sampled), threaded through the CLI (`--provider/--model/--api-key/--base-url/--sample`), API, and Studio dialog, and a `R2G_LLM_LIVE`-gated live smoke test. Implementation + test plan: `docs/internal/PLAN-llm-ontology-derivation.md`. |
| **Denormalization & normal-form analysis (Phase 11) — Planned** | **June 2026** | Added Phase 11: a deterministic (no-LLM) analyzer (`src/r2g/denorm.py`) that detects source-side denormalization — embedded lookups (functional/transitive dependencies via bounded value sampling), repeating column groups, multi-valued columns, redundant reference data, and over-split 1:1 tables — and emits scored, evidence-backed findings with recommended graph remedies (extract vertex / embed array / split / merge). Reuses the `fk_inference.py` machinery (name heuristics + `create_value_sampler` bounded sampling + Suggest-FKs-style review card). Advisory by default (no silent rewrite), classification-aware (no sampling of Phase-9 Restricted/PII columns), and grounds the Phase 10 LLM proposal. Implementation + test plan drafted (`docs/internal/PLAN-denormalization-analysis.md`); no code yet. |
| **Enforcement artifacts & re-sync (Phase 9c) — Implemented** | **June 2026** | Shipped the 9c "enable enforcement — emit, don't enforce" layer. `src/r2g/governance.py` emits a canonical `classification-manifest.json`, a `suggested-rbac.json` (per-clearance cumulative ArangoDB collection read-grants), a default-deny `policy.rego` OPA stub (collection level vs. principal clearance; no `opa` dependency), and an optional `tier-layout.json` recommendation — all keyed by target collection names, written under `<project>/governance/`. Surfaced via `r2g entitlements emit` (`--threshold`/`--out`/`--tier-layout`/`--no-rego`), `POST /api/projects/{name}/governance/emit`, and `emit_governance`/`tier_layout` on the load endpoint. `r2g catalog resync-classifications <source>` re-pulls classifications/owners/tier from the catalog the source was imported from (provenance now on `SourceConfig`: `catalog_name`/`catalog_asset_fqn`/`classifications_synced_at`) and re-merges onto the latest snapshot. Lane discipline preserved: r2g emits metadata + recommendations; the serving layer (ArangoDB RBAC / OPA / IdP) enforces. CDC/temporal carry of classification *changes* remains, folded into the temporal-mode work. |
| **Entitlement report, gate & masking (Phase 9b) — Implemented** | **June 2026** | Shipped the 9b advise-and-gate layer end to end (backend + Studio UI). `src/r2g/governance.py` builds an entitlement report (every target property, its source-column lineage, mosaic-recomputed level, masked flag) and a default-exclude threshold gate that adds above-threshold *unmasked* source columns to `exclude_fields` for the run (overridable with `allow_sensitive`), plus a `governance/lineage.json` manifest recording each field's handling (loaded/excluded/masked). `src/r2g/masking.py` provides `hash`/`tokenize`/`redact`/`nullify` masking via the existing `FieldExpression` engine (AQL-delegated for hashes), sentinel-tagged so the gate treats masked fields as safe. Surfaced through `r2g entitlements report <project> [--threshold] [--json]`, `GET /api/projects/{name}/entitlements`, and the load gate on `POST /api/projects/{name}/load` (reports the excluded set; writes lineage). Lane discipline preserved: advise + emit, never enforce. UI (sensitivity lens + entitlement panel + one-click mask) still pending. 1288 tests passing, ruff + mypy clean. |
| **Classification capture & propagate (Phase 9a) — Implemented** | **June 2026** | Shipped the 9a governance backbone (capture + carrier + lattice), fully unit-tested against the mocked OpenMetadata seam. New `Classification` model (`tags`/`tier`/`glossary_terms`/`source`) on `Column`; `CatalogAsset`/`ResolvedSource` gain `column_classifications` + `owners` + `tier`. `OpenMetadataProvider` now requests `columns,tags,owners` and parses per-column tags, table owners, and the `Tier.*` confidentiality tier (glossary terms split out by tag `source`); `resolve_source` captures `table → column → Classification` for table/schema/database assets (best-effort — a limited governance API never blocks import). The map persists on `SourceConfig.classifications` (+ `data_owners`/`data_tier`) at `catalog import-source` and is merged onto `Column.classification` at `source snapshot`. New `src/r2g/classification.py`: the sensitivity lattice (`public < internal < confidential < restricted`), an overridable tag/tier→level map, `max_sensitivity`/`exceeds_threshold`/`tier_of`, and `recompute_mosaic` (the P9.4 max-of-contributors rule over fan-in properties, collections, and edges). Advise/gate (9b) and emit-enforcement artifacts + re-sync (9c) remain. 1263 tests passing, ruff + mypy clean. |
| **Denormalization analysis (Phase 11c) — Implemented (advisory)** | **June 2026** | Closed Phase 11 with the advisory remediation + grounding layer. `remediation_hint(finding)` returns concrete, deterministic per-kind guidance, surfaced in the CLI (a "Suggested remediation" column + advisory footer), the API (a `hint` field on each finding via `with_hints`), and the Studio findings card. `summarize_findings_for_prompt(findings)` renders a compact, confidence-ranked digest for the Phase-10 ontology prompt builder (P11.10), with no raw sampled values. **Mechanical auto-apply was deliberately deferred**: the recommended remedies (extract-vertex, merge, split/combine-to-array) are not representable in the current source-table-bound `MappingConfig` + AQL expression engine (no synthetic collections, no `SPLIT`/array literals); auto-rewriting would fail `validate_config`. Deferred to a future mapping/expression-model extension — consistent with the phase principle "advise, never silently rewrite". 1234 tests passing, mypy clean. |
| **Denormalization analysis (Phase 11b) — Implemented** | **June 2026** | Added the remaining detectors and the Studio review surface. Detectors: **multi-valued** columns (a new bounded `delimiter_rate` probe on all four samplers, comma/semicolon/pipe/tab), **redundant reference** data (very low distinct ratio on text columns, suppressed where the column is already explained by an embedded-lookup finding), and **1:1 over-normalization** (purely structural — a table whose entire PK is also a FK to another table → recommend merge/embed). New read-only **Studio findings card** (`analyzeDenorm` / `renderDenormFindings`), reachable from the Actions menu, the canvas context menu, and the `n` shortcut, cloned from the Suggest-FKs card; advisory-only (remediation accept lands in 11c). 1229 tests passing, mypy clean. |
| **Denormalization analysis (Phase 11a) — Implemented** | **June 2026** | Shipped the deterministic core: `src/r2g/denorm.py` (`DenormFinding`, `AnalyzeOptions`, `analyze_denormalization`) with a structural repeating-group detector and a sampler-gated embedded-lookup (functional-dependency) detector. Extended all four value samplers (PG/MySQL/SQL Server/CSV) with bounded `distinct_ratio` + `group_single_valued` probes, surfaced through the existing `create_value_sampler`. New read-only entry points (P11.7 complete): `r2g source analyze-denorm <name> [--sample] [--sample-limit] [--min-confidence] [--no-sample-columns] [--json]` CLI and `POST /api/sources/{name}/analyze-denorm`. Advisory only; classification gate (`no_sample_columns` + `is_sampleable` hook) keeps sensitive columns out of the sampler. Unit-tested with a fake sampler; 1221 tests passing, mypy clean. 11b (remaining detectors + Studio review card) and 11c (remediation scaffolding + Phase-10 grounding) remain. |

| **Positioning / production boundary** | **June 2026** | Corrected r2g's status across README + PRD §1 + subtitle to reflect reality: other projects in ArangoDB Solutions actively adopt r2g's introspection/analysis core (`relational-schema-analyzer`, RSA); the contextual-data-fabric building blocks depend on it, so RSA carries the production bar. r2g is the *reference application* composing it. Downstream systems depend on RSA + named tested r2g modules, not on r2g wholesale. Removed the blanket "experimental/educational/not intended to supersede any ArangoDB roadmap" framing — an undersell of the tested core and, given active adoption, inaccurate. |
| **P12.7 Snowflake pulled forward + re-scoped (fabric Sprint 2)** | **July 2026** | Snowflake dialect due 2026-07-24 for the fabric's three-source federation demo; scope narrowed to CSI+R2RML export over a Snowflake-introspected schema (Ontop does SPARQL→SQL per fabric ADR-0001). Uppercase identifier folding: physical names flow verbatim from schema.json into rr:tableName/rr:column; conceptual names via the CC-12 naming layer. |
| **Federated query & runtime mappings (Phase 12) — Planned** | **July 2026** | Added Phase 12 for the Contextual Data Fabric (fabric M4/M5; spec `contextual-data-fabric/docs/architecture/_repo-enhancements/r2g-federated-query.md`): expose mappings as a versioned runtime artifact (P12.1), compile mapping + predicate to parameterized pushdown SQL (P12.2 — the new build; Postgres first, Snowflake/Databricks later), a documented no-materialization mode (P12.3), and a citation contract returning query text + bind params + source objects + as-of timestamp alongside results (P12.4). P2+: query fragmentation across sources, deterministic-first with optional LLM safety net via the Phase 10 provider seam, additional dialects. Ships behind a named, pinnable module (RSA precedent). Read-only by construction; classification-aware (Phase 9 gates apply to federated reads). No code yet. |
| **Phase 12 P12.1 shipped; Ontop adopted (Apache-2.0 OSS)** | **July 2026** | P12.1 — the durable contract — implemented: forward `CSI v1` emitter (`src/r2g/csi.py`, `export-csi`) and R2RML emitter (`src/r2g/r2rml.py`, `export-r2rml`), both load-independent, feeding the fabric's relational leg (R2RML→Ontop) and Arango leg (CSI→MappingBundle→arango-sparql-py). The ADR-0001 **adopt-vs-build** decision resolved to **adopt Ontop** — **Apache-2.0 open source, *not* a purchase** (no license cost; only the operational cost of running the service) — so native pushdown-SQL generation (P12.2/P12.4/P12.6/P12.7) is **retired as the target**, kept only as a documented fallback. Verified end-to-end against a live Ontop + Postgres + ArangoDB federation. |
| **ClickHouse source connector (federation mapping + ETL) — WP-CH2** | **July 2026** | ClickHouse added as a supported source following the Phase 6 `SourceConnector` pattern: `src/r2g/connectors/clickhouse.py` — `ClickHouseConnector.get_schema` introspects `system.tables`/`system.columns` (namespace = database; PK from `is_in_primary_key`; `Nullable`/`LowCardinality` stripped), feeding `generate-config → export-csi/export-r2rml` (federation); `ClickHouseSession` adds `count_rows`/`stream_rows` (server-side block streaming)/`dump_table_to_csv` (ETL into ArangoDB). Registered in `src/r2g/connectors/base.py` (`SUPPORTED_SOURCE_TYPES` + `create_source_connector`); `src/r2g/r2rml.py` maps ClickHouse `Int*`/`UInt*` → `xsd:integer`; optional `[clickhouse]` extra (`clickhouse-connect`). ClickHouse is the relational leg **Ontop has no dialect for**, so the fabric (contextual-data-fabric) federates it via a **native BGP→SQL executor driven by r2g's R2RML export** — the retired P12.2 native-generation path realized for exactly the engine the adopt-Ontop decision doesn't reach. Live-validated against clickhouse-server 24.8: introspection, streaming, and end-to-end 3-engine federation (Postgres⋈ClickHouse⋈ArangoDB on `accountId`). |
| **P6.7 Cross-source shared-key inference — Planned** | **August 2026** | Added P6.7 to the Phase 6 FK-inference family (no code yet). The ClickHouse work exposed the gap: P6.6 inference is single-schema and name-anchored to an in-schema parent table (RSA `_candidates_for_column` — `{prefix}_id → prefix(s).pk` or direct PK-name match; the value-overlap sampler only *reweights* name-derived candidates, never creates them), so a cross-source join key such as `account_id` (shared by ClickHouse `query_events`/`usage_metrics`, FK to the Postgres CRM `Account`) yields zero candidates — the federation join key is invisible to inference. P6.7 proposes shared-key **hub** relationships across two or more sources under the same confirm-to-accept discipline, feeding accepted keys into the Phase-12 CSI/R2RML → E1 routing path. Reverse-drift `new-requirement` accepted for review (prd_patches `r2g_REQ-XSK_20260807`). |
| **P6.7 Cross-source shared-key inference — Done (producer side)** | **August 2026** | P6.7 implemented: `src/r2g/xsk.py` groups key-shaped columns co-occurring across two or more sources, gates them on JSON-level type compatibility, and proposes a hub — the owning table (sole PK, else a name-matched table) or, when no source owns the key, a **virtual hub concept with no table invented for it**. Accepted keys persist on `MappingConfig.shared_keys` and export as `conceptualModel.joinKeys`; surfaced via `POST /api/shared-keys/infer`, `r2g shared-keys infer`, and a Studio card (shortcut `k`). Confirm-to-accept throughout, and every grouped-but-excluded column is returned with a machine-readable reason, so "no candidates" is always distinguishable from "filtered out". Verified live on `clickhouse_analytics` + `crm`: `account_id` → hub `Account` @ `crm.accounts.account_id` with `query_events` / `usage_metrics` as references — exactly the requirement's stated example. Also fixed `DEFAULT_TYPE_MAP`, which had no ClickHouse entries, so `UInt64`/`UInt8` fell through to `"string"` and the type-compatibility gate would have rejected a ClickHouse `UInt64` key against a PostgreSQL `bigint`. Consumer side (Phase-12 E1 partitioner routing, R2RML join keys) remains open — drift alert `P6.7-E1-ROUTING`. Applied from prd_patches `r2g_P6.7_20260815`. |
| **P12.1 Conceptual label uniqueness in CSI** | **August 2026** | Reported downstream with benchmark evidence: two entities emitting the same attribute label (`Contract.renewalDate` / `Opportunity.renewalDate`) made every question mentioning it ambiguous by construction for a consumer deriving a flat vocabulary, and renaming six colliding labels by hand moved that benchmark 0% → 25% on three-source questions and 60% → 69% overall, after four algorithmic attempts in the consuming repo moved nothing. Resolved at extraction time under `export-csi --label-policy` (`qualify` \| `roles` \| `warn` \| `off`), with collisions always recorded in `provenance.labelCollisions`. Measured over the six shipped catalogs: `roles` leaves **zero** duplicate labels everywhere, where `qualify` cannot resolve `id` in `crm_demo` / `library_graph`; classification totals 34 semantic / 23 owner-canonical / 6 identity with **zero** ambiguous (mixed-role) cases. `qualify` remains the default pending a benchmark comparison of the two policies in the consuming repo. Applied from prd_patches `r2g_P12.1-LABELS_20260815`. |
| **Phase 13 — LPG target layout** | **September 2026** | Added a selectable ArangoDB target shape: `graph_layout: pg` (default, unchanged) or `lpg` (one node collection with a `labels` array, one edge collection with a `type` property). Collapsing the per-type collections exposes two hazards that lose data *silently* — `customers/1` and `orders/1` both become `nodes/1`, and two edge types joining the same pair share one `_key` — so node keys are label-namespaced and edge keys lead with the type. Filtering is restored by **vertex-centric indexes** over copies of each edge's endpoint labels (`[_from, type, toLabels[*]]` and the inbound mirror); the copies are load-bearing, since labels left on the neighbour node are unreachable from an index on the edge collection. **Measured, not assumed:** a traversal uses those indexes only with the **nested** per-collection/per-direction/per-level `indexHint` — the flat `indexHint: 'name'` form is silently ignored, not even `forceIndexHint` overrides it — which made the limitation look fundamental when it was syntactic. Verified against live ArangoDB 3.12.9 for both directions including the array-expanded label field; the unhinted fallback is pinned by a test so the reason the hint exists stays visible. P13.6 (pipeline routing) and P13.7 (layout-aware CSI export) remain planned. Applied from prd_patches `r2g_P13_20260902`. |
| **Phase 13 multi-label filter contract (P13.10)** | **September 2026** | Verified the multi-label case the `labels`/`toLabels` arrays exist for: with 3-label nodes and multi-element edge copies on ArangoDB 3.12.9, all four AQL array-comparison operators are correct **and** engage the vertex-centric index, mapping 1:1 onto Cypher (`IN`=`:A`, `ALL IN`=`:A:B`, `ANY IN`=`:A|B`, `NONE IN`=negation), exposed as `label_predicate()`. The load-bearing finding is a silent-wrong-answer trap for AQL-transpiling consumers: for a **list-valued** field `p.edges[*].toLabels` expands to a *nested* array, so `… ALL == @l` and `@l IN p.edges[*]…` both match nothing and return an **empty result with no error** — the inverse of the scalar trap where `[*] ALL ==` over one edge is trivially true. `FLATTEN()` only expresses 'some hop'; chained operators are a syntax error; the correct form is a filter on the edge variable, applied per hop and index-servable. Also recorded two limitations the array shape invites people to assume away: r2g emits exactly **one** label per node, and an edge's endpoint labels are a denormalized copy that goes stale if labels change after load. Applied from prd_patches `r2g_P13.10_20260902`. |
| **Phase 13 index correction: label arrays must not be indexed on edges** | **September 2026** | Two P13 statements were falsified by measurement across ArangoDB 3.12.9 and 3.12.11. (1) The edge vertex-centric indexes dropped their array-expansion field: a persistent index over `toLabels[*]` stores one entry per element and a traversal scans every entry under the `_from`/`type` prefix, so each edge was returned **once per label on its target** — 1/2/3/4 rows for targets carrying 1/2/3/4 labels, versus exactly one from the built-in edge index and from the flat `[_from, type]` spec. The multiplier is introduced by the very multi-label support the arrays exist for, the symptom is inflated counts rather than an error, and there is no query-side repair (`uniqueEdges: 'global'` is ERR 10; `RETURN DISTINCT` hides it while the work is still done). It went unnoticed because the multi-label tests ended in `RETURN DISTINCT` — the defect was in the assertion's convenience, not the query. (2) The claim that no correct pushed-down all-hops label test exists was version-bound: **v3.12.11** added inline-`FILTER` pushdown, so the inverted form `p.edges[* FILTER @l NOT IN CURRENT.toLabels]._key NONE != null` is now pushed into the traverser. Its positive `ALL` counterpart is a vacuous-truth trap that accepts what it should reject. Applied from prd_patches `r2g_P13.3-P13.10_20260902b`. |
| **Multi-label node production (P13.13)** | **September 2026** | r2g can now emit more than one label per node: a collection declares `extra_labels` and its nodes carry `target_collection` plus those. The primary label stays the sole basis for the `_key` namespace and the CSI physical `typeValue`, so adding a label widens what a node answers to **without relocating it**. The load-bearing part is the edge copies — `fromLabels`/`toLabels` carry the endpoints' full label sets from **both** writers (streaming and CDC), because those copies are what traversals filter against: an edge carrying only the primary label while its endpoint carries three would make every filter on the other two silently miss, and updating only one writer would leave bulk-loaded and CDC-written edges disagreeing about the same graph. **Deliberately deferred until three prerequisites were resolved** — the `[*]` nesting bug (list-valued path filters returning empty with no error), the absence of a pushed-down all-hops label test (closed by inline-`FILTER` pushdown in v3.12.11), and the array-index duplication (indexing `toLabels[*]` on edges returned each edge once per label). Building multi-label before those were understood would have rested it on three silent bugs. Applied from prd_patches `r2g_P13.13_20260902`. |
| **Expression semantics corrections (P5c.1.4, P5c.1.6)** | **October 2026** | Three corrections from the transformation-language research (`docs/research/transformation-language-evaluation.md`, PR #10), reviewed as proposed patches. (1) P5c.1.4 no longer calls the evaluator AQL-semantic: it lists where it differs from ArangoDB 3.12.9 (null handling, `\|\|`/`&&`, `+` on text, cross-type comparison, empty-array truthiness, `COALESCE`/`??`), measured through r2g's own delegation query. (2) P5c.1.4 now requires engines with no backend (`ksql`, `python`) to be rejected when a load builds its transformers, and by `validate_config`, instead of loading the raw source value with a warning; P5c.2.5, the engines table and the out-of-scope note no longer promise KSQL. (3) P5c.1.6 now requires bulk and CDC loads to evaluate or refuse delegated expressions, never to omit or erase the field silently. P5c.1.4, P5c.1.6 and P5c.2.5 move from Done to Partial until the code follows. |
| **Control-plane mode** | **October 2026** | Phase 14 added: the Studio as a tool of a context-and-governance control plane (signed context per tab, the project's transformation project and ontology, storage per tenant), P14.1–P14.7; ontology terms on mapping elements and governed mapping changes, P14.8–P14.13 (detailed 2026-10-29: the class that governs each element, loads checked per collection, every change route governed, served as a read, or refused). Sources, targets and catalogs stay tenant-wide, changed only from the CLI under a control plane. |

The source files `PRD-gemini.md` and `PRD-notebooklm.md` remain in the repository for reference and are superseded by this file.
