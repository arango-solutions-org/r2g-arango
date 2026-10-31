# Changelog

All notable changes to `r2g` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aspires to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Under a control plane, the Studio's changes are checked and recorded (PRD
  P14.9–P14.13).** Saving a mapping compares it with the saved one in full (every field
  of every table and relationship, and every mapping-wide setting) and asks the control
  plane's policy about each difference before writing anything: a table on its class, a
  column on its table's class, a relationship on its start table's class, a class change
  on both classes, and anything naming no class on the project-wide rules. If any part is
  refused the save is refused whole (403), saying which part and why. Loads and
  migrations are checked on every table and relationship they write. Each change and
  each refusal goes into the project's history through a queue in the tenant's catalog
  folder, resent under the same id until delivered or until the user's context expires;
  every saved mapping version is kept under `projects/<name>/versions/`, named by its
  hash. Of the 30 change routes, 5 are governed, 14 store nothing and are served, and 11
  are refused saying why (sources, targets and catalogs, deleting the project, demo
  presets, governance files); any other is refused by default. A load that drops its
  collections is checked on everything those collections hold, so in the graph layout with
  shared collections, on every class. A load ends by declaring the project's named graph,
  which creates any collection it names that the database lacks, empty: that step is
  checked on the project-wide rules when it changes something (a new graph, one declared
  differently, or a collection the load does not itself write), and recorded with the
  load; once a project is laid out, a load is checked on its tables alone. Creating a
  project no longer reads a mapping file outside the tenant's catalog folder or inside
  another project's folder, and a project
  that starts from a folder left by a deleted one is checked on that folder's mapping.
  Suggesting an ontology no longer reads the server's environment for a request: a
  `$VAR` API key is refused, and so is a `base_url` without the request's own key, which
  would otherwise receive the server's key. r2g declares
  `mapping.update`, `data.load` and `data.migrate` (each taking a class optionally) and
  `transformation.update`, which need the control plane's SDK with optional terms.
  Without `CONTROL_PLANE_URL` nothing changes.

- **Mapping elements name the ontology term they stand for (PRD P14.8).** A table may
  name a class (`term`), each source column a property (`field_terms`), and a
  relationship a property (`term`), as absolute IRIs kept in the mapping file. They are
  optional and omitted while unset, so existing mappings are written byte-identically.
  Under a control plane the Studio offers the project ontology's classes and properties,
  read from AOE (every page of classes, then the properties), and a save that names a new
  or changed term is refused (400) unless the term is in the project ontology, of the
  right kind; when the ontology cannot be read, such a save is refused (503) rather than
  written unchecked. A term already saved is not checked again. The page carries every
  term in any mode, including in the copy of the mapping it opens a project with
  (`/graph-data`), so a save never drops them. `control_plane.ontology_classes` is
  replaced by `ontology_terms`, and `/api/control-plane/context` lists properties too.
  Without `CONTROL_PLANE_URL` nothing reads the terms.

- **The Studio can run as a tool of a context-and-governance control plane (r2g-arango#44, PRD
  P14.1–P14.7).** With `CONTROL_PLANE_URL` set, every call except the page, its
  assets, `/api/health`, `/api/mode` and (with `R2G_CORS_ORIGINS` set) a CORS preflight
  needs the control plane's
  signed context token, verified offline by its SDK (imported only in this mode);
  `R2G_API_TOKEN` is not used. The check reads the path the router matches, so it holds
  when the app is mounted with a `root_path`. Only the load-progress stream takes the
  token as `?token=`, and the access log records it as `<redacted>`. A context the
  control plane refuses when asked for its project is a 401, not a 503. A load's
  status, progress and rejected rows are reached only through the project that started
  it. Only the transformation project the control-plane
  project names for r2g can be listed, read, changed, loaded or created; any other
  answers 404. `GET /api/control-plane/context` returns who, which project, and the
  project ontology's classes read from `AOE_URL`. The page keeps the token per tab,
  renews it, and works under any path prefix. Fail-closed: the Studio refuses to start
  without `R2G_TENANT`, and the MCP server refuses to start in this mode. Sources,
  targets and catalogs stay tenant-wide. `control-plane.Dockerfile` builds the image.
  With `CONTROL_PLANE_URL` unset nothing changes.

### Changed

- **`r2g stream --source <name>` no longer needs `--schema`.** Without it, the source's
  latest snapshot is the schema, as for the Studio's load, so a catalog project loads
  from the command line with its source and mapping alone. `--pg-conn` still needs
  `--schema`; a source with no snapshot says how to take one.

- **The CLI honours `R2G_CATALOG_DIR`**, as the MCP server already did, so storage can
  be per tenant (PRD P14.6).
- **A load's rejected rows are kept with the catalog, per project**:
  `<catalog dir>/dlq/<project>/<load_id>.jsonl` (`~/.r2g/dlq/<project>/` by default; was
  `~/.r2g/dlq/<load_id>.jsonl`). Outside control-plane mode the Studio still shows rows
  kept at the old path (PRD P5b.3.3, P14.6).
- **The Studio's "Suggest model (AI)" sends the server's LLM key only to the provider's
  own address** (PRD P10.1). A request that sets a base URL passes its own key; a keyless
  local server (Ollama and the like) still works, and is sent no key. A gateway the
  server's key should go to is set on the server, as `$OPENAI_BASE_URL`. A `$VAR` key may
  name only `$OPENAI_API_KEY` or `$ANTHROPIC_API_KEY`. The CLI is unchanged.

### Fixed

- **The Studio no longer drops mapping settings it has no editor for.** Opening a
  project builds the editor from `/graph-data`, whose copy of the mapping left out the
  graph layout and its LPG settings and each collection's `extra_labels`; the page also
  never sent `naming_convention`. One reopen and save turned an LPG mapping back into the
  default layout without its extra labels, and lost the naming convention. All of them
  now go both ways.

- **`apiFetch` keeps the auth header when a caller passes its own headers**, and the
  expression editor's three direct `fetch` calls now send it.

- **R2RML names a Postgres table's schema outside `public`.** `rr:tableName` was written
  bare, and Ontop resolves a bare name in the connection's first schema only, so a
  mapping over tables in two or more schemas failed at start-up. A Postgres source read
  with `--pg-schema X` (not `public`) now writes `schema_name` in its snapshot and
  `"X"."table"` in its R2RML. `public`-only snapshots and mappings are unchanged
  (PRD P12.1).
  - Such a source gets a **new schema fingerprint once**, at its next snapshot, with no
    schema change: the recorded schema is now part of what is fingerprinted.
  - A schema produced outside r2g's connectors that records a schema other than `public`
    (for example `dbo`, or Snowflake's `PUBLIC`) now gets `"dbo"."t"` in its R2RML. That
    resolves in its own dialect; only the text changes.

## [0.8.2] — 2026-10-29

### Changed

- **`csiVersion` follows the mappings (r2g-arango#39, PRD P12.14).** CSI 1.1
  (arangodb-schema-analyzer 0.18.0, arango-sparql-py 0.5.0) adds `path` to a property
  mapping, for a value inside a nested object. `export-csi` now derives the version with
  `csi_version_for`: `"1.1"` only when some property mapping uses `path`, `"1"` otherwise.
  r2g writes `field` only, so its output is unchanged and still says `"1"`; a later change
  that emits `path` cannot keep the old version by accident.
- **CSI property mappings hold only CSI 1.1's strict keys.** `field`, `path`, `type`,
  `indexed`, `unique`, `required`, `role`, `sentinelValues`, `numericLike`,
  `sampleValues`, or an `x-` key (`CSI_PROPERTY_MAPPING_KEYS`). A test generates CSI six
  ways and fails if any property mapping holds another key.
- **The vendored CSI schema follows the analyzer's 0.18.0 copy**, and the
  `arangodb-schema-analyzer` floor in `[test]` and `[dev]` is `>=0.18.0`, so the drift
  test compares against a release that has the 1.1 schema.

## [0.8.1] — 2026-10-26

### Fixed

- **A foreign-key column that only repeats its table's name no longer names the class
  (PRD P12.13).** 0.8.0 read `orders.customers_id` -> `customers` as the schema
  designer's word for one row, so a schema that names every key `<table>_id` got
  `Customers` and `Orders` instead of `Customer` and `Order`. Nothing in one table tells
  that habit from a designer calling one row "goods", so such a column is no longer
  evidence at all, and a table's class name depends only on that table and the keys that
  reference it. Evidence is now a column that names one row with a singular of its own
  (`plans.basis_id` -> `bases` names `Basis`, where the singularizer gives `Base`).
  **Output change**: classes 0.8.0 named from a repeated table name return to the
  singularizer's name — `customers` -> `Customer` again, and also `goods` -> `Good`;
  name such a table with a curated `identifier` (`"identifier": "Goods"`, P12.12).
  Chinook, Northwind, Pagila and contextual-data-fabric's committed schemas are
  unchanged.
- **A collection two tables write to keeps the singularizer's class name**, instead of a
  name that depended on the order of the mapping.
- **`export-csi` without `--schema` warns** that its class names can differ from
  `export-r2rml`'s, which always reads the schema's foreign keys.

## [0.8.0] — 2026-10-26

### Changed

- **Class names consult a dictionary, then the schema's own foreign keys (r2g-arango#30,
  PRD P12.13).** The 0.7.0 rules still failed wherever nobody had listed the word:
  `cactus` -> `Cactu`, `knives` -> `Knive`, `analytics` -> `Analytic`, and `cacti`,
  `criteria`, `mice` unchanged. r2g's singularizer now checks the named lists, then
  relational-schema-analyzer's vendored noun dictionary, then the old rules for unknown
  words; on a held-out set of 72 words it scores 68 (0.7.0: 44). On top of that, when a
  schema's declared foreign-key columns name a table one way (`orders.goods_id` ->
  `goods`) and the singularizer says another (`Good`), the schema wins (`Goods`) and
  `provenance.entityNaming` records the columns. **This renames classes** for tables named
  this way the next time CSI or R2RML is exported; exported files and loaded graphs do
  not change, and ArangoDB collection names never do. Every class name in Chinook,
  Northwind, Pagila and contextual-data-fabric's committed CSI is unchanged.
- **relational-schema-analyzer 0.11.0 (band `>=0.11.0,<0.12.0`)**, which carries the
  dictionary; the `test` and `dev` extras require arangodb-schema-analyzer 0.17.0, whose
  vendored copy must match RSA's byte for byte.

### Added

- **Curated labels take an explicit `identifier` (r2g-arango#34, PRD P12.12).** An entry's
  `identifier` is used exactly as written, never singularized or re-cased (`Sales` stays
  `Sales`; without it the label `Sales` names `Sale`), and lets a label in a non-Latin
  script keep its words while the identifier names the concept. It must already be CC-12
  shaped and still goes through every collision and join-key refusal.

## [0.7.0] — 2026-10-21

### Added

- **Curated labels at export (PRD P12.12).** `export-csi` and `export-r2rml` take
  `--curated-labels FILE`: a curator's names for entities, properties and relationships,
  keyed by the names r2g generates (arango-ontoextract's lexicon exports this file). Both
  exports apply the same renames, so the SQL and graph legs agree; physical fields keep
  their names. The curator's words are kept as `prefLabel` and the old name as an
  `altLabels` entry. A rename that would break a join key or give two entities one label
  is refused and recorded in `provenance.curatedLabels`, and the export fails while any
  entry is refused or matches nothing, unless `--allow-unapplied-labels` is given.
  Accents in a label are folded (`Café owner` → `cafeOwner`); a label with letters that
  have no ASCII form, or no letters at all, is refused. A rename changes the name every
  consumer sees, so contextual-data-fabric manifests that list the old names must be
  updated with it.

### Changed

- **Class names no longer cut common plurals short (r2g-arango#30).** The singularizer
  behind every CSI and R2RML class name turned `warehouses` into `Warehous`, `courses` into
  `Cours`, `sizes` into `Siz`, `order_status` into `OrderStatu`, `analysis` into `Analysi`,
  `news` into `New` and `movies` into `Movy`. It now drops `-es` only after a sibilant
  (`addresses`, `boxes`, `batches`) or a singular that already ends in `s` (`statuses` →
  `Status`), otherwise only the `s`; turns `-ses` into `-sis` for `-is` words; keeps `-ie`
  words (`movies` → `Movie`); and leaves invariant words (`data`, `news`, `series`) and a
  short irregular table (`people` → `Person`) to named lists. `pluralize`, which the Forge
  uses for table names, is its inverse. **This renames classes** for any table named that
  way the next time CSI or R2RML is exported; already-exported files and loaded graphs do
  not change, and ArangoDB collection names never do. Cross-source key inference (`xsk`)
  now uses the same function instead of its own copy.
- **relational-schema-analyzer 0.10.0 (band `>=0.10.0,<0.11.0`).** RSA carries the same
  singularizer rules and publishes the list of known answers that
  `tests/test_naming_known_answers.py` checks r2g's copy, RSA's and
  arangodb-schema-analyzer's against. It also now reads declared `CHECK (col IN (…))`
  value lists from live PostgreSQL, MySQL and SQL Server, so such a schema gets a new
  `physicalSchemaFingerprint` once; bitemporal `valid_from` continuity by fingerprint
  restarts on that first export. The `test` and `dev` extras require
  arangodb-schema-analyzer 0.16.0, so CI's test job runs the ASA cross-check rather than
  skipping it.

### Security

- **Generated import scripts no longer put the ArangoDB password in the process list.**
  `arangoimport` and `arangosh` commands passed `--server.password "$ARANGO_PASSWORD"`,
  which the shell expands before running the command, so any user on the host could read
  the password with `ps` for as long as the import ran. They now pass
  `@ARANGO_PASSWORD@`, which the ArangoDB client tool reads from its own environment. The
  script exports `ARANGO_PASSWORD`, so a password set beforehand or written into the
  script both still work.
- **CSV-direct import no longer loads masked or excluded columns in clear.**
  `generate-csv-import` and Studio's CSV-direct preview ignored a mapping's masks,
  include and exclude filters, other field expressions and type overrides, because
  `arangoimport --type csv` loads every CSV column as it stands. A column masked with
  `redact` was stored with its original value, and an excluded column was loaded. The only
  notice was a comment inside the generated script, and the command exited 0. Such a
  mapping is now refused, with an error naming each collection and feature; use JSONL
  mode (`transform-all`, then `generate-import`), which applies them. Mappings that only
  rename columns (`field_mappings`) are unaffected. This refuses mappings that used to
  generate a script.

### Fixed

- **Generated import scripts stop when `arangoimport` silently loses rows.** This works
  around an `arangoimport` defect: it discards a row whose value is not valid for the type
  it was told to use (for example, an empty field in a column typed `number`), logs only a
  warning, and exits 0, so the script used to report success with rows missing. Each
  import now runs through a check of `arangoimport`'s printed summary and the script stops
  if any row was rejected. One rejection is expected and allowed: in CSV-direct mode, an
  edge whose foreign key column is nullable and holds NULL is skipped, with a note saying
  how many, because it has nothing to link to. Known limit: `arangoimport` logs at most
  20 rejections, so for a nullable foreign key that the source does not enforce, an
  unlogged rejection of another kind can be counted as a NULL.

## [0.6.1] — 2026-10-14

### Changed

- **`r2g source suggest-keys` works on PostgreSQL sources**, not only Snowflake. It
  proposes keys for tables that declare none, and, on tables that declare only a surrogate
  key, the natural key beside it (as a UNIQUE key, never as the primary key), with the
  references those keys unlock. The same cost controls apply on both: `--max-queries`, a
  per-query `--timeout` the database enforces, a query tag (`application_name` on
  PostgreSQL), and the cost report. For a PostgreSQL schema other than `public`, use
  `--schema`; the command then prints the `--pg-schema` the snapshot needs. Other source
  types are still refused. Requires relational-schema-analyzer 0.9.5.

### Fixed

- **`source infer-fks --accept` saved guessed foreign keys as if the source enforced them.**
  Each accepted key was written with the default `enforced: true`, which the serializer
  omits, and every key from one method shared one name (`inferred_name_suffix`). A
  snapshot or `schema.json` built from it read as constraints the database declared and
  enforces, so the visualizer called them source-declared, and relational-schema-analyzer's
  baseline treated those tables as authoritative and stopped inferring there. The new
  snapshot also lost its key-overlay record. Accepted keys are now `enforced: false`, named
  `inferred:<table>_<cols>_fk` the way overlay keys are, and their method and confidence
  are recorded on the snapshot (`inferred_keys`). The overlay record is carried forward,
  and the visualizer and Studio show these keys as `inferred`.
  **Migration:** snapshots written by earlier versions keep the old names and still read as
  enforced. Re-running `--accept` does not relabel them, because it skips keys already
  declared by column. To relabel, take a fresh `r2g source snapshot`, then accept again.

## [0.6.0] — 2026-10-12

### Added

- **Key suggestion from data (`r2g source suggest-keys`).** For sources that declare no keys
  (Snowflake), proposes primary keys and the references between them from the data and writes
  a **draft** key overlay with the evidence for every entry. Nothing is applied. Every query is
  counted against `--max-queries` (default 200), each is cancelled by the warehouse after
  `--timeout` seconds, and the run's cost is reported. Uses relational-schema-analyzer 0.9.0's
  key profiler and Snowflake sampler.
- **`r2g source set-key-overlay NAME FILE`** attaches a reviewed overlay to an existing source,
  applied at the next `source snapshot`; `--clear` removes it. A file still marked DRAFT is
  refused unless `--reviewed` is passed, and structural mistakes are rejected immediately
  rather than at the next snapshot.

## [0.5.0] — 2026-10-02

### Added

- **Snowflake Customer 360 Studio demo.** Snowflake URLs now support
  Contextual Data Fabric-style key-pair authentication with env-referenced key
  files and optional passphrases. The bundled idempotent demo seeds five
  constraint-free tables, applies a reviewed RSA key overlay (5 PKs / 6 FKs),
  persists overlay provenance through snapshots, and installs an editable
  five-vertex/six-edge project from the New Source surface. Credentials and key
  paths are scrubbed from errors and persisted API output.
- **Reviewed key overlays.** `r2g source add --key-overlay <json-or-yaml>`
  parses and stores reviewed PK/FK metadata with a source. RSA applies the
  overlay when a snapshot is captured and sets the resulting foreign-key
  models to `enforced=false`; that flag is not stored in the overlay JSON.
- **Generated arangoimport script viewer.** The Studio can render the current
  validated draft as a secret-safe JSONL or CSV-direct batch bundle, grouped
  document/edge commands, graph creation, or a focused collection/edge command.
  Project, Actions, canvas, collection, and edge context surfaces open a
  floating monospace viewer with warnings, copy, and client-side download.
  Previewing performs no command execution or server-side file writes; Studio
  Load remains the direct HTTP streaming path.

### Changed

- **relational-schema-analyzer raised to `>=0.9.0,<0.10.0`.** RSA 0.9.0 reports a unique
  index with no constraint as a UNIQUE key, so foreign keys that reference such a column are
  now inferred. On Postgres this is common (pagila's `store.manager_staff_id`). These keys
  are stored in `unique_constraints`, which snapshots already keep, so the inferred FK
  survives a catalog reload. Schemas with such indexes fingerprint differently once after
  upgrading. 0.9.0 also adds the Snowflake value sampler, primary-key profiling and draft
  key overlays that `r2g source suggest-keys` will build on. r2g's own connectors still
  read neither UNIQUE constraints nor unique indexes; that is Plan B.

### Fixed

- **UNIQUE keys were lost when a snapshot was saved.** r2g's table serializer kept primary and
  foreign keys but not `unique_constraints`, so a UNIQUE key present at capture -- including one
  declared in a reviewed key overlay -- vanished on catalog reload, and any FK inferred through
  that natural key vanished with it. UNIQUE keys are now persisted; tables without one serialize
  byte-identically to before. New snapshots record `schema_format_version: 2` ("can hold UNIQUE
  keys"); older snapshots read as format 1 and re-save unchanged. The marker versions storage,
  not introspection: r2g's connectors do not read declared UNIQUE constraints yet, so today
  UNIQUE keys reach a snapshot only through a key overlay. Re-snapshot a source to recover
  overlay UNIQUE keys an older snapshot dropped.
  Related: a reloaded table now re-derives each column's `is_unique` flag from its persisted
  single-column primary and UNIQUE keys, so ontology key detection agrees with the capture; a
  format-1 snapshot whose overlay declared UNIQUE keys is detected, logged on load, and never
  reused by the Customer 360 preset; and a snapshot from a newer r2g re-saves under this
  version's format marker rather than claiming data it just dropped.
- CSV-direct generation now honors `overwrite_on_initial`; both import modes
  resolve renamed endpoint collections consistently, warn when CSV cannot
  represent mapping transforms, and reject misleading LPG previews.
- Generated named-graph commands quote graph and project names for both shell
  and JavaScript contexts. Import previews accept an operator-selected,
  shell-quoted artifact directory, return runnable secret-safe graph scripts in
  both modes, and rebuild Snowflake demo snapshots when reviewed overlay
  content changes without changing its key counts.

## [0.4.3] — 2026-09-28

### Added
- **CSI: declared `uniqueConstraints` are now semantically validated.** A CSI entity
  is `additionalProperties: true`, so a malformed `uniqueConstraints` (a flat list, a
  source column name instead of a conceptual property, or a name belonging to no
  property on the entity) previously passed validation and only surfaced far
  downstream as a federated join grouping incorrectly. `validate_csi` now checks the
  field's shape and that every referenced name resolves to a property on its entity,
  raising `jsonschema.ValidationError` with the specific faults. The JSON Schema
  itself is `arango-schema-analyzer`'s to own — r2g only vendors a copy — so this is
  enforced in code rather than by widening the vendored schema (contextual-data-fabric
  ADR-0005 D1 v2).

### Changed
- **Forge Snowflake round-trip test uses the `R2G_RT_` throwaway-schema prefix**
  (was `FORGE_RT_`), aligning the integration test with the shared naming convention.

## [0.4.2] — 2026-09-21

### Added
- **Federation Forge S2 dialects: `snowflake`, `clickhouse`, `arango`** behind the
  unchanged `generate(ontology, dialect, seed)` seam (contextual-data-fabric
  ADR-0006 D-2..D-5). `r2g.forge` is now a package: `core` (ontology contract,
  dialect-independent `SchemaPlan`, seeded `synthesize_rows`) + `dialects/` (one
  module per target behind the `DIALECTS` registry; add a dialect by following
  `dialects/base.py` and adding one registry line). Rows are synthesized once and
  byte-identical across all four dialects for the same `(ontology, seed,
  rows_per_entity)`. Snowflake emits unquoted UPPERCASE identifiers with declared
  (recorded, unenforced) PK/FK; ClickHouse emits `MergeTree ORDER BY (id)` with the
  FK intent as column comments (no FK syntax exists); Arango emits a JSON collection
  manifest (`forge.collections.json`) plus a standalone python-arango loader
  (`forge.load.py`), edge collections named as the forward Auto-Map derives them.
  Live roundtrips through the real analyzers for each: RSA `SnowflakeConnector`
  (throwaway schema; the `NUMBER`→`float` type gap is pinned by name), r2g's
  `ClickHouseConnector` + FK inference (RSA has no ClickHouse connector; D-3's
  two-branch contract asserted both ways; new compose `clickhouse` service on
  8124), and ASA's deterministic baseline for Arango. Plan §"S2 dialects".
- **Breaking (Forge input):** a conceptual property or entity whose generated
  identifier is a SQL reserved word, or starts with a digit, is now refused at
  `generate` time rather than emitted into DDL the engine rejects at load
  (PLAN F-7). Ontologies accepted by the walking skeleton may need a property or
  class renamed — `primary`, `order`, `current_date` and `array` are examples,
  and an entity named `Value` (table `values`) or `Row` (table `rows`) is now
  refused too. The list is derived from the engines rather than written:
  `scripts/derive_reserved_words.py` probes Postgres's own `pg_get_keywords()`
  against live Postgres and ClickHouse and unions Snowflake's documented set.

- **Federation Forge walking skeleton** (`r2g forge generate --ontology … --dialect …
  [--seed] [--rows-per-entity] [--out-dir]`; contextual-data-fabric ADR-0006, D-4).
  `generate(ontology, dialect, seed, rows_per_entity=10)` returns
  `ForgeArtifacts(ddl, load_sql, rows)`, with naive seeded synthesis and declared
  PK/FK always emitted. The integration test checks that `introspect(generate(O))`
  matches `O` up to the CC-12 naming normalization, with generated `id` keys and FK
  columns allowed as extras. Landed as a single `src/r2g/forge.py` for Postgres
  only; superseded within this same release by the S2 entry above, which makes it a
  package and adds three dialects. Plan in `docs/internal/PLAN-federation-forge.md`
  (F-1..F-6 at the time; F-7 reserved words and F-8 followed).

### Changed
- **`snowflake` extra floor raised to `>=3.12,<4.0`.** Earlier
  `snowflake-connector-python` releases hard-pin `pyarrow==10.0.1`, which has no
  cp312 wheel and fails to build from sdist on Python 3.12 (missing
  `pkg_resources`), breaking `pip install -e ".[all,test]"` and the whole test
  matrix. 3.12+ loosen the pin and ship cp312 wheels.

### Fixed
- **`singularize` / `pluralize` are case-insensitive.** Snowflake uppercases
  unquoted identifiers and the suffix rules only matched lowercase, so
  `USAGE_METRICS` produced the plural class `UsageMetrics`. Replacement suffixes
  now follow the case of the word's trailing character (`USAGE_METRICS` singularizes
  to `USAGE_METRIC`, so the class becomes `UsageMetric`). The forced-lowercase
  workaround downstream consumers carried becomes removable once their mappings are
  regenerated against 0.4.2 (fixes ArthurKeen/r2g-arango#4).
- **CI green again on the 3.10/3.11/3.12 matrix.** The 3.12 dependency-install
  failure above cascaded (via `fail-fast`) into cancelled 3.10/3.11 legs; the
  matrix now sets `fail-fast: false` and upgrades pip/setuptools/wheel before
  install. Also fixed 4 mypy `union-attr` errors in `src/r2g/ui/server.py`
  (cross-source sampler loop re-fetched `source`/`snapshot` without a None-guard).
- **Forge walking-skeleton defects found in the S2 review.** Foreign keys named the
  child's primary key where the parent's belonged (correct only because every
  planned table shared one surrogate key). `split_sql_statements` silently dropped
  the rest of a script after an unterminated `/*`, and mis-scanned dollar quoting
  and backtick identifiers. The Arango loader skipped `create_graph` on an existing
  graph, so a second ontology kept the first one's edge definitions and still
  exited 0. The reserved-word guard accepted `current_date` and `array` and refused
  `key` and `range`; it is now derived from the engines (see the breaking entry
  above). The integration suite never loaded `.env`, so correctly configured
  Postgres and Snowflake credentials were invisible and the tests skipped as though
  the services were down.

## [0.4.1] — 2026-09-14

### Added

- **Bitemporal provenance passthrough (CDF §3.13.5).** `mapping_to_csi`
  (`src/r2g/csi.py`) threads the four bitemporal keys — `transactionTime`,
  `validTime`, `validTimeSource`, and `predecessorFingerprint` — onto the CSI
  `provenance` envelope when supplied, and **`r2g export-csi` now forwards them
  from the schema** (`--schema` pointing at a `schema.json` stamped by
  relational-schema-analyzer >= 0.8.0). r2g neither mints nor reshapes them.
  Converged with `arango-schema-analyzer` 0.13 and `relational-schema-analyzer`
  0.8. (Recorded under 0.4.0 in error on 2026-09-12; 0.4.0 shipped 2026-08-21
  without it.)

### Changed

- **`relational-schema-analyzer` band raised to `>=0.8.0,<0.9.0`.** The 0.7 band
  excluded the release that carries the bitemporal stamps this version forwards.
- **CSI schema: `validTimeSource` admits all five RSA values** (`catalog`,
  `event`, `file`, `fingerprint-continuity`, `observed`). The two-value enum
  rejected any catalog-dated relational schema at `export-csi` validation.
- **CSI schema is no longer trusted from the vendored copy alone.**
  `csi_schema()` prefers `arango-schema-analyzer`'s authoritative copy when that
  package is importable and falls back to the vendored file otherwise;
  `tests/test_csi.py::test_vendored_csi_schema_matches_installed_analyzer` fails
  the build if the two ever differ (contextual-data-fabric unified-architecture
  paper, step 2).

## [0.4.0] — 2026-08-21

### Added

- **Contextual Data Fabric federation producer (Phase 12 / M5).** r2g is now the
  forward producer of the interchange artifacts that drive the
  contextual-data-fabric federated-query engine:
  - `r2g export-csi` (`src/r2g/csi.py`) emits a **CSI v1** document pairing the
    conceptual model with its ArangoDB physical mapping. Load-independent — a
    mapping is exported without running a migration. Feeds the fabric's Arango leg
    (CSI → MappingBundle → `arango-sparql-py`).
  - `r2g export-r2rml` (`src/r2g/r2rml.py`) emits **R2RML** for the relational leg
    (R2RML → Ontop).
- **ClickHouse source connector — federation mapping *and* ETL** (new
  `r2g-arango[clickhouse]` extra). `ClickHouseConnector` introspects
  `system.tables`/`system.columns` (sorting-key PK via `is_in_primary_key`,
  `Nullable`/`LowCardinality` stripped) and feeds `generate-config` →
  `export-csi`/`export-r2rml`; `ClickHouseSession` streams rows via server-side
  block streaming for load into ArangoDB. `Int*`/`UInt*` map to `xsd:integer`.
  Registered in `SUPPORTED_SOURCE_TYPES` / `create_source_connector` alongside
  PostgreSQL, MySQL, SQL Server, Snowflake, CSV, and Kafka.
- **Cross-source shared-key inference (PRD P6.7).** Accepted shared keys are
  emitted as `conceptualModel.joinKeys` in the CSI export — the declared
  cross-source join keys the federated executor bind-joins on, with no
  materialized edge. `r2g.xsk` proposes shared-key hub relationships across
  sources under a confirm-to-accept discipline.
- **CC-12 OWL naming in exported conceptual models.** CSI/R2RML exports use
  singular PascalCase entity names and lowerCamel property names; the physical
  collection/table names are preserved in the physical mapping.
- **Attribute-label collision policy for CSI export**
  (`--label-policy qualify|roles|warn|off`). Labels shared by two entities are
  resolved deterministically and every collision recorded in
  `provenance.labelCollisions`. Declared P6.7 join keys are exempt — kept bare on
  every binding entity so the federation join spine survives.
- **Serialization compatibility corpus** (`tests/test_serialization_compat.py` +
  frozen fixtures) freezing the on-disk JSON shape of the physical types and
  `MappingConfig`, asserting byte-stability across the RSA type reversal.

### Changed

- **Physical types re-based on `relational-schema-analyzer` (Stage 2, step 2).**
  `r2g.types` `Schema`/`Table`/`Column` now subclass RSA's
  `PhysicalSchema`/`Table`/`Column`, and `ForeignKey` is re-exported from RSA, so
  the two projects share one physical-schema type core. `Column` keeps r2g's
  Phase-9 `classification` as a first-class field and a legacy-preserving
  serializer emits r2g's historical shape, so **existing snapshots,
  `~/.r2g/catalog.json`, and saved mappings are byte-identical — no migration**
  (guarded by a new serialization compatibility corpus). RSA-native
  `extra['classification']` is tolerated on input. `relational-schema-analyzer` is
  now a **core dependency** (`>=0.2.0,<0.3.0`); the `[ontology]` extra is retained
  as an empty back-compat alias. See `docs/internal/DESIGN-rsa-compat-layer.md`.
  The `--engine rsa` adapter now passes the r2g `Schema` straight to RSA's analyzer
  (no JSON round-trip), since the types are unified. No behavior change.
- **FK-inference heuristic engine shared with `relational-schema-analyzer`
  (Stage 2, step 4).** `r2g.fk_inference` now imports the inference engine
  (`infer_foreign_keys`, `InferenceOptions`, `InferredForeignKey`, sampler
  protocol) from RSA instead of duplicating ~500 lines of identical heuristics.
  r2g keeps a thin `InferredForeignKey` subclass that adds the ArangoDB-specific
  `to_edge_definition`, and a wrapper that re-wraps RSA's results as that subclass,
  so the public API is unchanged. The concrete value samplers (which carry r2g's
  `sample_values` and are coupled to r2g's connectors) stay in r2g for now and are
  reconciled in step 5. No behavior change (verified by the FK-inference suite).
- **FK/denorm value samplers share RSA's implementations (Stage 2, step 5 —
  sampler de-dup).** `PostgresValueSampler`, `MySQLValueSampler`,
  `SQLServerValueSampler`, and `CsvValueSampler` now subclass RSA's samplers,
  inheriting the FK-overlap query and the Phase-11 denormalization probes
  (`distinct_ratio`/`group_single_valued`/`delimiter_rate`) and adding only r2g's
  `sample_values` probe (used by `r2g.llm.sampling`). This removes ~700 lines of
  duplicated sampler code; the shared connector helpers (URL parsing, driver
  loading, CSV path resolution) are byte-identical, so behavior is unchanged (full
  suite green). The **introspection connectors** themselves remain in r2g: RSA's
  have diverged (enum sampling, provenance, `ordinal`/`is_unique`, duckdb/databricks
  vs r2g's kafka) and return RSA-typed objects, so swapping them is not byte-stable
  without a re-typing + classification-merge + live-DB parity effort — tracked as
  the remainder of steps 5–6.
- **Connector-layer shared helpers + RSA Stage 2 close-out (steps 5–6, descoped).**
  `r2g.connectors.session` now re-exports RSA's byte-identical `SourceSession`
  protocol, and `r2g.connectors.base` re-exports RSA's source-type helpers
  (`expand_env_vars`, `normalize_source_type`, `is_postgresql`/`is_mysql`/
  `is_sqlserver`, `serialize_rows`). The `SourceConnector` protocol stays local so
  its `get_schema()` remains typed to r2g's `Schema` subclass (which the snapshot,
  classification-annotate, and schema-diff paths depend on), as do
  `SUPPORTED_SOURCE_TYPES` (incl. `kafka`) and `create_source_connector`. The
  **introspection connectors and bulk-read sessions are kept in r2g by design** —
  RSA's have diverged and drive r2g's data-migration path — closing the RSA
  dependency reversal (Stage 2): the shared-semantics core (physical types,
  FK-inference engine, value samplers, source-type helpers, session protocol) is
  unified, with RSA a core dependency. A live-DB introspection parity audit is added
  at `tests/integration/test_rsa_introspection_parity.py` and expanded to an
  edge-case corpus (PostgreSQL northwind/chinook/pagila, MySQL shop, SQL Server shop,
  CSV csv_demo). Run against the docker-compose stack (2026-07-07): on every **shared
  base table** — including pagila's ENUM/`DOMAIN`/`text[]`/`tsvector`/composite-PK/
  partitioned-table edge cases — r2g and RSA agree exactly (0 column/PK/FK diffs). The
  **one divergence** is that RSA's Postgres introspector also returns views + a
  materialized view (8 objects on pagila) while r2g returns base tables only; a future
  reuse must filter view membership. The audit asserts parity on shared tables and
  records membership/column diffs (non-fatal) — see the ADR. No
  behavior change; import safety for minimal installs preserved (no DB drivers pulled
  by importing the connector base/session).
- **`relational-schema-analyzer` floor raised to `>=0.4.0,<0.5.0`** — r2g now
  builds on RSA's `extra` consumer-metadata passthrough (0.2.0), physical-model
  enrichment (0.3.0), and dbt/OSI data-catalog connectors (0.4.0).

### Fixed

- **CSI: declared P6.7 join keys are exempt from label qualification.** The
  default `qualify` policy no longer renames a shared join key
  (`accountId` → `accountAccountId`), which had silently desynced
  `conceptualModel.joinKeys` from the entities it references.
- **CSI: never emit one attribute label on two entities.** The label-collision
  resolver guarantees a conceptual label maps to at most one entity (or is
  recorded `unresolved`), so a downstream flat vocabulary cannot be ambiguous by
  construction.
- **R2RML: `TriplesMap` class IRI uses the CC-12 conceptual entity name** (not the
  physical table name), so the relational and graph legs agree on class identity.
- **UI: FK-inferred edges render under PascalCase naming.** The topology canvas
  resolves edge endpoints by source-table name as well as target-collection name,
  so join edges no longer silently vanish when a naming convention diverges the two
  (e.g. `film_actor` → `FilmActor`).
- **Catalog: a leading `~` in `mapping_config_path` is expanded** to the user's
  home directory.

## [0.3.0] — 2026-07-05

### Added

- **Deterministic ontology engine via `relational-schema-analyzer` (Phase 10,
  Stage 1)**: `r2g ontology suggest` gains an `--engine rsa` option (and the UI a
  matching **Engine** selector) that derives a conceptual model —
  semantic (PascalCase) collection names, join-table detection, foreign-key
  relationships, and provenance (confidence, detected patterns, fingerprint,
  review flags) — **deterministically and offline** (no rows, no network) using
  the shared [`relational-schema-analyzer`](https://github.com/ArthurKeen/relational-schema-analyzer)
  library (the introspection core originally extracted from r2g). The analyzer's
  tool-contract bundle is converted into an `OntologyProposal` and flows through
  the same validated `proposal_to_mapping` "hallucination gate" as the LLM path,
  so the resulting `MappingConfig` is always schema-valid and loadable. Pass
  `--refine` (UI: **Refine**) to additively LLM-improve the deterministic model.
  Install with the new `r2g-arango[ontology]` extra. Many-to-many join tables are
  surfaced as advisory review notes (loaded as a vertex + FK edges), consistent
  with embed hints.

## [0.2.0] — 2026-07-04

First tagged release. Everything below the 0.1.0 pre-release baseline: temporal
graph mode validated end-to-end, MySQL/MariaDB + SQL Server sources, the
object-centric Mapping Studio (naming conventions, rename change-management,
FK/denormalization suggestions), the ArangoDB-backed catalog, external data
catalog integration (Phase 8), classification propagation & entitlement-aware
loading (Phase 9), LLM-assisted ontology derivation (Phase 10a–c) with
deterministic grounding, denormalization analysis (Phase 11), and a hardened
security posture.

### Added

- **End-to-end validation for temporal graph mode (Phase 5)**: a
  skip-when-unavailable integration test (`tests/integration/test_temporal_e2e.py`)
  that closes the long-standing "field validation against a live temporal
  workload" gap. It drives INSERT → UPDATE → DELETE through `TemporalApplier`
  against a real ArangoDB and asserts the full immutable-proxy time-travel
  contract: ProxyIn/ProxyOut creation, entity versions with `[created, expired)`
  intervals, the two `hasVersion` edges (closed on update), `ttlExpireAt`
  stamping on expiry, the interval (`mdi`/`zkd`/`persistent`) and sparse TTL
  indexes, replay-safety (a duplicated INSERT creates no phantom version), and
  every point-in-time / version-history / interval-overlap / current-version AQL
  template returning correct results at each instant.

- **Deterministic grounding for ontology proposals (P11.10 → Phase 10)**: an
  opt-in `--ground` flag (CLI), `ground` field (`suggest-ontology` API), and
  "Add deterministic denormalization findings as advisory evidence" toggle
  (Studio dialog) that runs the Phase 11 analyzer over the schema and hands its
  findings to the model as advisory evidence, so a proposal is grounded in
  deterministic analysis (e.g. "zip determines city, state — consider a Location
  vertex") rather than name heuristics alone. New `r2g.llm.grounding.build_grounding`
  wraps `analyze_denormalization` + `summarize_findings_for_prompt`; structural
  detectors always run and functional-dependency detectors use the same value
  sampler as `--sample` when available. The grounding block is fence-neutralized
  in the prompt and carries only column names + counts/ratios (never raw values),
  and the **classification gate carries over** — Restricted/PII columns are added
  to the analyzer's `no_sample_columns` so they are never value-sampled while
  grounding. Provenance records whether the proposal was grounded. Tested in
  `tests/test_llm_grounding.py` plus CLI/API wiring cases.

- **LLM ontology enrichment — providers & sampling (Phase 10c)**: broadens the
  Phase-10 seam. **Two new providers behind the same factory:** an Anthropic
  (Claude) provider (`src/r2g/llm/anthropic_provider.py`, Messages API; JSON
  parsed from the response with Markdown-code-fence stripping; key from
  `$ANTHROPIC_API_KEY`) and an **OpenAI-compatible / local** provider for Ollama,
  vLLM, LM Studio, llama.cpp, and hosted compatibles — served by `OpenAIProvider`
  with `require_key=False` + a required `base_url` (the `Authorization` header is
  omitted when no key is present). Factory aliases (`claude`, `local`, `ollama`,
  `vllm`, `lmstudio`, …) resolve to the right provider. **Opt-in,
  classification-filtered value sampling:** `build_schema_digest` now takes a
  `table → column → values` sample map and renders a few bounded,
  injection-neutralized example values per column — **only** for columns below the
  redaction threshold, so Restricted/PII columns stay name-only and are never
  sampled. `r2g.llm.sampling.collect_samples` gathers them via a new bounded
  `sample_values` probe added to the Postgres/MySQL/SQL-Server/CSV value samplers
  (best-effort; redacted columns are skipped entirely). Wired through the CLI
  (`r2g ontology suggest --provider/--model/--api-key/--base-url/--sample/--samples-per-column`),
  the `suggest-ontology` API (`provider`/`base_url`/`sample`/`samples_per_column`),
  and the Studio dialog (provider select, base-URL field, "sample non-sensitive
  columns" toggle); provenance records whether and how many columns were sampled.
  A **`R2G_LLM_LIVE`-gated live smoke test** (`tests/test_llm_live.py`) exercises
  the real provider round-trip and asserts a valid `MappingConfig`; new
  network-free unit tests in `tests/test_llm_providers.py`,
  `tests/test_llm_sampling.py`, and added cases in `tests/test_llm_prompt.py` /
  `tests/test_cli_ontology.py`.

- **LLM ontology review & apply in the Studio (Phase 10b)**: brings Phase-10a's
  proposal into the Mapping Studio. Two UI-server endpoints:
  `POST /api/projects/{name}/suggest-ontology` (read-only — returns the structured
  proposal, the resulting *validated* candidate mapping, a `diff_mappings` diff vs
  the current mapping, and validation/provenance notes) and
  `POST /api/projects/{name}/apply-ontology` (rebuilds an accepted, possibly-subset
  proposal through the same `proposal_to_mapping` gate and returns an **editable
  draft** — nothing is persisted server-side, exactly like `apply-naming`). The
  Studio gains a "Suggest model (AI)" entry in the Actions menu, the canvas
  context menu, and an `m` keyboard shortcut; it opens a floating review panel
  listing each proposed relationship / collection change / property rename with its
  rationale and confidence and a **per-item checkbox**. "Apply selected" sends only
  the checked items, loads the returned draft into the mapper edit state, and marks
  the project dirty so the user Saves through the normal path (which offers an
  in-place migration when the target is already loaded). API key is sourced from
  the environment (`$OPENAI_API_KEY`, or a `$ENV_VAR` reference in the request) and
  never persisted or echoed. Context-menu-primary, overlay panel, no new route.
  Tested with a network-free fake provider (`tests/test_ui_api.py::TestSuggestOntologyApi`).

- **LLM-assisted ontology derivation (Phase 10a)**: an *optional* path where a
  model **proposes** a richer target ontology — the LLM proposes, the
  deterministic pipeline disposes. New `src/r2g/llm/` package mirroring
  `r2g.catalogs`: an `LLMProvider` Protocol + lazy-importing `create_llm_provider`
  factory (`base.py`); a REST-over-`httpx` `OpenAIProvider` in
  JSON-object/`temperature=0` mode reading `$OPENAI_API_KEY` from the environment
  (`openai_provider.py`); a **metadata-only** prompt builder (`prompt.py`) that
  redacts Phase-9 Restricted/PII columns to name-only (never sampled), fences
  schema text as untrusted data with fence/`\`\`\`` neutralization (prompt-injection
  hardening), and enforces a hard token budget; and `proposal_to_mapping`
  (`ontology.py`) — the **hallucination gate** that starts from the Auto-Map
  baseline, applies each proposed collection/edge/rename behind a guard mirroring
  `validate_config`, drops references to non-existent tables/columns (recorded in
  notes), de-dupes restated FK edges, rejects reserved-attribute rename targets,
  and guarantees the result always passes `validate_config` (worst case:
  Auto-Map-equivalent). Embed suggestions surface as advisory notes only (no
  mechanical apply in V1). CLI `r2g ontology suggest <project> [--domain]
  [--provider] [--model] [--api-key] [--apply] [--yes] [--json]` prints the
  proposal, a `diff_mappings` diff against the current mapping, and
  validation/provenance notes; nothing is written without `--apply` (with
  confirmation), which also drops a `llm-ontology-provenance.json` sidecar.
  Shipped as the optional `r2g-arango[llm]` extra (added to `[all]`); no LLM
  dependency or network call unless a suggestion is invoked. Fully unit-tested
  with a network-free fake provider (`tests/test_llm_base.py`,
  `test_llm_prompt.py`, `test_ontology_proposal.py`, `test_cli_ontology.py`).
  The Studio diff-review/apply UX (10b) and opt-in sampling / more providers
  (10c) follow.

- **Enforcement artifacts & classification re-sync (Phase 9c)**: the "enable
  enforcement — emit, don't enforce" layer. `src/r2g/governance.py` gains
  `classification_manifest` (canonical per-collection/edge/field classification
  + lineage + owners + sync timestamp), `suggested_rbac` (per-clearance ArangoDB
  collection read-grants, cumulative: a `confidential` clearance reads
  public/internal/confidential but not restricted), `policy_rego` (a default-deny
  OPA/Rego stub keyed on collection level vs. principal clearance, no `opa`
  dependency), and `tier_layout_recommendation` (group collections by tier with a
  suggested database/prefix per tier). `write_governance_artifacts` emits them all
  under `<project>/governance/`, keyed by **target** (ArangoDB) collection names.
  Surfaced via `r2g entitlements emit <project> [--threshold] [--out] [--tier-layout]
  [--no-rego]`, `POST /api/projects/{name}/governance/emit`, and `emit_governance` /
  `tier_layout` on the load endpoint. `r2g catalog resync-classifications <source>`
  re-pulls classifications from the bound catalog (provenance now stored on
  `SourceConfig`: `catalog_name`, `catalog_asset_fqn`, `classifications_synced_at`),
  refreshes the stored map, re-merges onto the latest snapshot, and reports
  lattice-level drift via the new `diff_classifications` helper (escalations
  called out). The CDC/temporal path carries the policy onto changed rows:
  `cdc-start` / `kafka-start` gain `--govern` (+ `--allow-sensitive`,
  `--sensitivity-threshold`) which applies the same sensitivity gate to the
  mapping so subsequent row changes don't launder newly sensitive columns.
  Retroactively re-gating already-loaded data on a tier rise is intentionally out
  of scope (a serving-layer/backfill concern); r2g advises and gates future
  writes. A gated OpenMetadata classification e2e test ships
  (skip-when-unavailable). r2g emits; the serving layer enforces. **Phase 9 is
  complete (9a + 9b + 9c).**
- **Entitlement report, load gate & masking (Phase 9b)**: the
  advise-and-gate layer on top of the 9a classification carrier. New
  `src/r2g/governance.py` builds an **entitlement report** (each target property,
  its source-column lineage, the mosaic-recomputed sensitivity level, and whether
  it is masked), a default-exclude **threshold gate** (above-threshold *unmasked*
  source columns are added to `exclude_fields` for the run unless `allow_sensitive`
  is set), and a `governance/lineage.json` **manifest** recording each field's
  handling. New `src/r2g/masking.py` supplies `hash`/`tokenize`/`redact`/`nullify`
  masking expressions built on the existing `FieldExpression` engine (hashes are
  AQL-delegated, so they work for every source type) and sentinel-tagged so the
  gate treats a masked field as safe to load. Surfaced via `r2g entitlements
  report <project> [--threshold] [--json]`, `GET /api/projects/{name}/entitlements`,
  and the load gate on `POST /api/projects/{name}/load` (`allow_sensitive`,
  `sensitivity_threshold`; the excluded set is reported, never silently dropped).
  The Mapping Studio gains an **entitlement-report panel** (Actions menu, canvas
  context menu, and the `g` shortcut) listing above-threshold fields with source
  lineage; a paint-only **sensitivity lens** (View as → Sensitivity / press `5`)
  that tints source columns and target properties by mosaic-recomputed tier
  (source-column color = the highest level it feeds) with a legend; and a
  one-click **"mask this field"** target-property context action that writes a
  masking `FieldExpression` (hash / tokenize / redact / nullify) so the field
  clears the load gate. r2g advises and emits; the serving layer enforces.
- **Classification capture & propagate (Phase 9a)**: the governance backbone for
  carrying catalog classifications across the relational→graph boundary. A new
  `Classification` model (`tags`, `tier`, `glossary_terms`, `source`) annotates
  `Column`; `CatalogAsset` and `ResolvedSource` gain `column_classifications`,
  `owners`, and `tier`. The OpenMetadata provider now reads `columns,tags,owners`
  and parses per-column tags, table owners, and the `Tier.*` confidentiality tier
  (glossary terms split out by tag source); `resolve_source` captures
  `table → column → Classification` for table/schema/database assets, best-effort
  so a limited governance API never blocks import. The resolved map persists on
  `SourceConfig.classifications` (with `data_owners`/`data_tier`) at
  `catalog import-source` and is merged onto `Column.classification` at
  `source snapshot`. New `r2g.classification` module supplies the sensitivity
  lattice (`public < internal < confidential < restricted`), an overridable
  tag/tier→level map, `max_sensitivity`/`exceeds_threshold`/`tier_of`, and
  `recompute_mosaic` — the max-of-contributors mosaic rule over fan-in
  properties, vertex collections, and edges. Advise/gate (9b) and
  emit-enforcement artifacts + re-sync (9c) remain; r2g carries governance
  metadata and never acts as a runtime authorization engine.
- **Denormalization analysis — advisory remediation + grounding (Phase 11c)**:
  each finding now carries concrete remediation guidance (`remediation_hint`),
  shown in the CLI (a "Suggested remediation" column), the API (a `hint` field on
  every finding), and the Studio card. Added `summarize_findings_for_prompt`, a
  compact confidence-ranked digest that grounds the (forthcoming) Phase-10
  ontology proposal in deterministic evidence. Mechanical auto-apply is
  intentionally deferred: the recommended remedies (vertex extraction, merge,
  split/combine to array) are not yet representable in the source-table-bound
  mapping + AQL expression model, so r2g advises rather than emit an invalid
  mapping — consistent with the phase's "advise, never silently rewrite".
- **Denormalization analysis — more detectors + Studio card (Phase 11b)**:
  three additional detectors — **multi-valued** columns (delimited lists, via a
  new bounded `delimiter_rate` probe on all four value samplers), **redundant
  reference** data (text columns with a very low distinct ratio, suppressed when
  already explained by an embedded-lookup finding), and **1:1 over-normalization**
  (structural: a table whose entire primary key is also a foreign key to another
  table → recommend merge/embed). Plus a read-only **Studio findings card** in
  the Mapping Studio, reachable from the Actions menu, the canvas right-click
  menu, and the `n` shortcut (clone of the Suggest-FKs card). Advisory only —
  accepting a finding into the mapping arrives in 11c.
- **Denormalization analysis — deterministic core (Phase 11a)**: a new
  `r2g source analyze-denorm <name>` command surfaces *advisory*, evidence-backed
  denormalization findings on a source's latest snapshot. Detects **repeating
  column groups** (`phone1/phone2/phone3`, structural, no sampling) and, with
  `--sample`, **embedded lookups** — a non-key column that functionally determines
  other non-key columns (e.g. `zip → city, state`), recommending extraction into a
  shared vertex. Backed by `src/r2g/denorm.py` (`analyze_denormalization`,
  mirroring `infer_foreign_keys`) and two new bounded probes (`distinct_ratio`,
  `group_single_valued`) added to all four value samplers (PostgreSQL, MySQL,
  SQL Server, CSV). Read-only, classification-aware (`--no-sample-columns` escape
  hatch plus a Phase-9 `is_sampleable` hook), and resilient (probe failures
  degrade to structural signals). `--json` output supported; no schema or data is
  ever rewritten. Also exposed over the API as
  `POST /api/sources/{name}/analyze-denorm` (mirrors `infer-fks`). 11b (remaining
  detectors + Studio review card) and 11c (remediation scaffolding + Phase-10
  grounding) remain.
- **External data catalog — MCP tools (Phase 8b, P8.6)**: five new MCP tools —
  `list_catalogs`, `add_catalog`, `remove_catalog`, `catalog_browse`, and
  `catalog_import_source` — let an agent register a catalog, browse its
  `service → database → schema → table` tree (or search), and import a discovered
  asset as a normal r2g source, mirroring the CLI/UI. Tokens are redacted on read
  and `$ENV_VAR` token references resolve at use time; errors are DSN-scrubbed and
  credentials are never taken from the catalog. `get_asset` is now part of the
  `CatalogProvider` protocol (the CLI, UI, and MCP all rely on it).
- **External data catalog — Studio UI import (Phase 8b)**: an "Import from
  catalog" path in the Studio (folder button on the Sources panel) — register a
  catalog, browse its `service → database → schema` tree, and import a
  database/schema as a source, all in the browser. New `/api/catalogs*`
  endpoints (list/add/remove/browse/import-source); catalog tokens redacted in
  responses. Validated end-to-end against a live OpenMetadata 1.13 instance.
- **Inline `$VAR` expansion in connection strings**: `create_source_connector`
  (and the FK sampler + MCP paths) now expand `$VAR` / `${VAR}` references
  *inline* within a DSN, not just whole-string — so catalog-imported sources
  whose credentials are `$R2G_DB_USER` / `$R2G_DB_PASSWORD` placeholders connect
  cleanly. Literal DSNs (no `$`) are unaffected.
- **External data catalog discovery — OpenMetadata (Phase 8a)**
  (`r2g-arango[openmetadata]`): a catalog-agnostic `CatalogProvider` abstraction
  + factory (`r2g.catalogs`) and an OpenMetadata provider that talks to the REST
  API via `httpx` (no heavy SDK). New `r2g catalog add/list/browse/import-source/
  remove` CLI: browse a catalog's `service → database → schema → table` tree (and
  Kafka topics), then import a selected database/schema/topic as a normal r2g
  source ("discover-then-connect"). Catalog provider tokens are encrypted at rest
  alongside source/target secrets. Credentials are **not** read from the catalog —
  imported sources use `$R2G_DB_USER` / `$R2G_DB_PASSWORD` placeholders resolved at
  connect time. Distinct from the internal Phase 5d catalog; read-only. Unit-tested
  with a mocked HTTP layer; a live OpenMetadata e2e (skipped when unavailable) is
  included for field validation.
- **SQL Server source connector** (`r2g-arango[sqlserver]`, pure-Python
  `pymssql`): schema introspection over `INFORMATION_SCHEMA` plus `sys.*`
  catalog views for declared foreign keys (clean composite-FK ordering), a
  batched cursor read session with CSV dump, and a SQL Server FK value-overlap
  sampler. `bit` columns are mapped to boolean; the schema namespace defaults
  to `dbo`. Works everywhere the other relational sources do —
  `source add --type sqlserver` (alias `mssql`), `source snapshot`,
  `source dump`, `source infer-fks --sample`, `stream --source`, the Studio
  UI, and MCP. Verified end-to-end against SQL Server 2022 via a docker-compose
  `sqlserver` service + `tests/integration/test_mssql_e2e.py`, wired into the
  CI integration job.
- **MySQL / MariaDB source connector** (`r2g-arango[mysql]`, pure-Python
  `pymysql`): schema introspection over `information_schema`, a
  consistent-snapshot read session (`REPEATABLE READ` + `START TRANSACTION
  WITH CONSISTENT SNAPSHOT`) with server-side cursor streaming and CSV dump,
  and a MySQL FK value-overlap sampler. Works everywhere PostgreSQL and
  Snowflake do — `source add --type mysql`, `source snapshot`, `source dump`,
  `source infer-fks --sample`, `stream --source`, the Studio UI, and MCP.
  `mariadb://` URLs and the `mariadb` source type alias are accepted.
  Verified end-to-end against MySQL 8.4 via a new docker-compose `mysql`
  service + `tests/integration/test_mysql_e2e.py` (introspection, streaming
  into ArangoDB, dry-run, and live session count/stream/dump), wired into the
  CI integration job.
- **Phase 5 — Temporal graph mode** (immutable-proxy time travel):
  ProxyIn/Entity/ProxyOut vertices with `hasVersion` edges, point-in-time
  AQL query helpers, `--temporal` / `--ttl-seconds` flags on `cdc-start`
  and `kafka-start`, mdi interval index plus sparse TTL index for version
  garbage collection, and `--smart-field` for SmartGraph key prefixes.
- **Phase 5g — Mapper UX refinements**: light-mode default, slim toolbar,
  graph explorer, on-demand panels, progressive disclosure, bidirectional
  highlight, function-node discoverability, composite-key nodes.
- Naming conventions for generated collections/edges with rename
  change-management, plus UI hints and bundled demo data.
- CSV directory and Kafka topic source types alongside PostgreSQL and
  Snowflake.
- Auto-creation of the target database and named graph on load.
- Expression preview and per-batch AQL delegation (Phase 5b/5c close-out).
- `r2g --version` flag; shell completion documented via Typer's built-in
  `--install-completion`.
- CI: coverage gate (`--cov-fail-under`), mypy type-check job, and an
  opt-in Docker-based integration test job.
- Public release preparation: Apache-2.0 LICENSE, NOTICE, CONTRIBUTING,
  CODE_OF_CONDUCT (Contributor Covenant 2.1), SECURITY policy, issue and
  pull-request templates, PyPI Trusted Publisher release workflow.
- `docs/` layout: canonical PRD moved to `docs/PRD.md`; internal planning
  notes moved to `docs/internal/`.

### Changed

- **Positioning corrected.** README + PRD now state the reality: r2g's
  introspection/analysis core — extracted into `relational-schema-analyzer`
  (RSA) — is actively adopted by other projects in ArangoDB Solutions (the
  contextual-data-fabric building blocks depend on it), so RSA is the
  production-grade dependency and carries the production bar. r2g is the
  *reference application* composing it.
  Downstream systems depend on RSA (and named, tested r2g modules), not on r2g
  as a whole. Removes the blanket "experimental / educational / not intended
  to supersede any ArangoDB roadmap" framing, which was both an undersell of
  the tested core and, given active adoption, simply inaccurate.
- Code-quality remediation: consolidated duplicated Python and JS helpers
  (edge-target map, PG connection factory, table preview, shared
  mapper/explorer UI helpers), removed dead code, unified HTML escapers.
- Partitioned tables collapse into a single logical table in the mapper.
- Major dependencies now declare upper version bounds in
  `pyproject.toml` to prevent silent breaking upgrades.
- PyPI distribution name is `r2g-arango`. The import package and CLI
  command remain `r2g`.
- `pyproject.toml` declares Apache-2.0 license, authors, keywords,
  trove classifiers, and project URLs.

### Fixed

- `drop_collections` no longer fails on collections that are members of a
  named graph.
- Document keys are sanitized to the ArangoDB legal character set.
- Preview SQL injection in table preview; DLQ now wired into streaming.
- Collapsed edge spine connects both endpoint tables; topology legend
  layout; `_key` node legibility.
- The docker-compose ArangoDB healthcheck no longer always reports
  "unhealthy" (the image ships no `curl`); it now uses BusyBox `wget` with a
  Basic-auth header, so `docker compose up --wait` succeeds in the CI
  integration job.

### Security

- Bearer-token auth for non-loopback UI binds (auto-generated token when
  none configured), CORS lockdown (same-origin unless `R2G_CORS_ORIGINS`
  set), and a path jail for mapping-file writes.
- Secret redaction in structured logs, DSN-credential scrubbing in error
  messages, redacted DLQ entries, MCP secret redaction.
- Closed inline-handler DOM XSS in mapper/explorer panes, with a static
  regression-guard test.
- MCP SSE transport now requires a Bearer token (reuses `R2G_API_TOKEN`,
  auto-generated on non-loopback bind); MCP tool errors are scrubbed of DSN
  credentials and `generate_mapping(save_path=...)` is confined to the
  catalog projects directory.
- CSV sources can be confined to a trusted directory tree via the opt-in
  `R2G_CSV_BASE_DIR` environment variable.

## [0.1.0] — pre-release

Initial phased implementation (not yet published to PyPI):

- **Phase 1** — Table dump file processing: schema ingestion, JSONL
  transforms, CSV-direct import via `arangoimport`, interactive HTML
  mapping visualiser.
- **Phase 2** — Direct PostgreSQL streaming with server-side cursors,
  REPEATABLE READ snapshot isolation, HTTP API bulk import, parallel
  workers, retry with backoff, topological ordering, table filtering,
  progress reporting.
- **Phase 3** — Change Data Capture (CDC): logical replication listener
  (`test_decoding` and `wal2json` parsers), delta transformer, handler,
  four conflict-resolution policies (`source_wins`, `last_write_wins`,
  `log_and_skip`, `fail`), CLI commands (`cdc-setup`, `cdc-teardown`,
  `cdc-status`, `cdc-start`).
- **Phase 4** — Kafka integration: Debezium and flat-JSON parsers,
  `confluent-kafka` consumer, `kafka-start` CLI.
- **Phase 5b/5c/5e** — Mapping studio UI (FastAPI + static HTML/JS):
  project catalog, schema introspection, mapping editor with YAML
  export, graph schema lens, edge mapping detail, expression evaluator.
- **Phase 6** — Snowflake source support: `SourceConnector` protocol,
  `SnowflakeConnector` schema introspection, source-agnostic streaming
  pipeline, `r2g source dump` CLI, pure-Python FK inference with
  optional value-overlap sampler.

[Unreleased]: https://github.com/ArthurKeen/r2g-arango/compare/v0.8.2...HEAD
[0.8.2]: https://github.com/ArthurKeen/r2g-arango/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/ArthurKeen/r2g-arango/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/ArthurKeen/r2g-arango/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/ArthurKeen/r2g-arango/compare/v0.6.1...v0.7.0
[0.6.1]: https://github.com/ArthurKeen/r2g-arango/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/ArthurKeen/r2g-arango/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/ArthurKeen/r2g-arango/compare/v0.4.3...v0.5.0
[0.4.3]: https://github.com/ArthurKeen/r2g-arango/compare/v0.4.2...v0.4.3
[0.4.2]: https://github.com/ArthurKeen/r2g-arango/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/ArthurKeen/r2g-arango/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/ArthurKeen/r2g-arango/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/ArthurKeen/r2g-arango/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/ArthurKeen/r2g-arango/releases/tag/v0.2.0
