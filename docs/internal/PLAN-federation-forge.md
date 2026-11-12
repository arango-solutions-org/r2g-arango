# PLAN — Federation Forge: the reverse generator

**Status:** S1 walking skeleton shipped (Postgres, r2g #1); **S2 dialects shipped**
(snowflake / clickhouse / arango behind the same seam — see "S2 dialects" below);
**volume** (self-links, scale knobs, streaming and bulk output — F-9 to F-11)
for CDF's S5
**Commissioned by:** contextual-data-fabric ADR-0006 (accepted 2026-09-08),
which assigns r2g the generator core (its D-4): r2g owns the ontology→schema
mapping machinery in both directions, so the *reverse* direction — generate a
physical schema and synthetic data **from** an ontology — lands here.
**Consumer:** the fabric's M15 orchestration (shape descriptors, partitioning
across engines, expected-catalog/golden computation) stays in
contextual-data-fabric. r2g ships only the per-dialect seam:

```
generate(ontology, dialect, seed) -> {ddl, loader, rows}
```

## Why (one paragraph)

Every correctness claim the fabric makes is validated against one hand-built
corpus. ADR-0006's insight: the estate already owns ontology↔schema mapping in
the forward direction (introspect → Auto-Map → CSI). Run it in reverse and
every generated federation is born with its ground truth attached — the
generating ontology IS the expected aligned ontology. The core correctness
property is the roundtrip through the REAL forward pipeline (never through
forge-internal shortcuts):

```
introspect(generate(O)) ≡ O        (ADR-0006 D-3)
```

with `≡` defined honestly: equality up to CC-12 naming normalization and
declared-constraint availability.

## Decisions (r2g-side)

### F-1 · The ontology input is the CSI v1 conceptual model

ADR-0006's shape descriptor names an `ontology.ttl`, but r2g has never parsed
OWL/TTL and the estate's only *validated* ontology interchange contract is CSI
v1 (`schemas/csi_v1.schema.json`), which `arango-schema-analyzer` already
produces in reverse. The forge therefore takes a **conceptual ontology** shaped
exactly like `conceptualModel` in CSI v1 — entities, properties, relationships
— with one extension the skeleton needs: each property carries a JSON `type`
(`integer | float | boolean | string`), which CSI already permits (the fabric's
`arango-cmf.json` emits it today). TTL→conceptual-model conversion is the
fabric orchestration's job when the descriptor lands (S3); if the team wants
TTL ingestion in r2g instead, that is a one-module follow-up, not a rework.
**Flagged for review in the skeleton PR.**

### F-2 · Naming is the declared inverse of CC-12, asserted through the real normalizer

`csi.owl_entity_name` / `csi.owl_property_name` are lossy and non-injective,
so the generator declares one canonical physical spelling and the roundtrip
asserts through the *normalizer*, never against a hoped-for literal:

- class `UsageMetric` → table `pluralize(convert_identifier(name, "snake"))`
  = `usage_metrics`; property `healthScore` → column `health_score`;
- the test asserts `owl_entity_name(generated_table) == input_class` and
  `owl_property_name(generated_column) == input_property` — the exact
  functions the forward CSI emitter runs.

Skeleton guard: the generator **refuses** an ontology whose class names don't
survive its own roundtrip (`owl_entity_name(pluralize(snake(C))) != C`, e.g.
singularize's naive trailing-`s` rule) — fail loudly at generate time, never
at compare time.

### F-3 · One canonical Postgres type per JSON type

`config.DEFAULT_TYPE_MAP` is many-to-one, so the inverse picks one
roundtrip-stable spelling per JSON type and the comparison runs through
`pg_type_to_json_type` on both sides:

| conceptual `type` | generated DDL | introspects back as | JSON type again |
|---|---|---|---|
| `integer` | `bigint` | `bigint` | `integer` |
| `float` | `double precision` | `double precision` | `float` |
| `boolean` | `boolean` | `boolean` | `boolean` |
| `string` | `text` | `text` | `string` |

Temporal/decimal/uuid categories (RSA's `normalized_type_category`) are still
out — see S2-5 for why; the four types cover every property in the fabric's
live CSIs. Each S2 dialect declares its own physical spelling per JSON type
(`dialects/<name>.py`); the comparison still runs through
`pg_type_to_json_type` on the introspected side.

### F-4 · Keys and relationships: declared constraints first

Every entity gets `id bigint PRIMARY KEY`. Every conceptual relationship
`fromEntity → toEntity` becomes a real `FOREIGN KEY` column
`<singular_snake(toEntity)>_id` on the from-table referencing `<to-table>(id)`
— so Auto-Map re-derives the edge as `<from>_to_<to>` and the CSI names it
back to the input's lowerCamel relationship type. The ADR's
constraint-stripped variant (keys recovered by inference OR reported absent —
never silently wrong) is exercised live for the first time by the `clickhouse`
dialect (S2-3, where no FK syntax exists at all); *injecting* it into dialects
that could declare keys is the S3 denormalizer's job.

### F-5 · Data synthesis: once, seeded, spine-safe (ADR-0006 D-2/D-5)

`random.Random(seed)` end to end; a descriptor re-run must be byte-identical.
Rows are synthesized per entity in relationship-topological order
(`topo_sort`), and every FK value is drawn from the already-synthesized parent
`id`s — join-spine agreement by construction. Values are type-driven and
deliberately naive (the skeleton's "naive synthesis"); vocabularies,
cardinality shaping, and statistics arrive with the fabric's descriptor knobs.

### F-6 · Collision-free by construction (skeleton)

`csi._resolve_label_collisions` actively mutates conceptual models (qualify
renames; `roles` drops PK-collision labels), so a colliding ontology cannot
roundtrip verbatim. The skeleton generator refuses input ontologies where two
entities share a property label; the roundtrip integration test emits with the
default policy and asserts `provenance.labelCollisions == []`. Deliberate
collisions are a *denormalizer feature* (the injected-collision report is the
expected artifact) — S3.

### F-7 · Generated identifiers must be safe unquoted, and are refused when they are not

A generated identifier — **table or column** — may not be a SQL reserved word,
and must start with a letter or underscore. Found by CDF's live mode on
2026-09-19: a property called `primary` emitted `primary boolean`, which
Postgres and Snowflake both reject.

The rule initially covered property columns only, which left two holes that a
review found the same day. An entity named `Value` generated
`CREATE TABLE values (` — reserved in Snowflake — because table names were
never checked. And `3dModel` generated the column `3d_model`, which *passes*
F-2 (it normalizes back to `3dModel`) and every other check, yet no target
accepts an unquoted identifier beginning with a digit. Both now go through one
`_refuse_unsafe_identifier` rule, so widening it again means editing one place.

Quoting is the obvious fix and it is the wrong one here. The forge emits
identifiers **unquoted on purpose** — Snowflake folds unquoted names to
uppercase, and F-2's roundtrip asserts *through the normalizer* against exactly
that folded spelling. A quoted `"primary"` comes back lowercase from
introspection, so quoting to dodge a reserved word would break the equivalence
the Forge exists to check. Refusal keeps `introspect(generate(O)) ≡ O` honest,
and it is what F-6 already does with a colliding label.

The list is **derived from the engines, not written**. One plan is projected onto
every dialect (D-2), so a name must be safe in all of them; type names are
deliberately absent — `text`, `integer` and friends are legal column names in
every target here.

`scripts/derive_reserved_words.py` probes all 471 entries of Postgres's own
`pg_get_keywords()` as real column names against live Postgres and live
ClickHouse, and unions what they reject with Snowflake's documented reserved
words: 101 + 1 (`index`) + 91 → **137**.
`tests/integration/test_reserved_words.py` re-probes both engines and fails if
the checked-in list has drifted, in **both** directions — a word the engines
reject and the forge accepts, and a word both accept that the forge refuses.

> **This paragraph used to say something else, and it was wrong.** The first
> list was written from memory as "the SQL:2016 reserved words" and claimed
> *zero false negatives* from a 140-word probe. It accepted `current_date`,
> `current_user`, `session_user`, `array`, `do`, `returning`, `ilike` and
> `binary` — all rejected by live Postgres — and refused `key`, `range`,
> `session`, `result`, `filter` and `partition`, which Postgres and ClickHouse
> both accept. The claim was not merely false but unfalsifiable: the probe
> covered only the words already in the list, so it could not fail. Deriving
> from the engines is the fix; re-probing in CI is what keeps it fixed.

Implementation: `RESERVED_COLUMN_NAMES` in `src/r2g/forge/core.py`, enforced in
`ForgeOntology.validate_for_forge`.


### F-8 · A dialect renders what the plan gives it and derives nothing itself

The seam is advertised for extension — "one module following `dialects/base.py`
plus one registry line" — so the rule a new dialect must follow belongs here,
beside F-3's type table and F-6's collision refusal. Two halves, both learned
the hard way on 2026-09-19.

**Cross-table facts live in the plan.** A foreign-key `ColumnPlan` carries
`references` (the parent table) *and* `references_column` (the column it points
at **in that parent**); an `EdgePlan` carries `to_key`, read off the same
`ColumnPlan` so the two cannot disagree. The specific error this rules out is
reaching for `table.primary_key` to name a referenced column — that is the key
of the table the constraint sits *on*, not the one it points *at*. Every dialect
did exactly that, and all of them were right by accident, because `plan_schema`
gives every table the same `SURROGATE_KEY`.

**Every emitted identifier goes through `physical_table` / `physical_column`** —
in comments and header lines as much as in DDL. A dialect that folds identifiers
(Snowflake uppercases both) must fold them everywhere, or it emits DDL its own
loader cannot target, and machine-readable FK intent naming tables that do not
exist.

**Testing this needs a deliberately lopsided fixture, and that is the part worth
remembering.** Neither failure is expressible in the shipped fixtures: every
planned table shares one surrogate key, and three of the four dialects map
identifiers to themselves. A regression test written the obvious way — a plan
from `plan_schema`, a stock dialect — *passes against the live bug*. A test that
defends either half must construct the asymmetry on purpose: a plan whose parent
and child keys differ (`TestForeignKeyReferencesTheParent`), or a subclass that
folds (`test_ddl_routes_every_name_through_the_seam`). Both bugs survived review
and a green suite precisely because the first tests written for them could not
have failed.


### F-9 · A class may be linked to itself; its rows form a forest

Commissioned by CDF's S5 "Forge at volume" (ADR-0011 D9 names the gap: "it
links no class to itself, so there is no reachability or shortest-path
workload"). A relationship whose `fromEntity` equals its `toEntity`
(`Employee -> Employee`, type `employeesToEmployees`) is accepted:

- It is **not** a cycle for the topological order — that order is between
  tables, and a self-link's ordering lives inside its one table. A cycle
  between two different classes is still refused.
- The FK column follows F-4 (`employee_id` on `employees`, referencing
  `employees(id)`), so Auto-Map re-derives `employees_to_employees` and the CSI
  names it back `employeesToEmployees`; the unit test runs that derivation in
  memory. The column is **nullable** (`ColumnPlan.optional`): row 1 is always a
  root, so no NOT NULL column could hold it.
- **Each row points at an earlier row** of the same table, never at itself and
  never forward. The links therefore form a forest with no cycle, a loader that
  inserts in id order never names a row that is not there yet, and declared
  constraints hold throughout. Under the default draw a row's parent is uniform
  over the rows before it — a random recursive tree, mean depth about `ln(n)`
  (≈14 at 10^6 rows), which is deep enough for a reachability workload.
- In ArangoDB a NULL foreign key has no edge (`project_documents`, the loader
  script's copy of it, and the bulk projector all skip it).

### F-10 · Volume knobs: row counts, fan-out skew, NULL rates

ADR-0006's descriptor has carried `scale: {rowsPerEntity}` since S1, with "S5
turns this knob". F-5's synthesis gave every class the same row count and
drew every FK uniformly, so a hop had about one neighbour on average.
`ForgeScale` (`src/r2g/forge/scale.py`) is the knob set, as a document:

```json
{
  "rows": {"Account": 1000, "Contact": 1000000},
  "relationships": {
    "contactsToAccounts": {"distribution": "zipf", "exponent": 1.1, "nullRate": 0.05},
    "employeesToEmployees": {"distribution": "uniform"}
  }
}
```

- `rows` — per class, overriding `rows_per_entity`. A relationship's mean
  fan-out is the child's rows over the parent's.
- `distribution` — `uniform` (F-5's draw) or `zipf` with `exponent` `s > 0`:
  the parent of rank `k` is picked with probability `k^-s / H(n, s)`, rank `k`
  being parent `id` `k` (parent 1 is the busiest). Sampled by
  rejection-inversion (Hörmann & Derflinger 1996) in O(1) memory; an
  inverse-CDF table would hold a float per parent, 800 MB at 10^8. On a
  self-link the ranks are the earlier rows, so skew makes the forest bushier
  and shallower.
- `nullRate` — the share of children with a NULL FK. A positive rate makes the
  FK column nullable, so the scale is an input to `plan_schema`, not only to
  synthesis.

**Refused by name, never ignored:** a knob naming no class or relationship of
the ontology, `zipf` without an exponent, an exponent on `uniform`, a rate
outside `[0, 1)`, a row count below one. A knob that silently did nothing
would make a benchmark measure a shape nobody asked for.

**The skeleton's draws do not move.** With no knob set, every draw is the one
F-5 made, in the same order: `rng.choice(range(1, n + 1))` consumes the
generator exactly as `rng.choice([1, …, n])` did, and a knob only adds draws
where it is set (a `nullRate` coin, a Zipf variate). `tests/test_forge_scale.py`
keeps 0.10.0's synthesizer verbatim and compares against it, and pins a digest
of its output, so a suite CDF committed from an earlier release stays
byte-identical.

### F-11 · Synthesis streams; volume output is chunked files

At 10^8 edges neither the rows nor their `INSERT` text fit in memory. Synthesis
is now `stream_rows(plan, seed, rows_per_entity, scale)`, a generator of
`(table, row)` in plan order. It holds no rows: an FK needs only its parent's
row **count**, because a parent's ids are `1..count` by construction (an FK
onto anything but the parent's primary key is refused by name). Arguments are
checked when it is called, not on the first `next()`. `synthesize_rows` is
that stream collected, for small shapes.

`generate_bulk` / `BulkWriter` (`src/r2g/forge/bulk.py`, CLI
`r2g forge generate --bulk [--chunk-rows N]`) write the stream as it arrives:

| File | What |
|---|---|
| `forge.sql` / `forge.collections.json` | the dialect's schema, unchanged |
| `data/<target>.<nnnnn>.csv` (SQL) / `.jsonl` (arango) | at most `chunk_rows` records each; CSV has no header, `\N` (unquoted) for NULL, every string quoted |
| `forge.bulk.sql` / `forge.bulk.py` | Postgres `\copy … WITH (FORMAT csv, NULL '\N')`; ClickHouse `INSERT … FROM INFILE … FORMAT CSV`; Snowflake `PUT` + `COPY INTO … PURGE`; ArangoDB `import_bulk` in batches |
| `forge.bulk.json` | every target, its columns and row count, each file with its rows and SHA-256, and the seed and scale |

Memory is one open file per target. Files are written in plan order (parents
first, a self-link's rows after the rows they name), so loading them in order
never inserts a dangling key. A writer that fails mid-stream writes no
manifest, so a half-written directory never looks complete. The two
`forge.rows.json`-shaped outputs are not written in bulk mode — they *are* the
N rows.

What is not exercised here: no bulk loader has been run against a live engine
in this repository's unit tests; the Snowflake one (`PUT 'file://…'` with a
relative path) has not been run anywhere yet.


## S2 dialects — the other three launch-set targets behind one seam

ADR-0006 D-4 names Postgres, Snowflake-SQL, ClickHouse-SQL, and Arango
collections as the launch set. `generate()` keeps its signature; internally it
is now three pieces so a dialect is a *projection*, never a re-derivation:

```
plan_schema(O)            -> SchemaPlan   (canonical snake_case tables/columns + roles; pure fn of O)
synthesize_rows(plan, s)  -> rows         (random.Random(seed) once; keyed by canonical names)
DIALECTS[d].render_*(…)   -> ddl, loader  (the only dialect-specific code)
```

`r2g.forge.dialects` is the registry (`DIALECTS`, `get_dialect`,
`SUPPORTED_DIALECTS`). Adding a dialect = one module following
`dialects/base.py` + one registry line; the shared unit tests parametrize over
the registry, so type-table completeness and the D-2 row-identity property are
asserted for every dialect automatically.

### S2-1 · Rows are byte-identical across dialects (ADR-0006 D-2)

`rows` is the pre-partition dataset and is keyed by *canonical* names
(`accounts.account_name`) in every dialect; the Snowflake loader spells them
`ACCOUNTS.ACCOUNT_NAME` at emit time, the Arango loader adds `_key` at load
time. `tests/test_forge_dialects.py` asserts the JSON of `rows` is identical
across all four dialects for the same `(ontology, seed, rows_per_entity)`.

### S2-2 · `snowflake`: unquoted UPPERCASE, declared-but-unenforced keys

Identifiers are emitted unquoted, so Snowflake folds them to UPPERCASE — the
spelling real customer schemas have and the one the forward pipeline handles
(`singularize`/`convert_identifier` are case-insensitive since #2:
`ACCOUNTS` → `Account`, `ACCOUNT_NAME` → `accountName`). Quoting lower-case
names would also round-trip but exercises a spelling no real schema has.
`PRIMARY KEY`/`FOREIGN KEY` are declared; Snowflake records them without
enforcing them, and RSA's `SnowflakeConnector` reads them back via
`SHOW PRIMARY KEYS` / `SHOW IMPORTED KEYS` — the declared path F-4 wants.

Types: `NUMBER(38,0)` / `FLOAT` / `BOOLEAN` / `VARCHAR`. **Known, pinned gap:**
Snowflake's only integer type is `NUMBER(38,0)` and `INFORMATION_SCHEMA`
reports it as bare `NUMBER`, which `pg_type_to_json_type` maps to `float`
(the connector does not carry `NUMERIC_SCALE`). The roundtrip test names this
in `KNOWN_TYPE_GAPS` and fails if the gap either widens or silently closes —
D-3's "never silently wrong", applied to types. Closing it is an RSA + r2g
follow-up (report scale; scale-0 `NUMBER` → `integer`).

Roundtrip: `tests/integration/test_forge_roundtrip_snowflake.py` — throwaway
schema `R2G_RT_<seed>_<pid>_<hex>` in the real account — the `R2G_` prefix keeps it disjoint from CDF's `FORGE_<SHAPE>_<SYSTEM>` in the same database — dropped in `finally`;
skips with a reason when creds are unset **or when the role may not
`CREATE SCHEMA`** (the fabric's `CDF_RO` cannot; set `SNOWFLAKE_FORGE_ROLE`).
RSA's connector is URL-driven and cannot carry a private-key path, so the
fixture shims `snowflake.connector.connect` for *auth only*; every
introspection statement is RSA's.

### S2-3 · `clickhouse`: the first live constraint-stripped shape

ClickHouse has no FK syntax at all. The dialect emits `MergeTree … ORDER BY
(id)` (so `system.columns.is_in_primary_key` surfaces the PK) and records the
FK *intent* as a column `COMMENT 'forge:foreign-key -> accounts(id)'` plus
loader header comments. Types `Int64`/`Float64`/`Bool`/`String`, properties
`Nullable(…)`.

**Caveat, stated plainly:** RSA has **no ClickHouse connector**. The roundtrip
therefore introspects through **r2g's own `ClickHouseConnector`** (system
tables into RSA's `PhysicalSchema` shape — the real forward leg the fabric uses
for ClickHouse), then runs `rsa_ontology` and Auto-Map + CSI over it. D-3's
two-branch contract is asserted both ways: `get_schema()` reports FKs *absent*
(never invented from the comment); r2g's `infer_foreign_keys` and RSA's
baseline both *recover* exactly the planned edges; with no keys applied,
Auto-Map derives **no** edge rather than a wrong one. Runs against the new
compose `clickhouse` service (HTTP 8124, so a developer's other stack on 8123
survives; `CLICKHOUSE_DSN` overrides) and in CI.

### S2-4 · `arango`: a manifest and a loader script, not SQL

One document collection per entity (`accounts`), one edge collection per
relationship named exactly as the forward Auto-Map derives it
(`contacts_to_accounts`, `_from`/`_to` on the spine ids), documents = rows with
`_key = str(id)`, edges carry only `_key/_from/_to`. `ForgeArtifacts` keeps its
field names but reinterprets them: `ddl` is a JSON collection manifest
(`forge.collections.json`: collections, edge projections, a named graph
`forge`), `load_sql` a standalone python-arango loader (`forge.load.py`, env
driven, idempotent). `project_documents(manifest, rows)` is the same projection
as a library call; a unit test execs the script in isolation and checks the two
agree.

Roundtrip: `tests/integration/test_forge_roundtrip_arango.py` loads by
*running the generated script*, then `arangodb-schema-analyzer`'s
`AgenticSchemaAnalyzer` (deterministic baseline, no LLM) under both the
`auto` and `collection` strategies reproduces the ontology up to the CC-12
normalizers (ASA reports `account_name` and `CONTACTS_TO_ACCOUNTS`). ASA's
baseline does not type properties (everything is `string`), so type fidelity
is asserted on the SQL dialects only.

### S2-5 · Temporal stays out (for now)

The brief lists `TIMESTAMP_NTZ` / `DateTime64`, but the forward map collapses
every temporal type to `string`, so a `temporal` conceptual type has no honest
roundtrip today. Adding one is a forward-pipeline change first (a distinct JSON
type in `DEFAULT_TYPE_MAP` and the CSI), then a one-line entry per dialect.

## Deliverables (S1 PR)

| # | Artifact | Where |
|---|---|---|
| 1 | This plan | `docs/internal/PLAN-federation-forge.md` |
| 2 | Generator core: `ForgeOntology` loader/validator, `generate(ontology, dialect="postgres", seed)` → `ForgeArtifacts(ddl, load_sql, rows)` | `src/r2g/forge/` (`core`, `generate`, `dialects/`) |
| 3 | CLI: `r2g forge generate --ontology o.json --seed 421 --out-dir …` (thin body, function-local imports, validate-before-try) | `main.py` sub-app |
| 4 | Unit tests: determinism (byte-identical re-run), naming inverse through the real `owl_*` normalizers, type inverse through `pg_type_to_json_type`, FK spine agreement, refusal cases | `tests/test_forge.py`, `tests/test_cli_forge.py` |
| 5 | Live roundtrip: temp PG schema ← DDL+load, `PostgresConnector.get_schema` → `generate_default_config` → `mapping_to_csi` → compare to input ontology (D-3, real pipeline, no forge shortcuts) | `tests/integration/test_forge_roundtrip.py` |

Non-goals here: partitioner/denormalizer/goldens (S3),
scale knobs (S4), any change to `MappingConfig`/`Schema` serialization
(byte-stability guard stays untouched), PRD phase-table entry (goes through
`/prd-sync` with the user, not a silent edit).

## The honest-fidelity caveat (from recon, worth keeping visible)

`MappingConfig` sits between schema and CSI and carries derived decisions
(join-table flags, edge names). The generator emits physical schemas and the
re-introspection re-derives those decisions independently via Auto-Map —
*those two derivations agreeing is the actual content of the fidelity claim*.
That is exactly what ADR-0006 wants tested (the estate is in the loop), and it
is why the roundtrip test compares CSI conceptual models, not intermediate
artifacts.
