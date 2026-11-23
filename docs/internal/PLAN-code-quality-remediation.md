# Code-Quality Remediation Plan

> Internal working doc, rewritten by the **2026-11-22** audit (`/code-quality-review`,
> six dimensions, against `main` at `b1f2a56`). It replaces the 2026-06-05 plan; items
> that audit left open are carried below with their current state.
> Baseline: **4022 passed, 2 skipped, 18 xfailed; 87% line coverage** on the unit suite
> (`pytest -m "not integration" --cov=r2g`), 181 integration tests deselected.

## How to read this plan

Each unit is one PR: one concern, reviewable in five minutes, reversible on its own,
green in isolation. **Test** means the unit lands with an assertion that fails without
its fix; **prose** means no assertion can catch it and the judgment is recorded here.
Units are numbered in the order they should land; dependencies are stated where they
exist.

---

## 1. Carried from the 2026-06-05 audit

| Item | State on 2026-11-22 |
|---|---|
| SSRF via target introspect / source snapshot — "revisit if a hosted or multi-tenant mode ships" | Revisited now that control-plane mode has shipped; see §2. Local mode is unchanged: connecting to user-chosen endpoints is the product. |
| LOW — rate limiting on expression compile/preview and `infer-fks?sample=true` | Still open; still deliberately not done for local mode (see §5). |
| LOW — preview-modal header/title not escaped | Title fixed (`textContent`); see §2 for the rest. |
| LOW — `~/.r2g` and DLQ files created with default permissions | Still open → unit 12. |
| LOW — `openProjectDatabase` opens a user-controlled endpoint | Still open, and it accepts any URL scheme → unit 12. |
| Python `_resolve_target` vs JS `_resolveProjectTarget` | The two agree. Retired as a duplication item; the Python copy moves to `CatalogManager` in unit 14 for a different reason. |
| Coverage: `main.py` 46%, `postgres.py` 51%, `mcp_server.py` 59%, `selective_reload.py` 67%, `ui/server.py` 73%, `dump_reader.py` 76%, `node_transformer.py` 77%, `pipeline.py` 78% | Now 69%, 69%, 74%, 67%, 84%, 90%, 79%, 80%; total 87%. No coverage-only unit (see §5). |
| PRD Phase 5g "Planned"; P5c.1.5 "Not started"; P5c.2.5 preview deferred; `r2g[…]` package name | Fixed. |
| README test count | Still stale, at a new number ("over 1,000"; actual 4042 unit + 181 integration) → unit 9. |
| README CLI table stops at `stream`; quick start omits `--source` | Fixed. Residual gaps → unit 9. |
| README tree lists deleted `transformers/converter.py`; diagram and prerequisites PostgreSQL-only | Still open → unit 9. |
| CDC section missing `--temporal` / `--ttl-seconds` / `--smart-field` | Mostly fixed; the CDC examples still omit the last two → unit 9. |
| `PLAN-mapping-ui-catalog-reingest.md` stale | Retired: it carries an ARCHIVED banner. |

---

## 2. Security

Five findings with a concrete exploit path were reproduced by execution and are
**reported in a private security advisory**, not here: this repository is public. Their
fixes are units 1–5 below, described here only once the advisory is published.

Hardening without a current exploit path is unit 12.

**Verified clean (2026-11-22, by execution unless noted):** table preview (hostile names
get 404 / an error; `preview_table_rows` uses `sql.Identifier`); Forge DDL (hostile
names refused by the naming round-trip); Snowflake streams/CDC SQL quoting and
connection-error scrubbing; AQL in CDC resync, the temporal applier and selective reload
(bind variables); `shlex.quote` on every arangoimport command argument; no
`eval`/`exec`/`subprocess`/`shell=True`/`pickle`/`yaml.load` in `src`; `catalog.json`
and `secret.key` written 0600; cross-site `text/plain` POST answers 422; under a control
plane another project's routes answer 404, unknown non-GET routes are refused, the MCP
server refuses to start, `suggest-ontology` sampling is refused; API responses redact
source, target and catalog credentials; CI uses `pull_request`, not
`pull_request_target`; release publishes through OIDC trusted publishing.

---

## 3. Units, in landing order

### A. Security fixes (private until the advisory is published)

1–5. Tracked in the private advisory, each with a regression test. One of them needs a
   design decision before it can be fixed, and that decision is coordinated with
   contextual-data-fabric. It lands before unit 14, which edits the same code.

### B. Small correctness fixes (independent of each other)

6. **`r2g secrets init|status` use the catalog's directory** — PR #110. Both hardwire
   `Path.home()/".r2g"` (`main.py:4329`, `:4371`) while the catalog reads
   `<catalog_dir>/secret.key`, honouring `R2G_CATALOG_DIR` (`catalog.py:300-309`).
   Under `R2G_CATALOG_DIR` (the control-plane image sets `/data`), `status` reports the
   wrong file and `init` writes a second key the catalog never reads. One
   `catalog_dir()` helper; fix `README.md:792,854` and `.env.example:16`. *Test.*
7. **CSV-direct import script honours `key_separator`** — PR #111. `generators/arangoimport.py:923,
   979, 991` join composite keys with a hardcoded `_`; the transformers use the
   mapping's separator. A non-default separator gives bulk CSV loads keys that streaming
   and CDC never produce. *Test* (parametrize the separator; compare with
   `NodeTransformer`).
8. **Small surface fixes, one PR each:**
   - Studio table preview does not expand `$VAR` connection strings (`ui/server.py:1023`);
     MCP does (`mcp_server.py:637`). Expand inside `preview_table_rows`. *Test.*
   - `transform-all` / `transform-nodes` / `transform-edges` ignore `graph_layout: lpg`
     (`main.py:266,391,411,824`) and write property-graph keys an LPG load would not;
     refuse LPG there, as `generators/arangoimport.py` does. *Test.*
   - `kafka-start` (`main.py:1912-1921`) and `project create` (`main.py:3661-3662`)
     lack the `ARANGO_*` `envvar=` every other Arango option has. *Test* (walk the Typer
     app; every Arango option declares its env var).
   - `LoadRequest` (`ui/server.py:2440-2441`) has no bounds on `workers` / `batch_size`;
     only the browser enforces 1–16 and ≥100. *Test.*
   - `FastAPI(version="0.1.0")` (`ui/server.py:243`) → `r2g.__version__`. *Test.*
   - `.env.example:6-12` placeholders, once copied to `.env`, override the
     integration conftest's compose defaults and silently skip the integration suite.
     *Test* (each uncommented key conftest reads equals conftest's default).
   - Four hand-rolled `$NAME` resolvers ignore `${NAME}` (`main.py:3360`,
     `ui/server.py:52,2300`, `mcp_server.py:258`) → `expand_env_vars`. `update_source`
     does not normalise the source type (`catalog.py:439-448`) and `cdc-start` refuses
     a stored `postgres` alias (`main.py:1788-1790`) → `is_postgresql()`. *Test.* Low.

### C. Documentation and PRD guards (each lands with the fixes its test demands)

9. **`tests/test_docs_consistency.py`**, with the doc fixes that make it pass:
   - every fenced `r2g …` command in `README.md`, `docs/`, `examples/` parses against
     the CLI — fails today on `docs/BIGQUERY-TESTING.md:168-170,187-188`
     (`add-source`, `introspect`, `map` do not exist);
   - every option named in the README CLI tables exists on its command — fails on
     `--allow-sensitive` (`README.md:578`) and `--verbose` as a per-command option
     (`README.md:614`; it is `r2g -v <command>`);
   - env vars read in `src/` are a subset of those documented in `README.md` ∪
     `.env.example` — fails on `CONTROL_PLANE_HOME`, `OPENAI_BASE_URL`,
     `R2G_CATALOG_DIR`, `R2G_TENANT`, `CONTROL_PLANE_URL`;
   - the README extras table equals `[project.optional-dependencies]` — fails on `llm`,
     `ontology`;
   - no status banner quotes an RSA band other than `pyproject.toml`'s — fails on
     `DESIGN-rsa-compat-layer.md:3`, `NOTE-denorm-engine-location.md:3`, and this
     skill's own `SKILL.md`;
   - every path in the README project tree exists — fails on `converter.py`.
   Prose fixes in the same PR: README roadmap (`:814-826`, contradicts `:787`), test
   counts (`:800`), PostgreSQL-only diagram and prerequisites, "ArangoDB tested with
   3.11+" (CI and compose run 3.12), `r2g ui` "opens" the page (it starts the server),
   CDC examples, `CONTRIBUTING.md:67-71` missing ClickHouse, internal plan status
   banners (`PLAN-denormalization-analysis.md:3`, `PLAN-external-data-catalogs.md:3`,
   `PLAN-rsa-dependency-reversal.md:3`).
10. **`tests/test_prd_shape.py`**, with the one PRD edit it demands (the commit subject
    names it): markers unique; no table row after a blank line — fails today, lines
    1119 and 1123 split the Phase 13 table so P13.9–P13.13 render as text (the same
    defect the 0.4.x review found); consistent column counts; every marker cited in
    the repo resolves; every PRD `file:line` passes `check_evidence.py`. The last three
    pass today (0 dangling of 54 cited markers; 154 of 154 citations verify) and guard
    against regression.
11. **The README's sensitive-data claim** (`README.md:578`) says above-threshold fields
    are excluded at load unless `--allow-sensitive`. Only the Studio load does that by
    default; `cdc-start`/`kafka-start` do it with `--govern` (default off); `r2g stream`
    never does. **Judgment:** add the gate to `stream`, or reword the README per path.
    Open drift gap P9.5 already tracks the code side.

### D. Hardening (one PR, after the advisory)

12. Hardening batch, *test* where noted: DLQ files and the catalog directory created
    0700/0600 (`dlq.py:23,40`, `catalog.py:306`, `governed.keep_version`) *test*;
    `openProjectDatabase` (`index.html:2487-2491`) accepts only `http(s):` *test*;
    `scrub_dsn_credentials` misses `://:secret@`, `?password=` and passwords containing
    `@` *test*; MCP `validate_mapping` / `diff_mappings` / `create_project` read any
    path and echo content in validation errors — jail them like `_jailed_save_path`
    *test*; gitignore `*.p8`, `*.pem`, `*.key`, `secret.key`, `catalog.json` *test*;
    constant-time bearer comparison (`ui/server.py:290`); CI `permissions:` block and
    SHA-pinned actions; delete or bind-parameterise the uncalled
    `temporal/queries.py:43-47`.

### E. Consolidations that need design (ordered after the PRs that touch the same code)

13. **One `_key` rule for bulk, streaming and CDC.** The rule is written three times
    (`node_transformer.py:180`, `edge_transformer.py:114`, `delta_transformer.py:190`)
    and the copies agree, but their inputs differ: bulk receives psycopg's typed values,
    CDC the decoder's text. For a numeric + `timestamptz` primary key the same row gets
    `1.50_2020-01-01%2000:00:00+00:00` from bulk and `1.5_2020-01-01%2000:00:00+00`
    from CDC (executed with each path's input types; not yet against a live
    replication slot), so a CDC delete misses and an update duplicates. Canonicalise
    on the CDC side so existing graphs keep their keys. *Test.* **After PR #108**,
    which changes the same rule for keyless tables.
14. **MCP `start_load` shares the Studio's load orchestration.** `mcp_server.py:676-741`
    ignores the project's linked target and credentials, and skips `validate_config`,
    the sensitivity gate and the DLQ, all of which the Studio load applies
    (`ui/server.py:1922-2060`). Move `_resolve_target` (`ui/server.py:320`) to
    `CatalogManager.resolve_target` and share one load function. *Test.* **After units 1–5
    and PR #106** (both edit these functions).
15. **The Snowflake URL parser moves to RSA.** r2g's (`connectors/snowflake.py:376`) and
    RSA's (`connectors/snowflake.py:463`) have drifted both ways: r2g refuses
    `authenticator=externalbrowser` without a password and validates `include_views`;
    RSA accepts the first and ignores the second. `suggest-keys` uses RSA's, snapshot /
    stream / cdc-start use r2g's, so an SSO source passes one and fails the other.
    RSA's `_safe_connection_error` also lacks r2g's URL scrubbing. **RSA work first**,
    then r2g imports it. *Test* (differential, both parsers on the same URLs) until then.

### F. Judgment calls (no unit until decided)

16. `generate-import --on-duplicate` defaults to `ignore` (`main.py:454`); the library,
    `generate-csv-import`, `stream`, `selective-reload` and the Studio default to
    `replace`. Align, or document why.
17. Every surface defaults the database to `_system`, which the Studio refuses to load
    into (`ui/server.py:1480,2015`); the CLI and MCP write there without complaint.
18. Repository URLs name `ArthurKeen/r2g-arango` (CHANGELOG ×18, README badge,
    `pyproject.toml [project.urls]`) while PRs live on `arango-solutions-org`. The
    ArthurKeen repo is the PyPI publisher, so this may be deliberate; decide, then
    guard with a test.
19. `CHANGELOG.md:81` says 0.11.1 needs Polars ≥1.41.1; `pyproject.toml:55` allows
    `polars>=1.0.0`. Raise the floor (a dependency move) or reword.
20. **PRD restructuring**, after the open PRs that edit PRD rows (#104, #105, #106,
    #108) merge, or every one of them conflicts:
    - stale status prose, proposed via `/prd-sync` as `OUTDATED-PRD` patches, not
      edited directly: header Version 0.1.0 and Status cell (`:8,10`), roadmap overview
      stopping at Phase 6 (`:86`), §6 "P6.7, planned" against row P6.7 Done
      (`:1205` vs `:202`), Phase 12 heading "P12.1 shipped" (`:1041`), superseded
      single-label text in P13.3 (`:1113`), encryption "AES-256 or OS keychain"
      (`:271`) against Fernet (`:186,353`), the reference to `PRD-gemini.md` /
      `PRD-notebooklm.md`, which are not in the repository (`:1280`). P14.15
      "unreleased" is already fixed in PR #104;
    - one Status cell per requirement from a fixed list — 62 rows have none
      (P5.1–8, P5d.1–5, P8.1–8, P9.1–9, P10.1–8, P11.1–10, P12.2–8), and P12.7 still
      says "due 2026-07-24";
    - bloat: the file is 1,280 lines but 259k characters (longest line 5,728). The
      Document history (`:1213-1280`, 46k characters) repeats `CHANGELOG.md`; phasing
      and research narrative (~28k characters at `:550-587, 688-740, 779-823, 855-930,
      998-1022, 1145-1153`) belongs in the matching plans. Then add a size budget in
      characters to unit 10's test;
    - split the omnibus rows (P6.16, P12.1, P6.4, P12.12, P12.13) into child markers,
      updating code citations and the drift ledger together.

---

## 4. Belongs elsewhere

- **relational-schema-analyzer:** unit 15 (Snowflake URL parser, authenticator policy,
  `include_views`, URL scrubbing in `_safe_connection_error`).
- **contextual-data-fabric:** one of units 1–5 needs its agreement (see the advisory).
  Unit 6 changes where `secrets` looks under `R2G_CATALOG_DIR`, which the
  control-plane image sets.

## 5. Deliberately not proposed

- **SSRF blocking in local mode** — connecting to the endpoint the user names is the
  product.
- **Rate limiting** — the Studio is a local tool; revisit if a shared control-plane
  deployment needs it.
- **Importing RSA's byte-identical copies** (`topo_sort.py`, `naming.py`, the MySQL and
  SQL Server URL parsers, three CSV helpers) — none has drifted (a 38,198-word
  differential on `naming` found 0 mismatches); swapping them is churn until one does.
- **A coverage-only push** — 87% clears the 80% gate. `main.py` (69%) is the one large
  gap, and units 6, 8 and 9 add tests exactly where this audit found defects in it.
- **Marketing copies** of the supported-engine list (`docs/marketing/`).
- **147 PRD markers never cited in code** — informational; citing is not required.
