---
name: code-quality-review
description: Run the periodic repo-wide quality audit of r2g — duplicate code, hardwired values, security risks, documentation drift from code, duplicated content across documents, and PRD quality (readability, bloat, requirement traceability) — across the whole tree rather than one diff. Produces a ranked report plus a remediation plan grouped into PR-sized units, and turns every mechanizable finding into a test. Use when asked for a code quality review, a codebase audit, a security review, a documentation audit, a PRD review, or a check for drift/duplication/hardcoded values/doc redundancy; and before a release. NOT for reviewing a single PR or diff — that is `/pr-review`. NOT for reconciling the PRD against the code — that is `/prd-sync`.
---

# Code Quality Review

The periodic whole-repo audit. `/pr-review` looks at one pull request; this looks at
the codebase, in six dimensions, and is meant to be run every few weeks or before a
release rather than per PR. Ported from contextual-data-fabric's skill of the same
name (2026-10-14); the method is theirs, the context below is r2g's.

## Invocation

`/code-quality-review` — all six dimensions.
`/code-quality-review security` (or `duplication`, `hardwired`, `docs`,
`docs-duplication`, `prd`) — one.

## The rule that makes it worth running

**Verify every finding against the source before reporting it, and say which ones you
executed.** A review that reports plausible findings is worse than no review: it costs a
maintainer the afternoon it takes to discover the claim was inferred. Quote `file:line`
you have actually opened. Where a claim is reproducible, reproduce it and paste the
output. Where it is not, say what you read and what remains assumption.

r2g has its own calibration for why this matters: in one 2026-09 session a stale `.pyc`
made 13 tests fail and a reviewer nearly reported a regression in merged code; mutation
testing then found real bugs the passing suite had hidden (a MySQL functional-index key
reported on one column, an upper-case `singularize` that matched nothing). Execution, not
reading, found all of them. Clear `__pycache__` before trusting a red or a green.

## Start from the previous audit

`docs/internal/PLAN-code-quality-remediation.md` is the output of the last audit
(rewritten 2026-11-22, baseline 4022 unit tests / 87% coverage). Before fanning out:

1. Check each of its **open** items against the current code — fixed, still open, or
   moot — and verify, don't assume.
2. Hand each agent the items in its dimension, so it confirms or retires them rather
   than rediscovering them.
3. At the end, **rewrite that plan** to the new state (see "Rewrite what you retire"
   below). Never start a second plan beside it.

## Method

Fan out one subagent per dimension in parallel, then verify their headline claims
yourself before any of it reaches the user. r2g is ~34k lines of Python under
`src/r2g`, a large single-file Studio front end (`src/r2g/ui/static/index.html`), ~7k
lines of Markdown under `docs/`, and a 1,280-line (259k-character) PRD; serial review in the main thread
does not fit.

Give each agent the repo context below, and tell it explicitly: rank by consequence,
distinguish deliberate design from drift, skip trivia, and state what came up clean so
the next run can skip it.

### 1. Duplicate code
Copy-paste families, and above all **copies that have already drifted** — a guard
present in one and missing in its twin. Deliberate parallel structure is not a finding
unless it has diverged: the per-engine connectors (`src/r2g/connectors/*`), the Forge
dialects (`src/r2g/forge/dialects/*`), and the samplers that subclass
relational-schema-analyzer's. Watch especially for **logic duplicated between r2g and
relational-schema-analyzer (RSA)** that has drifted: `naming.singularize` and the
Snowflake URL parser were both found duplicated-and-diverged in 2026-09/10. Name the
seam any consolidation should live in, including "move it to RSA".

### 2. Hardwired values
Hosts, ports, DSNs, database and collection names, magic numbers, paths. Values reach
code through env vars, CLI options or the catalog (`~/.r2g/catalog.json`) with a
documented default; a value baked into a `.py` or into `index.html` is a finding. The
sharpest findings are **drift between a knob and a copy of its value** — a default
stated in the CLI, the README and the test conftest that disagree.

### 3. Security
This repository is **public** (`arango-solutions-org/r2g-arango`, mirrored to
`ArthurKeen/r2g-arango`), and is published to PyPI as `r2g-arango`. Priorities, in order:

1. **Query and identifier injection on every generated query** — SQL against sources
   (table previews, dumps, value samplers), AQL in the streaming delegation path, and the
   Forge's generated DDL (identifiers are *refused*, not quoted — `forge/core.py`).
   Previously fixed: table-preview SQL injection in `ui/server.py` and `mcp_server.py`.
2. **Generated shell scripts** — the `arangoimport` preview (`generators/arangoimport.py`)
   emits a script an operator runs; every operator-controlled value must be
   `shlex.quote`d.
3. **Credentials** — catalog credentials encrypted by `security.CredentialCipher` (key
   from env or a `0600` key file); `scrub_dsn_credentials` and the structlog
   `_redact_secrets` processor keep DSNs out of logs; Snowflake key-pair files are
   referenced via `$VAR`, never stored. Check every path that prints, logs, returns over
   HTTP or MCP, or writes an error message.
4. **Surfaces** — the Studio FastAPI app (`ui/server.py`, binds 127.0.0.1 by default)
   and the MCP server (`mcp_server.py`, 23 tools): what an attacker who can reach them
   controls, and what they can read or write.
5. **Supply chain and CI** — dependency bands, the release workflow (a tag push
   publishes to PyPI), `pull_request` vs `pull_request_target`.

Demand a concrete exploit path — who the attacker is, what they control, what they get —
and label anything without one as hardening rather than a vulnerability.

### 4. Documentation consistency
Docs-vs-code drift: a documented CLI command, option, env var, path, port, default or
count that no longer matches the source. `README.md` and `docs/README.md` matter most,
because users follow them literally; then `CHANGELOG.md` (every entry must describe code
that exists), `examples/*/README.md`, and the `docs/internal/` plans, whose status lines
go stale fastest. Unlike contextual-data-fabric, **r2g has almost no automated docs
checks yet** (`TestPlanCitations` in `tests/test_forge.py` is the only one), so most
findings here should end as tests — see "Prefer a test over a finding".

### 5. Duplicate content across documents
Docs repeating *each other*, which produces dimension-4 drift a month later. The unit is
a **fact** — a number, a command, a path, an env var, a version band. Every place it is
written is a place it can rot independently. r2g's concentrations: `README.md` versus
`docs/README.md` versus the PRD's status prose; the RSA compatibility story told in
`DESIGN-rsa-compat-layer.md`, `PLAN-rsa-dependency-reversal.md`, `PLAN-key-identification.md`
and the PRD; release facts in `CHANGELOG.md` and the PRD. For each real finding, name
which copy should be canonical and what the others become: a link, a pointer, or nothing.

### 6. PRD quality
Scope is `docs/PRD.md`. This dimension judges the document itself — `/prd-sync`
reconciles it against the code, so do not re-run that here.

- **Readability.** Can a newcomer find the requirement governing one behaviour without
  reading the file end to end? Requirements should be atomic and citable by their
  marker (`P13.3`, `P5c.1.4`, …), because the code and the drift ledger cite them that
  way. A requirement that is really three cannot be closed independently, so it never is.
- **Bloat.** At ~1,280 lines (and 259k characters, so lines are long) the PRD is past the 1,000-line ceiling contextual-data-fabric
  holds its own documents to, so length here is a real signal, not just density. Look
  for status narrative and dated history that belongs in `CHANGELOG.md` or a plan,
  implementation detail (file paths, line numbers) that has grown into requirement text,
  superseded text left standing beside its replacement, and the same requirement stated
  twice.
- **Traceability, both directions.** Every PRD marker cited in `src/` resolves to a
  requirement that exists; every requirement is implemented, tracked as an open drift
  alert, or explicitly deferred.

## Prefer a test over a finding

The most useful output of a review is often a test, not a paragraph. A finding that can
be checked mechanically should become an assertion that fails in CI for everyone, rather
than a paragraph someone has to rediscover. Candidates r2g does not have yet, each worth
checking on the first run:

- every PRD marker cited in `src/` resolves to a requirement in `docs/PRD.md`;
- every `r2g …` command and option named in `README.md` / `docs/` exists in the CLI;
- every env var a document names is read somewhere in `src/` (and the reverse);
- key and secret files (`*.p8`, `*.pem`, `*.key`) are gitignored — Snowflake key-pair
  auth now tells users to create one, and only `.env` is ignored today;
- version and dependency-band facts quoted in docs match `pyproject.toml`.

When you write one, **verify it fails without its fix** — a guard nobody has watched
fail is not yet a guard. Reserve prose findings for what no assertion can catch: drifted
duplication, exploit paths, judgment calls.

## Repo context to hand each agent

- Python ≥3.10. r2g maps relational sources (PostgreSQL, MySQL/MariaDB, SQL Server,
  Snowflake, ClickHouse, CSV, Kafka) into ArangoDB property graphs. Surfaces: the `r2g`
  CLI (`main.py`, Typer), the Studio web UI (`ui/server.py` + `ui/static/index.html`),
  the MCP server (`mcp_server.py`), and the library API. `src/r2g/forge/` generates
  synthetic schemas and data for testing; `generators/` emits artifacts (arangoimport
  scripts, visualizer HTML).
- **The repo is public and published to PyPI.** Nothing committed is private, now or
  retroactively.
- **Depends on relational-schema-analyzer (RSA)**, pinned to a band in `pyproject.toml`
  (read the file; never quote the band here). Introspection types,
  FK inference, samplers and key overlays come from RSA. A defect may belong upstream.
- **Consumed by contextual-data-fabric**, which pins r2g exactly in
  `deploy/pins/r2g-arango.txt`. A breaking change here is a coordinated bump there.
- Spec-driven: `docs/PRD.md` holds requirements; `docs/internal/PLAN-*.md` hold plans;
  `/prd-sync` keeps a drift ledger in the shared ArangoDB memory.
- `mypy` covers `src/r2g`; coverage is enforced at 80% on the unit suite. Integration
  tests (`-m integration`) need the Docker services and are skipped otherwise — expect
  findings to cluster in code only integration tests reach.
- `CLAUDE.md` and `.claude/` (except this skill) are gitignored local state.
- Every push goes to two remotes; PRs target `arango-solutions-org/r2g-arango`.

## Known clean

Verified on **2026-11-22** against `main` at `b1f2a56`. Skip these next run unless the
code they name has moved since; `git log --since=2026-11-22 -- <path>` says whether it has.

- **Security:** table preview in Studio and MCP (`sql.Identifier`, snapshot allowlist);
  Forge DDL refuses hostile identifiers; Snowflake streams/CDC SQL quoting and
  connection-error scrubbing; AQL in CDC resync, the temporal applier and selective
  reload uses bind variables; arangoimport command arguments are `shlex.quote`d; no
  `eval`/`exec`/`subprocess`/`shell=True`/`pickle`/`yaml.load` in `src`; `catalog.json`
  and `secret.key` are 0600; a cross-site `text/plain` POST gets 422; control-plane
  routing only (another project 404s, unknown non-GET routes refused, MCP refuses to
  start) — the rest of project isolation is not known clean;
  credentials redacted in API responses; CI uses `pull_request`, release uses OIDC.
- **Duplication:** `naming.py` matches RSA exactly (38,198-word differential); offline
  sources are refused on every connection path (`require_connection_string`); every
  surface scrubs errors through `scrub_dsn_credentials`; CDC edge deletes reuse
  `EdgeTransformer`; Python `_resolve_target` and JS `_resolveProjectTarget` agree.
- **Hardwired values:** Studio port 8501, MCP SSE port 8502, bulk batch size 10000,
  CDC batch sizes, the `key_separator` default `_` (except the CSV generator, plan unit
  7), sampling defaults, MySQL/SQL Server/ClickHouse test DSNs.
- **Docs:** every fenced `r2g` command in `README.md` and the examples parses; every
  option named in CHANGELOG [Unreleased], 0.11.1 and 0.11.0 exists; every MCP tool the
  docs name is registered; Python 3.10+ is stated consistently; RSA band is not quoted
  in user-facing docs.
- **PRD:** no duplicate markers; no dangling marker citations in the repo; all 154
  `file:line` citations pass `check_evidence.py`.

## Rewrite what you retire

When the audit changes a fact a document states — an item fixed, a status flipped, a
count updated — **rewrite the sentences the new fact retires**. Do not append "Done:"
beside a sentence that still says otherwise; a stamp beside a contradiction is a
contradiction, not a record. Read the whole file, then grep the docs tree for the
retired claim, and change status in every place it lives.

## Output

Two parts, and the second is the one that gets acted on.

### The report

Ranked by consequence, most severe first, leading with the single thing to fix today.
Say what is healthy before what is broken — it is load-bearing context for how much the
findings mean.

For each finding: `file:line`, the defect in one sentence, the scenario in which it
bites, the fix, and **whether you verified it by execution or it remains inference**.
Never blur those two.

### The plan

Group the findings into PR-sized units of one concern each — small enough to review in
five minutes, reversible on its own, and passing the suite in isolation. Then:

- **Order them**, and say why: which must land first because another reads its output
  or edits the same lines, and which are independent.
- **For each unit, say whether it becomes a test or stays a prose finding.**
- **Separate the mechanizable from the judgment calls**, because they need different
  people and different amounts of time.
- **Say what you are deliberately not proposing**, and why. A finding left unfixed on
  purpose is a decision; a finding left out silently is a gap.
- **Flag anything that belongs in RSA or contextual-data-fabric** rather than here.

Rewrite `docs/internal/PLAN-code-quality-remediation.md` to match, then offer to open the
units as separate PRs.
