---
name: pr-review
description: Review one pull request on arango-solutions-org/r2g-arango the way this repository needs — verify every claim the PR makes with a command rather than by reading, apply the repo's own rules (PRD traceability and the drift gate, CHANGELOG discipline, public-repo redaction, downstream pins in contextual-data-fabric, the relational-schema-analyzer band), and produce a draft review for the user to approve before anything is posted. Use when asked to review a PR, check whether a PR can merge, re-check a PR after its author pushed fixes, or review a PR's claims against the code. NOT a whole-repo audit, and NOT for PRs in sibling repos (relational-schema-analyzer, arango-schema-analyzer, contextual-data-fabric have their own).
---

# PR Review

One pull request, reviewed by verification. The built-in `/code-review` reads a
diff and finds bugs; this runs r2g's own checks against the branch, checks every
claim in the description and in any doc the PR adds, and drafts a review that
says which claims were executed and which remain read.

## Invocation

`/pr-review 15` — full review, draft only.
`/pr-review 15 recheck` — the author pushed after review: re-run each finding's
reproduction on the new head, nothing else.
`/pr-review 15 post` — post the approved draft (only after the user has seen it).

## The two rules that make it worth running

**Nothing is posted until the user has read the draft and said so.** Draft in
the reply, offer to post in one line, post verbatim when told. The repo is
**public**; the user decides tone, timing and whether it goes out at all.

**Verify, don't opine.** Every finding names the command that produced it or
says "read, not executed". The October 2026 reviews are the calibration. What
changed anything was found by running something:

- **#10** (research report): the headline finding — bulk and streamed rows get
  different `_key`s — was impossible. `config.py` rejects expressions that
  target `_key`, and routing is per expression, not per load path. The probe
  scripts did not reproduce the report's tables (two errors compared as
  DIFFERENT; CEL error values recorded as successes; a database a fresh stack
  lacks).
- **#13** (PRD wording): a requirement could be satisfied by code that still
  left the problem in place. The rejection was placed in `validate_config`,
  which no load command calls. CDC was left out although the PR's own patch
  record named it. The same PR left P5c.2.5 and the engines table contradicting
  it.
- **#4/#5, post-merge**: the 0.4.3 release had overwritten the 0.4.2 CHANGELOG
  heading. A blank line ended the Phase 13 PRD table early. A drift test
  described as guarding CI never ran there (the analyzer it needs is only in the
  `dev` extra, which the test job does not install).

Re-run before you post. A finding you did not re-check is a finding that can be
wrong.

## This repo's shape — read before anything

- **Two GitHub repos.** PRs live on **`arango-solutions-org/r2g-arango`**. `origin`
  fetches from it and pushes to it *and* to `ArthurKeen/r2g-arango`. Pass
  `--repo arango-solutions-org/r2g-arango` to every `gh` call.
- **Merges are merge commits** and `delete_branch_on_merge` is off. A
  **stacked PR** (base is not `main`) merges *into its parent branch*, not into
  `main`, if the parent merged first. That stranded two stacked PRs in
  relational-schema-analyzer (fixed with a follow-up PR). Say in the review who
  retargets the stacked PR, and when.
- **The venv is an editable install of the main checkout's `src/`.** In a
  review worktree, put the worktree first, or you silently test `main`:
  `PYTHONPATH=$W/src .venv/bin/python -m pytest …`.
- **Releases.** A `v*.*.*` tag push *is* the publish. Only the ArthurKeen run
  publishes; the arango-solutions-org Release run fails by design (not a registered
  publisher). Never push a tag as part of a review.
- **Downstream.** contextual-data-fabric pins r2g **exactly**
  (`deploy/pins/r2g-arango.txt`) and commits Forge output, so a change to
  `src/r2g/forge/` output is a CDF bump with regenerated shapes. r2g pins
  relational-schema-analyzer to a band (`pyproject.toml`), and `r2g.fk_inference`
  re-exports RSA's, so an RSA behaviour change reaches r2g through that band.

## Method

### 0. Load the context before reading the diff

```sh
R=arango-solutions-org/r2g-arango
gh pr view N --repo $R \
  --json title,author,headRefName,baseRefName,headRefOid,isDraft,mergeStateStatus,files,body,reviews,comments
gh pr checks N --repo $R
```

Read **existing reviews and comments first**. If one already lists findings, the
job is usually `recheck`. Fetch by **branch name**, never by short SHA:

```sh
git fetch origin && H=origin/<headRefName>
git log --oneline origin/main..$H
git diff origin/main...$H --stat
```

Then read the whole diff and every new file in full. For a docs or research PR,
"the diff" includes every claim the document makes about r2g's code.

### 1. Does it merge, really

```sh
W=$SCRATCH/wt-check && git worktree add -q --detach $W origin/main && cd $W \
  && (git merge -q --no-edit $H && echo clean || { git diff --name-only --diff-filter=U; git merge --abort; }) \
  ; cd - && git worktree remove --force $W
```

### 2. Run what CI runs, and what the description says was run

In a worktree at the head (`git worktree add --detach $W $H`):

```sh
.venv/bin/ruff check src/ tests/                                   # CI lint scope (not `.`)
PYTHONPATH=$W/src .venv/bin/python -m mypy src/r2g
PYTHONPATH=$W/src .venv/bin/python -m pytest tests/ -m "not integration" -q \
  --cov=r2g --cov-fail-under=80
PYTHONPATH=$W/src .venv/bin/python -m pytest tests/integration -m integration -q   # needs the containers
```

The integration tests use the compose stack (`docker compose up -d --wait`):
Postgres 5432, MySQL 3306, SQL Server 1433, ArangoDB 8540, ClickHouse 8124, with
the defaults in `tests/integration/conftest.py` (which loads `.env`). If a
container is down, restart that one and say so; otherwise write "integration
read, not executed". Compare every count with the description. Two different
"N passed" figures in one PR is a finding.

### 3. Verify every claim that can be verified

| The PR says | Run |
|---|---|
| "fixes X" / "regression test" | revert the fix in the worktree and run the new test; it must fail without the fix |
| a behaviour claim about r2g ("routes to", "bulk does", "never") | find the code path and **reproduce it** (a 10-line script against `NodeTransformer`, `validate_config`, a load command). Docs PRs make these claims too |
| "every result can be re-run from the probes" | run the probes; compare their output with the published tables cell by cell |
| "compared with ArangoDB / Postgres" | run the comparison yourself against the test containers, with bind variables if the claim is about delegation |
| "requires / validated by `validate_config`" | `grep -rn "validate_config(" src/r2g` — which commands actually call it? |
| "see `path`" / "built by `scripts/foo.py`" | `git cat-file -e "${H}:path"` |
| "closes #N" | read the issue; does the PR address each bullet, and say which it does not |
| "RSA 0.X does Y" | install that version in a scratch venv and check, or read it on RSA's `main` |

### 4. The repository's own rules

- **PRD traceability and the drift gate** (`CLAUDE.md`, `docs/PRD.md`). A PRD
  row that ships with its code is edited in the same PR, and **the commit
  subject names the PRD edit**. Anything else (code drifted from a requirement,
  an unplanned guarantee) is a proposed patch via `/prd-sync`. No silent PRD
  edits either way. A row marked done carries `file:line` evidence that passes
  `.claude/skills/prd-sync/check_evidence.py`.
- **Requirement wording that a fix can satisfy without solving the problem** is
  a must-fix. Check that the place the requirement names is on every path it
  must cover (bulk, streaming, CDC; every load command), and that nothing
  elsewhere in the PRD now contradicts it.
- **PRD tables.** A blank line inside a table ends it, and the rows after it
  stop being rows. A new status cell keeps the table's column count.
- **CHANGELOG discipline** (PR template). A user-facing change has an entry
  under `## [Unreleased]`, with one `### Added/Changed/Fixed` heading each. A
  release PR adds a new version heading *above* the last one and never replaces
  it, and fixes the compare links at the bottom.
- **Public repo.** No credentials, no customer or prospect names, no live
  account state, no agent-session noise in committed text or in the review.
  Access-control findings go to a private security advisory, not a comment.
- **Test what you touch.** New behaviour has a test that fails without the fix.
  A test that "pins a known gap" should say what will flip it.
- **Dependency moves are deliberate.** A PR that changes the RSA band says what
  output changes (fingerprints, inferred FKs) and updates the parity tests that
  pinned the old behaviour. A Forge output change names the CDF bump it implies.

### 5. Flag for the next code-quality-review

`code-quality-review` is this repo's periodic whole-tree audit (duplication,
hardwired values, security, docs drift, docs duplication, PRD quality) — run
every few weeks across six parallel agents, not scoped to one diff. Don't
invoke it here. But if this PR's diff introduces or reveals a duplication, a
value that should flow through env but is baked in instead, or a pattern one
of its six dimensions would flag, say so explicitly in the report — that's
what lets the next periodic run skip ground this review already covered,
instead of rediscovering it cold. If `code-quality-review`'s own file has a
"Known clean" section, check it first: don't re-raise something already
verified there unless the code it covers has moved.

### 6. `recheck` — after the author pushes

Do not re-read the diff. For each finding, re-run the reproduction it gave on
the new head and report pass/fail per item. If a finding gave none, construct
one before judging, and say you did.

## Output

Draft in the reply, for the user. Say who it is written for in one line, then
give the review ready to paste. Shape, in this order:

1. **Verdict in one sentence**: merge / merge after X / don't merge yet, and why.
2. **Verified**: what was run and passed. Lead with this.
3. **Must fix before merge**, numbered, each with the command or `file:line`
   that found it.
4. **Should fix** and **Minor**.
5. **Answers to the author's own open questions**, if the PR asks any.
6. **Decisions for the user**, kept separate: things only they can call
   (release timing, a version number, a requirement trade-off).

Plain language throughout. The user often asks for **CAR form** per finding
(Context: what happens now; Action: the change; Result: what becomes true). Use
it when asked, and for any review the user will forward. Name strengths
specifically. No internal code names a reader cannot look up.

When posting: `gh pr review N --repo $R --comment|--request-changes|--approve
--body-file -`. The user is usually the author, and GitHub refuses
`--request-changes` on your own PR, so use `--comment`.

## Known gotchas

- `$H:tests/x.py` in zsh: `:t` is a modifier. Quote it: `"${H}:tests/x.py"`.
  The same goes for `$ref:r…`.
- `git fetch origin <short-sha>` fails. Fetch the branch.
- The r2g drift hook (`.claude/hooks/drift_queue.py`) runs on every Edit/Write
  and errors in a scratch worktree, which has no hooks. The edit still lands;
  use shell edits there to avoid the noise.
- `gh pr checks --watch` can exit while a job is pending. Loop on
  `grep -qE "pending|queued|in_progress"` instead. A bare `sleep N` is blocked.
- PyPI's JSON API lags a fresh upload by minutes. Check the simple index or a
  fresh `pip install --no-cache-dir` before saying a release is missing.
- "Merged" on a release PR does not mean tagged. Check
  `git ls-remote --tags` on both repos.
- `git stash` is shared across worktrees. Use a WIP commit instead.
