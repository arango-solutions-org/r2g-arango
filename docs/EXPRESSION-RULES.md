# Expression rules

**Status:** accepted 2026-11-04. Required by PRD P5c.1.4. r2g's routes do not all
follow it yet: see "Where the rules are checked" below.
**Why it exists:** r2g can calculate a field in two ways. It can use its own Python
evaluator, or it can send the expression to ArangoDB. The two give different answers
for the same row, because nobody has decided what the right answer is. This page
decides it. Every way of calculating a field, now and later, must follow it. That
includes the Python evaluator, ArangoDB, streaming, and SQL sent to the source
database.
**Background:** [`research/transformation-language-evaluation.md`](research/transformation-language-evaluation.md),
sections 4 and 5.

A **blank** below means a missing value, which is `null` in the expression language.

---

## The decisions at a glance

Each row is one decision, accepted on 2026-11-04.
"Changes r2g" means an existing mapping could store a different value after the change.

| # | Question | The rule | Changes r2g? |
|---|---|---|---|
| 1 | What is a blank plus a number? | Blank | No |
| 2 | Is a condition about a blank ever true? | Never, not even when negated | **Yes**, for `NOT` and `!=` |
| 3 | What can be used as a condition? | Only true or false values | **Yes** |
| 4 | What do `&&` and `\|\|` return? | True, false, or a blank when a blank leaves the answer open | **Yes**, for blanks |
| 5 | How are blanks joined into text? | `CONCAT` treats a blank as empty text. `CONCAT_SEPARATOR` leaves blanks out | **Yes**, for `CONCAT_SEPARATOR` |
| 6 | What does `+` do with text? | Nothing. `+` is for numbers. Join text with `CONCAT` | **Yes** |
| 7 | What is text that isn't a number, as a number? | Blank | No |
| 8 | How are numbers written as text? | Whole numbers without a decimal point. Others in the shortest exact form | **Yes** |
| 9 | Where do text positions start, and what do they count? | At 0, counting characters | No |
| 10 | What is `7 / 2`, and what is dividing by zero? | `3.5`. Dividing by zero gives a blank | No |
| 11 | Can text be compared with a number? | No. Refused when checked, blank if it slips through | **Yes** |
| 12 | Is empty text the same as a blank? | No | No |
| 13 | Do `COALESCE` and `??` stay? | Yes, both, meaning the same thing | No |
| 14 | Which time zone do times use? | UTC, written in ISO-8601 with `Z` | No, r2g has no time functions yet |

Two rules sit above all of these:

- **This page is the definition. No engine is right by default.** Where ArangoDB,
  Postgres or the Python evaluator disagree with a rule, that engine's route has to be
  changed or guarded to match it.
- **A route that cannot follow a rule refuses the expression.** It does not run the
  expression a different way. The refusal names the collection, the field and the rule.
  An error is never stored as a field's value.

---

## How the evidence was gathered

Every "today" answer below was measured, not assumed:

- **r2g:** the Python evaluator on `main` at `c64a838`, run on 2026-11-03.
- **ArangoDB:** version 3.12.11, run on 2026-11-03 with the row passed in as a bind
  variable, the same way r2g's delegation query passes it.
- **Postgres:** version 16.14, run on 2026-11-03.
- **DuckDB, ClickHouse and CEL:** the 2026-09-29 probes in
  [`research/transformation-language-probes/`](research/transformation-language-probes/).

---

## The rules

### 1. A blank in arithmetic gives a blank

`@x + 1` where `x` is blank.

| r2g today | ArangoDB | Postgres, DuckDB, ClickHouse | Rule |
|---|---|---|---|
| blank | `1`, because it reads a blank as 0 | blank | **blank** |

This keeps r2g's current behaviour, which matches every SQL source. ArangoDB's answer
invents a value the source never had. **What has to change:** the ArangoDB route must
guard arithmetic so that a blank operand gives a blank result.

### 2. A condition about a blank is never true

`@score < 30 ? "low" : "high"` where `score` is blank, and the same with `NOT (...)`.

| Expression, `score` blank | r2g today | ArangoDB | Postgres | Rule |
|---|---|---|---|---|
| `@score < 30` | not true, so `"high"` | **true**, so `"low"` | not true, so `"high"` | not true |
| `NOT (@score < 30)` | **true** | not true | not true | not true |
| `@score != 5` | **true** | true | not true | not true |
| `@score == null` | true | true | needs `IS NULL` | true |

Today no two of the three engines agree on all of these rows. The rule is the
one SQL uses, put in one sentence: **a missing value never makes a condition
true, and negating the condition doesn't change that.** As a value, a comparison
with a blank gives a blank, and `NOT` of a blank is a blank. To test for a blank,
write `@x == null` or `@x != null`. These are the only conditions that look at blanks
on purpose.

**Why this one:** every SQL source already works this way, so source-database
pushdown gets it for free. ArangoDB needs guards whichever rule is chosen.

**What changes for r2g:** a mapping that uses `NOT` or `!=` on a column that can be
blank picks the other branch for rows where that column is blank.

### 3. Only true or false values can be used as conditions

`@nickname ? @nickname : @name`, or a list column used as a condition.

| Value used as a condition | r2g today | ArangoDB | SQL | Rule |
|---|---|---|---|---|
| empty text `""` | false | false | not allowed | **refused** |
| `0` | false | false | not allowed | **refused** |
| an empty list | false | **true** | not allowed | **refused** |
| a true or false column | as is | as is | as is | as is |

Each engine has its own idea of which values count as false, and SQL doesn't allow
non-boolean conditions at all. **The rule:** when a mapping is checked, refuse a
condition that is text, a number or a list, using the source column types. The
curator writes what they mean, for example `@nickname != null && @nickname != ""`.
If a value of the wrong type still reaches a condition at load time, the condition is
not true.

**What changes for r2g:** expressions that rely on text or numbers counting as true
or false will be refused when the mapping is checked, with a message saying how to
write the test.

### 4. `&&` and `||` give true, false or a blank

`@a || @b` where `a` is blank, and `"y" && "x"`.

| Expression | r2g today | ArangoDB | Postgres | Rule |
|---|---|---|---|---|
| `@a \|\| false`, `a` blank | false | false | blank | **blank** |
| `@a \|\| true`, `a` blank | true | true | true | **true** |
| `@a && true`, `a` blank | false | blank | blank | **blank** |
| `@a && false`, `a` blank | false | false | false | **false** |
| `"y" && "x"` | true | `"x"`, which is one of the operands | not allowed | **refused**, see rule 3 |

A blank decides the answer only when the other side can't: `blank || true` is true
and `blank && false` is false. Otherwise the answer is a blank, which rule 2 treats
as not true. This is what keeps rule 2 holding under `NOT`: if `blank || false` were
false, `NOT (blank || false)` would be true. Neither operator ever returns one of
its operands.

**What changes for r2g:** today a blank inside `&&` or `||` counts as false. A
mapping that negates such a condition picks the other branch for rows where the
blank decides it.

### 5. `CONCAT` treats a blank as empty text

`CONCAT(UPPER(@first), " ", @last)` where `first` is blank.

| Expression, blank argument | r2g today | ArangoDB | Postgres | Rule |
|---|---|---|---|---|
| `CONCAT(UPPER(@first), " ", "Smith")` | `" Smith"` | `" Smith"` | `" Smith"` with `CONCAT`, blank with `\|\|` | **`" Smith"`** |
| `CONCAT_SEPARATOR("-", "a", @n, "b")` | **`"a--b"`** | `"a-b"` | `"a-b"` with `CONCAT_WS` | **`"a-b"`** |
| `UPPER(@n)` | blank | **`""`** | blank | **blank** |
| `LENGTH(@n)` | blank | **`0`** | blank | **blank** |

`CONCAT` treats a blank argument as empty text. `CONCAT_SEPARATOR` leaves a blank
argument out, so no doubled separator appears. ArangoDB and Postgres already do this.
Functions that work on a single value, such as `UPPER`, `LOWER`, `TRIM`, `SUBSTRING`
and `LENGTH`, give a blank for a blank. The SQL route must use `CONCAT` and
`CONCAT_WS`, never `||`.

**What changes for r2g:** a `CONCAT_SEPARATOR` with a blank argument loses its
doubled separator. **What has to change on the ArangoDB route:** single-value
functions must be guarded so that a blank stays blank.

### 6. `+` is for numbers only

`UPPER(@first) + " " + @last`, and `@n + "1"` where `n` is 1.

| Expression | r2g today | ArangoDB | Rule |
|---|---|---|---|
| `UPPER(@first) + " " + @last` | `"ADA Lovelace"` | **`0`** | **refused when checked** |
| `@n + "1"` | `"11"` | **`2`** | **refused when checked** |

Making `+` join text only works if every route can tell at run time whether it has
text or numbers. SQL routes can't do that cleanly. ArangoDB quietly turns the text
into `0`. **The rule:** `+` adds numbers. A `+` with a text literal or a text
column is refused when the mapping is checked, with a message pointing to `CONCAT`.
A text value that reaches `+` at load time gives a blank.

**What changes for r2g:** no shipped example or test mapping uses `+` to join text.
The tests that use `+` add numbers. A customer mapping that joins text with
`+` will be refused when it is checked, so it is never silently given a new value.

### 7. Text that isn't a number becomes a blank

`TO_NUMBER("12a")` and `TO_NUMBER("")`.

| r2g today | ArangoDB | Postgres | DuckDB, ClickHouse | CEL | Rule |
|---|---|---|---|---|---|
| blank | **`0`** | error | error, or blank with their `TRY` forms | error | **blank** |

A blank says "we don't know". `0` is a real number that the source never contained.
Stopping the whole load for one bad cell is the other reasonable choice. It was not
chosen, because one bad row in a large bulk load would block the rest of it.
**What has to change:** the ArangoDB route must guard `TO_NUMBER`, and SQL routes
must use a safe conversion.

### 8. Numbers are written as text in one fixed way

`CONCAT(@tenant, "_", @id)` where `tenant` is 7 and `id` is 42.0. This is how
composite keys are built, so the same number must always produce the same text.

| Where the 42 comes from | r2g today | ArangoDB | Postgres | DuckDB | Rule |
|---|---|---|---|---|---|
| a floating-point column | `"7_42.0"` | `"7_42"` | `"7_42"` | `"7_42.0"` | **`"7_42"`** |
| a decimal column holding `42.0` | `"7_42.0"` | `"7_42"`* | **`"7_42.0"`** | not measured | **`"7_42"`** |
| a decimal column holding `42.00` | `"7_42.00"` | `"7_42"`* | `"7_42.00"` | not measured | **`"7_42"`** |

\* r2g converts decimals to floating point before it sends a row to ArangoDB, so the
server never sees the trailing zeros.

So the text depends on both the engine and the source column's type. Postgres keeps
a decimal column's trailing zeros. r2g's local route keeps them too, but its server
route drops them.

**The rule:** a whole number is written without a decimal point (`42`). Any other
number is written in the shortest form that reads back as exactly the same number
(`0.30000000000000004`, `1e+20`). This is how ArangoDB and JSON write numbers, and
it is how the value looks once it is stored in a document.

**What changes for r2g:** text built from a whole-number floating-point or decimal
value changes, for example `"7_42.0"` becomes `"7_42"`. A document's own key is not
affected, because r2g builds `_key` from the primary key and expressions can't write
it. A stored field that holds such text will change on the next full reload.

### 9. Text positions start at 0 and count characters

`SUBSTRING("Zürich", 0, 3)`.

| r2g today | ArangoDB | Postgres | ClickHouse `substring` | Rule |
|---|---|---|---|---|
| `"Zür"` | `"Zür"` | `"Zür"` when written from position 1 | **`"Zü"`**, because it counts bytes | **`"Zür"`** |

The expression language looks like ArangoDB's, and existing mappings already count
from 0. Switching to SQL's or spreadsheets' 1-based counting would silently shift
every existing `SUBSTRING`. `LENGTH` counts characters too, so `LENGTH("Zürich")` is
6. **What has to change:** SQL generators add 1 to the start position, and ClickHouse
must use its character-counting functions.

### 10. Division gives the exact fraction

`7 / 2`, and `1 / 0`.

| Expression | r2g today | ArangoDB | Postgres | DuckDB, ClickHouse | CEL | Rule |
|---|---|---|---|---|---|---|
| `7 / 2` | `3.5` | `3.5` | **`3`** | `3.5` | **`3`** | **`3.5`** |
| `1 / 0`, `7 % 0` | blank | blank | error | not measured | not measured | **blank** |

This keeps r2g's current behaviour. **What has to change:** the Postgres route must
convert whole numbers before dividing, and guard against dividing by zero.

### 11. Text and numbers can't be compared with each other

`"10" < 9`, `"1" == 1`, `true == 1`.

| Expression | r2g today | ArangoDB | Postgres | Rule |
|---|---|---|---|---|
| `"10" < 9` | **true**, compared as text | false, compared by type | error | **refused when checked** |
| `"1" == 1` | false | false | error | **refused when checked** |
| `true == 1` | **true** | false | error | **refused when checked** |
| `1 == 1.0` | true | true | true | true |

Whole numbers and fractions are the same kind of value, so `1 == 1.0` stays true.
Comparing text with a number, or true/false with a number, is refused when the
mapping is checked. The curator converts one side with `TO_NUMBER` or `TO_STRING`.
If such a comparison slips through to load time, it is not true, as in rule 2.

### 12. Empty text is not a blank

`COALESCE(@phone, @mobile, "unknown")` where `phone` is `""`.

| r2g today | ArangoDB `NOT_NULL` | Postgres, DuckDB, ClickHouse | Rule |
|---|---|---|---|
| `""` | `""` | `""` | **`""`** |

All engines agree, so this keeps r2g's current behaviour. "The first one that isn't
empty" has to be written out:
`@phone != null && @phone != "" ? @phone : (...)`. A helper function for this can be
added later. It would need to be defined here first.

### 13. `COALESCE` and `??` both stay, and mean the same thing

`COALESCE(@a, @b)` and `@a ?? @b`.

| r2g today | ArangoDB | SQL | Rule |
|---|---|---|---|
| both work | **both rejected.** ArangoDB calls it `NOT_NULL` | `COALESCE` works, `??` doesn't exist | **keep both** |

Both give the first argument that isn't blank. Neither is ArangoDB syntax, so the
ArangoDB route must translate them to `NOT_NULL`. Today an expression that uses
either one fails as soon as it is sent to the server.

### 14. Times are in UTC

The expression language has no date or time functions today, so nothing changes now.
This rule is recorded first so that none ship without it. When they are added:
results are in UTC and written in ISO-8601 with a `Z`, for example
`2026-03-28T23:30:00Z`. They never depend on the database server's or host's time
zone. The research found that Postgres uses the server's time zone and DuckDB uses
the host's.

---

## Where the rules are checked

Every rule except 14 has worked examples in
[`tests/expression_rules_cases.py`](../tests/expression_rules_cases.py). The expected
answers are copied from this page, not from any engine. Three things are checked
against them:

- **The mapping check:** an expression this page says to refuse must fail to compile
  (`tests/test_expression_rules.py`).
- **r2g's own evaluator:** every other example must give this page's answer, in the
  same test file.
- **The ArangoDB route:** the same examples, sent through the delegation query exactly
  as the streaming pipeline sends them
  (`tests/integration/test_expression_rules_server.py`, which runs in CI's
  integration job).

Where a route breaks a rule today, the example records how, in words. Those examples
run as expected failures. When a fix makes one pass, the build fails until its record
is removed, so a fix cannot land unnoticed. The same tests also check that the
"Changes r2g?" column at the top of this page matches the recorded breaks in r2g's
own evaluator.

## What happens next

- **Bring both routes into line.**
  - Context: the tests record each route's breaks. Today the mapping check misses 7
    expressions it should refuse, the evaluator breaks 15 examples, and the ArangoDB
    route breaks 21.
  - Action: change the evaluator where "Changes r2g?" says yes, and guard or
    translate the ArangoDB route for the rest.
  - Result: every recorded break is gone, and a field has the same value whichever
    route calculated it.
- **Changing a rule later.**
  - Context: a rule may turn out to be wrong once it meets real mappings.
  - Action: change its row on this page, its section, and its expected answers in
    the examples file, in one pull request that names the rule.
  - Result: the tests hold every route to the new answer.
