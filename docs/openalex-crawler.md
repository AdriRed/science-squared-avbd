# OpenAlex Unified Crawler

Unified, resumable, quota-aware crawler that produces the complete
`../data/external/` dataset from a single call. It replaces the previous
per-table scripts and fetches **works**, **sources**, **open-access**,
**host organizations**, and **authorships** in a single API pass, plus a
standalone **fields** taxonomy refresh.

> Implementation lives in `oa_fetch.py`. The notebook only orchestrates:
> one `run()` for the crawl, one `refresh_fields()` for the taxonomy.

---

## Table of contents

1. [Quick start](#1-quick-start)
2. [Requirements](#2-requirements)
3. [Architecture overview](#3-architecture-overview)
4. [Tables produced](#4-tables-produced)
5. [How a run works](#5-how-a-run-works)
6. [Resume semantics](#6-resume-semantics)
7. [Commit semantics (`works` as commit marker)](#7-commit-semantics-works-as-commit-marker)
8. [Quota & rate limiting](#8-quota--rate-limiting)
9. [Failure handling](#9-failure-handling)
10. [API reference](#10-api-reference)
11. [Configuration reference](#11-configuration-reference)
12. [Design decisions & gotchas](#12-design-decisions--gotchas)
13. [Troubleshooting](#13-troubleshooting)
14. [Adding new per-work tables](#14-adding-new-per-work-tables)

---

## 1. Quick start

```python
import oa_fetch

# Crawl everything that isn't already in works.parquet.
summary = oa_fetch.run()

# Refresh the static taxonomy (decoupled from the crawl; safe to run any time).
oa_fetch.refresh_fields()
```

Re-running `run()` is safe: it resumes exactly where it stopped, using
`works.parquet` as the set of DOIs already done.

---

## 2. Requirements

- Python 3.9+ (`ThreadPoolExecutor.shutdown(cancel_futures=...)`).
- Environment variables set **before import**:
  - `OPENALEX_EMAIL`
  - `OPENALEX_API_KEY`
- Packages: `pandas`, `pyarrow`, `requests`, `pyalex`, `tqdm`, `python-dotenv`.
- Input file: `../data/cache/dois.parquet` with a `doi` column.
- Output directory: `../data/external/` (must exist).

`oa_fetch.py` loads `.env` at import time via `dotenv.load_dotenv(find_dotenv())`.

---

## 3. Architecture overview

### One request per batch

The previous codebase made **two** API passes:

1. Fetch `works` (filtered by DOI).
2. For each backfill table, fetch by `openalex_id` with a different `select()`.

The unified crawler makes **one** request per DOI batch, with a single
`select()` that unions every field any table needs:

```python
FIELDS = [
    "ids",
    "apc_list", "apc_paid", "awards", "funders",
    "primary_topic", "topics", "referenced_works",
    "primary_location",         # sources + host_orgs
    "open_access",              # open-access table
    "authorships",              # authorship table
]
```

From each work, `_extract_rows()` returns `{table: [rows]}` for all five
per-work tables. Net result: **~5× fewer API credits** for the same output.

### Batch & checkpoint constants

| Constant | Default | Meaning |
|---|---|---|
| `BATCH_SIZE` | 50 | DOIs per API call |
| `PER_PAGE` | 50 | `per_page` sent to OpenAlex (must be ≥ `BATCH_SIZE`) |
| `MAX_WORKERS` | 5 | Concurrent batches |
| `MAX_RETRIES` | 5 | Retries per batch on 429 / 5xx |
| `CHECKPOINT_EVERY` | 50 batches | = 2500 DOIs between disk commits |
| `LONG_WAIT_SECONDS` | 120 | `Retry-After` above this halts the crawl |

### Layered structure

```
oa_fetch.run()
    ├── check_openalex_quota()          # optional pre-flight
    ├── _load_existing() per table      # read all 5 parquets
    ├── compute todo DOIs               # dois.parquet − works.parquet
    ├── _crawl()
    │     ├── ThreadPoolExecutor (_fetch_batch per batch)
    │     ├── _extract_rows() per work  # fan-out to 5 tables
    │     └── _commit() every N batches # writes tables in COMMIT_ORDER
    └── _commit() in finally            # final flush
```

---

## 4. Tables produced

All files live in `../data/external/`. Join key is the OpenAlex work ID
(`openalex_id` or `work_openalex_id`, depending on the table).

### `works.parquet` — *commit marker*

| Column | Type | Source |
|---|---|---|
| `openalex_id` | str | `ids.openalex` → short ID |
| `doi` | str | `ids.doi` → normalized (lowercased, prefix stripped) |
| `apc_paid_value_usd` | float | `apc_paid.value_usd` |
| `apc_list_value_usd` | float | `apc_list.value_usd` |
| `awards` | list[str] | `awards[].funder_award_id` |
| `funders` | list[str] | `funders[].id` → short ID |
| `primary_topic_field_id` | str | `primary_topic.field.id` → short ID |
| `topics_field_ids` | list[str] | `topics[].field.id` → short IDs |
| `referenced_works` | list[str] | `referenced_works[]` → short IDs |

Empty lists for missing `awards`/`funders`/`topics`/`references`. Dedupe key:
`openalex_id`.

### `work_sources.parquet`

One row per work, derived from `primary_location.source`.

| Column | Source |
|---|---|
| `work_openalex_id` | `ids.openalex` |
| `source_id` | `primary_location.source.id` → short ID |
| `source_display_name` | `primary_location.source.display_name` |
| `source_type` | `primary_location.source.type` |

Dedupe: `work_openalex_id`. A work with no source still produces one row, with
`NaN` in the last three columns.

### `work_open_access.parquet`

| Column | Source |
|---|---|
| `work_openalex_id` | `ids.openalex` |
| `is_open_access` | `open_access.is_oa` |
| `open_access_status` | `open_access.oa_status` |

Dedupe: `work_openalex_id`. Always one row per work (nulls if `open_access`
missing).

### `host_orgs.parquet`

| Column | Source |
|---|---|
| `openalex_id` | `ids.openalex` (this is the work, not the org) |
| `host_organization_id` | `primary_location.source.host_organization` → short ID |
| `host_organization_name` | `primary_location.source.host_organization_name` |

Dedupe: `openalex_id`. Note: **`openalex_id` is the work ID here** — the
naming mirrors the historical schema, not the semantics.

### `work_authorships.parquet`

One row per `(work, author, institution)`. **Authors with no institution
are dropped** (no row).

| Column | Source |
|---|---|
| `work_openalex_id` | `ids.openalex` |
| `author_id` | `authorships[].author.id` → short ID |
| `institution_id` | `authorships[].institutions[].id` → short ID |
| `institution_country` | `authorships[].institutions[].country_code` |
| `institution_code` | `authorships[].institutions[].ror` |

Dedupe: `(work_openalex_id, author_id, institution_id)`.

> **Note on `institution_code`:** currently the full ROR URL
> (`https://ror.org/xxxx`) is stored. If you want the short ROR ID instead,
> change one line in `_extract_rows` (`inst.get("ror")` → last path segment).

### `fields.parquet` — *static taxonomy*

Not produced by the crawl. Refresh explicitly with `oa_fetch.refresh_fields()`.

| Column | Source |
|---|---|
| `field_id` | `/fields[].id` → short ID |
| `field_name` | `/fields[].display_name` |
| `domain_id` | `/fields[].domain.id` → short ID |
| `domain_name` | `/fields[].domain.display_name` |

---

## 5. How a run works

Pseudo-code of `run()`:

```
1. (optional) quota probe → abort if <= 0
2. Load the 5 existing parquets from data_dir
3. already = set(normalized DOI in works.parquet)
4. todo = {normalized doi in dois.parquet} − already
5. If todo is empty: skip API, still commit any pending state
6. Else: _crawl(todo)
7. Finally: _commit() to flush works fetched after the last checkpoint
8. Print summary (works_total, works_fetched, failed, halted)
```

Inside `_crawl`:

- DOIs are sliced into batches of `BATCH_SIZE`.
- Each batch is submitted as a future to a `ThreadPoolExecutor`.
- Results are consumed **in arrival order** via `as_completed`.
- Per work: `_extract_rows` fans out to all five tables.
- Every `CHECKPOINT_EVERY` successful batches: `_commit()` writes to disk.
- `tqdm` bar shows `works=N failed=F cp=M` where `cp=M` is the number of
  works **committed to disk** so far (`len(works.parquet)`), and `💾` in the
  postfix marks the batch that triggered a checkpoint.

---

## 6. Resume semantics

**The set of "already done" DOIs is exactly `works.parquet`.**

- A DOI is considered *done* iff its normalized form appears in
  `works.parquet["doi"]`.
- Being present in any other per-work table (sources, OA, etc.) is **not**
  sufficient for resume.

This is deliberate and tied to the commit-marker design (§7).

**Implication for orphans:** if `works.parquet` already has a work but
`work_sources.parquet` doesn't (e.g. because of a corrupted file, or because
the previous run was a partial backfill), `run()` will **not** automatically
fill the gap. The work is considered done; backfilling it would require a
manual intervention. (See §14 for how to add one.)

Re-running `run()` after an interruption resumes exactly where it left off —
no duplicates, no missed works, no wasted credits.

---

## 7. Commit semantics (`works` as commit marker)

The five per-work tables are written **in this order**, never out of order:

```
work_sources → work_open_access → host_orgs → work_authorships → works
```

`works.parquet` is the **commit marker**: if `works.parquet` contains a DOI,
all four other tables are guaranteed to contain that work as well (modulo
rows being dropped on purpose — authors without institutions — or fields
missing from the API response).

### What a commit does

For each table that has **new rows** since the last commit:

1. Concatenate `existing` + `new`.
2. Deduplicate on the table's key, `keep="last"` (newer wins).
3. Reindex to the canonical column list (fills missing cols with `NaN`).
4. Overwrite the parquet.
5. Move `new` to `existing` in memory and clear the `new` buffer.

Tables with an **empty new buffer** are **not rewritten** — cheap and
idempotent.

### Failure modes during commit

- If `_write_one` fails on a per-work table, the exception is caught by the
  caller in `_crawl`'s checkpoint loop (`⚠ commit failed at batch N`); the
  `works.parquet` file is *not* touched → those DOIs remain **not done** and
  will be re-fetched next run.
- If `_write_one` on `works.parquet` fails, same story: the commit is
  incomplete, `works.parquet` still doesn't list those DOIs, so they're
  retried.
- Because per-work tables are written with `keep="last"`, re-committing the
  same works next run is harmless (no duplicates).

### Why this design

The alternative — a single transactional write — would require a temp dir
and an atomic rename across five files, which is fragile if the process dies
between renames. Using `works` as the marker makes each commit two-phase
and, more importantly, **idempotent under any interleaving**.

---

## 8. Quota & rate limiting

### Pre-flight probe

If `CHECK_QUOTA_FIRST = True`, `run()` calls `check_openalex_quota()` before
the first batch. If the API reports 0 credits, `run()` aborts immediately
with `stopped=True, reason="quota pre-exhausted"` and returns without
touching disk.

If the probe fails (network, changed schema), it's silently ignored and the
crawl proceeds — the per-batch 429 handling below acts as a fallback.

### 429 handling per batch

Retries are handled **by us**, not by `pyalex`
(`pyalex.config.max_retries = 0`), so backoff can be interrupted by a `stop`
event.

On a 429:

- Parse `X-RateLimit-Remaining` and `Retry-After` headers.
- **Halt the crawl** (`CrawlHalted`) if:
  - `X-RateLimit-Remaining ≤ 0`, **or**
  - `Retry-After > LONG_WAIT_SECONDS` (default 120s).
- Otherwise: exponential backoff up to 30s, at most `MAX_RETRIES` times,
  **interruptible** (`stop.wait(...)`).
- After `MAX_RETRIES` failed 429s, halt with reason
  `"persistent 429 after N retries"`.

A halt is **global** — it stops every in-flight batch (the `stop` event is
shared) and flushes the finally block. Nothing partial is lost; the resume
logic handles the rest.

### Other HTTP errors

- **4xx** (other than 429) on a batch: print a warning and skip the batch.
  These DOIs are **not** added to `works.parquet`, so they'll be retried next
  run — and probably fail again with the same 4xx. If you have a batch with
  an invalid DOI, either fix the DOI in `dois.parquet` or remove it.
- **5xx / network** errors: retry with backoff, then give up on the batch.
  Recorded in `failed` (visible in the final summary).

### Keyboard interrupt

`Ctrl-C` sets the same `stop` event, breaks the loop, and the finally block
flushes pending work. The summary reports `stopped=True,
reason="user interrupt"`.

---

## 9. Failure handling

Three classes of failures, with distinct consequences:

| Class | Where it's caught | Effect on `works.parquet` | Retried next run |
|---|---|---|---|
| Quota halt (429) | `_fetch_batch` raises `CrawlHalted` | Not written for the batch | ✓ |
| Other 4xx | `_fetch_batch` returns `[]` | Not written for the batch | ✓ (probably fails again) |
| 5xx / network after retries | `_fetch_batch` re-raises | Not written for the batch | ✓ |
| Commit write error | `_crawl` checkpoint loop | Partial; `works` not written if it failed | ✓ |

Batch-level failures never abort the run; only quota halt and user
interrupt do. Failed batches are reported in the final summary.

---

## 10. API reference

### `oa_fetch.run(data_dir=None, dois_path=None, dois_col="doi", checkpoint_every=CHECKPOINT_EVERY)`

Run the full unified crawl. Returns a summary dict:

```python
{
    "works_total":    int,          # len(works.parquet) after commit
    "works_fetched": int,           # works returned by API this run
    "failed_batches": int,          # batches that gave up after retries
    "stopped":        bool,         # halted early (quota / interrupt)
    "reason":         str | None,   # human-readable halt reason
}
```

Edge case: if the quota probe finds 0 credits, returns
`works_total=None` and `reason="quota pre-exhausted"`.

All arguments have sane defaults pointing at `../data/external/` and
`../data/cache/dois.parquet`.

### `oa_fetch.refresh_fields(data_dir=None)`

Download `/fields` and overwrite `fields.parquet`. Returns the DataFrame.
Independent of the crawl — safe to call any time, does not require quota for
the main crawl (it uses one credit itself). Raises on HTTP error (no retry
loop).

### `oa_fetch.check_openalex_quota()`

Probe `https://api.openalex.org/rate-limit`. Returns the remaining credits
as `int`/`float`, or `None` if the value can't be extracted (network error,
schema change, etc.). Never raises.

### `oa_fetch.CrawlHalted`

Exception class raised when the crawl is halted. Only raised inside
`_crawl`; `run()` catches it and converts it into a summary.

---

## 11. Configuration reference

All in `oa_fetch.py`, top of file. Change and reload the module.

| Name | Default | Purpose |
|---|---|---|
| `FIELDS` | see §3 | Fields requested from OpenAlex |
| `BATCH_SIZE` | 50 | DOIs per API call |
| `PER_PAGE` | 50 | `per_page` for the API (≥ `BATCH_SIZE`) |
| `MAX_WORKERS` | 5 | Concurrency |
| `MAX_RETRIES` | 5 | Retries per batch on 429/5xx |
| `CHECKPOINT_EVERY` | 50 | Batches between disk commits |
| `LONG_WAIT_SECONDS` | 120 | `Retry-After` above which the crawl halts |
| `CHECK_QUOTA_FIRST` | True | Pre-flight quota probe |
| `DEFAULT_DATA_DIR` | `../data/external` | Output dir |
| `DEFAULT_DOIS_PATH` | `../data/cache/dois.parquet` | Input DOIs |
| `TABLE_SPECS` | see §4 | Per-table filename + columns + dedupe key |
| `COMMIT_ORDER` | see §7 | Write order (`works` last) |

---

## 12. Design decisions & gotchas

### `host_orgs.openalex_id` is a *work* ID

The column name is a legacy artifact of the previous schema. It holds the
**work**'s OpenAlex ID, not the host organization's. The
`host_organization_id` column holds the organization ID. Don't confuse them.

### `institution_code` is the full ROR URL

`authorships[].institutions[].ror` returns something like
`https://ror.org/01234567`. If you need the short ROR ID, transform before
writing or change `_extract_rows`.

### Authors without institution are dropped

As per design: rows in `work_authorships.parquet` require at least one
institution. An authorship with `institutions: []` contributes no row. This
means a work with `N` authors where `M` have institutions produces
`sum(len(a.institutions) for a in authorships if a has institutions)` rows —
typically `M` or more.

### `works.parquet` requires `openalex_id` to be non-null

`_extract_rows` returns `{}` for a work with no `openalex_id`. That work
appears in **no** table. Very rare, but possible with malformed API
responses.

### `author_id` is also required

Authors with no `author.id` are dropped before institutions are considered.

### Normalization of DOIs

Both sides (`dois.parquet["doi"]` and `works.parquet["doi"]`) are normalized
identically: lowercased, prefix stripped (`https://doi.org/`,
`http://dx.doi.org/`, `doi:`, etc.), whitespace stripped. A DOI that
normalizes to `None` is dropped.

### Why `keep="last"` on commit?

Because the "new" slice is *appended* to the existing rows. If a work was
re-fetched (which shouldn't happen — see §6 — but might in edge cases like
manual edits to `works.parquet`), the fresh row wins.

### Checkpoint writes to all tables, not just `works`

The whole commit (`_commit`) writes every table with a non-empty `new`
buffer. That's why `works` must be **last**: if the process is killed after
committing sources/OA but before `works`, the next run will re-fetch those
DOIs (since they're not yet in `works.parquet`) and re-write the other
tables harmlessly. If `works` were written first, we'd have "done" DOIs
without their per-work tables.

### The `tqdm` postfix `cp=` is not cumulative works fetched

It's `works_committed = len(works.parquet)` at the moment of the last
postfix update. Between commits, `cp` stays constant while `works` grows
(uncommitted works in the `new` buffer).

---

## 13. Troubleshooting

### `run()` prints `0 DOIs to fetch`

All DOIs in `dois.parquet` are already in `works.parquet`. Nothing to do —
add more DOIs to `dois.parquet` or delete stale rows from `works.parquet`.

### `run()` reports `failed_batches > 0`

Some batches gave up after retries (5xx or network). They will be retried
next run. If the problem is systematic (e.g. one DOI consistently causes
a 500), the same batch will keep failing — investigate the DOIs in that
batch by looking at the failed batch summary in stdout.

### `run()` keeps reporting the same `failed_batches` count across runs

Suspect:
- The API keeps returning 4xx for a specific DOI batch (silently skipped,
  retried next run — infinite loop of "0 new works"). Check `dois.parquet`
  for malformed DOIs.
- A persistent 5xx for a specific batch. Contact OpenAlex or remove the
  offending DOIs.

### `works.parquet` is missing a work that appears in other tables

This should not happen under normal operation — the commit-marker design
prevents it. If it does, either:
- Someone edited `works.parquet` manually.
- A commit was killed after `work_sources` etc. but before `works` (which
  is the designed fallback — retry next run).
- A parquet read error left `existing` empty at startup.

To fix: delete the offending work's rows from the other tables and re-run.
`run()` will re-fetch the DOI and re-commit everything.

### `⚠ commit failed at batch N: <error>`

A parquet write failed (disk full, permissions, file locked). The in-memory
state is still there — the final `_commit` in the `finally` block may retry
and succeed, or the run will end with those works not committed (so they'll
be re-fetched next run). Check the error message.

### `STOPPED EARLY — OpenAlex quota/rate limit`

Daily budget exhausted. Nothing partial is lost. Re-run tomorrow — the
resume logic will pick up exactly where it left off.

### `Institution codes are URLs, not short IDs`

Working as designed (see §12). Fix in `_extract_rows` if desired.

---

## 14. Adding new per-work tables

The pipeline is designed to be extended. To add a new table (say
`work_concepts.parquet` derived from `concepts`):

1. **Add the field** to `FIELDS`.
2. **Add the schema** to `TABLE_SPECS`:
   ```python
   "concepts": ("work_concepts.parquet",
                ["work_openalex_id", "concept_id", "score"],
                ["work_openalex_id", "concept_id"]),
   ```
3. **Add the extractor** in `_extract_rows`:
   ```python
   concepts_rows = [
       {"work_openalex_id": wid,
        "concept_id": _short_id(c.get("id")),
        "score": c.get("score")}
       for c in (work.get("concepts") or [])
   ]
   return {"...": ..., "concepts": concepts_rows}
   ```
4. **Add to `COMMIT_ORDER`** — but **before `works`**:
   ```python
   COMMIT_ORDER = ["sources", "oa", "hosts", "authors",
                   "concepts", "works"]
   ```
5. **Load it** in `run()` — the `TABLE_SPECS` loop already does this
   automatically; no change needed.
6. **Update `TODO` computation** only if the new table should affect the
   resume set. By design, resume is based on `works.parquet` only, so no
   change.

### Adding a backfill for orphan works

If you want to fill a table for works already present in `works.parquet`
but missing from (say) `work_sources.parquet`, you'd need a separate
function — the main `run()` doesn't do it. Sketch:

```python
def backfill_missing(table_name, data_dir=None):
    """For each work in works.parquet missing from <table_name>, fetch
    its DOI and write the missing rows. Does not touch works.parquet."""
    ...
```

This requires a second API pass (by DOI) and is intentionally out of scope
of the main unified crawler.

---

## Changelog of design decisions

- **Single API pass** replaces the previous two-phase crawl (works, then
  per-table backfill). ~5× fewer requests for the same output.
- **`works.parquet` as commit marker** replaces per-table "already done"
  sets. Resume is now a single check.
- **`fields.parquet` is decoupled** from the crawl. It's a static taxonomy
  and does not depend on any work, so it's refreshed with its own function.
- **Backoff is interruptible** (`stop.wait(...)`) so keyboard interrupts
  work even during rate-limit waits.
- **429 halts the crawl globally** if the budget looks spent or the wait
  is unreasonably long. Rationale: cheaper than blocking a notebook for
  minutes on retries that are likely to fail.
