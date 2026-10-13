---
parent: core
prefix: CORE-SILVER
---

# Core silver helpers

## Context

`grecohome_core.silver` is the source-agnostic machinery every silver table is built from: read
bronze safely into DuckDB, type and deduplicate with shared SQL idioms, and write Parquet
atomically without ever touching the layer below. Gold reuses the writer, with silver as its
protected root. Which payload field becomes which column is never decided here; that's each
silver table module's job.

## Connection

`connect()` returns a fresh **in-memory** DuckDB connection. A silver table is a whole-table
rebuild per run, so there's no persistent database and no state shared between runs.

## Reading bronze

- **`list_payload_files(bronze_root, source, collection)`** returns every payload file under
  `{bronze_root}/{source}/{collection}/dt=*/`, sorted, **excluding `*.meta.json` sidecars and
  directories**. Sidecars are dropped in Python, before DuckDB sees any file, so their keys
  (`sha256`, `fetched_at` …) can never contaminate a parsed payload. A collection not yet
  captured yields an empty list, which is a valid state.
  - The glob is one level deep (`dt=*/*`): a file nested deeper inside a `dt=` folder is never
    read.
  - Dotfiles don't match, so the bronze writer's in-flight hidden temporary files are never read.
- **Each reader returns a SQL subquery**, always with a `filename` column, so a table can break
  ties by capture time (the 13-digit fetch time in the bronze filename). Each returns a
  **correctly typed empty relation** (`… WHERE false`) when there are no files, so a table over
  a not-yet-captured collection gets the right columns instead of an error on an empty glob.

| Reader | Yields | For |
|---|---|---|
| `payloads_relation_sql(files)` | `filename`, `j` (the whole file parsed as one `JSON` value, via `read_text` → `json(content)`) | JSON payloads |
| `csv_relation_sql(files, columns)` | `filename` plus the named columns (an alias → exact header map, since headers can hold spaces and brackets), read all-VARCHAR with a header row (`header=true`) and `union_by_name` to tolerate column drift | CSV payloads (Lingo) |
| `text_lines_relation_sql(files)` | `filename`, `line` (one row per non-blank physical line, read with a delimiter, ASCII Unit Separator, that never occurs in the data; DuckDB's default `"` quoting still applies, which is harmless for quote-free data) | Headerless, whitespace- or fixed-width text (USCRN) |

JSON payloads are read as raw text rather than struct-typed (`read_json_auto`): heterogeneous,
drifting bronze breaks struct typing, while JSON-path extraction from a raw value returns NULL
for a missing key.

**A file that isn't valid JSON (corrupt or empty) raises** when a query reads its `j` value, failing
the table's whole rebuild. Bronze writes are atomic, so this means real corruption, and failing
loudly beats silently dropping a capture.

## SQL idioms

- **`json_str(expr, path)`** extracts a dotted path (no leading `$.`) as text, `NULL` when the
  path is absent.
- **`json_num(expr, path)`** extracts a path as `DOUBLE`, `NULL` when absent or non-numeric.
- **`json_date(expr, path)`** casts the path's **first 10 characters** to `DATE`, so an ISO
  datetime and a bare date both give the calendar date. `NULL` when unparseable.
- **`dedup_latest_sql(inner_sql, partition_key=…, order_by=…)`** keeps one row per key, the
  latest by `order_by`, using `row_number() OVER (PARTITION BY key ORDER BY recency DESC NULLS
  LAST) = 1`. **NULL recency sorts last**, so any real value wins a tie-break. Rows that tie on
  an equal, non-null recency are picked arbitrarily. The helper's `_rn` column is excluded from
  the result.

## Writing Parquet

`write_parquet_atomic(con, select_sql, dest_path, *, protected_root)`:

1. **Refuses** a destination that equals or lies inside `protected_root`, raising `ValueError`.
   That's bronze for silver, and silver for gold. Both paths are compared **after resolving
   symlinks** (`os.path.realpath`), so a destination that reaches the protected root through a
   symlink is refused too. Nothing checks the roots at startup, so a misconfigured root fails at
   the first write.
2. Creates the destination directory, and `COPY`s `select_sql` to a hidden temporary file there
   as Parquet with zstd compression.
3. Renames the temporary file over the destination, so readers never see a half-written table.
   The temporary file is removed if an exception is raised. A killed process can leave an orphan
   hidden temporary file beside the table, but never a half-written table.
4. Returns the row count, **read back from the written Parquet**, so a large transform runs once,
   not a second time just to count.

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Payload reading | Raw JSON text plus JSON-path extraction | `read_json_auto` struct typing | Bronze is heterogeneous and drifts (older Garmin nights lack fields); struct typing crashes on that, while JSON paths give NULL. |
| Sidecar exclusion | In Python, before DuckDB | A SQL `WHERE filename NOT LIKE '%.meta.json'` | DuckDB infers a schema across every globbed file before any row filter, so sidecar keys would leak into the inferred columns. |
| Empty collections | A typed empty relation | Raise; skip the table | A table over a not-yet-captured collection should rebuild to an empty, correctly typed result. |
| Overwrite | Temp file plus rename in the destination directory | Write in place | A crash never leaves a half-written table, and a rename within one directory stays on one filesystem. |
| Write guard | `protected_root`, checked on every write | Trust configuration | The layer below must never be written; a misconfigured root should fail loudly. |
| Guard comparison | Resolved real paths | Lexical absolute paths | A symlinked root, or a symlink anywhere in the destination's path, would otherwise slip a write past the guard into the layer below. |
| Row count | Read back from the written Parquet | Re-run the SELECT | Avoids running a multi-join transform twice. |

## References

- `packages/core/src/grecohome_core/silver/` (`duck.py`, `transform.py`)
