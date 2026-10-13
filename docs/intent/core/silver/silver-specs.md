# Core silver helpers — EARS specs

Design: [silver-design.md](silver-design.md). Prefix `CORE-SILVER`. All specs are about
`grecohome_core.silver`.

## Connection and reading bronze

- [x] **CORE-SILVER-001**: When `connect` is called, the system shall return a new in-memory DuckDB connection.
- [x] **CORE-SILVER-002**: When `list_payload_files(bronze_root, source, collection)` is called, the system shall return, sorted, every regular file directly inside a `{bronze_root}/{source}/{collection}/dt=*/` folder, excluding `*.meta.json` sidecars, directories and dotfiles, and an empty list when there are none.
- [x] **CORE-SILVER-003**: When `payloads_relation_sql` is given payload files, the system shall return a SQL relation with one row per file holding `filename` and `j` (the whole file parsed as one JSON value); when given no files, it shall return a relation with the same columns and types and no rows.
- [x] **CORE-SILVER-004**: When `csv_relation_sql` is given CSV files and an alias-to-header column map, the system shall return a SQL relation holding `filename` and each mapped column as VARCHAR, reading a header row and matching columns by name across files; when given no files, it shall return a relation with the same columns and types and no rows.
- [x] **CORE-SILVER-005**: When `text_lines_relation_sql` is given text files, the system shall return a SQL relation with one row per non-blank physical line holding `filename` and `line`; when given no files, it shall return a relation with the same columns and types and no rows.
- [x] **CORE-SILVER-006**: If a file given to `payloads_relation_sql` isn't valid JSON, then any query that reads the relation's `j` column shall raise, rather than skipping the file.

## SQL idioms

- [x] **CORE-SILVER-007**: The `json_str`, `json_num` and `json_date` helpers shall extract a dotted JSON path as text, as `DOUBLE`, and as the `DATE` of the value's first 10 characters respectively, each yielding `NULL` when the path is absent or the value can't be converted.
- [x] **CORE-SILVER-008**: When `dedup_latest_sql` wraps a relation, the system shall keep exactly one row per `partition_key`, the one with the greatest `order_by` value with `NULL` values ranked last, and shall not include its row-number column in the result.

## Writing Parquet

- [x] **CORE-SILVER-009**: When `write_parquet_atomic` writes a table, the system shall create the destination directory, write the query result as zstd-compressed Parquet to a hidden temporary file in that directory, rename it over the destination, and return the row count read back from the written file.
- [x] **CORE-SILVER-010**: If writing or renaming the temporary file raises, then `write_parquet_atomic` shall remove the temporary file, leave any existing destination file unchanged, and re-raise.
- [x] **CORE-SILVER-011**: If the destination of `write_parquet_atomic` equals or lies inside `protected_root` once both paths are resolved through symlinks, then the system shall raise `ValueError` and write nothing.
