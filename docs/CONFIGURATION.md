# Database configuration

> **Disclaimer:** go-pduckdb is just glue, not a secure wrapper. Design your own
> security model when you use this package.

A database can be opened with DuckDB configuration options applied before it
opens:

```go
// A Go map, written in your source.
db, err := pduckdb.NewDuckDBWithSettings("warehouse.duckdb",
    map[string]string{"access_mode": "READ_ONLY"})

// A DSN string, through database/sql.
db, err := sql.Open("duckdb", "warehouse.duckdb?access_mode=READ_ONLY&threads=2")
```

Both reach `duckdb_open_ext`, and both accept every option DuckDB accepts at
open time — 299 of them in DuckDB 1.5.5. List them with `duckdb_config_count` /
`duckdb_get_config_flag`, or approximately with
`SELECT name, description FROM duckdb_settings()`.

The rest of this page is reference: what those options can reach, so that the
security model you design is informed by it.

## DSN parsing

- **The last `?` separates**, not the first, so a path containing `?` stays
  openable.
- **A trailing `?` says "no options"**: `odd?name.duckdb?` opens the file
  `odd?name.duckdb`. Double it — `odd?name.duckdb??` — to name a file that
  really does end in `?`.
- **A DSN with no `?` takes the plain `duckdb_open` path**, unchanged.
- **A malformed query is an error**, not a filename. `w.duckdb?access_mode=%ZZ`
  is refused rather than creating a file named after the typo.
- **An unrecognised option fails the open** with DuckDB's message
  (`The following options were not recognized: ...`) rather than being silently
  dropped. So does a bad value: `access_mode=NONSENSE` refuses to open.
- **A repeated option is an error.**
  `?access_mode=READ_ONLY&access_mode=READ_WRITE` is refused rather than
  resolved, because a DSN built by concatenation can ask for two different
  things and quietly honouring one is how a read-only database becomes
  writable.

Note that a DSN is a string, and strings get built. A Go map is written by a
developer in source; a DSN may be assembled from a flag, a config file, or a
request.

## What the options can reach

### Privileged execution

A DuckDB extension is native code loaded into your process, with your process's
privileges. Anything widening which extensions may load is a path to arbitrary
code execution.

| Option | Default | Effect |
| --- | --- | --- |
| `allow_unsigned_extensions` | `false` | Loads extensions with invalid or missing signatures. |
| `allow_community_extensions` | `true` | Community-built extensions, not maintained by the DuckDB core team. |
| `allow_extensions_metadata_mismatch` | `false` | Loads extensions whose metadata does not match this build. |
| `allow_parser_override_extension` | `DEFAULT` | Lets an extension replace the SQL parser. |
| `autoload_known_extensions` | `true` | A query referencing an unloaded extension loads it. |
| `autoinstall_known_extensions` | `true` | ...and installs it first, over the network. |
| `custom_extension_repository` | *empty* | Where extensions are installed from. |
| `autoinstall_extension_repository` | *empty* | The same, for autoloading. |
| `extension_directory`, `extension_directories` | *empty* | Where extensions are loaded from on disk. |

Note the direction of each default: `allow_unsigned_extensions` defaults to
`false`, so setting it is a strict gain in capability. With
`custom_extension_repository`, the pair turns "open a database" into "fetch and
run code from a chosen host".

### Permission escalation

| Option | Effect |
| --- | --- |
| `access_mode` | `READ_ONLY` makes the engine refuse writes, rather than the application intending not to make any. |
| `allowed_paths`, `allowed_directories` | Files and directories readable *even when* `enable_external_access` is false. |
| `allowed_configs` | Options changeable *even when* `lock_configuration` is true. |
| `lock_configuration` | Freezes configuration for the life of the instance. |

### Filesystem and network reach

DuckDB reads files and URLs from SQL — `read_csv`, `read_parquet`, `glob`,
`COPY ... FROM`. Configuration decides how far that reaches.

| Option | Default | Effect |
| --- | --- | --- |
| `enable_external_access` | `true` | Master switch for anything outside the database file. Can only ever be turned off: once running, DuckDB refuses to re-enable it. |
| `file_search_path` | *empty* | Directories searched for input files; relative names resolve somewhere new. |
| `home_directory` | *empty* | Changes what `~` means. |
| `disabled_filesystems` | *empty* | Can disable `LocalFileSystem`. |
| `secret_directory`, `allow_persistent_secrets`, `default_secret_storage` | — | Where secrets are stored and loaded from. |
| `allow_unredacted_secrets` | `false` | Whether secrets can be printed. With `duckdb_secrets()`, reads back credentials the application loaded for itself. |

### Resource exhaustion

These execute nothing; they decide how much of the machine one database takes.

| Option | Effect |
| --- | --- |
| `memory_limit`, `max_memory` | Raised, one connection can starve the host; lowered, ordinary queries fail. |
| `threads`, `worker_threads`, `external_threads` | CPU, and so every other tenant on a shared box. |
| `temp_directory`, `max_temp_directory_size` | Spill files. The size defaults to 90% of available disk. |
| `block_allocator_memory` | Never freed and cannot be reduced once set. |
| `checkpoint_threshold`, `wal_autocheckpoint` | Raised far enough, the WAL grows without bound. |

### Durability and correctness

About 20 `debug_*`, `force_*` and `experimental_*` options are settable at open
time and reachable from a DSN like any other. They exist for DuckDB's own test
suite: `debug_skip_checkpoint_on_commit` commits without checkpointing;
`disable_database_invalidation` keeps using a database after a fatal error,
which DuckDB documents as unable to guarantee correct behaviour afterwards;
`force_column_metadata_reuse` is documented as breaking storage
backward-compatibility with older DuckDB versions. `storage_compatibility_version`
is not a debug option but decides which DuckDB versions can read what you write.

## DuckDB's own levers

Two settings apply after opening, so they hold even when the open-time
configuration was not yours to choose:

- `SET lock_configuration = true` — no further configuration changes.
- `SET enable_external_access = false` — no reads or writes outside the database
  file. One-way: DuckDB answers `Cannot enable external access while database is
  running` to any attempt to turn it back on.
