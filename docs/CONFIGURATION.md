# Database configuration

A database can be opened with DuckDB configuration options applied before it
opens. There are two ways in, and they are not equally trustworthy:

```go
// A Go map, written in your source.
db, err := pduckdb.NewDuckDBWithSettings("warehouse.duckdb",
    map[string]string{"access_mode": "READ_ONLY"})

// A DSN string, through database/sql.
db, err := sql.Open("duckdb", "warehouse.duckdb?access_mode=READ_ONLY&threads=2")
```

Both reach `duckdb_open_ext`, and both accept **every** option DuckDB accepts at
open time — 299 of them in DuckDB 1.5.5, which you can list with
`duckdb_config_count` / `duckdb_get_config_flag`, or approximately with
`SELECT name, description FROM duckdb_settings()`.

This driver applies no allowlist and no denylist. It passes what you give it to
DuckDB and reports what DuckDB says. That is deliberate: a driver cannot know
whether your process is a trusted ETL job or a multi-tenant query endpoint, and
a policy that fits one is wrong for the other. Deciding which options a caller
may set is your call. This page is what you need to make it.

## The trust boundary is the DSN

The Go map is written by a developer in source. A DSN is a string, and strings
get built:

```go
dsn := userSuppliedPath                       // from a request, a config file, a flag
db, _ := sql.Open("duckdb", dsn)
```

Anything that can influence that string can set any DuckDB open-time option. If
your DSN is a compile-time constant, most of this page is background reading.
If any part of it comes from outside your process, read on — and see
[Restricting options yourself](#restricting-options-yourself).

### DSN parsing rules

- **The last `?` separates**, not the first, so a path containing `?` stays
  openable. A path whose own name ends in `?` is written `./odd?name.duckdb?`.
- **A DSN with no `?` takes the plain `duckdb_open` path**, unchanged.
- **An unrecognised option fails the open** with DuckDB's message
  (`The following options were not recognized: ...`), rather than being
  silently dropped. So does a bad value: `access_mode=NONSENSE` refuses to open.
- **A repeated key currently takes the last value, silently.**
  `?access_mode=READ_ONLY&access_mode=READ_WRITE` opens read-write. If you
  concatenate DSNs, an appended duplicate wins over what you set earlier.

## What to consider

The concerns below are ordered by how bad the worst case is, not by how likely
you are to hit them.

### Privileged execution

A DuckDB extension is native code, loaded into your process, running with your
process's privileges. Anything that widens which extensions may load is a path
to arbitrary code execution:

| Option | Default | Why it matters |
| --- | --- | --- |
| `allow_unsigned_extensions` | `false` | Loads extensions with invalid or missing signatures. The single most dangerous option here. |
| `allow_community_extensions` | `true` | Community-built extensions, not maintained by the DuckDB core team. |
| `allow_extensions_metadata_mismatch` | `false` | Loads extensions whose metadata does not match this DuckDB build. |
| `allow_parser_override_extension` | `DEFAULT` | Lets an extension replace the SQL parser. |
| `autoload_known_extensions` | `true` | A query that references an unloaded extension loads it. |
| `autoinstall_known_extensions` | `true` | ...and installs it first, over the network. |
| `custom_extension_repository` | *empty* | Where extensions are installed from. Points installation at a chosen host. |
| `autoinstall_extension_repository` | *empty* | The same, for autoloading. |
| `extension_directory` / `extension_directories` | *empty* | Where extensions are loaded from on disk. |

Note the direction of each default. `allow_unsigned_extensions` defaults to
`false`, so being able to set it is a strict gain in capability. Combining
`custom_extension_repository` with `allow_unsigned_extensions` turns "open a
database" into "fetch and execute code from a chosen server".

If you never use extensions, `SET lock_configuration = true` after opening — or
`?lock_configuration=true` — freezes configuration for the life of the instance.

### Permission escalation

| Option | Why it matters |
| --- | --- |
| `access_mode` | `READ_ONLY` is the reason this feature exists. It is also the thing an appended duplicate key can undo — see the parsing rules above. |
| `allowed_paths`, `allowed_directories` | Files and directories that stay readable *even when* `enable_external_access` is false. Exceptions to your own restriction. |
| `allowed_configs` | Options that stay changeable *even when* `lock_configuration` is true. An escape hatch from the lock. |
| `lock_configuration` | Useful to you, and a denial-of-service to a later caller who needed to set something. |

A read-only database is enforced by the engine rather than intended by the
application, which is worth a great deal. It is worth correspondingly more to be
sure nothing downstream can flip it back.

### Filesystem and network reach

DuckDB can read files and URLs from SQL — `read_csv`, `read_parquet`, `glob`,
`COPY ... FROM`. Configuration decides how far that reaches:

| Option | Default | Why it matters |
| --- | --- | --- |
| `enable_external_access` | `true` | The master switch for reading and writing anything outside the database file. It can only ever be turned off: once the database is running DuckDB refuses to re-enable it. The risk is therefore a caller who *opens* with it on, not one who flips it later. |
| `file_search_path` | *empty* | Directories searched for input files — relative names resolve somewhere new. |
| `home_directory` | *empty* | Changes what `~` means. |
| `disabled_filesystems` | *empty* | Can disable `LocalFileSystem`; leaving it unset keeps local reads available. |

Credentials deserve their own row. `secret_directory`, `allow_persistent_secrets`
and `default_secret_storage` decide where secrets are stored and loaded from;
`allow_unredacted_secrets` (default `false`) decides whether they can be
printed. A query that can both set that and select from `duckdb_secrets()` can
read back credentials your application loaded for its own use.

### Resource exhaustion

None of these execute anything. They decide how much of the machine one database
can take, which makes them the denial-of-service surface:

| Option | Why it matters |
| --- | --- |
| `memory_limit`, `max_memory` | Raised, one connection can starve the host. Lowered, ordinary queries start failing. |
| `threads`, `worker_threads`, `external_threads` | CPU. A high value on a shared box affects every other tenant. |
| `temp_directory`, `max_temp_directory_size` | Spill files. The size defaults to 90% of available disk — pointing `temp_directory` at the wrong volume can fill it. |
| `block_allocator_memory` | Never freed and cannot be reduced once set. |
| `checkpoint_threshold`, `wal_autocheckpoint` | Raised far enough, the WAL grows without bound. |

Set `memory_limit` and `threads` explicitly if you run more than one database in
a process. The defaults are sized for a machine running one DuckDB, not ten.

### Durability and correctness

DuckDB exposes about 20 `debug_*`, `force_*` and `experimental_*` options at
open time, and they are reachable from a DSN like any other. They exist for
DuckDB's own test suite:

- `debug_skip_checkpoint_on_commit` — commits that do not checkpoint.
- `disable_database_invalidation` — keeps using a database after a fatal error,
  which DuckDB documents as unable to guarantee correct behaviour afterwards.
- `force_column_metadata_reuse` — documented as breaking storage
  backward-compatibility with older DuckDB versions.
- `disabled_optimizers`, `force_compression`, `force_bitpacking_mode` — wrong
  results are not expected, but these are not paths that production traffic
  exercises.

`storage_compatibility_version` is not a debug option but belongs here: it
decides what other DuckDB versions can read the file you write.

## Restricting options yourself

The driver does not filter, so if your DSN is not fully under your control, do
it at the boundary. An allowlist is about a dozen lines:

```go
var allowedOptions = map[string]bool{
    "access_mode": true, "threads": true, "memory_limit": true,
    "temp_directory": true, "default_order": true, "timezone": true,
}

func openRestricted(path string, opts map[string]string) (*sql.DB, error) {
    for name := range opts {
        if !allowedOptions[name] {
            return nil, fmt.Errorf("option %q is not permitted here", name)
        }
    }
    // ... build the settings map and call NewDuckDBWithSettings
}
```

Prefer an allowlist to a denylist. A new DuckDB release adds settings, and a
denylist admits every one of them by default.

Two further levers are DuckDB's own, and they apply after opening, so they hold
even if the open-time configuration was not yours to choose:

- `SET lock_configuration = true` — no further configuration changes.
- `SET enable_external_access = false` — no reads or writes outside the
  database file (`read_csv('/etc/hosts')` then fails with a `Permission Error`).
  This is a one-way door: DuckDB answers `Cannot enable external access while
  database is running` to any attempt to turn it back on, so setting it early
  holds for the life of the instance whether or not you also lock the
  configuration.

The safest configuration remains the one you do not accept from elsewhere. A
constant DSN, or a Go map built in source from values you validated, avoids
every concern on this page.
