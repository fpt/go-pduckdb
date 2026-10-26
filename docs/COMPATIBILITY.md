# Compatibility

Supported feature matrix for go-pduckdb. Statuses reflect the current `main` branch.

Legend: ✅ supported · ⚠️ supported with caveats · ❌ not supported (yet)

## Platforms

| Platform | Status | CI-tested | Notes |
|---|---|---|---|
| Linux (amd64/arm64) | ✅ | amd64 | Requires purego ≥ v0.10.0 (struct-by-value support) |
| macOS (amd64/arm64) | ✅ | arm64 | |
| Windows (amd64) | ⚠️ | amd64 | Works via an ABI workaround — see [Windows workaround](#windows-workaround) |
| FreeBSD / NetBSD | ⚠️ | no | Compiles (covered by build tags), untested |

### Windows workaround

purego does not support passing structs **by value** on Windows, but two DuckDB C
API functions take a `duckdb_result` struct by value:

- `duckdb_fetch_chunk`
- `duckdb_result_return_type`

The driver works around this by relying on a Win64 calling-convention detail:
aggregates larger than 8 bytes are passed as a **hidden pointer** to a
caller-allocated copy. Since `duckdb_result` is 48 bytes, at the ABI level these
functions actually receive a `duckdb_result *`. On Windows the driver registers
them with an explicit pointer argument and wraps them to preserve the by-value
signature used on other platforms (see
`internal/duckdb/register_result_windows.go`).

This is an ABI-level workaround, not an officially supported purego feature. It
is exercised in CI on `windows-latest` (amd64) on every push, but be aware of it
if you hit Windows-specific crashes around result fetching.

DLL discovery on Windows: `DUCKDB_LIBRARY_PATH`, then `duckdb.dll` in the
current directory, `%ProgramFiles%\DuckDB\`, `%ProgramFiles(x86)%\DuckDB\`, and
finally the standard `LoadLibrary` search path (`PATH`).

## Opening a database

| Feature | Status | Notes |
|---|---|---|
| Path | ✅ | `sql.Open("duckdb", "warehouse.duckdb")`, or `:memory:` |
| Configuration options | ✅ | As a DSN query string: `"warehouse.duckdb?access_mode=READ_ONLY&threads=2"` |
| Read-only | ✅ | `access_mode=READ_ONLY` — the database refuses writes, rather than the caller intending not to make any |

Options go through `duckdb_open_ext`; a path with no `?` uses `duckdb_open`
unchanged. The **last** `?` separates path from options, and a trailing `?`
means "no options", so a database file whose name contains one is still
reachable. An option DuckDB does not recognise fails the open with DuckDB's own
message rather than being ignored, as does a malformed query string.

Every option DuckDB accepts at open time is accepted here; a repeated option is
an error rather than a silent last-one-wins. See
[CONFIGURATION.md](./CONFIGURATION.md) for what those options can reach.

## database/sql driver interface

| Feature | Status | Notes |
|---|---|---|
| Query / Exec | ✅ | |
| Prepared statements | ✅ | Positional `?` placeholders; values are converted to the type DuckDB infers for each parameter |
| Parameter binding | ✅ | See [Parameter conversions](#parameter-conversions-go--duckdb) |
| Slice arguments | ✅ | `driver.NamedValueChecker`; bound as LIST |
| Transactions (`Begin`/`Commit`/`Rollback`) | ✅ | `driver.ConnBeginTx` |
| Context support | ⚠️ | Cancellation is checked before execution, not during a running query |
| `Ping` | ✅ | `driver.Pinger` |
| `ColumnTypes()`: scan type | ✅ | `RowsColumnTypeScanType` |
| `ColumnTypes()`: database type name | ✅ | `RowsColumnTypeDatabaseTypeName` (e.g. `INTEGER`, `VARCHAR`) |
| `ColumnTypes()`: nullable | ✅ | `RowsColumnTypeNullable` |
| `ColumnTypes()`: precision & scale | ✅ | `RowsColumnTypePrecisionScale` |
| `Result.RowsAffected()` | ⚠️ | Broken for parameterized `Exec` — see [#23](https://github.com/fpt/go-pduckdb/issues/23) |
| `Result.LastInsertId()` | ❌ | Not supported by DuckDB; returns an error |
| Named parameters | ❌ | Positional only |

## Data types (reading results)

Result values are decoded through DuckDB's data-chunk / vector API.

| DuckDB type | Go value | Notes |
|---|---|---|
| BOOLEAN | `bool` | |
| TINYINT / SMALLINT / INTEGER / BIGINT | `int8` / `int16` / `int32` / `int64` | |
| UTINYINT / USMALLINT / UINTEGER / UBIGINT | `uint8` / `uint16` / `uint32` / `uint64` | |
| HUGEINT | `int64` or `string` | `string` when the value exceeds the int64 range |
| UHUGEINT | `string` | |
| FLOAT / DOUBLE | `float32` / `float64` | |
| DECIMAL | `string` | Exact decimal representation |
| VARINT | `string` | Arbitrary-precision integer, decimal string |
| VARCHAR | `string` | |
| JSON | `string` | Alias of VARCHAR. v0.2.0 and earlier returned `[]byte`. Scan into `*string` or `*[]byte`; `*json.RawMessage` is not supported |
| BLOB | `[]byte` | |
| UUID | `string` | |
| ENUM | `string` | Dictionary value |
| DATE / TIME | `time.Time` | |
| TIME WITH TIME ZONE | `time.Time` | Clock component + fixed-offset zone; the date component is not meaningful |
| TIMESTAMP / TIMESTAMP_S / TIMESTAMP_MS / TIMESTAMP_NS | `time.Time` | UTC |
| TIMESTAMP WITH TIME ZONE | `time.Time` | |
| INTERVAL | `string` | |
| LIST / ARRAY | `[]any` | Recursive — nested types decode fully |
| STRUCT | `map[string]any` | Recursive |
| MAP | `[]MapEntry` (`{Key, Value any}`) | Recursive. Note: `MapEntry` is defined in an internal package, so it cannot be referenced by name from user code yet |
| UNION | ❌ | Scans as `NULL` |
| BIT | ❌ | Scans as `NULL` |

## Parameter conversions (Go → DuckDB)

| Go type | DuckDB type |
|---|---|
| `bool` | BOOLEAN |
| `int`, `int8`–`int64`, `uint8`–`uint64` | Numeric types, with range validation |
| `float32`, `float64` | FLOAT / DOUBLE |
| `string` | VARCHAR (or inferred type based on content) |
| `[]byte` | BLOB |
| `time.Time` | DATE, TIME, or TIMESTAMP |
| `[]T` for `T` in `bool`, `int`, `int8`–`int64`, `uint`, `uint16`–`uint64`, `float32`, `float64`, `string`, or such a slice | LIST of the matching type (`[]int64` → `BIGINT[]`, `[][]string` → `VARCHAR[][]`), cast by DuckDB where the parameter has a known type. Works where DuckDB cannot infer the parameter type, e.g. `unnest(?)` or `read_json(?)` with a list of paths. Types implementing `driver.Valuer` use `Value()` instead |

Where DuckDB cannot infer a parameter's type, such as the argument of a table
function (`read_json(?)`) or of a function taking `ANY` (`from_json(?, …)`), the
value is bound by its Go type instead: `bool` → BOOLEAN, signed integers →
BIGINT, unsigned integers → UBIGINT, `float32` / `float64` → FLOAT / DOUBLE,
`string` → VARCHAR, `[]byte` → BLOB, `time.Time` → TIMESTAMP. No `?::VARCHAR`
cast is needed.

## Requirements

| Requirement | Version | Notes |
|---|---|---|
| Go | ≥ 1.24 | |
| purego | ≥ v0.10.0 | v0.10.0 added struct-by-value arguments on Linux, needed for `duckdb_fetch_chunk` |
| DuckDB shared library | v1.5.x | CI tests against v1.5.4; nearby versions generally work since the C API is stable |
