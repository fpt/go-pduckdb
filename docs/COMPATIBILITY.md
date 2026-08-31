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
unchanged. The **last** `?` separates path from options, so a database file
whose name contains one is still reachable. An option DuckDB does not
recognise fails the open with DuckDB's own message rather than being ignored.

Every option DuckDB accepts at open time is accepted here — the driver applies
no allowlist. Some of them govern extension loading, filesystem reach and
machine resources. See [CONFIGURATION.md](./CONFIGURATION.md) before building a
DSN from anything you did not write yourself.

## database/sql driver interface

| Feature | Status | Notes |
|---|---|---|
| Query / Exec | ✅ | |
| Prepared statements | ✅ | Positional `?` placeholders; parameter type inference |
| Parameter binding | ✅ | See [Parameter conversions](#parameter-conversions-go--duckdb) |
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

## Requirements

| Requirement | Version | Notes |
|---|---|---|
| Go | ≥ 1.24 | |
| purego | ≥ v0.10.0 | v0.10.0 added struct-by-value arguments on Linux, needed for `duckdb_fetch_chunk` |
| DuckDB shared library | v1.5.x | CI tests against v1.5.4; nearby versions generally work since the C API is stable |
