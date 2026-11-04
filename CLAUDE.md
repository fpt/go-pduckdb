# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A pure-Go (no CGO) DuckDB driver. It loads the native `libduckdb` shared library at runtime via [purego](https://github.com/ebitengine/purego) and registers C API functions as Go function pointers. It exposes both a standard `database/sql` driver (registered as `"duckdb"`) and a small native API.

## Commands

```bash
make test          # go test -v ./...
make lint          # golangci-lint run (v2 config)
make fmt           # gofumpt -extra -w .  (gofumpt, not gofmt)
make run           # run all example programs with CGO_ENABLED=0
make integ         # Docker-based integration tests (linux/amd64)
make integ-arm64   # same, linux/arm64
```

Run a single test: `go test -run TestName ./...`

**Tests require `libduckdb` installed on the host** — they dlopen the real library (no mocks for the C API). CI tests against DuckDB v2.0.0 and v1.5.6 (2.0 release assets are `duckdb-shared-libs-<os>-<arch>.tar.gz`, 1.x are `libduckdb-<os>-<arch>.zip`); on macOS `brew install duckdb` works. Use `DUCKDB_LIBRARY_PATH=/path/to/libduckdb.dylib` to point at a specific library. Official DuckDB Linux builds require glibc (not musl).

## Architecture

Three layers, top to bottom:

1. **`driver.go`** — `database/sql/driver` implementation (`Driver`, `Conn`, `Stmt`, `Rows`, `Tx`, `Result`). Wraps layer 2.
2. **`pduckdb.go`** — thin native public API (`DuckDB`, `NewDuckDB`, `Connect`).
3. **`internal/duckdb/`** — the actual purego bindings. Everything interesting lives here.

### How the bindings work (`internal/duckdb/`)

- `db.go` defines `DB`, a struct whose fields are Go function pointers for ~86 DuckDB C functions. `NewDB` loads the library (`library.go` — search-path logic + `DUCKDB_LIBRARY_PATH` env var) and fills the fields with `purego.RegisterLibFunc`. Functions that may be absent in older DuckDB versions are nil-checked at call sites (e.g. `Result.RowsChanged`).
- **Platform splits via build tags**:
  - `library_unix.go` / `library_windows.go` — `dlopen` vs `syscall.LoadLibrary`.
  - `register_result_unix.go` / `register_result_windows.go` — two C functions (`duckdb_fetch_chunk`, `duckdb_result_return_type`) take `duckdb_result` **by value**, which purego doesn't support on Windows. The Windows file exploits the Win64 ABI (aggregates > 8 bytes pass as a hidden pointer) by registering them with a pointer argument and wrapping. Documented in `docs/COMPATIBILITY.md`.
- **Result reading uses the data-chunk / vector API** (`result.go`, `chunk.go`, `nested.go`), not the deprecated `duckdb_value_*` accessors. `result.go` fetches and caches chunks lazily; `nested.go`'s `decodeValue` is the single type-dispatch point that decodes every supported DuckDB type (recursively for LIST/ARRAY/STRUCT/MAP). To add support for a new type, extend that switch.
- `internal/convert/` handles Go→DuckDB parameter conversion for prepared statements.

### Invariants to preserve

- No CGO anywhere (`CGO_ENABLED=0` must keep working) — never add cgo imports.
- purego ≥ v0.11.0 is required (struct-by-value args for `duckdb_fetch_chunk`); it needs Go ≥ 1.25.
- DuckDB C objects need explicit destroy calls; results/chunks/logical types are freed via `Close()`/`Destroy*` — be careful about use-after-free when returning wrappers whose underlying result may be closed (see issue #23 for a live example of this bug class).
- `docs/COMPATIBILITY.md` is the supported feature matrix — update it when adding types, platforms, or driver interfaces.

## CI

GitHub Actions: `go.yml` (unit tests on ubuntu/macos/windows matrix), `lint.yml` (golangci-lint), `integ.yml`. Workflows pin Go via `go-version-file: go.mod` — keep it that way; omitting `go-version` breaks on macOS runners.
