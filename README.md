# go-pduckdb is a PureGO driver for [DuckDB](https://duckdb.org/docs/stable/clients/c/api.html)

## Introduction

A DuckDB module for Go which doesn't require CGO.
Uses [purego](https://github.com/ebitengine/purego) to interface with DuckDB's native library.

## Features

- Pure Go implementation - no CGO required
- Support for all DuckDB data types including DATE, TIME, and TIMESTAMP
- Connection pooling
- Query execution and result handling
- Clear error reporting
- Cross-platform compatibility
- Standard database/sql interface support
- Support for prepared statements with automatic type inference

## Installation

```bash
go get github.com/fpt/go-pduckdb
```

Also, make sure to install DuckDB on your platform:

### macOS
```bash
brew install duckdb
```

Typically, `/opt/homebrew/lib/libduckdb.dylib` is installed.

### Linux (Ubuntu/Debian)
```bash
curl -sSL https://github.com/duckdb/duckdb/releases/download/v1.2.2/libduckdb-linux-amd64.zip -o archive.zip
sudo unzip -j archive.zip libduckdb.so -d /usr/local/lib
sudo ldconfig
rm archive.zip
```

You can find a download URL in [official releases of DuckDB](https://github.com/duckdb/duckdb/releases).
Assets starting with `libduckdb-` contains glibc build of `libduckdb.so`.

For other Linux, Check official instruction: [Building DuckDB](https://duckdb.org/docs/stable/dev/building/linux.html).

### Windows
Download the DuckDB CLI from the [official website](https://duckdb.org/docs/installation/) and place the DLL in your system path.

## Library Path Configuration

go-pduckdb searches for the DuckDB library in several locations. You can configure the search path using environment variables:

- `DUCKDB_LIBRARY_PATH` - specify the exact path to the DuckDB library file
- `DYLD_LIBRARY_PATH` - on macOS, specify directories to search for the DuckDB library
- `LD_LIBRARY_PATH` - on Linux, specify directories to search for the DuckDB library

Example usage:

```bash
# Specify exact library path
DUCKDB_LIBRARY_PATH=/path/to/libduckdb.dylib ./your_program

# Or specify directory to search (macOS)
DYLD_LIBRARY_PATH=/path/to/lib ./your_program

# Or specify directory to search (Linux)
LD_LIBRARY_PATH=/path/to/lib ./your_program
```

If no environment variables are set, the library will be searched in standard system locations.

## Usage Examples

### Using Standard database/sql Interface

go-pduckdb implements the Go standard database/sql interface, allowing you to work with DuckDB like any other SQL database in Go:

```go
package main

import (
	"database/sql"
	"fmt"
	"log"
	
	_ "github.com/fpt/go-pduckdb" // Import for driver registration
)

func main() {
	// Open a database connection
	db, err := sql.Open("duckdb", "example.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	// Create a table
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY, 
		name VARCHAR, 
		email VARCHAR
	)`)
	if err != nil {
		log.Fatal(err)
	}
	
	// Insert data
	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, 
		1, "John Doe", "john@example.com")
	if err != nil {
		log.Fatal(err)
	}
	
	// Query data
	rows, err := db.Query("SELECT id, name, email FROM users")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	
	// Process results
	for rows.Next() {
		var id int
		var name, email string
		if err := rows.Scan(&id, &name, &email); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("User %d: %s (%s)\n", id, name, email)
	}
}
```

For a more comprehensive example, see the [database/sql example](./example/databasesql/main.go).

### Parameter Binding and Type Conversion

go-pduckdb features a sophisticated type conversion system that automatically handles type conversions for prepared statement parameters:

```go
// Prepare a statement
stmt, err := conn.Prepare("INSERT INTO users (id, name, created_date) VALUES (?, ?, ?)")
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

// Execute with different parameter types
// The driver will automatically convert these to the appropriate types
err = stmt.Execute(
    1,                                 // int -> INTEGER
    "John Doe",                        // string -> VARCHAR
    time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),  // time.Time -> DATE
)
```

Supported conversions include:
- Go bool -> DuckDB BOOLEAN
- Go numeric types -> DuckDB numeric types with range validation
- Go string -> Various DuckDB types based on content
- Go []byte -> DuckDB BLOB
- Go time.Time -> DuckDB DATE, TIME, or TIMESTAMP
- Custom Date, Time, and Interval types for precise control

For more examples, check the [example](./example) directory.

## API Documentation

### Standard database/sql Interface

go-pduckdb registers itself as a driver named "duckdb" with the standard database/sql package, supporting:

- Connection management (Open, Close)
- Query execution (Exec, Query)
- Prepared statements
- Transactions
- Context handling
- Parameter binding

### Native API

go-pduckdb also provides a native API for more direct interaction with DuckDB:

- **DuckDB**: Represents a database instance
- **DuckDBConnection**: Handles connections to the database
- **DuckDBResult**: Manages query results
- **DuckDBDate**, **DuckDBTime**, **DuckDBTimestamp**: Date and time types

### Date and Time Handling

go-pduckdb provides native Go type conversions for DuckDB's date and time types:

```go
// Get date value
dateVal, hasValue := result.ValueDate(columnIndex, rowIndex)
if hasValue {
    fmt.Println("Date:", dateVal.Format("2006-01-02"))
}

// Get timestamp value
tsVal, hasValue := result.ValueTimestamp(columnIndex, rowIndex)
if hasValue {
    fmt.Println("Timestamp:", tsVal.Format("2006-01-02 15:04:05.000000"))
}
```

## Limitations

### Unsupported types (yet)

- List
- Struct

### Unsupported types due to purego limitation

These types use struct return value which is not supported by purego in some platform.

- Blob
- Interval

## Project Structure

This project follows the [standard Go project layout](https://go.dev/doc/modules/layout) with:

```
go-pduckdb/
├── conn.go          # Connection handling
├── datetime.go      # Date/time type support
├── pduckdb.go       # Core functionality
├── result.go        # Result processing
├── type.go          # Type definitions
├── *_test.go        # Unit tests
└── example/         # Example code
```

## Contributing

Contributions are welcome! Please read our [contributing guidelines](CONTRIBUTING.md) before submitting a pull request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## References

### DuckDB

- [Official Documentation](https://duckdb.org/docs/stable/clients/c/api.html)
- [C API Source](https://github.com/duckdb/duckdb/tree/main/src/main/capi)
- [C Header](https://github.com/duckdb/duckdb/tree/main/src/include/duckdb.h)
