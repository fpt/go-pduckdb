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

## Installation

```bash
go get github.com/fpt/go-pduckdb
```

Also, make sure to install DuckDB on your platform:

### macOS
```bash
brew install duckdb
```

### Linux (Ubuntu/Debian)
```bash
apt-get install duckdb
```

### Windows
Download the DuckDB CLI from the [official website](https://duckdb.org/docs/installation/) and place the DLL in your system path.

## Example

Here's a simple example of using go-pduckdb:

```go
package main

import (
	"fmt"
	"os"
	
	"github.com/fpt/go-pduckdb"
)

func main() {
	// Open a database connection
	db, err := pduckdb.NewDuckDB("example.db")
	if err != nil {
		fmt.Printf("Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	
	// Create a connection
	conn, err := db.Connect()
	if err != nil {
		fmt.Printf("Error connecting to database: %v\n", err)
		os.Exit(1)
	}
	
	// Create a table
	err = conn.Execute(`
		CREATE TABLE users (
			id INTEGER,
			name VARCHAR,
			email VARCHAR,
			created_at TIMESTAMP
		)
	`)
	if err != nil {
		fmt.Printf("Error creating table: %v\n", err)
		os.Exit(1)
	}
	
	// Insert data
	err = conn.Execute(`
		INSERT INTO users VALUES 
		(1, 'John Doe', 'john@example.com', '2025-01-15 08:30:00'),
		(2, 'Jane Smith', 'jane@example.com', '2025-02-20 14:45:30')
	`)
	if err != nil {
		fmt.Printf("Error inserting data: %v\n", err)
		os.Exit(1)
	}
	
	// Query data
	result, err := conn.Query("SELECT * FROM users")
	if err != nil {
		fmt.Printf("Error querying data: %v\n", err)
		os.Exit(1)
	}
	defer result.Close()
	
	// Display results
	rowCount := result.RowCount()
	fmt.Printf("Found %d users:\n", rowCount)
	
	for r := int32(0); r < int32(rowCount); r++ {
		id, _ := result.ValueString(0, r)
		name, _ := result.ValueString(1, r)
		email, _ := result.ValueString(2, r)
		timestamp, _ := result.ValueTimestamp(3, r)
		
		fmt.Printf("User %s: %s (%s) - Created: %s\n",
			id, name, email, timestamp.Format("2006-01-02 15:04:05"))
	}
}
```

For more examples, check the [example](./example) directory.

## API Documentation

### Main Types

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
