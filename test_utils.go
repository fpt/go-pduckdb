package pduckdb

import (
	"github.com/fpt/go-pduckdb/internal/duckdb"
)

// testDuckDB creates a mock DuckDB instance for testing
func testDuckDB() *DuckDB {
	return &DuckDB{
		db: duckdb.TestDB(),
	}
}

// testConnection creates a mock connection for testing
func testConnection() *DuckDBConnection {
	var mockDuckDBConnection duckdb.DuckDBConnection
	return &DuckDBConnection{
		handle: mockDuckDBConnection,
		db:     duckdb.TestDB(),
	}
}
