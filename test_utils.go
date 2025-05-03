package pduckdb

import (
	"unsafe"

	"github.com/fpt/go-pduckdb/internal/duckdb"
)

// testDuckDB creates a mock DuckDB instance for testing
func testDuckDB() *DuckDB {
	return &DuckDB{
		db: duckdb.TestDB(),
	}
}

// testDuckDBResult creates a mock DuckDBResult for testing
func testDuckDBResult() *DuckDBResult {
	return &DuckDBResult{
		internal: duckdb.TestResult(),
	}
}

// mockTimeResult configures a result to return specific date/time values
func mockTimeResult(r *DuckDBResult) {
	duckdb.MockTimeResult(r.internal)
}

// mockStringResult configures a result to return specific string values
func mockStringResult(r *DuckDBResult, values []string) {
	duckdb.MockStringResult(r.internal, values)
}

// testConnection creates a mock connection for testing
func testConnection() *DuckDBConnection {
	return &DuckDBConnection{
		handle: new(byte),
		db:     duckdb.TestDB(),
	}
}

// testPreparedStatement creates a mock prepared statement for testing
func testPreparedStatement() *PreparedStatement {
	conn := testConnection()
	// Add the destroy prepared function to the mock
	conn.db.DestroyPrepared = func(*unsafe.Pointer) {}
	handle := duckdb.TestPreparedStatement(conn.db)

	return &PreparedStatement{
		handle:    handle,
		conn:      conn,
		numParams: 3,
	}
}

// setStatementClosed marks a test prepared statement as closed for testing purposes
func setStatementClosed(stmt *PreparedStatement) {
	stmt.handle = nil
}
