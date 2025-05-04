package pduckdb

import (
	"testing"
	"unsafe"

	"github.com/fpt/go-pduckdb/internal/duckdb"
)

func TestConnectionQuery(t *testing.T) {
	// Create a test connection with a custom query function
	conn := testConnection()

	// Override the query function to return a successful result
	conn.db.Query = func(_ *byte, sql string, result *duckdb.DuckDBResultRaw) duckdb.DuckDBState {
		if sql == "SELECT 1" {
			return duckdb.DuckDBSuccess
		}
		return duckdb.DuckDBError
	}

	// Test successful query
	result, err := conn.Query("SELECT 1")
	if err != nil {
		t.Errorf("Expected successful query, got error: %v", err)
	}
	if result == nil {
		t.Errorf("Expected non-nil result")
	}

	// Test failed query
	result, err = conn.Query("INVALID SQL")
	if err == nil {
		t.Errorf("Expected error for invalid SQL")
	}
	if result != nil {
		t.Errorf("Expected nil result for failed query")
	}
}

func TestConnectionExecute(t *testing.T) {
	// Create a test connection
	conn := testConnection()

	// Override the query function for testing
	conn.db.Query = func(_ *byte, sql string, result *duckdb.DuckDBResultRaw) duckdb.DuckDBState {
		if sql == "CREATE TABLE test (id INT)" {
			return duckdb.DuckDBSuccess
		}
		return duckdb.DuckDBError
	}

	// Test successful execution
	err := conn.Execute("CREATE TABLE test (id INT)")
	if err != nil {
		t.Errorf("Expected successful execution, got error: %v", err)
	}

	// Test failed execution
	err = conn.Execute("INVALID SQL")
	if err == nil {
		t.Errorf("Expected error for invalid SQL execution")
	}
}

func TestConnectionPrepare(t *testing.T) {
	// Create a test connection
	conn := testConnection()

	// Set up prepare function
	conn.db.Prepare = func(_, _ *byte, stmt *unsafe.Pointer) duckdb.DuckDBState {
		// Use a valid pointer conversion pattern
		dummyVal := 12345
		*stmt = unsafe.Pointer(&dummyVal)
		return duckdb.DuckDBSuccess
	}

	// Set up number of parameters function
	conn.db.NumParams = func(unsafe.Pointer) int64 {
		return 3
	}

	// Test successful prepare
	stmt, err := conn.Prepare("SELECT * FROM test WHERE id = ?")
	if err != nil {
		t.Errorf("Expected successful prepare, got error: %v", err)
	}
	if stmt == nil {
		t.Errorf("Expected non-nil prepared statement")
		return // Return early to avoid nil dereference
	}
	if stmt.numParams != 3 {
		t.Errorf("Expected 3 parameters, got %d", stmt.numParams)
	}

	// Test prepare error
	conn.db.Prepare = func(_, _ *byte, _ *unsafe.Pointer) duckdb.DuckDBState {
		return duckdb.DuckDBError
	}

	stmt, err = conn.Prepare("INVALID SQL")
	if err == nil {
		t.Errorf("Expected error for invalid SQL prepare")
	}
	if stmt != nil {
		t.Errorf("Expected nil statement for failed prepare")
	}
}

func TestPreparedStatementClose(t *testing.T) {
	// Create a test prepared statement
	stmt := testPreparedStatement()

	// Set up destroy function
	stmt.conn.db.DestroyPrepared = func(*unsafe.Pointer) {}

	// Test close
	err := stmt.Close()
	if err != nil {
		t.Errorf("Expected successful close, got error: %v", err)
	}

	// Test double close (should be no-op)
	err = stmt.Close()
	if err != nil {
		t.Errorf("Expected successful second close, got error: %v", err)
	}
}

func TestPreparedStatementExecute(t *testing.T) {
	// Create a test prepared statement
	stmt := testPreparedStatement()

	// Set up execute function
	stmt.conn.db.ExecutePrepared = func(_ unsafe.Pointer, result *duckdb.DuckDBResultRaw) duckdb.DuckDBState {
		return duckdb.DuckDBSuccess
	}

	// Test successful execute
	result, err := stmt.Execute()
	if err != nil {
		t.Errorf("Expected successful execute, got error: %v", err)
	}
	if result == nil {
		t.Errorf("Expected non-nil result")
	}

	// Test execute error
	stmt.conn.db.ExecutePrepared = func(_ unsafe.Pointer, _ *duckdb.DuckDBResultRaw) duckdb.DuckDBState {
		return duckdb.DuckDBError
	}

	result, err = stmt.Execute()
	if err == nil {
		t.Errorf("Expected error for failed execute")
	}
	if result != nil {
		t.Errorf("Expected nil result for failed execute")
	}

	// Test execute after close
	if err := stmt.Close(); err != nil {
		t.Errorf("Error closing statement: %v", err)
	}
	result, err = stmt.Execute()
	if err == nil {
		t.Errorf("Expected error executing closed statement")
	}
	if result != nil {
		t.Errorf("Expected nil result for closed statement")
	}
}
