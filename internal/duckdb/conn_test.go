package duckdb

import (
	"testing"
)

func TestConnectionQuery(t *testing.T) {
	// Create a test connection with a custom query function
	conn := testConnection()

	// Override the query function to return a successful result
	conn.db.Query = func(_ DuckDBConnection, sql *byte, result *DuckDBResultRaw) DuckDBState {
		if GoString(sql) == "SELECT 1" {
			return DuckDBSuccess
		}
		return DuckDBError
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
	conn.db.Query = func(_ DuckDBConnection, sql *byte, result *DuckDBResultRaw) DuckDBState {
		if GoString(sql) == "CREATE TABLE test (id INT)" {
			return DuckDBSuccess
		}
		return DuckDBError
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
	conn.db.Prepare = func(_ DuckDBConnection, _ *byte, stmt *DuckDBPreparedStatement) DuckDBState {
		// Use a valid pointer conversion pattern
		dummyVal := 12345
		*stmt = DuckDBPreparedStatement(&dummyVal)
		return DuckDBSuccess
	}

	// Set up number of parameters function
	conn.db.NumParams = func(stmt DuckDBPreparedStatement) int64 {
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
	conn.db.Prepare = func(_ DuckDBConnection, _ *byte, stmt *DuckDBPreparedStatement) DuckDBState {
		return DuckDBError
	}

	stmt, err = conn.Prepare("INVALID SQL")
	if err == nil {
		t.Errorf("Expected error for invalid SQL prepare")
	}
	if stmt != nil {
		t.Errorf("Expected nil statement for failed prepare")
	}
}
