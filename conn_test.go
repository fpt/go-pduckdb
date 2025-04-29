package pduckdb

import (
	"testing"
)

// mockDB creates a mock DuckDB for connection tests
func mockDB() *DuckDB {
	return &DuckDB{
		columnCount:    func(*DuckDBResultRaw) int32 { return 0 },
		rowCount:       func(*DuckDBResultRaw) int64 { return 0 },
		columnName:     func(*DuckDBResultRaw, int32) *byte { return nil },
		valueString:    func(*DuckDBResultRaw, int64, int32) *byte { return nil },
		valueDate:      func(*DuckDBResultRaw, int64, int32) DuckDBDate { return 0 },
		valueTime:      func(*DuckDBResultRaw, int64, int32) DuckDBTime { return 0 },
		valueTimestamp: func(*DuckDBResultRaw, int64, int32) DuckDBTimestamp { return 0 },
		destroyResult:  func(*DuckDBResultRaw) {},
	}
}

func TestDuckDBConnection_Query_Success(t *testing.T) {
	// Create a connection with a mock query function that returns success
	conn := &DuckDBConnection{
		db: mockDB(),
		query: func(*byte, string, *DuckDBResultRaw) DuckDBState {
			return DuckDBSuccess
		},
	}

	result, err := conn.Query("SELECT 1")
	if err != nil {
		t.Errorf("Query() error = %v, want nil", err)
	}

	if result == nil {
		t.Error("Query() result is nil, want non-nil")
	}
}

func TestDuckDBConnection_Query_Error(t *testing.T) {
	// Create a connection with a mock query function that returns error
	conn := &DuckDBConnection{
		db: mockDB(),
		query: func(*byte, string, *DuckDBResultRaw) DuckDBState {
			return DuckDBError
		},
	}

	result, err := conn.Query("SELECT 1")

	if err == nil {
		t.Error("Query() error is nil, want non-nil")
	}

	if result != nil {
		t.Errorf("Query() result = %v, want nil", result)
	}

	// Check that error message contains the query
	if err != nil && err.Error() != "Query failed: SELECT 1" {
		t.Errorf("Query() error = %v, want 'Query failed: SELECT 1'", err)
	}
}

func TestDuckDBConnection_Execute_Success(t *testing.T) {
	// Keep track of whether Close() was called
	closeCalled := false

	conn := &DuckDBConnection{
		db: mockDB(),
		query: func(*byte, string, *DuckDBResultRaw) DuckDBState {
			return DuckDBSuccess
		},
	}

	// Replace the db.destroyResult function to track when Close is called
	conn.db.destroyResult = func(*DuckDBResultRaw) {
		closeCalled = true
	}

	err := conn.Execute("CREATE TABLE test (id INT)")
	if err != nil {
		t.Errorf("Execute() error = %v, want nil", err)
	}

	if !closeCalled {
		t.Error("Execute() did not call Close() on the result")
	}
}

func TestDuckDBConnection_Execute_Error(t *testing.T) {
	conn := &DuckDBConnection{
		db: mockDB(),
		query: func(*byte, string, *DuckDBResultRaw) DuckDBState {
			return DuckDBError
		},
	}

	err := conn.Execute("CREATE TABLE test (id INT)")

	if err == nil {
		t.Error("Execute() error is nil, want non-nil")
	}

	// Check that error message contains the query
	if err != nil && err.Error() != "Query failed: CREATE TABLE test (id INT)" {
		t.Errorf("Execute() error = %v, want 'Query failed: CREATE TABLE test (id INT)'", err)
	}
}

func TestDuckDBConnection_Close(t *testing.T) {
	// This is a no-op test since Close() doesn't do anything currently
	// It's included for completeness
	conn := &DuckDBConnection{
		db: mockDB(),
	}

	// Shouldn't panic
	conn.Close()
}
