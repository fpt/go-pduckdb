package pduckdb

import (
	"testing"

	"github.com/fpt/go-pduckdb/internal/duckdb"
)

// testDuckDB creates a mock DuckDB instance for testing
func testDuckDB() *DuckDB {
	return &DuckDB{
		db: duckdb.TestDB(),
	}
}

func TestNewDuckDB(t *testing.T) {
	// Test successful database creation
	// (We can't easily mock the internal NewDB function, so we'll test the public API)
	db, err := NewDuckDB(":memory:")
	if err != nil {
		t.Errorf("Expected successful creation, got error: %v", err)
	}
	if db == nil {
		t.Errorf("Expected non-nil database")
	}

	// Clean up
	if db != nil {
		db.Close()
	}
}

func TestDuckDBConnect(t *testing.T) {
	// Create a test database
	db := testDuckDB()

	// Configure the Connect function to succeed
	db.db.Connect = func(_ duckdb.DuckDBDatabase, handle *duckdb.DuckDBConnection) duckdb.DuckDBState {
		var mockConnection duckdb.DuckDBConnection
		*handle = mockConnection // Assign a non-nil connection
		return duckdb.DuckDBSuccess
	}

	// Test successful connection
	conn, err := db.Connect()
	if err != nil {
		t.Errorf("Expected successful connection, got error: %v", err)
	}
	if conn == nil {
		t.Errorf("Expected non-nil connection")
	}

	// Test failed connection
	db.db.Connect = func(_ duckdb.DuckDBDatabase, _ *duckdb.DuckDBConnection) duckdb.DuckDBState {
		return duckdb.DuckDBError
	}

	conn, err = db.Connect()
	if err == nil {
		t.Errorf("Expected error for failed connection")
	}
	if conn != nil {
		t.Errorf("Expected nil connection for failure")
	}
}

func TestDuckDBClose(t *testing.T) {
	// Create a test database
	db := testDuckDB()

	// Mock the Close function to track if it was called
	var closeCalled bool
	db.db.Close = func(_ *duckdb.DuckDBDatabase) {
		closeCalled = true
	}

	// Test Close
	db.Close()

	if !closeCalled {
		t.Errorf("Close function was not called")
	}
}

func TestGoString(t *testing.T) {
	// Test empty string
	emptyStr := goString(nil)
	if emptyStr != "" {
		t.Errorf("Expected empty string for nil, got %q", emptyStr)
	}

	// We can't easily test non-empty strings without creating C strings
	// This would require CGO or more complex mocking
}
