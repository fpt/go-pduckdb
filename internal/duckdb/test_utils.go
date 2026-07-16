package duckdb

// testConnection creates a mock connection for testing
func testConnection() *Connection {
	var mockDuckDBConnection DuckDBConnection
	return &Connection{
		handle: mockDuckDBConnection,
		db:     TestDB(),
	}
}

// TestDB creates a mock DB for testing. Only the handful of function pointers
// exercised by the unit tests (connect/query/prepare lifecycle) are populated;
// value reading goes through the data-chunk API against a real library.
func TestDB() *DB {
	var mockDuckDBDatabase DuckDBDatabase
	return &DB{
		Handle:        mockDuckDBDatabase,
		Connect:       func(DuckDBDatabase, *DuckDBConnection) DuckDBState { return DuckDBSuccess },
		Close:         func(*DuckDBDatabase) {},
		Query:         func(DuckDBConnection, *byte, *DuckDBResultRaw) DuckDBState { return DuckDBSuccess },
		ColumnCount:   func(*DuckDBResultRaw) int64 { return 0 },
		ColumnName:    func(*DuckDBResultRaw, int64) *byte { return nil },
		DestroyResult: func(*DuckDBResultRaw) {},
	}
}

// TestResult creates a mock Result for testing
func TestResult() *Result {
	return &Result{
		Raw: DuckDBResultRaw{},
		Db:  TestDB(),
	}
}
