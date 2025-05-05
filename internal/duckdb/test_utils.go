package duckdb

import (
	"time"
	"unsafe"
)

// TestDB creates a mock DB for testing
func TestDB() *DB {
	var mockDuckDBDatabase DuckDBDatabase
	return &DB{
		Handle:         mockDuckDBDatabase,
		Connect:        func(DuckDBDatabase, *DuckDBConnection) DuckDBState { return DuckDBSuccess },
		Close:          func(*DuckDBDatabase) {},
		Query:          func(DuckDBConnection, *byte, *DuckDBResultRaw) DuckDBState { return DuckDBSuccess },
		ColumnCount:    func(*DuckDBResultRaw) int64 { return 0 },
		RowCount:       func(*DuckDBResultRaw) int64 { return 0 },
		ColumnName:     func(*DuckDBResultRaw, int64) *byte { return nil },
		ValueString:    func(*DuckDBResultRaw, int64, int32) *byte { return nil },
		ValueDate:      func(*DuckDBResultRaw, int64, int32) int32 { return 0 },
		ValueTime:      func(*DuckDBResultRaw, int64, int32) int64 { return 0 },
		ValueTimestamp: func(*DuckDBResultRaw, int64, int32) int64 { return 0 },
		DestroyResult:  func(*DuckDBResultRaw) {},
	}
}

// TestResult creates a mock Result for testing
func TestResult() *Result {
	mockRaw := DuckDBResultRaw{}

	return &Result{
		Raw:            mockRaw,
		ColumnCount:    func(*DuckDBResultRaw) int64 { return 3 },
		RowCount:       func(*DuckDBResultRaw) int64 { return 2 },
		ColumnName:     func(*DuckDBResultRaw, int64) *byte { return nil },
		ValueString:    func(*DuckDBResultRaw, int64, int32) *byte { return nil },
		ValueDate:      func(*DuckDBResultRaw, int64, int32) int32 { return 0 },
		ValueTime:      func(*DuckDBResultRaw, int64, int32) int64 { return 0 },
		ValueTimestamp: func(*DuckDBResultRaw, int64, int32) int64 { return 0 },
		DestroyResult:  func(*DuckDBResultRaw) {},
	}
}

// MockTimeResult configures a Result to return specific date/time values
func MockTimeResult(r *Result) {
	// Mock current date (2025-05-01)
	r.ValueDate = func(*DuckDBResultRaw, int64, int32) int32 {
		// Calculate days since epoch (1970-01-01) to 2025-05-01
		// First calculate seconds, then convert to days
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		target := time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC)
		days := int32(target.Sub(epoch).Hours() / 24)
		return days
	}

	// Mock time (14:30:45)
	r.ValueTime = func(*DuckDBResultRaw, int64, int32) int64 {
		return 14*60*60*1000000 + 30*60*1000000 + 45*1000000 // 14:30:45 in microseconds
	}

	// Mock timestamp (2025-05-01 14:30:45)
	r.ValueTimestamp = func(*DuckDBResultRaw, int64, int32) int64 {
		t := time.Date(2025, 5, 1, 14, 30, 45, 0, time.UTC)
		return t.Unix()*1000000 + int64(t.Nanosecond())/1000
	}
}

// MockStringResult configures a Result to return specific string values
func MockStringResult(r *Result, values []string) {
	// Create C strings for all values
	cstrings := make([]*byte, len(values))
	for i, val := range values {
		cstrings[i] = ToCString(val)
	}

	// For column names
	r.ColumnName = func(_ *DuckDBResultRaw, col int64) *byte {
		if int(col) < 3 { // We have 3 columns in our test
			return cstrings[col]
		}
		return nil
	}

	// For row values
	r.ValueString = func(_ *DuckDBResultRaw, col int64, row int32) *byte {
		// The test expects:
		// Values: ["1", "John", "john@example.com", "2", "Jane", "jane@example.com"]
		// For Row 0: values[0]=1, values[1]=John, values[2]=john@example.com
		// For Row 1: values[3]=2, values[4]=Jane, values[5]=jane@example.com

		// So we need to calculate the index as: row*3 + col
		idx := int(row)*3 + int(col)
		if idx < len(values) {
			return cstrings[idx]
		}
		return nil
	}
}

// TestPreparedStatement creates a mock PreparedStatement for testing
func TestPreparedStatement(db *DB) unsafe.Pointer {
	// Create a valid underlying value to point to instead of a magic number
	dummyVal := 12345
	return unsafe.Pointer(&dummyVal)
}
