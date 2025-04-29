package pduckdb

import (
	"time"
)

// DuckDBResultRaw is the raw C structure for DuckDB query results
type DuckDBResultRaw struct {
	DeprecatedColumnCount  int64
	DeprecatedRowCount     int64
	DeprecatedRowsChanged  int64
	DeprecatedColumns      uintptr
	DeprecatedErrorMessage *byte
	InternalData           uintptr
}

// DuckDBResult wraps the raw result and provides methods to access the data
type DuckDBResult struct {
	raw            DuckDBResultRaw
	columnCount    func(*DuckDBResultRaw) int32
	rowCount       func(*DuckDBResultRaw) int64
	columnName     func(*DuckDBResultRaw, int32) *byte
	valueString    func(*DuckDBResultRaw, int64, int32) *byte
	valueDate      func(*DuckDBResultRaw, int64, int32) DuckDBDate
	valueTime      func(*DuckDBResultRaw, int64, int32) DuckDBTime
	valueTimestamp func(*DuckDBResultRaw, int64, int32) DuckDBTimestamp
	destroyResult  func(*DuckDBResultRaw)
}

// ColumnCount returns the number of columns in the result
func (r *DuckDBResult) ColumnCount() int32 {
	return r.columnCount(&r.raw)
}

// RowCount returns the number of rows in the result
func (r *DuckDBResult) RowCount() int64 {
	return r.rowCount(&r.raw)
}

// ColumnName returns the name of the column at the given index
func (r *DuckDBResult) ColumnName(column int32) string {
	ptr := r.columnName(&r.raw, column)
	return GoString(ptr)
}

// ValueString returns the string value at the given row and column
func (r *DuckDBResult) ValueString(column int64, row int32) (string, bool) {
	// According to DuckDB documentation:
	// duckdb_string duckdb_value_string(duckdb_result *result, idx_t col, idx_t row);
	// Column index comes first, then row index
	ptr := r.valueString(&r.raw, column, row)
	if ptr == nil {
		return "", false // NULL value
	}
	return GoString(ptr), true
}

// ValueDate returns the date value at the given column and row
func (r *DuckDBResult) ValueDate(column int64, row int32) (time.Time, bool) {
	// Check if we have the function available
	if r.valueDate == nil {
		return time.Time{}, false
	}

	date := r.valueDate(&r.raw, column, row)
	// DuckDB uses a special value for NULL dates
	if date == 0 {
		return time.Time{}, false // NULL value
	}

	return date.ToTime(), true
}

// ValueTime returns the time value at the given column and row
func (r *DuckDBResult) ValueTime(column int64, row int32) (time.Time, bool) {
	// Check if we have the function available
	if r.valueTime == nil {
		return time.Time{}, false
	}

	timeVal := r.valueTime(&r.raw, column, row)
	// DuckDB typically uses 0 for NULL times
	if timeVal == 0 {
		return time.Time{}, false
	}

	return timeVal.ToTime(), true
}

// ValueTimestamp returns the timestamp (datetime) value at the given column and row
func (r *DuckDBResult) ValueTimestamp(column int64, row int32) (time.Time, bool) {
	// Check if we have the function available
	if r.valueTimestamp == nil {
		return time.Time{}, false
	}

	timestamp := r.valueTimestamp(&r.raw, column, row)
	// DuckDB typically uses 0 for NULL timestamps
	if timestamp == 0 {
		return time.Time{}, false
	}

	return timestamp.ToTime(), true
}

// Close destroys the result and frees associated resources
func (r *DuckDBResult) Close() {
	r.destroyResult(&r.raw)
}
