package duckdb

import (
	"time"
)

// Result wraps the raw result and provides methods to access the data
type Result struct {
	Raw DuckDBResultRaw
	Db  *DB
}

// NewResult creates a new Result from a database and raw result
func NewResult(db *DB, raw DuckDBResultRaw) *Result {
	return &Result{
		Raw: raw,
		Db:  db,
	}
}

// GetColumnCount returns the number of columns in the result
func (r *Result) GetColumnCount() int64 {
	return r.Db.ColumnCount(&r.Raw)
}

// GetRowCount returns the number of rows in the result
func (r *Result) GetRowCount() int64 {
	return r.Db.RowCount(&r.Raw)
}

// GetColumnName returns the name of the column at the given index
func (r *Result) GetColumnName(column int64) string {
	ptr := r.Db.ColumnName(&r.Raw, column)
	return GoString(ptr)
}

// GetColumnType returns the type of the column at the given index
func (r *Result) GetColumnType(column int64) DuckDBType {
	typ := r.Db.ColumnType(&r.Raw, column)
	return typ
}

// GetColumnLogicalType returns the logical type of the column at the given index
func (r *Result) GetColumnLogicalType(column int64) DuckDBLogicalType {
	typ := r.Db.ColumnLogicalType(&r.Raw, column)
	return typ
}

// GetValueString returns the string value at the given row and column
func (r *Result) GetValueString(column int64, row int32) (string, bool) {
	ptr := r.Db.ValueString(&r.Raw, column, row)
	if ptr == nil {
		return "", false // NULL value
	}
	return GoString(ptr), true
}

// GetValueDate returns the date value at the given column and row
func (r *Result) GetValueDate(column int64, row int32) (time.Time, bool) {
	// Check if we have the function available
	if r.Db.ValueDate == nil {
		return time.Time{}, false
	}

	date := r.Db.ValueDate(&r.Raw, column, row)
	// DuckDB uses a special value for NULL dates
	if date == 0 {
		return time.Time{}, false // NULL value
	}

	// Convert from days since 1970-01-01 to a time.Time
	return time.Unix(int64(date)*24*60*60, 0).UTC(), true
}

// GetValueTime returns the time value at the given column and row
func (r *Result) GetValueTime(column int64, row int32) (time.Time, bool) {
	// Check if we have the function available
	if r.Db.ValueTime == nil {
		return time.Time{}, false
	}

	timeVal := r.Db.ValueTime(&r.Raw, column, row)
	// DuckDB typically uses 0 for NULL times
	if timeVal == 0 {
		return time.Time{}, false
	}

	// Convert from microseconds since 00:00:00 to a time.Time on the current date
	now := time.Now().UTC()
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	return midnight.Add(time.Duration(timeVal) * time.Microsecond), true
}

// GetValueTimestamp returns the timestamp (datetime) value at the given column and row
func (r *Result) GetValueTimestamp(column int64, row int32) (time.Time, bool) {
	// Check if we have the function available
	if r.Db.ValueTimestamp == nil {
		return time.Time{}, false
	}

	timestamp := r.Db.ValueTimestamp(&r.Raw, column, row)
	// DuckDB typically uses 0 for NULL timestamps
	if timestamp == 0 {
		return time.Time{}, false
	}

	// Convert from microseconds since epoch to time.Time
	seconds := timestamp / 1_000_000
	remainingMicros := timestamp % 1_000_000
	return time.Unix(seconds, remainingMicros*1000).UTC(), true
}

// GetValueBoolean returns the boolean value at the given column and row
func (r *Result) GetValueBoolean(column int64, row int32) (bool, bool) {
	// Check if we have the function available
	if r.Db.ValueBoolean == nil {
		return false, false
	}

	// First check if value is NULL
	if r.Db.ValueNull != nil && r.Db.ValueNull(&r.Raw, column, row) {
		return false, false
	}

	return r.Db.ValueBoolean(&r.Raw, column, row), true
}

// GetValueInt8 returns the int8 value at the given column and row
func (r *Result) GetValueInt8(column int64, row int32) (int8, bool) {
	// Check if we have the function available
	if r.Db.ValueInt8 == nil {
		return 0, false
	}

	// First check if value is NULL
	if r.Db.ValueNull != nil && r.Db.ValueNull(&r.Raw, column, row) {
		return 0, false
	}

	return r.Db.ValueInt8(&r.Raw, column, row), true
}

// GetValueInt16 returns the int16 value at the given column and row
func (r *Result) GetValueInt16(column int64, row int32) (int16, bool) {
	// Check if we have the function available
	if r.Db.ValueInt16 == nil {
		return 0, false
	}

	// First check if value is NULL
	if r.Db.ValueNull != nil && r.Db.ValueNull(&r.Raw, column, row) {
		return 0, false
	}

	return r.Db.ValueInt16(&r.Raw, column, row), true
}

// GetValueInt32 returns the int32 value at the given column and row
func (r *Result) GetValueInt32(column int64, row int32) (int32, bool) {
	// Check if we have the function available
	if r.Db.ValueInt32 == nil {
		return 0, false
	}

	// First check if value is NULL
	if r.Db.ValueNull != nil && r.Db.ValueNull(&r.Raw, column, row) {
		return 0, false
	}

	return r.Db.ValueInt32(&r.Raw, column, row), true
}

// GetValueInt64 returns the int64 value at the given column and row
func (r *Result) GetValueInt64(column int64, row int32) (int64, bool) {
	// Check if we have the function available
	if r.Db.ValueInt64 == nil {
		return 0, false
	}

	// First check if value is NULL
	if r.Db.ValueNull != nil && r.Db.ValueNull(&r.Raw, column, row) {
		return 0, false
	}

	return r.Db.ValueInt64(&r.Raw, column, row), true
}

// GetValueUint8 returns the uint8 value at the given column and row
func (r *Result) GetValueUint8(column int64, row int32) (uint8, bool) {
	// Check if we have the function available
	if r.Db.ValueUint8 == nil {
		return 0, false
	}

	// First check if value is NULL
	if r.Db.ValueNull != nil && r.Db.ValueNull(&r.Raw, column, row) {
		return 0, false
	}

	return r.Db.ValueUint8(&r.Raw, column, row), true
}

// GetValueUint16 returns the uint16 value at the given column and row
func (r *Result) GetValueUint16(column int64, row int32) (uint16, bool) {
	// Check if we have the function available
	if r.Db.ValueUint16 == nil {
		return 0, false
	}

	// First check if value is NULL
	if r.Db.ValueNull != nil && r.Db.ValueNull(&r.Raw, column, row) {
		return 0, false
	}

	return r.Db.ValueUint16(&r.Raw, column, row), true
}

// GetValueUint32 returns the uint32 value at the given column and row
func (r *Result) GetValueUint32(column int64, row int32) (uint32, bool) {
	// Check if we have the function available
	if r.Db.ValueUint32 == nil {
		return 0, false
	}

	// First check if value is NULL
	if r.Db.ValueNull != nil && r.Db.ValueNull(&r.Raw, column, row) {
		return 0, false
	}

	return r.Db.ValueUint32(&r.Raw, column, row), true
}

// GetValueUint64 returns the uint64 value at the given column and row
func (r *Result) GetValueUint64(column int64, row int32) (uint64, bool) {
	// Check if we have the function available
	if r.Db.ValueUint64 == nil {
		return 0, false
	}

	// First check if value is NULL
	if r.Db.ValueNull != nil && r.Db.ValueNull(&r.Raw, column, row) {
		return 0, false
	}

	return r.Db.ValueUint64(&r.Raw, column, row), true
}

// GetValueFloat returns the float32 value at the given column and row
func (r *Result) GetValueFloat(column int64, row int32) (float32, bool) {
	// Check if we have the function available
	if r.Db.ValueFloat == nil {
		return 0, false
	}

	// First check if value is NULL
	if r.Db.ValueNull != nil && r.Db.ValueNull(&r.Raw, column, row) {
		return 0, false
	}

	return r.Db.ValueFloat(&r.Raw, column, row), true
}

// GetValueDouble returns the float64 value at the given column and row
func (r *Result) GetValueDouble(column int64, row int32) (float64, bool) {
	// Check if we have the function available
	if r.Db.ValueDouble == nil {
		return 0, false
	}

	// First check if value is NULL
	if r.Db.ValueNull != nil && r.Db.ValueNull(&r.Raw, column, row) {
		return 0, false
	}

	return r.Db.ValueDouble(&r.Raw, column, row), true
}

// IsNull returns true if the value at the given column and row is NULL
func (r *Result) IsNull(column int64, row int32) bool {
	if r.Db.ValueNull == nil {
		// Fall back to checking if string value is nil
		return r.Db.ValueString(&r.Raw, column, row) == nil
	}
	return r.Db.ValueNull(&r.Raw, column, row)
}

// Close destroys the result and frees associated resources
func (r *Result) Close() {
	r.Db.DestroyResult(&r.Raw)
}
