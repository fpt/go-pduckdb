package duckdb

import (
	"time"
)

// Result wraps the raw result and provides methods to access the data
type Result struct {
	Raw            DuckDBResultRaw
	ColumnCount    func(*DuckDBResultRaw) int32
	RowCount       func(*DuckDBResultRaw) int64
	ColumnName     func(*DuckDBResultRaw, int32) *byte
	ValueString    func(*DuckDBResultRaw, int64, int32) *byte
	ValueDate      func(*DuckDBResultRaw, int64, int32) int32
	ValueTime      func(*DuckDBResultRaw, int64, int32) int64
	ValueTimestamp func(*DuckDBResultRaw, int64, int32) int64
	// Additional value functions
	ValueBoolean  func(*DuckDBResultRaw, int64, int32) bool
	ValueInt8     func(*DuckDBResultRaw, int64, int32) int8
	ValueInt16    func(*DuckDBResultRaw, int64, int32) int16
	ValueInt32    func(*DuckDBResultRaw, int64, int32) int32
	ValueInt64    func(*DuckDBResultRaw, int64, int32) int64
	ValueUint8    func(*DuckDBResultRaw, int64, int32) uint8
	ValueUint16   func(*DuckDBResultRaw, int64, int32) uint16
	ValueUint32   func(*DuckDBResultRaw, int64, int32) uint32
	ValueUint64   func(*DuckDBResultRaw, int64, int32) uint64
	ValueFloat    func(*DuckDBResultRaw, int64, int32) float32
	ValueDouble   func(*DuckDBResultRaw, int64, int32) float64
	ValueVarchar  func(*DuckDBResultRaw, int64, int32) *byte
	ValueNull     func(*DuckDBResultRaw, int64, int32) bool
	DestroyResult func(*DuckDBResultRaw)
}

// CreateResult creates a new Result from a database and raw result
func CreateResult(db *DB, raw DuckDBResultRaw) *Result {
	return &Result{
		Raw:            raw,
		ColumnCount:    db.ColumnCount,
		RowCount:       db.RowCount,
		ColumnName:     db.ColumnName,
		ValueString:    db.ValueString,
		ValueDate:      db.ValueDate,
		ValueTime:      db.ValueTime,
		ValueTimestamp: db.ValueTimestamp,
		// Additional value functions
		ValueBoolean:  db.ValueBoolean,
		ValueInt8:     db.ValueInt8,
		ValueInt16:    db.ValueInt16,
		ValueInt32:    db.ValueInt32,
		ValueInt64:    db.ValueInt64,
		ValueUint8:    db.ValueUint8,
		ValueUint16:   db.ValueUint16,
		ValueUint32:   db.ValueUint32,
		ValueUint64:   db.ValueUint64,
		ValueFloat:    db.ValueFloat,
		ValueDouble:   db.ValueDouble,
		ValueVarchar:  db.ValueVarchar,
		ValueNull:     db.ValueNull,
		DestroyResult: db.DestroyResult,
	}
}

// GetColumnCount returns the number of columns in the result
func (r *Result) GetColumnCount() int32 {
	return r.ColumnCount(&r.Raw)
}

// GetRowCount returns the number of rows in the result
func (r *Result) GetRowCount() int64 {
	return r.RowCount(&r.Raw)
}

// GetColumnName returns the name of the column at the given index
func (r *Result) GetColumnName(column int32) string {
	ptr := r.ColumnName(&r.Raw, column)
	return GoString(ptr)
}

// GetValueString returns the string value at the given row and column
func (r *Result) GetValueString(column int64, row int32) (string, bool) {
	ptr := r.ValueString(&r.Raw, column, row)
	if ptr == nil {
		return "", false // NULL value
	}
	return GoString(ptr), true
}

// GetValueDate returns the date value at the given column and row
func (r *Result) GetValueDate(column int64, row int32) (time.Time, bool) {
	// Check if we have the function available
	if r.ValueDate == nil {
		return time.Time{}, false
	}

	date := r.ValueDate(&r.Raw, column, row)
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
	if r.ValueTime == nil {
		return time.Time{}, false
	}

	timeVal := r.ValueTime(&r.Raw, column, row)
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
	if r.ValueTimestamp == nil {
		return time.Time{}, false
	}

	timestamp := r.ValueTimestamp(&r.Raw, column, row)
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
	if r.ValueBoolean == nil {
		return false, false
	}

	// First check if value is NULL
	if r.ValueNull != nil && r.ValueNull(&r.Raw, column, row) {
		return false, false
	}

	return r.ValueBoolean(&r.Raw, column, row), true
}

// GetValueInt8 returns the int8 value at the given column and row
func (r *Result) GetValueInt8(column int64, row int32) (int8, bool) {
	// Check if we have the function available
	if r.ValueInt8 == nil {
		return 0, false
	}

	// First check if value is NULL
	if r.ValueNull != nil && r.ValueNull(&r.Raw, column, row) {
		return 0, false
	}

	return r.ValueInt8(&r.Raw, column, row), true
}

// GetValueInt16 returns the int16 value at the given column and row
func (r *Result) GetValueInt16(column int64, row int32) (int16, bool) {
	// Check if we have the function available
	if r.ValueInt16 == nil {
		return 0, false
	}

	// First check if value is NULL
	if r.ValueNull != nil && r.ValueNull(&r.Raw, column, row) {
		return 0, false
	}

	return r.ValueInt16(&r.Raw, column, row), true
}

// GetValueInt32 returns the int32 value at the given column and row
func (r *Result) GetValueInt32(column int64, row int32) (int32, bool) {
	// Check if we have the function available
	if r.ValueInt32 == nil {
		return 0, false
	}

	// First check if value is NULL
	if r.ValueNull != nil && r.ValueNull(&r.Raw, column, row) {
		return 0, false
	}

	return r.ValueInt32(&r.Raw, column, row), true
}

// GetValueInt64 returns the int64 value at the given column and row
func (r *Result) GetValueInt64(column int64, row int32) (int64, bool) {
	// Check if we have the function available
	if r.ValueInt64 == nil {
		return 0, false
	}

	// First check if value is NULL
	if r.ValueNull != nil && r.ValueNull(&r.Raw, column, row) {
		return 0, false
	}

	return r.ValueInt64(&r.Raw, column, row), true
}

// GetValueUint8 returns the uint8 value at the given column and row
func (r *Result) GetValueUint8(column int64, row int32) (uint8, bool) {
	// Check if we have the function available
	if r.ValueUint8 == nil {
		return 0, false
	}

	// First check if value is NULL
	if r.ValueNull != nil && r.ValueNull(&r.Raw, column, row) {
		return 0, false
	}

	return r.ValueUint8(&r.Raw, column, row), true
}

// GetValueUint16 returns the uint16 value at the given column and row
func (r *Result) GetValueUint16(column int64, row int32) (uint16, bool) {
	// Check if we have the function available
	if r.ValueUint16 == nil {
		return 0, false
	}

	// First check if value is NULL
	if r.ValueNull != nil && r.ValueNull(&r.Raw, column, row) {
		return 0, false
	}

	return r.ValueUint16(&r.Raw, column, row), true
}

// GetValueUint32 returns the uint32 value at the given column and row
func (r *Result) GetValueUint32(column int64, row int32) (uint32, bool) {
	// Check if we have the function available
	if r.ValueUint32 == nil {
		return 0, false
	}

	// First check if value is NULL
	if r.ValueNull != nil && r.ValueNull(&r.Raw, column, row) {
		return 0, false
	}

	return r.ValueUint32(&r.Raw, column, row), true
}

// GetValueUint64 returns the uint64 value at the given column and row
func (r *Result) GetValueUint64(column int64, row int32) (uint64, bool) {
	// Check if we have the function available
	if r.ValueUint64 == nil {
		return 0, false
	}

	// First check if value is NULL
	if r.ValueNull != nil && r.ValueNull(&r.Raw, column, row) {
		return 0, false
	}

	return r.ValueUint64(&r.Raw, column, row), true
}

// GetValueFloat returns the float32 value at the given column and row
func (r *Result) GetValueFloat(column int64, row int32) (float32, bool) {
	// Check if we have the function available
	if r.ValueFloat == nil {
		return 0, false
	}

	// First check if value is NULL
	if r.ValueNull != nil && r.ValueNull(&r.Raw, column, row) {
		return 0, false
	}

	return r.ValueFloat(&r.Raw, column, row), true
}

// GetValueDouble returns the float64 value at the given column and row
func (r *Result) GetValueDouble(column int64, row int32) (float64, bool) {
	// Check if we have the function available
	if r.ValueDouble == nil {
		return 0, false
	}

	// First check if value is NULL
	if r.ValueNull != nil && r.ValueNull(&r.Raw, column, row) {
		return 0, false
	}

	return r.ValueDouble(&r.Raw, column, row), true
}

// IsNull returns true if the value at the given column and row is NULL
func (r *Result) IsNull(column int64, row int32) bool {
	if r.ValueNull == nil {
		// Fall back to checking if string value is nil
		return r.ValueString(&r.Raw, column, row) == nil
	}
	return r.ValueNull(&r.Raw, column, row)
}

// Close destroys the result and frees associated resources
func (r *Result) Close() {
	r.DestroyResult(&r.Raw)
}
