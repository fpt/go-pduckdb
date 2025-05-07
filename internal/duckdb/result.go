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

// ColumnCount returns the number of columns in the result
func (r *Result) ColumnCount() int64 {
	return r.Db.ColumnCount(&r.Raw)
}

// RowCount returns the number of rows in the result
func (r *Result) RowCount() int64 {
	return r.Db.RowCount(&r.Raw)
}

func (r *Result) RowsChanged() int64 {
	return r.Db.RowsChanged(&r.Raw)
}

// ColumnName returns the name of the column at the given index
func (r *Result) ColumnName(column int64) string {
	ptr := r.Db.ColumnName(&r.Raw, column)
	return GoString(ptr)
}

func (r *Result) ColumnNames() []string {
	names := make([]string, r.ColumnCount())
	for i := int64(0); i < r.ColumnCount(); i++ {
		names[i] = r.ColumnName(i)
	}
	return names
}

// ColumnType returns the type of the column at the given index
func (r *Result) ColumnType(column int64) DuckDBType {
	typ := r.Db.ColumnType(&r.Raw, column)
	return typ
}

// ColumnLogicalType returns the logical type of the column at the given index
func (r *Result) ColumnLogicalType(column int64) DuckDBLogicalType {
	typ := r.Db.ColumnLogicalType(&r.Raw, column)
	return typ
}

// ValueString returns the string value at the given row and column
// NOTE: This is a wrapper around ValueVarchar to avoid purego limitations.
func (r *Result) ValueString(column int64, row int32) (string, bool) {
	ptr := r.Db.ValueVarchar(&r.Raw, column, row)
	if ptr == nil {
		return "", false // NULL value
	}
	return GoString(ptr), true
}

func (r *Result) ValueVarchar(column int64, row int32) ([]byte, bool) {
	ptr := r.Db.ValueVarchar(&r.Raw, column, row)
	if ptr == nil {
		return nil, false // NULL value
	}

	return GoBytes(ptr), true
}

// ValueDate returns the date value at the given column and row
func (r *Result) ValueDate(column int64, row int32) (time.Time, bool) {
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

// ValueTime returns the time value at the given column and row
func (r *Result) ValueTime(column int64, row int32) (time.Time, bool) {
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

// ValueTimestamp returns the timestamp (datetime) value at the given column and row
func (r *Result) ValueTimestamp(column int64, row int32) (time.Time, bool) {
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

// ValueBoolean returns the boolean value at the given column and row
func (r *Result) ValueBoolean(column int64, row int32) (bool, bool) {
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

// ValueInt8 returns the int8 value at the given column and row
func (r *Result) ValueInt8(column int64, row int32) (int8, bool) {
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

// ValueInt16 returns the int16 value at the given column and row
func (r *Result) ValueInt16(column int64, row int32) (int16, bool) {
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

// ValueInt32 returns the int32 value at the given column and row
func (r *Result) ValueInt32(column int64, row int32) (int32, bool) {
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

// ValueInt64 returns the int64 value at the given column and row
func (r *Result) ValueInt64(column int64, row int32) (int64, bool) {
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

// ValueUint8 returns the uint8 value at the given column and row
func (r *Result) ValueUint8(column int64, row int32) (uint8, bool) {
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

// ValueUint16 returns the uint16 value at the given column and row
func (r *Result) ValueUint16(column int64, row int32) (uint16, bool) {
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

// ValueUint32 returns the uint32 value at the given column and row
func (r *Result) ValueUint32(column int64, row int32) (uint32, bool) {
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

// ValueUint64 returns the uint64 value at the given column and row
func (r *Result) ValueUint64(column int64, row int32) (uint64, bool) {
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

// ValueFloat returns the float32 value at the given column and row
func (r *Result) ValueFloat(column int64, row int32) (float32, bool) {
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

// ValueDouble returns the float64 value at the given column and row
func (r *Result) ValueDouble(column int64, row int32) (float64, bool) {
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
func (r *Result) ValueNull(column int64, row int32) bool {
	if r.Db.ValueNull == nil {
		// Fall back to checking if varchar value is nil
		return r.Db.ValueVarchar(&r.Raw, column, row) == nil
	}
	return r.Db.ValueNull(&r.Raw, column, row)
}

// DecimalInfo returns the precision and scale for decimal types
func (r *Result) DecimalInfo(column int64) (precision, scale int64, ok bool) {
	// Get the column type
	colType := r.ColumnType(column)

	// Check if it's a decimal type
	if colType != DuckDBTypeDecimal {
		return 0, 0, false
	}

	// Get the logical type
	logicalType := r.ColumnLogicalType(column)

	// If we don't have a logical type, we can't get precision and scale
	if logicalType == nil {
		return 0, 0, false
	}

	// Get precision and scale from the logical type
	if r.Db.DecimalWidth != nil && r.Db.DecimalScale != nil {
		precision := int64(r.Db.DecimalWidth(logicalType))
		scale := int64(r.Db.DecimalScale(logicalType))
		return precision, scale, true
	}

	return 0, 0, false
}

// Close destroys the result and frees associated resources
func (r *Result) Close() {
	r.Db.DestroyResult(&r.Raw)
}
