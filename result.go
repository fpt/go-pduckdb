package pduckdb

import (
	"time"

	"github.com/fpt/go-pduckdb/internal/duckdb"
)

// DuckDBResult wraps the internal result and provides methods to access the data
type DuckDBResult struct {
	internal *duckdb.Result
}

// ColumnCount returns the number of columns in the result
func (r *DuckDBResult) ColumnCount() int64 {
	return r.internal.ColumnCount()
}

// RowCount returns the number of rows in the result
func (r *DuckDBResult) RowCount() int64 {
	return r.internal.RowCount()
}

// RowsChanged returns the number of rows changed by the last operation
func (r *DuckDBResult) RowsChanged() int64 {
	return r.internal.RowsChanged()
}

// ColumnName returns the name of the column at the given index
func (r *DuckDBResult) ColumnName(column int64) string {
	return r.internal.ColumnName(column)
}

func (r *DuckDBResult) ColumnNames() []string {
	names := make([]string, r.ColumnCount())
	for i := int64(0); i < r.ColumnCount(); i++ {
		names[i] = r.ColumnName(i)
	}
	return names
}

// ColumnType returns the logical type of the column at the given index
func (r *DuckDBResult) ColumnType(column int64) duckdb.DuckDBType {
	return r.internal.ColumnType(column)
}

// ColumnType returns the logical type of the column at the given index
func (r *DuckDBResult) ColumnLogicalType(column int64) duckdb.DuckDBLogicalType {
	return r.internal.ColumnLogicalType(column)
}

// ValueString returns the string value at the given row and column
func (r *DuckDBResult) ValueString(column int64, row int32) (string, bool) {
	return r.internal.ValueString(column, row)
}

// ValueDate returns the date value at the given column and row
func (r *DuckDBResult) ValueDate(column int64, row int32) (time.Time, bool) {
	return r.internal.ValueDate(column, row)
}

// ValueTime returns the time value at the given column and row
func (r *DuckDBResult) ValueTime(column int64, row int32) (time.Time, bool) {
	return r.internal.ValueTime(column, row)
}

// ValueTimestamp returns the timestamp (datetime) value at the given column and row
func (r *DuckDBResult) ValueTimestamp(column int64, row int32) (time.Time, bool) {
	return r.internal.ValueTimestamp(column, row)
}

// ValueBoolean returns the boolean value at the given column and row
func (r *DuckDBResult) ValueBoolean(column int64, row int32) (bool, bool) {
	return r.internal.ValueBoolean(column, row)
}

// ValueDouble returns the double (float64) value at the given column and row
func (r *DuckDBResult) ValueDouble(column int64, row int32) (float64, bool) {
	return r.internal.ValueDouble(column, row)
}

// ValueNull checks if the value at the given column and row is NULL
func (r *DuckDBResult) ValueNull(column int64, row int32) bool {
	return r.internal.ValueNull(column, row)
}

// DecimalInfo returns the precision and scale for decimal types
func (r *DuckDBResult) DecimalInfo(column int64) (precision, scale int64, ok bool) {
	// Get the column type
	colType := r.ColumnType(column)

	// Check if it's a decimal type
	if colType != duckdb.DuckDBTypeDecimal {
		return 0, 0, false
	}

	// Get the logical type
	logicalType := r.ColumnLogicalType(column)

	// If we don't have a logical type, we can't get precision and scale
	if logicalType == nil {
		return 0, 0, false
	}

	// Get precision and scale from the logical type
	if r.internal.Db.DecimalWidth != nil && r.internal.Db.DecimalScale != nil {
		precision := int64(r.internal.Db.DecimalWidth(logicalType))
		scale := int64(r.internal.Db.DecimalScale(logicalType))
		return precision, scale, true
	}

	return 0, 0, false
}

// Close destroys the result and frees associated resources
func (r *DuckDBResult) Close() {
	r.internal.Close()
}
