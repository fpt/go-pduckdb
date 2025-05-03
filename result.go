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
func (r *DuckDBResult) ColumnCount() int32 {
	return r.internal.GetColumnCount()
}

// RowCount returns the number of rows in the result
func (r *DuckDBResult) RowCount() int64 {
	return r.internal.GetRowCount()
}

// ColumnName returns the name of the column at the given index
func (r *DuckDBResult) ColumnName(column int32) string {
	return r.internal.GetColumnName(column)
}

// ValueString returns the string value at the given row and column
func (r *DuckDBResult) ValueString(column int64, row int32) (string, bool) {
	return r.internal.GetValueString(column, row)
}

// ValueDate returns the date value at the given column and row
func (r *DuckDBResult) ValueDate(column int64, row int32) (time.Time, bool) {
	return r.internal.GetValueDate(column, row)
}

// ValueTime returns the time value at the given column and row
func (r *DuckDBResult) ValueTime(column int64, row int32) (time.Time, bool) {
	return r.internal.GetValueTime(column, row)
}

// ValueTimestamp returns the timestamp (datetime) value at the given column and row
func (r *DuckDBResult) ValueTimestamp(column int64, row int32) (time.Time, bool) {
	return r.internal.GetValueTimestamp(column, row)
}

// Close destroys the result and frees associated resources
func (r *DuckDBResult) Close() {
	r.internal.Close()
}
