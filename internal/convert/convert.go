// Package convert provides internal conversion functions for the pduckdb driver
package convert

import (
	"time"
)

// Import types from the main package
type Date struct {
	Days int32
}

// ToTime converts a DuckDB Date to a Go time.Time
func (d Date) ToTime() time.Time {
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	return epoch.AddDate(0, 0, int(d.Days))
}

type Time struct {
	Micros int64
}

// ToTime converts a DuckDB Time to a Go time.Time
func (t Time) ToTime() time.Time {
	return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).
		Add(time.Duration(t.Micros) * time.Microsecond)
}

// For compatibility with DuckDB types
type DuckDBDate = int32
type DuckDBTime = int64
