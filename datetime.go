package pduckdb

import (
	"time"
)

// DuckDBDate represents a date value in DuckDB
// In DuckDB, dates are stored as days since 1970-01-01
type DuckDBDate int32

// ToTime converts a DuckDBDate to a Go time.Time
func (d DuckDBDate) ToTime() time.Time {
	// Convert from days since 1970-01-01 to a time.Time
	return time.Unix(int64(d)*24*60*60, 0).UTC()
}

// DuckDBTime represents a time value in DuckDB
// In DuckDB, times are stored as microseconds since 00:00:00
type DuckDBTime int64

// ToDuration converts a DuckDBTime to a Go time.Duration
func (t DuckDBTime) ToDuration() time.Duration {
	return time.Duration(t) * time.Microsecond
}

// ToTime converts a DuckDBTime to a Go time.Time (on the current date)
func (t DuckDBTime) ToTime() time.Time {
	now := time.Now().UTC()
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	return midnight.Add(t.ToDuration())
}

// DuckDBTimestamp represents a timestamp value in DuckDB
// In DuckDB, timestamps are stored as microseconds since 1970-01-01 00:00:00
type DuckDBTimestamp int64

// ToTime converts a DuckDBTimestamp to a Go time.Time
func (ts DuckDBTimestamp) ToTime() time.Time {
	// Convert from microseconds since epoch to time.Time
	seconds := int64(ts) / 1_000_000
	remainingMicros := int64(ts) % 1_000_000
	return time.Unix(seconds, remainingMicros*1000).UTC()
}
