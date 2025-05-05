// Package types
package types

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

// DateFromTime converts a Go time.Time to a DuckDBDate
func DateFromTime(t time.Time) DuckDBDate {
	// Convert to UTC first to ensure consistent behavior
	utc := t.UTC()
	// Get the date part only (zero out the time component)
	date := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
	// Calculate days since epoch (1970-01-01)
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	days := int32(date.Sub(epoch).Hours() / 24)
	return DuckDBDate(days)
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

// TimeFromTime converts a Go time.Time to a DuckDBTime
func TimeFromTime(t time.Time) DuckDBTime {
	// Get just the time portion of the time.Time
	utc := t.UTC()
	hours, minutes, seconds := utc.Clock()
	nanos := utc.Nanosecond()

	// Calculate total microseconds
	totalMicros := int64(hours) * 3600 * 1000000
	totalMicros += int64(minutes) * 60 * 1000000
	totalMicros += int64(seconds) * 1000000
	totalMicros += int64(nanos) / 1000

	return DuckDBTime(totalMicros)
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
