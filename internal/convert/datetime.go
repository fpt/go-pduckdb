package convert

import (
	"fmt"
	"time"
)

// ToDate converts a value to a Date
func ToDate(value any) (Date, error) {
	switch v := value.(type) {
	case Date:
		return v, nil
	case DuckDBDate:
		return Date{Days: int32(v)}, nil
	case time.Time:
		// Convert to UTC first to ensure consistent behavior
		utc := v.UTC()
		// Get the date part only (zero out the time component)
		date := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
		// Calculate days since epoch (1970-01-01)
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		days := int32(date.Sub(epoch).Hours() / 24)
		return Date{Days: days}, nil
	case string:
		// Try to parse as ISO date
		t, err := time.Parse("2006-01-02", v)
		if err != nil {
			return Date{}, fmt.Errorf("cannot parse string '%s' as date: %v", v, err)
		}
		// Convert to UTC first to ensure consistent behavior
		utc := t.UTC()
		// Calculate days since epoch (1970-01-01)
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		days := int32(utc.Sub(epoch).Hours() / 24)
		return Date{Days: days}, nil
	default:
		return Date{}, fmt.Errorf("cannot convert %T to Date", value)
	}
}

// ToTime converts a value to a Time
func ToTime(value any) (Time, error) {
	switch v := value.(type) {
	case Time:
		return v, nil
	case DuckDBTime:
		return Time{Micros: int64(v)}, nil
	case time.Time:
		// Get just the time portion of the time.Time
		utc := v.UTC()
		hours, minutes, seconds := utc.Clock()
		nanos := utc.Nanosecond()

		// Calculate total microseconds
		totalMicros := int64(hours) * 3600 * 1000000
		totalMicros += int64(minutes) * 60 * 1000000
		totalMicros += int64(seconds) * 1000000
		totalMicros += int64(nanos) / 1000

		return Time{Micros: totalMicros}, nil
	case string:
		// Try to parse as ISO time
		t, err := time.Parse("15:04:05.999999", v)
		if err != nil {
			return Time{}, fmt.Errorf("cannot parse string '%s' as time: %v", v, err)
		}

		// Extract time components
		hours, minutes, seconds := t.Clock()
		nanos := t.Nanosecond()

		// Calculate total microseconds
		totalMicros := int64(hours) * 3600 * 1000000
		totalMicros += int64(minutes) * 60 * 1000000
		totalMicros += int64(seconds) * 1000000
		totalMicros += int64(nanos) / 1000

		return Time{Micros: totalMicros}, nil
	default:
		return Time{}, fmt.Errorf("cannot convert %T to Time", value)
	}
}

// ToTimestamp converts a value to a time.Time
func ToTimestamp(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		// Try several common timestamp formats
		formats := []string{
			"2006-01-02 15:04:05.999999",
			"2006-01-02T15:04:05.999999",
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02",
		}

		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("cannot parse string '%s' as timestamp", v)
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", value)
	}
}
