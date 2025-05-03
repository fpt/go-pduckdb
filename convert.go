package pduckdb

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Conversion helper functions

func convertToBoolean(value any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		// Convert numeric types to boolean (0 = false, non-zero = true)
		return v != 0, nil
	case string:
		// Accept "true"/"false" or "1"/"0"
		v = strings.ToLower(strings.TrimSpace(v))
		return v == "true" || v == "1" || v == "t" || v == "yes" || v == "y", nil
	default:
		return false, fmt.Errorf("cannot convert %T to boolean", value)
	}
}

func convertToInt8(value any) (int8, error) {
	switch v := value.(type) {
	case int8:
		return v, nil
	case int16:
		if v < math.MinInt8 || v > math.MaxInt8 {
			return 0, fmt.Errorf("value %d out of range for int8", v)
		}
		return int8(v), nil
	case int32:
		if v < math.MinInt8 || v > math.MaxInt8 {
			return 0, fmt.Errorf("value %d out of range for int8", v)
		}
		return int8(v), nil
	case int64:
		if v < math.MinInt8 || v > math.MaxInt8 {
			return 0, fmt.Errorf("value %d out of range for int8", v)
		}
		return int8(v), nil
	case int:
		if v < math.MinInt8 || v > math.MaxInt8 {
			return 0, fmt.Errorf("value %d out of range for int8", v)
		}
		return int8(v), nil
	case uint8:
		if v > math.MaxInt8 {
			return 0, fmt.Errorf("value %d out of range for int8", v)
		}
		return int8(v), nil
	case uint16, uint32, uint64, uint:
		// These would require a runtime check
		if v.(uint64) > uint64(math.MaxInt8) {
			return 0, fmt.Errorf("value %d out of range for int8", v)
		}
		return int8(v.(uint64)), nil
	case float32:
		if v < math.MinInt8 || v > math.MaxInt8 || float32(int8(v)) != v {
			return 0, fmt.Errorf("value %f cannot be exactly represented as int8", v)
		}
		return int8(v), nil
	case float64:
		if v < math.MinInt8 || v > math.MaxInt8 || float64(int8(v)) != v {
			return 0, fmt.Errorf("value %f cannot be exactly represented as int8", v)
		}
		return int8(v), nil
	case string:
		i, err := strconv.ParseInt(v, 10, 8)
		if err != nil {
			return 0, fmt.Errorf("cannot convert string '%s' to int8: %v", v, err)
		}
		return int8(i), nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int8", value)
	}
}

// Similar conversion functions for other types (int16, int32, int64, uint8, etc.)
// The pattern would be the same, just with different range checks

func convertToInt16(value any) (int16, error) {
	// Similar to convertToInt8 but with int16 range checks
	// ...implementation...
	return 0, fmt.Errorf("not implemented")
}

func convertToInt32(value any) (int32, error) {
	switch v := value.(type) {
	case int32:
		return v, nil
	case int8:
		return int32(v), nil
	case int16:
		return int32(v), nil
	case int64:
		if v < math.MinInt32 || v > math.MaxInt32 {
			return 0, fmt.Errorf("value %d out of range for int32", v)
		}
		return int32(v), nil
	case int:
		if int64(v) < math.MinInt32 || int64(v) > math.MaxInt32 {
			return 0, fmt.Errorf("value %d out of range for int32", v)
		}
		return int32(v), nil
	case uint8:
		return int32(v), nil
	case uint16:
		return int32(v), nil
	case uint32:
		if v > uint32(math.MaxInt32) {
			return 0, fmt.Errorf("value %d out of range for int32", v)
		}
		return int32(v), nil
	case uint64:
		if v > uint64(math.MaxInt32) {
			return 0, fmt.Errorf("value %d out of range for int32", v)
		}
		return int32(v), nil
	case uint:
		if uint64(v) > uint64(math.MaxInt32) {
			return 0, fmt.Errorf("value %d out of range for int32", v)
		}
		return int32(v), nil
	case float32:
		if v < float32(math.MinInt32) || v > float32(math.MaxInt32) || float32(int32(v)) != v {
			return 0, fmt.Errorf("value %f cannot be exactly represented as int32", v)
		}
		return int32(v), nil
	case float64:
		if v < math.MinInt32 || v > math.MaxInt32 || float64(int32(v)) != v {
			return 0, fmt.Errorf("value %f cannot be exactly represented as int32", v)
		}
		return int32(v), nil
	case string:
		i, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return 0, fmt.Errorf("cannot convert string '%s' to int32: %v", v, err)
		}
		return int32(i), nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int32", value)
	}
}

func convertToInt64(value any) (int64, error) {
	// Similar to convertToInt8 but with int64 range checks
	// ...implementation...
	return 0, fmt.Errorf("not implemented")
}

func convertToUint8(value any) (uint8, error) {
	// Similar to convertToInt8 but with uint8 range checks
	// ...implementation...
	return 0, fmt.Errorf("not implemented")
}

func convertToUint16(value any) (uint16, error) {
	// Similar to convertToInt8 but with uint16 range checks
	// ...implementation...
	return 0, fmt.Errorf("not implemented")
}

func convertToUint32(value any) (uint32, error) {
	// Similar to convertToInt8 but with uint32 range checks
	// ...implementation...
	return 0, fmt.Errorf("not implemented")
}

func convertToUint64(value any) (uint64, error) {
	// Similar to convertToInt8 but with uint64 range checks
	// ...implementation...
	return 0, fmt.Errorf("not implemented")
}

func convertToFloat32(value any) (float32, error) {
	// ...implementation...
	return 0, fmt.Errorf("not implemented")
}

func convertToFloat64(value any) (float64, error) {
	// ...implementation...
	return 0, fmt.Errorf("not implemented")
}

func convertToString(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case fmt.Stringer:
		return v.String(), nil
	default:
		// Use fmt.Sprint as a last resort
		return fmt.Sprint(value), nil
	}
}

func convertToBlob(value any) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("cannot convert %T to []byte", value)
	}
}

func convertToDate(value any) (Date, error) {
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

func convertToTime(value any) (Time, error) {
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

func convertToTimestamp(value any) (time.Time, error) {
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

func convertToInterval(value any) (Interval, error) {
	switch v := value.(type) {
	case Interval:
		return v, nil
	case string:
		// Very simplified interval parsing - would need more complete implementation
		var months, days int32
		var micros int64

		_, err := fmt.Sscanf(v, "%d months %d days %d microseconds", &months, &days, &micros)
		if err != nil {
			return Interval{}, fmt.Errorf("cannot parse string '%s' as interval: %v", v, err)
		}

		return Interval{
			Months: months,
			Days:   days,
			Micros: micros,
		}, nil
	default:
		return Interval{}, fmt.Errorf("cannot convert %T to Interval", value)
	}
}
