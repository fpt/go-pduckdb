package pduckdb

import (
	"math"
	"testing"
	"time"
)

func TestConvertToBoolean(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected bool
		wantErr  bool
	}{
		{
			name:     "convert bool true",
			input:    true,
			expected: true,
			wantErr:  false,
		},
		{
			name:     "convert bool false",
			input:    false,
			expected: false,
			wantErr:  false,
		},
		{
			name:     "convert int 1",
			input:    1,
			expected: true,
			wantErr:  false,
		},
		{
			name:     "convert int 0",
			input:    0,
			expected: false,
			wantErr:  false,
		},
		{
			name:     "convert string 'true'",
			input:    "true",
			expected: true,
			wantErr:  false,
		},
		{
			name:     "convert string 'false'",
			input:    "false",
			expected: false,
			wantErr:  false,
		},
		{
			name:     "convert string 'yes'",
			input:    "yes",
			expected: true,
			wantErr:  false,
		},
		{
			name:     "convert string '1'",
			input:    "1",
			expected: true,
			wantErr:  false,
		},
		{
			name:     "convert unsupported type",
			input:    []string{"not", "a", "boolean"},
			expected: false,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertToBoolean(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertToBoolean() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("convertToBoolean() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConvertToInt8(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected int8
		wantErr  bool
	}{
		{
			name:     "convert int8",
			input:    int8(42),
			expected: 42,
			wantErr:  false,
		},
		{
			name:     "convert int16 in range",
			input:    int16(100),
			expected: 100,
			wantErr:  false,
		},
		{
			name:     "convert int16 out of range",
			input:    int16(1000),
			expected: 0,
			wantErr:  true,
		},
		{
			name:     "convert string valid",
			input:    "123",
			expected: 123,
			wantErr:  false,
		},
		{
			name:     "convert string invalid",
			input:    "not a number",
			expected: 0,
			wantErr:  true,
		},
		{
			name:     "convert bool true",
			input:    true,
			expected: 1,
			wantErr:  false,
		},
		{
			name:     "convert bool false",
			input:    false,
			expected: 0,
			wantErr:  false,
		},
		{
			name:     "convert unsupported type",
			input:    []int{1, 2, 3},
			expected: 0,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertToInt8(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertToInt8() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected && !tt.wantErr {
				t.Errorf("convertToInt8() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConvertToString(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected string
		wantErr  bool
	}{
		{
			name:     "convert string",
			input:    "hello world",
			expected: "hello world",
			wantErr:  false,
		},
		{
			name:     "convert []byte",
			input:    []byte("hello bytes"),
			expected: "hello bytes",
			wantErr:  false,
		},
		{
			name:     "convert integer",
			input:    42,
			expected: "42",
			wantErr:  false,
		},
		{
			name:     "convert boolean",
			input:    true,
			expected: "true",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertToString(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertToString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("convertToString() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConvertToDate(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected Date
		wantErr  bool
	}{
		{
			name:     "convert Date",
			input:    Date{Days: 10},
			expected: Date{Days: 10},
			wantErr:  false,
		},
		{
			name:     "convert DuckDBDate",
			input:    DuckDBDate(20),
			expected: Date{Days: 20},
			wantErr:  false,
		},
		{
			name:  "convert time.Time",
			input: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			// 2023-01-01 is 19358 days since 1970-01-01
			expected: Date{Days: 19358},
			wantErr:  false,
		},
		{
			name:     "convert string valid",
			input:    "2023-01-01",
			expected: Date{Days: 19358},
			wantErr:  false,
		},
		{
			name:     "convert string invalid",
			input:    "not a date",
			expected: Date{},
			wantErr:  true,
		},
		{
			name:     "convert unsupported type",
			input:    42,
			expected: Date{},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertToDate(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertToDate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got.Days != tt.expected.Days {
				t.Errorf("convertToDate() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConvertToTime(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected Time
		wantErr  bool
	}{
		{
			name:     "convert Time",
			input:    Time{Micros: 3600000000}, // 1 hour
			expected: Time{Micros: 3600000000},
			wantErr:  false,
		},
		{
			name:     "convert DuckDBTime",
			input:    DuckDBTime(7200000000), // 2 hours
			expected: Time{Micros: 7200000000},
			wantErr:  false,
		},
		{
			name:  "convert time.Time",
			input: time.Date(2023, 1, 1, 12, 30, 45, 500000000, time.UTC),
			// 12:30:45.5 is 45045500000 microseconds since midnight
			expected: Time{Micros: 45045500000},
			wantErr:  false,
		},
		{
			name:     "convert string valid",
			input:    "12:30:45.5",
			expected: Time{Micros: 45045500000},
			wantErr:  false,
		},
		{
			name:     "convert string invalid",
			input:    "not a time",
			expected: Time{},
			wantErr:  true,
		},
		{
			name:     "convert unsupported type",
			input:    42,
			expected: Time{},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertToTime(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertToTime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got.Micros != tt.expected.Micros {
				t.Errorf("convertToTime() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConvertToTimestamp(t *testing.T) {
	refTime := time.Date(2023, 1, 1, 12, 30, 45, 500000000, time.UTC)

	tests := []struct {
		name     string
		input    any
		expected time.Time
		wantErr  bool
	}{
		{
			name:     "convert time.Time",
			input:    refTime,
			expected: refTime,
			wantErr:  false,
		},
		{
			name:     "convert string ISO format with T",
			input:    "2023-01-01T12:30:45.5",
			expected: refTime,
			wantErr:  false,
		},
		{
			name:     "convert string ISO format with space",
			input:    "2023-01-01 12:30:45.5",
			expected: refTime,
			wantErr:  false,
		},
		{
			name:     "convert string date only",
			input:    "2023-01-01",
			expected: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			wantErr:  false,
		},
		{
			name:     "convert string invalid",
			input:    "not a timestamp",
			expected: time.Time{},
			wantErr:  true,
		},
		{
			name:     "convert unsupported type",
			input:    42,
			expected: time.Time{},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertToTimestamp(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertToTimestamp() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !got.Equal(tt.expected) {
				t.Errorf("convertToTimestamp() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConvertToInterval(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected Interval
		wantErr  bool
	}{
		{
			name:     "convert Interval",
			input:    Interval{Months: 1, Days: 2, Micros: 3000000},
			expected: Interval{Months: 1, Days: 2, Micros: 3000000},
			wantErr:  false,
		},
		{
			name:     "convert string valid",
			input:    "5 months 6 days 7000000 microseconds",
			expected: Interval{Months: 5, Days: 6, Micros: 7000000},
			wantErr:  false,
		},
		{
			name:     "convert string invalid",
			input:    "not an interval",
			expected: Interval{},
			wantErr:  true,
		},
		{
			name:     "convert unsupported type",
			input:    42,
			expected: Interval{},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertToInterval(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertToInterval() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if got.Months != tt.expected.Months || got.Days != tt.expected.Days || got.Micros != tt.expected.Micros {
					t.Errorf("convertToInterval() = %v, want %v", got, tt.expected)
				}
			}
		})
	}
}

// Add dummy implementations of the incomplete conversion functions to avoid test failures
// These can be removed when the real implementations are added

func TestConvertToInt16(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected int16
		wantErr  bool
	}{
		{
			name:     "convert int16",
			input:    int16(32767),
			expected: 32767,
			wantErr:  false,
		},
		{
			name:     "convert int8 to int16",
			input:    int8(127),
			expected: 127,
			wantErr:  false,
		},
		{
			name:     "convert int32 in range",
			input:    int32(32767),
			expected: 32767,
			wantErr:  false,
		},
		{
			name:     "convert int32 out of range",
			input:    int32(32768),
			expected: 0,
			wantErr:  true,
		},
		{
			name:     "convert string valid",
			input:    "12345",
			expected: 12345,
			wantErr:  false,
		},
		{
			name:     "convert string out of range",
			input:    "100000",
			expected: 0,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertToInt16(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertToInt16() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected && !tt.wantErr {
				t.Errorf("convertToInt16() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConvertToInt32(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected int32
		wantErr  bool
	}{
		{
			name:     "convert int32",
			input:    int32(2147483647),
			expected: 2147483647,
			wantErr:  false,
		},
		{
			name:     "convert int16 to int32",
			input:    int16(32767),
			expected: 32767,
			wantErr:  false,
		},
		{
			name:     "convert int64 in range",
			input:    int64(2147483647),
			expected: 2147483647,
			wantErr:  false,
		},
		{
			name:     "convert int64 out of range",
			input:    int64(2147483648),
			expected: 0,
			wantErr:  true,
		},
		{
			name:     "convert string valid",
			input:    "1000000",
			expected: 1000000,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertToInt32(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertToInt32() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected && !tt.wantErr {
				t.Errorf("convertToInt32() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConvertToInt64(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected int64
		wantErr  bool
	}{
		{
			name:     "convert int64",
			input:    int64(9223372036854775807),
			expected: 9223372036854775807,
			wantErr:  false,
		},
		{
			name:     "convert int32 to int64",
			input:    int32(2147483647),
			expected: 2147483647,
			wantErr:  false,
		},
		{
			name:     "convert string valid",
			input:    "9223372036854775807",
			expected: 9223372036854775807,
			wantErr:  false,
		},
		{
			name:     "convert string out of range",
			input:    "9223372036854775808",
			expected: 0,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertToInt64(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertToInt64() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected && !tt.wantErr {
				t.Errorf("convertToInt64() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConvertToUint8(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected uint8
		wantErr  bool
	}{
		{
			name:     "convert uint8",
			input:    uint8(255),
			expected: 255,
			wantErr:  false,
		},
		{
			name:     "convert int8 in range",
			input:    int8(127),
			expected: 127,
			wantErr:  false,
		},
		{
			name:     "convert int8 negative",
			input:    int8(-1),
			expected: 0,
			wantErr:  true,
		},
		{
			name:     "convert uint16 in range",
			input:    uint16(255),
			expected: 255,
			wantErr:  false,
		},
		{
			name:     "convert uint16 out of range",
			input:    uint16(256),
			expected: 0,
			wantErr:  true,
		},
		{
			name:     "convert string valid",
			input:    "200",
			expected: 200,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertToUint8(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertToUint8() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected && !tt.wantErr {
				t.Errorf("convertToUint8() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConvertToFloat32(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected float32
		wantErr  bool
	}{
		{
			name:     "convert float32",
			input:    float32(3.14159),
			expected: 3.14159,
			wantErr:  false,
		},
		{
			name:     "convert float64 in range",
			input:    float64(3.14159),
			expected: 3.14159,
			wantErr:  false,
		},
		{
			name:     "convert int32",
			input:    int32(42),
			expected: 42.0,
			wantErr:  false,
		},
		{
			name:     "convert string valid",
			input:    "3.14159",
			expected: 3.14159,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertToFloat32(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertToFloat32() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Use approximate comparison for floating point
			if !tt.wantErr && math.Abs(float64(got-tt.expected)) > 0.00001 {
				t.Errorf("convertToFloat32() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConvertToFloat64(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected float64
		wantErr  bool
	}{
		{
			name:     "convert float64",
			input:    float64(3.141592653589793),
			expected: 3.141592653589793,
			wantErr:  false,
		},
		{
			name:     "convert float32",
			input:    float32(3.14159),
			expected: 3.14159,
			wantErr:  false,
		},
		{
			name:     "convert int64",
			input:    int64(42),
			expected: 42.0,
			wantErr:  false,
		},
		{
			name:     "convert string valid",
			input:    "3.141592653589793",
			expected: 3.141592653589793,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertToFloat64(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertToFloat64() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Use appropriate epsilon based on input type
			epsilon := 0.00000000001 // Default for float64
			if _, isFloat32 := tt.input.(float32); isFloat32 {
				// Use larger epsilon for float32->float64 conversions
				epsilon = 0.0000001
			}

			// Use approximate comparison for floating point
			if !tt.wantErr && math.Abs(got-tt.expected) > epsilon {
				t.Errorf("convertToFloat64() = %v, want %v", got, tt.expected)
			}
		})
	}
}
