package convert

import (
	"math"
	"testing"
	"time"
)

func TestToBoolean(t *testing.T) {
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
			got, err := ToBoolean(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToBoolean() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("ToBoolean() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestToInt8(t *testing.T) {
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
			got, err := ToInt8(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToInt8() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected && !tt.wantErr {
				t.Errorf("ToInt8() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestToString(t *testing.T) {
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
			got, err := ToString(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("ToString() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestToDate(t *testing.T) {
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
			got, err := ToDate(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToDate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got.Days != tt.expected.Days {
				t.Errorf("ToDate() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestToTime(t *testing.T) {
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
			got, err := ToTime(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToTime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got.Micros != tt.expected.Micros {
				t.Errorf("ToTime() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestToTimestamp(t *testing.T) {
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
			got, err := ToTimestamp(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToTimestamp() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !got.Equal(tt.expected) {
				t.Errorf("ToTimestamp() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestToInt16(t *testing.T) {
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
			got, err := ToInt16(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToInt16() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected && !tt.wantErr {
				t.Errorf("ToInt16() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestToInt32(t *testing.T) {
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
			got, err := ToInt32(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToInt32() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected && !tt.wantErr {
				t.Errorf("ToInt32() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestToInt64(t *testing.T) {
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
			got, err := ToInt64(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToInt64() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected && !tt.wantErr {
				t.Errorf("ToInt64() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestToUint8(t *testing.T) {
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
			got, err := ToUint8(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToUint8() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected && !tt.wantErr {
				t.Errorf("ToUint8() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestToFloat32(t *testing.T) {
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
			got, err := ToFloat32(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToFloat32() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Use approximate comparison for floating point
			if !tt.wantErr && math.Abs(float64(got-tt.expected)) > 0.00001 {
				t.Errorf("ToFloat32() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestToFloat64(t *testing.T) {
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
			got, err := ToFloat64(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToFloat64() error = %v, wantErr %v", err, tt.wantErr)
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
				t.Errorf("ToFloat64() = %v, want %v", got, tt.expected)
			}
		})
	}
}