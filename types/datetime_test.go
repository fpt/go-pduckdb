package types

import (
	"testing"
	"time"
)

func TestDuckDBDate_ToTime(t *testing.T) {
	tests := []struct {
		name     string
		date     DuckDBDate
		expected time.Time
	}{
		{
			name:     "Unix epoch",
			date:     0, // 1970-01-01
			expected: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Positive days since epoch",
			date:     365, // 1971-01-01
			expected: time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Negative days since epoch",
			date:     -365, // 1969-01-01
			expected: time.Date(1969, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.date.ToTime()
			if !result.Equal(tt.expected) {
				t.Errorf("DuckDBDate.ToTime() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestDuckDBTime_ToDuration(t *testing.T) {
	tests := []struct {
		name     string
		time     DuckDBTime
		expected time.Duration
	}{
		{
			name:     "Midnight",
			time:     0,
			expected: 0,
		},
		{
			name:     "One hour",
			time:     3600 * 1000000, // 1 hour in microseconds
			expected: time.Hour,
		},
		{
			name:     "Mixed duration",
			time:     3723 * 1000000, // 1h 2m 3s in microseconds
			expected: time.Hour + 2*time.Minute + 3*time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.time.ToDuration()
			if result != tt.expected {
				t.Errorf("DuckDBTime.ToDuration() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestDuckDBTimestamp_ToTime(t *testing.T) {
	tests := []struct {
		name      string
		timestamp DuckDBTimestamp
		expected  time.Time
	}{
		{
			name:      "Unix epoch",
			timestamp: 0, // 1970-01-01 00:00:00
			expected:  time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:      "One second after epoch",
			timestamp: 1000000, // 1 second in microseconds
			expected:  time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC),
		},
		{
			name:      "With microseconds",
			timestamp: 1000123, // 1.000123 seconds
			expected:  time.Date(1970, 1, 1, 0, 0, 1, 123000, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.timestamp.ToTime()
			if !result.Equal(tt.expected) {
				t.Errorf("DuckDBTimestamp.ToTime() = %v, want %v", result, tt.expected)
			}
		})
	}
}
