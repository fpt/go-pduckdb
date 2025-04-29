package pduckdb

import (
	"testing"
	"time"
)

// mockResult creates a mock result struct for testing
func mockResult() *DuckDBResult {
	return &DuckDBResult{
		columnCount: func(*DuckDBResultRaw) int32 { return 3 },
		rowCount:    func(*DuckDBResultRaw) int64 { return 2 },
		columnName: func(*DuckDBResultRaw, int32) *byte {
			// Return different strings based on the column index
			// Note: This is simplified and won't work in real tests without more setup
			return nil
		},
		valueString: func(*DuckDBResultRaw, int64, int32) *byte {
			return nil
		},
		valueDate: func(*DuckDBResultRaw, int64, int32) DuckDBDate {
			return 0
		},
		valueTime: func(*DuckDBResultRaw, int64, int32) DuckDBTime {
			return 0
		},
		valueTimestamp: func(*DuckDBResultRaw, int64, int32) DuckDBTimestamp {
			return 0
		},
		destroyResult: func(*DuckDBResultRaw) {},
	}
}

func TestDuckDBResult_ColumnCount(t *testing.T) {
	result := &DuckDBResult{
		columnCount: func(*DuckDBResultRaw) int32 { return 5 },
	}

	if count := result.ColumnCount(); count != 5 {
		t.Errorf("ColumnCount() = %v, want 5", count)
	}
}

func TestDuckDBResult_RowCount(t *testing.T) {
	result := &DuckDBResult{
		rowCount: func(*DuckDBResultRaw) int64 { return 10 },
	}

	if count := result.RowCount(); count != 10 {
		t.Errorf("RowCount() = %v, want 10", count)
	}
}

func TestDuckDBResult_ColumnName(t *testing.T) {
	// This is a simplified test that uses string literals instead of actual C strings
	colNameData := "test_column"
	colNameBytes := []byte(colNameData + "\x00") // null-terminated C string
	colNamePtr := &colNameBytes[0]

	result := &DuckDBResult{
		columnName: func(*DuckDBResultRaw, int32) *byte {
			return colNamePtr
		},
	}

	if name := result.ColumnName(0); name != "test_column" {
		t.Errorf("ColumnName() = %v, want 'test_column'", name)
	}
}

func TestDuckDBResult_ValueString(t *testing.T) {
	// Setup a test case with a C string
	valData := "test_value"
	valBytes := []byte(valData + "\x00") // null-terminated C string
	valPtr := &valBytes[0]

	tests := []struct {
		name      string
		mockValue *byte
		want      string
		wantOk    bool
	}{
		{
			name:      "Normal value",
			mockValue: valPtr,
			want:      "test_value",
			wantOk:    true,
		},
		{
			name:      "NULL value",
			mockValue: nil,
			want:      "",
			wantOk:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &DuckDBResult{
				valueString: func(*DuckDBResultRaw, int64, int32) *byte {
					return tt.mockValue
				},
			}

			got, gotOk := result.ValueString(0, 0)
			if got != tt.want {
				t.Errorf("ValueString() got = %v, want %v", got, tt.want)
			}
			if gotOk != tt.wantOk {
				t.Errorf("ValueString() ok = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestDuckDBResult_ValueDate(t *testing.T) {
	tests := []struct {
		name      string
		mockValue DuckDBDate
		want      time.Time
		wantOk    bool
	}{
		{
			name:      "Normal value",
			mockValue: 365, // 1971-01-01
			want:      time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
			wantOk:    true,
		},
		{
			name:      "NULL value",
			mockValue: 0,
			want:      time.Time{},
			wantOk:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &DuckDBResult{
				valueDate: func(*DuckDBResultRaw, int64, int32) DuckDBDate {
					return tt.mockValue
				},
			}

			got, gotOk := result.ValueDate(0, 0)
			if !got.Equal(tt.want) && gotOk {
				t.Errorf("ValueDate() got = %v, want %v", got, tt.want)
			}
			if gotOk != tt.wantOk {
				t.Errorf("ValueDate() ok = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestDuckDBResult_ValueTime(t *testing.T) {
	tests := []struct {
		name      string
		mockValue DuckDBTime
		wantHour  int
		wantMin   int
		wantSec   int
		wantOk    bool
	}{
		{
			name:      "Normal value",
			mockValue: 3600 * 1000000, // 1 hour
			wantHour:  1,
			wantMin:   0,
			wantSec:   0,
			wantOk:    true,
		},
		{
			name:      "NULL value",
			mockValue: 0,
			wantHour:  0,
			wantMin:   0,
			wantSec:   0,
			wantOk:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &DuckDBResult{
				valueTime: func(*DuckDBResultRaw, int64, int32) DuckDBTime {
					return tt.mockValue
				},
			}

			got, gotOk := result.ValueTime(0, 0)
			if gotOk {
				if got.Hour() != tt.wantHour || got.Minute() != tt.wantMin || got.Second() != tt.wantSec {
					t.Errorf("ValueTime() got = %v, want %d:%d:%d",
						got, tt.wantHour, tt.wantMin, tt.wantSec)
				}
			}
			if gotOk != tt.wantOk {
				t.Errorf("ValueTime() ok = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestDuckDBResult_ValueTimestamp(t *testing.T) {
	// 2020-01-01 12:30:45 in microseconds since epoch
	epochMicros := int64(1577880645000000)

	tests := []struct {
		name      string
		mockValue DuckDBTimestamp
		wantYear  int
		wantMonth time.Month
		wantDay   int
		wantHour  int
		wantOk    bool
	}{
		{
			name:      "Normal value",
			mockValue: DuckDBTimestamp(epochMicros),
			wantYear:  2020,
			wantMonth: time.January,
			wantDay:   1,
			wantHour:  12,
			wantOk:    true,
		},
		{
			name:      "NULL value",
			mockValue: 0,
			wantYear:  0,
			wantMonth: 0,
			wantDay:   0,
			wantHour:  0,
			wantOk:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &DuckDBResult{
				valueTimestamp: func(*DuckDBResultRaw, int64, int32) DuckDBTimestamp {
					return tt.mockValue
				},
			}

			got, gotOk := result.ValueTimestamp(0, 0)
			if gotOk {
				if got.Year() != tt.wantYear || got.Month() != tt.wantMonth ||
					got.Day() != tt.wantDay || got.Hour() != tt.wantHour {
					t.Errorf("ValueTimestamp() got = %v, want %d-%d-%d %d:XX:XX",
						got, tt.wantYear, tt.wantMonth, tt.wantDay, tt.wantHour)
				}
			}
			if gotOk != tt.wantOk {
				t.Errorf("ValueTimestamp() ok = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestDuckDBResult_Close(t *testing.T) {
	// This test verifies that Close calls the destroyResult function
	called := false

	result := &DuckDBResult{
		destroyResult: func(*DuckDBResultRaw) {
			called = true
		},
	}

	result.Close()

	if !called {
		t.Error("Close() did not call destroyResult")
	}
}
