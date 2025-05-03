package pduckdb

import (
	"testing"
	"time"
)

func TestResultColumnCount(t *testing.T) {
	// Create a test result with 3 columns
	result := testDuckDBResult()

	if count := result.ColumnCount(); count != 3 {
		t.Errorf("Expected column count 3, got %d", count)
	}
}

func TestResultRowCount(t *testing.T) {
	// Create a test result with 2 rows
	result := testDuckDBResult()

	if count := result.RowCount(); count != 2 {
		t.Errorf("Expected row count 2, got %d", count)
	}
}

func TestResultColumnName(t *testing.T) {
	result := testDuckDBResult()

	// Set up column names in the test result
	colNames := []string{"id", "name", "email"}
	mockStringResult(result, colNames)

	// Test column name retrieval
	for i, expected := range colNames {
		if name := result.ColumnName(int32(i)); name != expected {
			t.Errorf("Column %d: expected name %s, got %s", i, expected, name)
		}
	}
}

func TestResultValueString(t *testing.T) {
	result := testDuckDBResult()

	// Set up string values in the test result
	values := []string{"1", "John", "john@example.com", "2", "Jane", "jane@example.com"}
	mockStringResult(result, values)

	// Test string value retrieval
	expectedValues := []struct {
		col      int64
		row      int32
		expected string
		isNull   bool
	}{
		{0, 0, "1", false},
		{1, 0, "John", false},
		{2, 0, "john@example.com", false},
		{0, 1, "2", false},
		{1, 1, "Jane", false},
		{2, 1, "jane@example.com", false},
	}

	for _, tc := range expectedValues {
		value, ok := result.ValueString(tc.col, tc.row)
		if ok != !tc.isNull {
			t.Errorf("Row %d, Col %d: expected null status %v, got %v", tc.row, tc.col, tc.isNull, !ok)
		}
		if value != tc.expected {
			t.Errorf("Row %d, Col %d: expected %s, got %s", tc.row, tc.col, tc.expected, value)
		}
	}
}

func TestResultDateTimeValues(t *testing.T) {
	result := testDuckDBResult()

	// Configure test result with date/time values
	mockTimeResult(result)

	// Test date value
	date, ok := result.ValueDate(0, 0)
	if !ok {
		t.Errorf("Expected date to be non-null")
	}
	expectedDate := time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC)
	if !date.Equal(expectedDate) {
		t.Errorf("Expected date %v, got %v", expectedDate, date)
	}

	// Test time value
	timeVal, ok := result.ValueTime(0, 0)
	if !ok {
		t.Errorf("Expected time to be non-null")
	}
	// Time is on the current date, we just check hour, minute, second
	if timeVal.Hour() != 14 || timeVal.Minute() != 30 || timeVal.Second() != 45 {
		t.Errorf("Expected time 14:30:45, got %02d:%02d:%02d",
			timeVal.Hour(), timeVal.Minute(), timeVal.Second())
	}

	// Test timestamp value
	timestamp, ok := result.ValueTimestamp(0, 0)
	if !ok {
		t.Errorf("Expected timestamp to be non-null")
	}
	expectedTimestamp := time.Date(2025, 5, 1, 14, 30, 45, 0, time.UTC)
	if !timestamp.Equal(expectedTimestamp) {
		t.Errorf("Expected timestamp %v, got %v", expectedTimestamp, timestamp)
	}
}

func TestResultClose(t *testing.T) {
	result := testDuckDBResult()

	// Test that Close doesn't panic
	result.Close()
}
