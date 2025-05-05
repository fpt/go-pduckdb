package pduckdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResultMethods(t *testing.T) {
	// Create database instance
	db, err := NewDuckDB(":memory:")
	assert.NoError(t, err, "Error creating database")
	defer db.Close()

	// Create connection
	conn, err := db.Connect()
	assert.NoError(t, err, "Error connecting to database")
	defer conn.Close()

	// Create table with various data types including DECIMAL with precision and scale
	err = conn.Execute(`CREATE TABLE sample (
		i INTEGER,
		b BOOLEAN,
		s VARCHAR,
		d DECIMAL(10,2),
		d2 DECIMAL(5,3)
	);`)
	assert.NoError(t, err, "Error creating table")

	// Insert test data
	err = conn.Execute(`INSERT INTO sample VALUES 
		(1, true, 'Sample1', 123.45, 12.345),
		(2, false, 'Sample2', 678.90, 6.789);`)
	assert.NoError(t, err, "Error inserting values")

	// Query the data
	result, err := conn.Query("SELECT * FROM sample")
	assert.NoError(t, err, "Error querying results")
	defer result.Close()

	// Test ColumnCount method
	t.Run("ColumnCount", func(t *testing.T) {
		count := result.ColumnCount()
		assert.Equal(t, int64(5), count, "Expected column count 5, got %d", count)
	})

	// Test RowCount method
	t.Run("RowCount", func(t *testing.T) {
		count := result.RowCount()
		assert.Equal(t, int64(2), count, "Expected row count 2, got %d", count)
	})

	// Test ColumnName method
	t.Run("ColumnName", func(t *testing.T) {
		expectedNames := []string{"i", "b", "s", "d", "d2"}
		for i, expected := range expectedNames {
			name := result.ColumnName(int64(i))
			assert.Equal(t, expected, name, "Expected column name %s at index %d, got %s", expected, i, name)
		}
	})

	// Test ColumnType method
	t.Run("ColumnType", func(t *testing.T) {
		// Test column types by index
		intType := result.ColumnType(0)
		boolType := result.ColumnType(1)
		strType := result.ColumnType(2)
		decimalType1 := result.ColumnType(3)
		decimalType2 := result.ColumnType(4)

		// Verify the types are different
		assert.NotEqual(t, intType, boolType, "INTEGER and BOOLEAN types should be different")
		assert.NotEqual(t, intType, strType, "INTEGER and VARCHAR types should be different")
		assert.NotEqual(t, intType, decimalType1, "INTEGER and DECIMAL types should be different")
		assert.Equal(t, decimalType1, decimalType2, "Both DECIMAL columns should have the same type")
	})

	// Test ValueString method
	t.Run("ValueString", func(t *testing.T) {
		// Check string values in the third column (VARCHAR)
		value1, ok1 := result.ValueString(2, 0)
		assert.True(t, ok1, "Expected to get a string value")
		assert.Equal(t, "Sample1", value1, "Expected string value 'Sample1', got '%s'", value1)

		value2, ok2 := result.ValueString(2, 1)
		assert.True(t, ok2, "Expected to get a string value")
		assert.Equal(t, "Sample2", value2, "Expected string value 'Sample2', got '%s'", value2)
	})

	// Test DecimalInfo method (for RowsColumnTypePrecisionScale interface)
	t.Run("DecimalInfo", func(t *testing.T) {
		// Test first decimal column (DECIMAL(10,2))
		precision1, scale1, ok1 := result.DecimalInfo(3)
		if assert.True(t, ok1, "Expected to get decimal info for column 3") {
			assert.Equal(t, int64(10), precision1, "Expected precision 10, got %d", precision1)
			assert.Equal(t, int64(2), scale1, "Expected scale 2, got %d", scale1)
		}

		// Test second decimal column (DECIMAL(5,3))
		precision2, scale2, ok2 := result.DecimalInfo(4)
		if assert.True(t, ok2, "Expected to get decimal info for column 4") {
			assert.Equal(t, int64(5), precision2, "Expected precision 5, got %d", precision2)
			assert.Equal(t, int64(3), scale2, "Expected scale 3, got %d", scale2)
		}

		// Test non-decimal column
		_, _, ok3 := result.DecimalInfo(0)
		assert.False(t, ok3, "Should not get decimal info for non-decimal column")
	})

	// Test Close method indirectly
	t.Run("Close", func(t *testing.T) {
		// Create a separate result to close explicitly
		tempResult, err := conn.Query("SELECT 1")
		assert.NoError(t, err, "Error creating temporary result")

		// Close should not panic
		assert.NotPanics(t, func() {
			tempResult.Close()
		}, "Close method should not panic")

		// After closing, attempting to use the result should panic or fail
		// This is implementation-dependent, so we're not testing further
	})
}
