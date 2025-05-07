package pduckdb

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDriverMethods(t *testing.T) {
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

func TestAllValueMethods(t *testing.T) {
	// Create database instance
	db, err := NewDuckDB(":memory:")
	assert.NoError(t, err, "Error creating database")
	defer db.Close()

	// Create connection
	conn, err := db.Connect()
	assert.NoError(t, err, "Error connecting to database")
	defer conn.Close()

	// Create a table with all supported data types
	err = conn.Execute(`CREATE TABLE all_types (
		bool_col BOOLEAN,
		tinyint_col TINYINT,
		smallint_col SMALLINT,
		int_col INTEGER,
		bigint_col BIGINT,
		utinyint_col UTINYINT,
		usmallint_col USMALLINT,
		uint_col UINTEGER,
		ubigint_col UBIGINT,
		float_col FLOAT,
		double_col DOUBLE,
		date_col DATE,
		time_col TIME,
		timestamp_col TIMESTAMP,
		varchar_col VARCHAR,
		json_col JSON
	);`)
	assert.NoError(t, err, "Error creating table with all types")

	// Insert test data with all types of values
	err = conn.Execute(`INSERT INTO all_types VALUES (
		true, 
		127, 
		32767, 
		2147483647, 
		9223372036854775807, 
		255, 
		65535, 
		4294967295, 
		18446744073709551615, 
		3.14159, 
		1.7976931348623157e+308, 
		'2025-05-07', 
		'12:34:56', 
		'2025-05-07 12:34:56', 
		'Test string', 
		'{"key": "value"}'
	);`)
	assert.NoError(t, err, "Error inserting values")

	// Query the data
	result, err := conn.Query("SELECT * FROM all_types")
	assert.NoError(t, err, "Error querying results")
	defer result.Close()

	// Test column count
	assert.Equal(t, int64(16), result.ColumnCount(), "Expected 16 columns")

	// Test row count
	assert.Equal(t, int64(1), result.RowCount(), "Expected 1 row")

	// Test each value type
	t.Run("TestBooleanValue", func(t *testing.T) {
		val, ok := result.ValueBoolean(0, 0)
		assert.True(t, ok, "Expected to get a boolean value")
		assert.Equal(t, true, val, "Expected boolean value true")
	})

	t.Run("TestTinyintValue", func(t *testing.T) {
		val, ok := result.ValueInt8(1, 0)
		assert.True(t, ok, "Expected to get a tinyint value")
		assert.Equal(t, int8(127), val, "Expected tinyint value 127")
	})

	t.Run("TestSmallintValue", func(t *testing.T) {
		val, ok := result.ValueInt16(2, 0)
		assert.True(t, ok, "Expected to get a smallint value")
		assert.Equal(t, int16(32767), val, "Expected smallint value 32767")
	})

	t.Run("TestIntegerValue", func(t *testing.T) {
		val, ok := result.ValueInt32(3, 0)
		assert.True(t, ok, "Expected to get an integer value")
		assert.Equal(t, int32(2147483647), val, "Expected integer value 2147483647")
	})

	t.Run("TestBigintValue", func(t *testing.T) {
		val, ok := result.ValueInt64(4, 0)
		assert.True(t, ok, "Expected to get a bigint value")
		assert.Equal(t, int64(9223372036854775807), val, "Expected bigint value 9223372036854775807")
	})

	t.Run("TestUTinyintValue", func(t *testing.T) {
		val, ok := result.ValueUint8(5, 0)
		assert.True(t, ok, "Expected to get a utinyint value")
		assert.Equal(t, uint8(255), val, "Expected utinyint value 255")
	})

	t.Run("TestUSmallintValue", func(t *testing.T) {
		val, ok := result.ValueUint16(6, 0)
		assert.True(t, ok, "Expected to get a usmallint value")
		assert.Equal(t, uint16(65535), val, "Expected usmallint value 65535")
	})

	t.Run("TestUIntegerValue", func(t *testing.T) {
		val, ok := result.ValueUint32(7, 0)
		assert.True(t, ok, "Expected to get a uinteger value")
		assert.Equal(t, uint32(4294967295), val, "Expected uinteger value 4294967295")
	})

	t.Run("TestUBigintValue", func(t *testing.T) {
		val, ok := result.ValueUint64(8, 0)
		assert.True(t, ok, "Expected to get a ubigint value")
		assert.Equal(t, uint64(18446744073709551615), val, "Expected ubigint value 18446744073709551615")
	})

	t.Run("TestFloatValue", func(t *testing.T) {
		val, ok := result.ValueFloat(9, 0)
		assert.True(t, ok, "Expected to get a float value")
		assert.InDelta(t, float32(3.14159), val, 0.0001, "Expected float value ~3.14159")
	})

	t.Run("TestDoubleValue", func(t *testing.T) {
		val, ok := result.ValueDouble(10, 0)
		assert.True(t, ok, "Expected to get a double value")
		assert.InDelta(t, 1.7976931348623157e+308, val, 1e+300, "Expected double value ~1.7976931348623157e+308")
	})

	t.Run("TestDateValue", func(t *testing.T) {
		val, ok := result.ValueDate(11, 0)
		assert.True(t, ok, "Expected to get a date value")
		assert.Equal(t, "2025-05-07", val.Format("2006-01-02"), "Expected date value 2025-05-07")
	})

	t.Run("TestTimeValue", func(t *testing.T) {
		val, ok := result.ValueTime(12, 0)
		assert.True(t, ok, "Expected to get a time value")
		// Check only the time portion (hour, minute, second)
		assert.Equal(t, "12:34:56", val.Format("15:04:05"), "Expected time value 12:34:56")
	})

	t.Run("TestTimestampValue", func(t *testing.T) {
		val, ok := result.ValueTimestamp(13, 0)
		assert.True(t, ok, "Expected to get a timestamp value")
		assert.Equal(t, "2025-05-07 12:34:56", val.Format("2006-01-02 15:04:05"), "Expected timestamp value 2025-05-07 12:34:56")
	})

	t.Run("TestVarcharValue", func(t *testing.T) {
		val, ok := result.ValueString(14, 0)
		assert.True(t, ok, "Expected to get a varchar value")
		assert.Equal(t, "Test string", val, "Expected varchar value 'Test string'")
	})

	t.Run("TestJSONValue", func(t *testing.T) {
		val, ok := result.ValueVarchar(15, 0)
		assert.True(t, ok, "Expected to get a JSON value")
		assert.Contains(t, string(val), "key", "Expected JSON to contain 'key'")
		assert.Contains(t, string(val), "value", "Expected JSON to contain 'value'")
	})

	// Test the Row interface by using query through standard database/sql API
	t.Run("TestRowInterface", func(t *testing.T) {
		// We need to explicitly close the previous connection to avoid locks
		conn.Close()
		db.Close()

		// Open through database/sql interface
		sqlDB, err := sql.Open("duckdb", ":memory:")
		assert.NoError(t, err, "Error opening database through sql interface")
		defer func() {
			if err := sqlDB.Close(); err != nil {
				t.Errorf("Error closing database through sql interface: %v", err)
			}
		}()

		// Create the same test table
		_, err = sqlDB.Exec(`CREATE TABLE all_types (
			bool_col BOOLEAN,
			tinyint_col TINYINT,
			smallint_col SMALLINT,
			int_col INTEGER,
			bigint_col BIGINT,
			utinyint_col UTINYINT,
			usmallint_col USMALLINT,
			uint_col UINTEGER,
			ubigint_col UBIGINT,
			float_col FLOAT,
			double_col DOUBLE,
			date_col DATE,
			time_col TIME,
			timestamp_col TIMESTAMP,
			varchar_col VARCHAR,
			json_col JSON
		);`)
		assert.NoError(t, err, "Error creating table with all types in sql interface")

		// Insert test data
		_, err = sqlDB.Exec(`INSERT INTO all_types VALUES (
			true, 
			127, 
			32767, 
			2147483647, 
			9223372036854775807, 
			255, 
			65535, 
			4294967295, 
			18446744073709551615, 
			3.14159, 
			1.7976931348623157e+308, 
			'2025-05-07', 
			'12:34:56', 
			'2025-05-07 12:34:56', 
			'Test string', 
			'{"key": "value"}'
		);`)
		assert.NoError(t, err, "Error inserting values in sql interface")

		// Query the data
		rows, err := sqlDB.Query("SELECT * FROM all_types")
		assert.NoError(t, err, "Error querying in sql interface")
		defer func() {
			if err := rows.Close(); err != nil {
				t.Errorf("Error closing rows in sql interface: %v", err)
			}
		}()

		// Verify we can scan all the values correctly
		columns, err := rows.Columns()
		assert.NoError(t, err, "Error getting columns in sql interface")
		assert.Equal(t, 16, len(columns), "Expected 16 columns in sql interface")

		for rows.Next() {
			// Create variables to scan into
			var (
				boolVal      bool
				tinyintVal   int8
				smallintVal  int16
				intVal       int32
				bigintVal    int64
				utinyintVal  uint8
				usmallintVal uint16
				uintVal      uint32
				ubigintVal   uint64
				floatVal     float32
				doubleVal    float64
				dateVal      time.Time
				timeVal      time.Time
				timestampVal time.Time
				varcharVal   string
				jsonVal      string
			)

			// Scan the row into variables
			err := rows.Scan(
				&boolVal,
				&tinyintVal,
				&smallintVal,
				&intVal,
				&bigintVal,
				&utinyintVal,
				&usmallintVal,
				&uintVal,
				&ubigintVal,
				&floatVal,
				&doubleVal,
				&dateVal,
				&timeVal,
				&timestampVal,
				&varcharVal,
				&jsonVal,
			)
			assert.NoError(t, err, "Error scanning row in sql interface")

			// Verify values
			assert.Equal(t, true, boolVal, "Expected boolean value true")
			assert.Equal(t, int8(127), tinyintVal, "Expected tinyint value 127")
			assert.Equal(t, int16(32767), smallintVal, "Expected smallint value 32767")
			assert.Equal(t, int32(2147483647), intVal, "Expected integer value 2147483647")
			assert.Equal(t, int64(9223372036854775807), bigintVal, "Expected bigint value 9223372036854775807")
			assert.Equal(t, uint8(255), utinyintVal, "Expected utinyint value 255")
			assert.Equal(t, uint16(65535), usmallintVal, "Expected usmallint value 65535")
			assert.Equal(t, uint32(4294967295), uintVal, "Expected uinteger value 4294967295")
			assert.Equal(t, uint64(18446744073709551615), ubigintVal, "Expected ubigint value 18446744073709551615")
			assert.InDelta(t, float32(3.14159), floatVal, 0.0001, "Expected float value ~3.14159")
			assert.InDelta(t, 1.7976931348623157e+308, doubleVal, 1e+300, "Expected double value ~1.7976931348623157e+308")
			assert.Equal(t, "2025-05-07", dateVal.Format("2006-01-02"), "Expected date value 2025-05-07")
			assert.Equal(t, "12:34:56", timeVal.Format("15:04:05"), "Expected time value 12:34:56")
			assert.Equal(t, "2025-05-07 12:34:56", timestampVal.Format("2006-01-02 15:04:05"), "Expected timestamp value 2025-05-07 12:34:56")
			assert.Equal(t, "Test string", varcharVal, "Expected varchar value 'Test string'")
			assert.Contains(t, jsonVal, "key", "Expected JSON to contain 'key'")
			assert.Contains(t, jsonVal, "value", "Expected JSON to contain 'value'")
		}

		assert.NoError(t, rows.Err(), "Error iterating rows in sql interface")
	})
}
