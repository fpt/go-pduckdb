package pduckdb

import (
	"testing"
	"time"

	"github.com/fpt/go-pduckdb/internal/duckdb"
	"github.com/fpt/go-pduckdb/types"
)

func TestConnectionQuery(t *testing.T) {
	// Create a test connection with a custom query function
	conn := testConnection()

	// Override the query function to return a successful result
	conn.db.Query = func(_ duckdb.DuckDBConnection, sql *byte, result *duckdb.DuckDBResultRaw) duckdb.DuckDBState {
		if duckdb.GoString(sql) == "SELECT 1" {
			return duckdb.DuckDBSuccess
		}
		return duckdb.DuckDBError
	}

	// Test successful query
	result, err := conn.Query("SELECT 1")
	if err != nil {
		t.Errorf("Expected successful query, got error: %v", err)
	}
	if result == nil {
		t.Errorf("Expected non-nil result")
	}

	// Test failed query
	result, err = conn.Query("INVALID SQL")
	if err == nil {
		t.Errorf("Expected error for invalid SQL")
	}
	if result != nil {
		t.Errorf("Expected nil result for failed query")
	}
}

func TestConnectionExecute(t *testing.T) {
	// Create a test connection
	conn := testConnection()

	// Override the query function for testing
	conn.db.Query = func(_ duckdb.DuckDBConnection, sql *byte, result *duckdb.DuckDBResultRaw) duckdb.DuckDBState {
		if duckdb.GoString(sql) == "CREATE TABLE test (id INT)" {
			return duckdb.DuckDBSuccess
		}
		return duckdb.DuckDBError
	}

	// Test successful execution
	err := conn.Execute("CREATE TABLE test (id INT)")
	if err != nil {
		t.Errorf("Expected successful execution, got error: %v", err)
	}

	// Test failed execution
	err = conn.Execute("INVALID SQL")
	if err == nil {
		t.Errorf("Expected error for invalid SQL execution")
	}
}

func TestConnectionPrepare(t *testing.T) {
	// Create a test connection
	conn := testConnection()

	// Set up prepare function
	conn.db.Prepare = func(_ duckdb.DuckDBConnection, _ *byte, stmt *duckdb.DuckDBPreparedStatement) duckdb.DuckDBState {
		// Use a valid pointer conversion pattern
		dummyVal := 12345
		*stmt = duckdb.DuckDBPreparedStatement(&dummyVal)
		return duckdb.DuckDBSuccess
	}

	// Set up number of parameters function
	conn.db.NumParams = func(stmt duckdb.DuckDBPreparedStatement) int64 {
		return 3
	}

	// Test successful prepare
	stmt, err := conn.Prepare("SELECT * FROM test WHERE id = ?")
	if err != nil {
		t.Errorf("Expected successful prepare, got error: %v", err)
	}
	if stmt == nil {
		t.Errorf("Expected non-nil prepared statement")
		return // Return early to avoid nil dereference
	}
	if stmt.numParams != 3 {
		t.Errorf("Expected 3 parameters, got %d", stmt.numParams)
	}

	// Test prepare error
	conn.db.Prepare = func(_ duckdb.DuckDBConnection, _ *byte, stmt *duckdb.DuckDBPreparedStatement) duckdb.DuckDBState {
		return duckdb.DuckDBError
	}

	stmt, err = conn.Prepare("INVALID SQL")
	if err == nil {
		t.Errorf("Expected error for invalid SQL prepare")
	}
	if stmt != nil {
		t.Errorf("Expected nil statement for failed prepare")
	}
}

func TestPreparedStatement(t *testing.T) {
	// Skip if integration tests are disabled
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Open a database connection
	db, err := NewDuckDB(":memory:")
	if err != nil {
		t.Fatalf("Error opening database: %v", err)
	}
	defer db.Close()

	// Create a connection
	conn, err := db.Connect()
	if err != nil {
		t.Fatalf("Error connecting to database: %v", err)
	}

	// Create a table for testing prepared statements
	err = conn.Execute(`
		CREATE TABLE users (
			id INTEGER,
			name VARCHAR,
			email VARCHAR,
			active BOOLEAN,
			score DOUBLE,
			created_at TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Error creating table: %v", err)
	}

	t.Run("PrepareAndBindParameters", func(t *testing.T) {
		// Test preparing a statement
		stmt, err := conn.Prepare(`
			INSERT INTO users (id, name, email, active, score, created_at)
			VALUES (?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			t.Fatalf("Error preparing statement: %v", err)
		}
		defer func() {
			if err := stmt.Close(); err != nil {
				t.Errorf("Error closing statement: %v", err)
			}
		}()

		defer func() {
			if err := stmt.Close(); err != nil {
				t.Errorf("Error closing statement: %v", err)
			}
		}()
		// Verify parameter count
		expectedParams := int32(6)
		if stmt.ParameterCount() != expectedParams {
			t.Errorf("Expected %d parameters, got %d", expectedParams, stmt.ParameterCount())
		}

		// Test binding different types of parameters
		now := time.Now()

		// Bind parameters with different types
		err = stmt.BindParameter(1, 1) // int
		if err != nil {
			t.Errorf("Error binding parameter 1: %v", err)
		}

		err = stmt.BindParameter(2, "Test User") // string
		if err != nil {
			t.Errorf("Error binding parameter 2: %v", err)
		}

		err = stmt.BindParameter(3, "test@example.com") // string
		if err != nil {
			t.Errorf("Error binding parameter 3: %v", err)
		}

		err = stmt.BindParameter(4, true) // bool
		if err != nil {
			t.Errorf("Error binding parameter 4: %v", err)
		}

		err = stmt.BindParameter(5, 95.5) // float64
		if err != nil {
			t.Errorf("Error binding parameter 5: %v", err)
		}

		err = stmt.BindParameter(6, now) // time.Time
		if err != nil {
			t.Errorf("Error binding parameter 6: %v", err)
		}

		// Execute the statement
		result, err := stmt.Execute()
		if err != nil {
			t.Fatalf("Error executing statement: %v", err)
		}
		result.Close()

		// Verify the data was inserted correctly
		queryResult, err := conn.Query("SELECT * FROM users WHERE id = 1")
		if err != nil {
			t.Fatalf("Error querying data: %v", err)
		}
		defer queryResult.Close()

		if queryResult.RowCount() != 1 {
			t.Errorf("Expected 1 row, got %d", queryResult.RowCount())
		}

		// Verify the values match what we inserted
		name, _ := queryResult.ValueString(1, 0)
		if name != "Test User" {
			t.Errorf("Expected name 'Test User', got '%s'", name)
		}

		active, _ := queryResult.ValueBoolean(3, 0)
		if !active {
			t.Errorf("Expected active to be true")
		}

		score, _ := queryResult.ValueDouble(4, 0)
		if score != 95.5 {
			t.Errorf("Expected score 95.5, got %f", score)
		}
	})

	t.Run("ClearBindings", func(t *testing.T) {
		// Prepare a statement
		stmt, err := conn.Prepare("INSERT INTO users (id, name) VALUES (?, ?)")
		if err != nil {
			t.Fatalf("Error preparing statement: %v", err)
		}
		defer func() {
			if err := stmt.Close(); err != nil {
				t.Errorf("Error closing statement: %v", err)
			}
		}()

		// Bind parameters
		err = stmt.BindParameter(1, 2)
		if err != nil {
			t.Errorf("Error binding parameter 1: %v", err)
		}

		err = stmt.BindParameter(2, "Clear Test")
		if err != nil {
			t.Errorf("Error binding parameter 2: %v", err)
		}

		// Clear bindings
		err = stmt.ClearBindings()
		if err != nil {
			t.Errorf("Error clearing bindings: %v", err)
		}

		// Now bind again with different values
		err = stmt.BindParameter(1, 3)
		if err != nil {
			t.Errorf("Error binding parameter 1 after clear: %v", err)
		}

		err = stmt.BindParameter(2, "After Clear")
		if err != nil {
			t.Errorf("Error binding parameter 2 after clear: %v", err)
		}

		// Execute
		result, err := stmt.Execute()
		if err != nil {
			t.Fatalf("Error executing statement after clearing bindings: %v", err)
		}
		result.Close()

		// Verify the new data was inserted correctly
		queryResult, err := conn.Query("SELECT * FROM users WHERE id = 3")
		if err != nil {
			t.Fatalf("Error querying data: %v", err)
		}
		defer queryResult.Close()

		if queryResult.RowCount() != 1 {
			t.Errorf("Expected 1 row, got %d", queryResult.RowCount())
		}

		name, _ := queryResult.ValueString(1, 0)
		if name != "After Clear" {
			t.Errorf("Expected name 'After Clear', got '%s'", name)
		}
	})

	t.Run("StatementType", func(t *testing.T) {
		// Test different statement types
		testCases := []struct {
			sql  string
			want duckdb.DuckDBStatementType
		}{
			{"SELECT * FROM users", duckdb.DuckDBStatementTypeSelect},
			{"INSERT INTO users (id) VALUES (5)", duckdb.DuckDBStatementTypeInsert},
			{"UPDATE users SET name = 'Updated' WHERE id = 5", duckdb.DuckDBStatementTypeUpdate},
			{"DELETE FROM users WHERE id = 5", duckdb.DuckDBStatementTypeDelete},
		}

		for _, tc := range testCases {
			stmt, err := conn.Prepare(tc.sql)
			if err != nil {
				t.Fatalf("Error preparing statement %q: %v", tc.sql, err)
			}

			stmtType, err := stmt.StatementType()
			if err != nil {
				t.Errorf("Error getting statement type for %q: %v", tc.sql, err)
			}

			if stmtType != tc.want {
				t.Errorf("For %q: expected statement type %v, got %v",
					tc.sql, tc.want, stmtType)
			}

			if err := stmt.Close(); err != nil {
				t.Errorf("Error closing statement: %v", err)
			}
		}
	})

	t.Run("ComplexTypes", func(t *testing.T) {
		// Create a table for complex types
		err := conn.Execute(`
			CREATE TABLE complex_data (
				id INTEGER,
				json_data VARCHAR,
				map_data VARCHAR,
				array_data VARCHAR
			)
		`)
		if err != nil {
			t.Fatalf("Error creating complex_data table: %v", err)
		}

		// Prepare statement
		stmt, err := conn.Prepare(`
			INSERT INTO complex_data (id, json_data, map_data, array_data) 
			VALUES (?, ?, ?, ?)
		`)
		if err != nil {
			t.Fatalf("Error preparing statement: %v", err)
		}
		defer func() {
			if err := stmt.Close(); err != nil {
				t.Errorf("Error closing statement: %v", err)
			}
		}()

		// Test binding JSON type
		jsonObj := types.NewJSON(`{"name":"JSON Test","active":true,"count":42}`)

		// Test binding a map
		mapData := map[string]any{
			"name":   "Map Test",
			"values": []int{1, 2, 3},
			"nested": map[string]string{
				"key": "value",
			},
		}

		// Test binding an array
		arrayData := []any{1, "two", 3.0, true}

		// Bind parameters
		err = stmt.BindParameter(1, 10)
		if err != nil {
			t.Errorf("Error binding parameter 1: %v", err)
		}

		err = stmt.BindParameter(2, jsonObj)
		if err != nil {
			t.Errorf("Error binding parameter 2 (JSON): %v", err)
		}

		err = stmt.BindParameter(3, mapData)
		if err != nil {
			t.Errorf("Error binding parameter 3 (map): %v", err)
		}

		err = stmt.BindParameter(4, arrayData)
		if err != nil {
			t.Errorf("Error binding parameter 4 (array): %v", err)
		}

		// Execute
		result, err := stmt.Execute()
		if err != nil {
			t.Fatalf("Error executing statement with complex types: %v", err)
		}
		result.Close()

		// Query back the data to verify
		queryResult, err := conn.Query("SELECT * FROM complex_data WHERE id = 10")
		if err != nil {
			t.Fatalf("Error querying complex data: %v", err)
		}
		defer queryResult.Close()

		if queryResult.RowCount() != 1 {
			t.Errorf("Expected 1 row, got %d", queryResult.RowCount())
		}
	})

	t.Run("ParameterReuse", func(t *testing.T) {
		// Prepare a statement with parameters
		stmt, err := conn.Prepare(`
			INSERT INTO users (id, name, email) VALUES (?, ?, ?)
		`)
		if err != nil {
			t.Fatalf("Error preparing statement: %v", err)
		}
		defer func() {
			if err := stmt.Close(); err != nil {
				t.Errorf("Error closing statement: %v", err)
			}
		}()

		// Execute with first set of parameters
		err = stmt.BindParameter(1, 100)
		if err != nil {
			t.Errorf("Error binding parameter 1 (first time): %v", err)
		}

		err = stmt.BindParameter(2, "User 100")
		if err != nil {
			t.Errorf("Error binding parameter 2 (first time): %v", err)
		}

		err = stmt.BindParameter(3, "user100@example.com")
		if err != nil {
			t.Errorf("Error binding parameter 3 (first time): %v", err)
		}

		result, err := stmt.Execute()
		if err != nil {
			t.Fatalf("Error executing statement (first time): %v", err)
		}
		result.Close()

		// Execute again with different parameters (reusing the statement)
		err = stmt.BindParameter(1, 101)
		if err != nil {
			t.Errorf("Error binding parameter 1 (second time): %v", err)
		}

		err = stmt.BindParameter(2, "User 101")
		if err != nil {
			t.Errorf("Error binding parameter 2 (second time): %v", err)
		}

		err = stmt.BindParameter(3, "user101@example.com")
		if err != nil {
			t.Errorf("Error binding parameter 3 (second time): %v", err)
		}

		result, err = stmt.Execute()
		if err != nil {
			t.Fatalf("Error executing statement (second time): %v", err)
		}
		result.Close()

		// Verify both rows were inserted
		queryResult, err := conn.Query("SELECT COUNT(*) FROM users WHERE id IN (100, 101)")
		if err != nil {
			t.Fatalf("Error querying data: %v", err)
		}
		defer queryResult.Close()

		countStr, _ := queryResult.ValueString(0, 0)
		if countStr != "2" {
			t.Errorf("Expected count 2, got %s", countStr)
		}
	})

	t.Run("NullValues", func(t *testing.T) {
		// Prepare a statement with parameters that will be null
		stmt, err := conn.Prepare(`
			INSERT INTO users (id, name, email) VALUES (?, ?, ?)
		`)
		if err != nil {
			t.Fatalf("Error preparing statement: %v", err)
		}
		defer func() {
			if err := stmt.Close(); err != nil {
				t.Errorf("Error closing statement: %v", err)
			}
		}()

		// Bind parameters with NULL values
		err = stmt.BindParameter(1, 200)
		if err != nil {
			t.Errorf("Error binding parameter 1: %v", err)
		}

		err = stmt.BindParameter(2, "Null Test")
		if err != nil {
			t.Errorf("Error binding parameter 2: %v", err)
		}

		err = stmt.BindParameter(3, nil) // NULL value
		if err != nil {
			t.Errorf("Error binding NULL parameter: %v", err)
		}

		result, err := stmt.Execute()
		if err != nil {
			t.Fatalf("Error executing statement with NULL value: %v", err)
		}
		result.Close()

		// Verify the NULL was inserted correctly
		queryResult, err := conn.Query("SELECT * FROM users WHERE id = 200")
		if err != nil {
			t.Fatalf("Error querying data: %v", err)
		}
		defer queryResult.Close()

		if queryResult.RowCount() != 1 {
			t.Errorf("Expected 1 row, got %d", queryResult.RowCount())
		}

		// Check that email is NULL
		isNull := queryResult.ValueNull(2, 0)
		if !isNull {
			t.Errorf("Expected email to be NULL")
		}
	})

	t.Run("ErrorHandling", func(t *testing.T) {
		// Test invalid SQL
		_, err := conn.Prepare("INVALID SQL STATEMENT")
		if err == nil {
			t.Errorf("Expected error for invalid SQL, got none")
		}

		// Test preparing a valid statement but executing with wrong parameter types
		stmt, err := conn.Prepare("INSERT INTO users (id) VALUES (?)")
		if err != nil {
			t.Fatalf("Error preparing statement: %v", err)
		}
		defer func() {
			if err := stmt.Close(); err != nil {
				t.Errorf("Error closing statement: %v", err)
			}
		}()

		// Try binding a string to an integer column (should work due to type conversion)
		err = stmt.BindParameter(1, "not_an_integer")
		if err != nil {
			t.Logf("Expected behavior: binding non-integer to integer column gave error: %v", err)
		}

		// Try binding an out-of-bounds parameter index
		err = stmt.BindParameter(999, 42)
		if err == nil {
			t.Errorf("Expected error for out-of-bounds parameter index, got none")
		}
	})
}
