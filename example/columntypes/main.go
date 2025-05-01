package main

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"

	_ "github.com/fpt/go-pduckdb" // Import for driver registration
)

func main() {
	// Open a temporary in-memory database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}()

	// Create a table with different column types
	_, err = db.Exec(`
		CREATE TABLE columntypes_example (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			created_at TIMESTAMP,
			birth_date DATE,
			login_time TIME,
			is_active BOOLEAN,
			score DOUBLE,
			data BLOB
		)
	`)
	if err != nil {
		log.Fatalf("Error creating table: %v", err)
	}

	// Insert some data
	_, err = db.Exec(`
		INSERT INTO columntypes_example 
		(id, name, created_at, birth_date, login_time, is_active, score, data)
		VALUES 
		(1, 'John Doe', '2025-01-15 08:30:00', '1990-05-10', '08:30:00', true, 92.5, 'binary data')
	`)
	if err != nil {
		log.Fatalf("Error inserting data: %v", err)
	}

	// Query the data to get column type information
	rows, err := db.Query("SELECT * FROM columntypes_example")
	if err != nil {
		log.Fatalf("Error querying data: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("Error closing rows: %v", err)
		}
	}()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		log.Fatalf("Error getting column names: %v", err)
	}

	// Get column types
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		log.Fatalf("Error getting column types: %v", err)
	}

	// Print header
	fmt.Println("Column Type Information:")
	fmt.Println("=======================")
	fmt.Printf("%-15s %-15s %-15s %-15s\n", "COLUMN NAME", "DATABASE TYPE", "NULLABLE", "GO TYPE")
	fmt.Println("---------------------------------------------------------------")

	// Print information for each column
	for i, ct := range columnTypes {
		// Get nullable information
		nullable, ok := ct.Nullable()
		nullableStr := fmt.Sprintf("%v (ok: %v)", nullable, ok)

		// Get scan type
		scanType := ct.ScanType()
		scanTypeStr := "unknown"
		if scanType != nil {
			scanTypeStr = scanType.String()
		}

		// Get database type
		dbType := ct.DatabaseTypeName()

		// Print column information
		fmt.Printf("%-15s %-15s %-15s %-15s\n",
			columns[i],
			dbType,
			nullableStr,
			scanTypeStr)

		// Get additional type information if available
		length, hasLength := ct.Length()
		if hasLength {
			fmt.Printf("  - Length: %d\n", length)
		}

		precision, scale, hasPrecision := ct.DecimalSize()
		if hasPrecision {
			fmt.Printf("  - Precision: %d, Scale: %d\n", precision, scale)
		}
	}

	// Demonstrate scanning values using the type information
	fmt.Println("\nData Values:")
	fmt.Println("===========")

	// Iterate through rows
	for rows.Next() {
		// Create a slice of interface{} to hold the row values
		rowValues := make([]interface{}, len(columns))
		rowPointers := make([]interface{}, len(columns))

		// Create pointers to the correct types based on column type information
		for i, ct := range columnTypes {
			scanType := ct.ScanType()
			if scanType == nil {
				// Default to string if type is unknown
				var str string
				rowValues[i] = &str
			} else {
				// Create a pointer to the correct type
				rowValues[i] = reflect.New(scanType).Interface()
			}
			rowPointers[i] = rowValues[i]
		}

		// Scan the row into our value pointers
		if err := rows.Scan(rowPointers...); err != nil {
			log.Fatalf("Error scanning row: %v", err)
		}

		// Print the values
		for i, colName := range columns {
			val := reflect.ValueOf(rowValues[i]).Elem().Interface()
			fmt.Printf("%s: %v (%T)\n", colName, val, val)
		}
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		log.Fatalf("Error iterating over rows: %v", err)
	}
}
