package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

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

	// Create a table with various data types
	_, err = db.Exec(`
		CREATE TABLE enhanced_types (
			id INTEGER PRIMARY KEY,
			int_val INTEGER,
			float_val DOUBLE,
			bool_val BOOLEAN,
			date_val DATE,
			time_val TIME,
			timestamp_val TIMESTAMP,
			varchar_val VARCHAR,
			blob_val BLOB
		)
	`)
	if err != nil {
		log.Fatalf("Error creating table: %v", err)
	}

	// Get current time to use in example
	now := time.Now()
	date := now.Format("2006-01-02")
	timeStr := now.Format("15:04:05")
	timestamp := now.Format("2006-01-02 15:04:05")

	// Insert data with different types
	_, err = db.Exec(`
		INSERT INTO enhanced_types 
		(id, int_val, float_val, bool_val, date_val, time_val, timestamp_val, varchar_val, blob_val)
		VALUES 
		(1, 42, 3.14159, true, ?, ?, ?, 'Hello DuckDB', 'binary data')
	`, date, timeStr, timestamp)
	if err != nil {
		log.Fatalf("Error inserting data: %v", err)
	}

	// Query the data and demonstrate type information
	rows, err := db.Query("SELECT * FROM enhanced_types")
	if err != nil {
		log.Fatalf("Error querying data: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("Error closing rows: %v", err)
		}
	}()

	// Get column information
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		log.Fatalf("Error getting column types: %v", err)
	}

	// Display column type information
	fmt.Println("Column Type Information:")
	fmt.Println("=======================")
	fmt.Printf("%-15s %-15s %-15s %-15s\n", "COLUMN NAME", "DATABASE TYPE", "NULLABLE", "GO TYPE")
	fmt.Println("---------------------------------------------------------------")

	for _, ct := range columnTypes {
		// Get nullable information
		nullable, ok := ct.Nullable()
		nullableStr := fmt.Sprintf("%v (ok: %v)", nullable, ok)

		// Get scan type
		scanType := ct.ScanType()
		scanTypeStr := "unknown"
		if scanType != nil {
			scanTypeStr = scanType.String()
		}

		// Print column information
		fmt.Printf("%-15s %-15s %-15s %-15s\n",
			ct.Name(),
			ct.DatabaseTypeName(),
			nullableStr,
			scanTypeStr)

		// Get length information if applicable
		length, hasLength := ct.Length()
		if hasLength {
			fmt.Printf("  - Length: %d\n", length)
		}

		// Get decimal information if applicable
		precision, scale, hasDecimal := ct.DecimalSize()
		if hasDecimal {
			fmt.Printf("  - Precision: %d, Scale: %d\n", precision, scale)
		}
	}

	// Scan and print data
	fmt.Println("\nData Values:")
	fmt.Println("===========")

	for rows.Next() {
		var (
			id, intVal                     int
			floatVal                       float64
			boolVal                        bool
			dateVal, timeVal, timestampVal string
			varcharVal                     string
			blobVal                        []byte
		)

		err := rows.Scan(&id, &intVal, &floatVal, &boolVal, &dateVal, &timeVal, &timestampVal, &varcharVal, &blobVal)
		if err != nil {
			log.Fatalf("Error scanning row: %v", err)
		}

		fmt.Printf("ID: %d\n", id)
		fmt.Printf("Integer: %d\n", intVal)
		fmt.Printf("Float: %f\n", floatVal)
		fmt.Printf("Boolean: %t\n", boolVal)
		fmt.Printf("Date: %s\n", dateVal)
		fmt.Printf("Time: %s\n", timeVal)
		fmt.Printf("Timestamp: %s\n", timestampVal)
		fmt.Printf("Varchar: %s\n", varcharVal)
		fmt.Printf("Blob: %s\n", string(blobVal))
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Error iterating rows: %v", err)
	}
}
