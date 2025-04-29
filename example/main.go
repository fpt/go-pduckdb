package main

import (
	"fmt"
	"os"
	"time"

	pd "github.com/fpt/go-pduckdb"
)

func Exists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

func cleanupDatabaseFiles(path string) error {
	if Exists(path) {
		if err := os.Remove(path); err != nil {
			return err
		}
	}

	walPath := path + ".wal"
	if Exists(walPath) {
		if err := os.Remove(walPath); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	// Clean up any existing database files
	if err := cleanupDatabaseFiles("my_database.db"); err != nil {
		fmt.Printf("Error cleaning up database files: %v\n", err)
		os.Exit(1)
	}

	// Create database instance
	db, err := pd.NewDuckDB("my_database.db")
	if err != nil {
		fmt.Printf("Error creating database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Println("DB opened successfully!")

	// Create connection
	conn, err := db.Connect()
	if err != nil {
		fmt.Printf("Error connecting to database: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("DB connected successfully!")

	// Create table with various date/time types
	if err := conn.Execute(`CREATE TABLE sample (
		i INTEGER, 
		b BOOLEAN, 
		s VARCHAR, 
		d DATE,
		t TIME,
		ts TIMESTAMP
	);`); err != nil {
		fmt.Printf("Error creating table: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("DB table created successfully!")

	// Insert data with date, time, and timestamp values
	if err := conn.Execute(`INSERT INTO sample VALUES 
		(3, TRUE, 'Sample1', '1992-09-20', '11:30:45', '1992-09-20 11:30:45.123456'),
		(5, FALSE, 'Sample2', '2023-04-15', '14:20:30', '2023-04-15 14:20:30'),
		(7, NULL, 'Sample3', NULL, NULL, NULL);`); err != nil {
		fmt.Printf("Error inserting values: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("DB values inserted successfully!")

	// Query data
	result, err := conn.Query("SELECT * FROM sample")
	if err != nil {
		fmt.Printf("Error querying values: %v\n", err)
		os.Exit(1)
	}
	defer result.Close()
	fmt.Println("DB values queried successfully!")

	// Process results
	columnCount := result.ColumnCount()
	rowCount := result.RowCount()
	fmt.Printf("Result has %d columns and %d rows\n", columnCount, rowCount)

	// Print column names
	for i := int32(0); i < columnCount; i++ {
		fmt.Printf("Column %d: %s\n", i, result.ColumnName(i))
	}

	// Get column types to properly handle date/time values
	columnNames := make([]string, columnCount)
	for i := int32(0); i < columnCount; i++ {
		columnNames[i] = result.ColumnName(i)
	}

	// Print data with comma formatting, handling date and time types
	for r := int32(0); r < int32(rowCount); r++ {
		fmt.Printf("Row %d: ", r)

		values := make([]string, columnCount)
		for c := int64(0); c < int64(columnCount); c++ {
			// Check column name to determine how to handle the value
			colName := columnNames[c]

			switch colName {
			case "d": // Date column
				dateVal, hasValue := result.ValueDate(c, r)
				if hasValue {
					values[c] = dateVal.Format("2006-01-02")
				} else {
					values[c] = "NULL"
				}

			case "t": // Time column
				timeVal, hasValue := result.ValueTime(c, r)
				if hasValue {
					values[c] = timeVal.Format("15:04:05")
				} else {
					values[c] = "NULL"
				}

			case "ts": // Timestamp column
				tsVal, hasValue := result.ValueTimestamp(c, r)
				if hasValue {
					values[c] = tsVal.Format("2006-01-02 15:04:05.000000")
				} else {
					values[c] = "NULL"
				}

			default: // Other columns (use string representation)
				val, hasValue := result.ValueString(c, r)
				if hasValue {
					values[c] = val
				} else {
					values[c] = "NULL"
				}
			}
		}

		// Print the row with comma separation
		for i, val := range values {
			if i > 0 {
				fmt.Print(", ")
			}
			fmt.Print(val)
		}
		fmt.Println()
	}

	// Example of working with date/time values programmatically
	fmt.Println("\nDemonstrating date/time operations:")
	result2, _ := conn.Query("SELECT d, t, ts FROM sample WHERE i = 3")
	defer result2.Close()

	if date, hasDate := result2.ValueDate(0, 0); hasDate {
		// Calculate days since this date
		daysSince := time.Since(date).Hours() / 24
		fmt.Printf("Days since %s: %.0f days\n", date.Format("2006-01-02"), daysSince)
	}

	if timeVal, hasTime := result2.ValueTime(1, 0); hasTime {
		// Extract hour from time
		hour := timeVal.Hour()
		fmt.Printf("Hour from time value: %d\n", hour)
	}

	if ts, hasTs := result2.ValueTimestamp(2, 0); hasTs {
		// Format timestamp in a different way
		fmt.Printf("Formatted timestamp: %s\n", ts.Format("January 2, 2006 at 3:04 PM"))

		// Calculate if the timestamp is in the past
		if ts.Before(time.Now()) {
			fmt.Printf("This timestamp is %.1f years in the past\n",
				time.Since(ts).Hours()/24/365)
		}
	}

	fmt.Println("DB closed successfully!")
}
