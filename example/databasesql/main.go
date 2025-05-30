package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/fpt/go-pduckdb" // Import for driver registration only
)

func main() {
	// Open a database/sql connection
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}()

	// Check connection is working
	err = db.Ping()
	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
	}
	fmt.Println("Connected to DuckDB successfully!")

	// Create a table using standard database/sql
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY, 
		name VARCHAR, 
		email VARCHAR, 
		created_at TIMESTAMP
	)`)
	if err != nil {
		log.Fatalf("Error creating table: %v", err)
	}
	fmt.Println("Table created successfully")

	// Delete any existing data to avoid duplicate key errors
	_, err = db.Exec("DELETE FROM users")
	if err != nil {
		log.Fatalf("Error preparing delete statement: %v", err)
	}

	// Insert data one row at a time to work around driver limitations
	stmt, err := db.Prepare("INSERT INTO users (id, name, email, created_at) VALUES (?, ?, ?, ?)")
	if err != nil {
		log.Fatalf("Error preparing insert statement: %v", err)
	}

	// Insert first row
	_, err = stmt.Exec(1, "John Doe", "john@example.com", "2025-01-15 08:30:00")
	if err != nil {
		log.Fatalf("Error inserting first row: %v", err)
	}

	// Insert second row
	_, err = stmt.Exec(2, "Jane Smith", "jane@example.com", "2025-02-20 14:45:30")
	if err != nil {
		log.Fatalf("Error inserting second row: %v", err)
	}

	if err := stmt.Close(); err != nil {
		log.Printf("Error closing statement: %v", err)
	}
	fmt.Println("Data inserted successfully")

	// Query using standard database/sql Query
	rows, err := db.Query("SELECT id, name, email, created_at FROM users")
	if err != nil {
		log.Fatalf("Error querying data: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("Error closing rows: %v", err)
		}
	}()

	fmt.Println("\nUser data:")
	fmt.Println("---------------------------------------------------")
	fmt.Printf("%-5s %-15s %-20s %s\n", "ID", "Name", "Email", "Created At")
	fmt.Println("---------------------------------------------------")

	// Iterate through the result rows
	for rows.Next() {
		var id int
		var name, email string
		var createdAt time.Time

		if err := rows.Scan(&id, &name, &email, &createdAt); err != nil {
			log.Fatalf("Error scanning row: %v", err)
		}

		fmt.Printf("%-5d %-15s %-20s %s\n", id, name, email,
			createdAt.Format("2006-01-02 15:04:05"))
	}

	if err = rows.Err(); err != nil {
		log.Fatalf("Error during row iteration: %v", err)
	}

	// Demonstrate prepared statements with context and parameters
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stmt, err = db.PrepareContext(ctx, "SELECT name, email FROM users WHERE id = ?")
	if err != nil {
		log.Fatalf("Error preparing statement: %v", err)
	}
	defer func() {
		if err := stmt.Close(); err != nil {
			log.Printf("Error closing statement: %v", err)
		}
	}()

	var name, email string
	err = stmt.QueryRowContext(ctx, 1).Scan(&name, &email)
	if err != nil {
		log.Fatalf("Error executing prepared statement: %v", err)
	}

	fmt.Println("\nPrepared statement result:")
	fmt.Printf("User ID 1: %s (%s)\n", name, email)

	// Demonstrate transaction
	fmt.Println("\nStarting transaction...")
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  false,
	})
	if err != nil {
		log.Fatalf("Error starting transaction: %v", err)
	}

	// Prepare statement within transaction
	txStmt, err := tx.PrepareContext(ctx, "UPDATE users SET name = ? WHERE id = ?")
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Printf("Error rolling back transaction: %v", rbErr)
		}
		log.Fatalf("Error preparing statement in transaction: %v", err)
	}

	// Execute update within transaction
	_, err = txStmt.ExecContext(ctx, "John Updated", 1)
	if err != nil {
		if closeErr := txStmt.Close(); closeErr != nil {
			log.Printf("Error closing transaction statement: %v", closeErr)
		}
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Printf("Error rolling back transaction: %v", rbErr)
		}
		log.Fatalf("Error in transaction: %v", err)
	}
	if err := txStmt.Close(); err != nil {
		log.Printf("Error closing transaction statement: %v", err)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		log.Fatalf("Error committing transaction: %v", err)
	}
	fmt.Println("Transaction committed successfully")

	// Verify the update
	stmt, err = db.PrepareContext(ctx, "SELECT name FROM users WHERE id = ?")
	if err != nil {
		log.Fatalf("Error preparing verification query: %v", err)
	}

	var updatedName string
	err = stmt.QueryRowContext(ctx, 1).Scan(&updatedName)
	if err := stmt.Close(); err != nil {
		log.Printf("Error closing statement: %v", err)
	}
	if err != nil {
		log.Fatalf("Error querying updated data: %v", err)
	}
	fmt.Printf("Updated name: %s\n", updatedName)

	fmt.Println("\nDatabase operations completed successfully!")
}
