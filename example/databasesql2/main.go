package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/fpt/go-pduckdb" // Import for driver registration
)

func main() {
	// Open a database connection
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY, 
		name VARCHAR, 
		email VARCHAR
	)`)
	if err != nil {
		log.Fatal(err)
	}

	// Insert data
	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`,
		1, "John Doe", "john@example.com")
	if err != nil {
		fmt.Printf("Error inserting data: %v\n", err)
		log.Fatal(err)
	}

	// Query data
	rows, err := db.Query("SELECT id, name, email FROM users")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Process results
	for rows.Next() {
		var id int
		var name, email string
		if err := rows.Scan(&id, &name, &email); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("User %d: %s (%s)\n", id, name, email)
	}
}
