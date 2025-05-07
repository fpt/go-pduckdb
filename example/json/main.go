package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	_ "github.com/fpt/go-pduckdb" // Import for driver registration
)

// Person represents a person with name, age, and custom attributes
type Person struct {
	Name       string         `json:"name"`
	Age        int            `json:"age"`
	Attributes map[string]any `json:"attributes"`
}

// Implement the sql.Scanner interface for Person
func (p *Person) Scan(src any) error {
	b, ok := src.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(b, p)
}

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

	// Create a table with JSON column
	_, err = db.Exec(`
		CREATE TABLE people (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			data JSON
		)
	`)
	if err != nil {
		log.Fatalf("Error creating table: %v", err)
	}
	fmt.Println("Created table with JSON column")

	// Create some sample data
	person1 := Person{
		Name: "John Doe",
		Age:  30,
		Attributes: map[string]any{
			"height":   180,
			"weight":   75.5,
			"hobbies":  []string{"reading", "running", "coding"},
			"employed": true,
			"address": map[string]any{
				"city":    "New York",
				"country": "USA",
			},
		},
	}

	person2 := Person{
		Name: "Jane Smith",
		Age:  28,
		Attributes: map[string]any{
			"height":   165,
			"weight":   62.0,
			"hobbies":  []string{"painting", "traveling", "swimming"},
			"employed": true,
			"address": map[string]any{
				"city":    "San Francisco",
				"country": "USA",
			},
		},
	}

	// Insert first person using a JSON string directly
	personJSON, err := json.Marshal(person1)
	if err != nil {
		log.Fatalf("Error marshaling JSON: %v", err)
	}

	// Use a different syntax for JSON insertion - cast string to JSON
	_, err = db.Exec("INSERT INTO people (id, name, data) VALUES (1, ?, ?::JSON)", person1.Name, string(personJSON))
	if err != nil {
		log.Fatalf("Error inserting first person: %v", err)
	}

	// Insert second person using prepared statement and native JSON parameter binding
	stmt, err := db.Prepare("INSERT INTO people (id, name, data) VALUES (?, ?, ?::JSON)")
	if err != nil {
		log.Fatalf("Error preparing statement: %v", err)
	}

	// Marshal the object to ensure we pass a valid JSON string
	person2JSON, err := json.Marshal(person2)
	if err != nil {
		log.Fatalf("Error marshaling second person: %v", err)
	}

	_, err = stmt.Exec(2, person2.Name, string(person2JSON))
	if err != nil {
		log.Fatalf("Error inserting second person: %v", err)
	}
	if err := stmt.Close(); err != nil {
		log.Printf("Error closing statement: %v", err)
	}

	fmt.Println("Inserted data with JSON values")

	// Query using SQL JSON extraction
	fmt.Println("\nQuerying data with JSON extraction:")
	rows, err := db.Query(`
		SELECT 
			id, 
			name, 
			data,
			data->>'age' AS age,
			data->'attributes'->>'height' AS height,
			data->'attributes'->'hobbies'->0 AS first_hobby
		FROM people
		ORDER BY id
	`)
	if err != nil {
		log.Fatalf("Error querying data: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("Error closing rows: %v", err)
		}
	}()

	fmt.Printf("%-3s %-12s %-10s %-10s %-10s %-10s\n", "ID", "NAME", "AGE", "HEIGHT", "FIRST HOBBY", "JSON")
	fmt.Println("----------------------------------------------------------")

	for rows.Next() {
		var (
			id         int
			name       string
			data       Person
			age        string
			height     string
			firstHobby string
		)

		if err := rows.Scan(&id, &name, &data, &age, &height, &firstHobby); err != nil {
			log.Fatalf("Error scanning row: %v", err)
		}

		fmt.Printf("%-3d %-12s %-10s %-10s %-10s %v\n", id, name, age, height, firstHobby, data)
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Error iterating rows: %v", err)
	}

	// Query and filter by JSON values
	fmt.Println("\nQuerying and filtering by JSON values:")
	rows, err = db.Query(`
		SELECT id, name, data->>'age' AS age
		FROM people
		WHERE CAST(data->>'age' AS INTEGER) > 25
		ORDER BY CAST(data->>'age' AS INTEGER) DESC
	`)
	if err != nil {
		log.Fatalf("Error querying with filter: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("Error closing rows: %v", err)
		}
	}()

	for rows.Next() {
		var (
			id   int
			name string
			age  string
		)

		if err := rows.Scan(&id, &name, &age); err != nil {
			log.Fatalf("Error scanning row: %v", err)
		}

		fmt.Printf("ID: %d, Name: %s, Age: %s\n", id, name, age)
	}

	// Testing JSON update
	fmt.Println("\nUpdating JSON data:")
	_, err = db.Exec(`
		UPDATE people
		SET data = json_merge_patch(data, '{"attributes":{"verified":true}}')
		WHERE id = 1
	`)
	if err != nil {
		log.Fatalf("Error updating JSON: %v", err)
	}

	// Query to verify the update
	var verified string
	err = db.QueryRow(`
		SELECT data->'attributes'->>'verified'
		FROM people
		WHERE id = 1
	`).Scan(&verified)
	if err != nil {
		log.Fatalf("Error querying verified status: %v", err)
	}
	fmt.Printf("Person 1 verified status: %s\n", verified)

	fmt.Println("\nJSON support test completed successfully!")
}
