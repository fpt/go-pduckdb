package main

import (
	"context"
	"database/sql"
	"log"

	_ "github.com/fpt/go-pduckdb"
)

func main() {
	ctx := context.Background()
	conn, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer func(conn *sql.DB) {
		if err := conn.Close(); err != nil {
			panic(err)
		}
	}(conn)

	sql := `BEGIN TRANSACTION;
DROP TABLE IF EXISTS products; 
CREATE TABLE products AS SELECT 1 as col;
COMMIT;`
	_, err = conn.ExecContext(ctx, sql)
	if err != nil {
		log.Fatal(err)
	}

	// Query the table to verify it was created
	rows, err := conn.QueryContext(ctx, "SELECT * FROM products")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("Error closing rows: %v", err)
		}
	}()
	for rows.Next() {
		var col int
		if err := rows.Scan(&col); err != nil {
			log.Fatal(err)
		}
		log.Printf("col: %d", col)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	// Clean up
	_, err = conn.ExecContext(ctx, "DROP TABLE products")
	if err != nil {
		log.Fatal(err)
	}
	// Close the connection
	if err := conn.Close(); err != nil {
		log.Fatal(err)
	}
}
