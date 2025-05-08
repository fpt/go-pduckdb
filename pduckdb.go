// Package pduckdb provides a pure Go DuckDB driver
package pduckdb

import (
	"github.com/fpt/go-pduckdb/internal/duckdb"
)

// DuckDB represents a DuckDB database instance
type DuckDB struct {
	db *duckdb.DB
}

// NewDuckDB creates a new DuckDB instance
func NewDuckDB(path string) (*DuckDB, error) {
	db, err := duckdb.NewDB(path)
	if err != nil {
		return nil, err
	}

	return &DuckDB{
		db: db,
	}, nil
}

// Connect creates a new connection to the database
func (d *DuckDB) Connect() (*duckdb.Connection, error) {
	conn, err := d.db.ConnectDB()
	if err != nil {
		return nil, ErrDuckDB{Message: "Failed to connect to database"}
	}

	return conn, nil
}

// Close closes the database and releases resources
func (d *DuckDB) Close() {
	d.db.CloseDB()
}

// goString converts a C string to a Go string
// This is kept for backward compatibility
func goString(c *byte) string {
	return duckdb.GoString(c)
}
