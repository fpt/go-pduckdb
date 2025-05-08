package duckdb

import (
	"fmt"
)

// DuckDBConnection represents a connection to a DuckDB database
type Connection struct {
	handle DuckDBConnection
	db     *DB
}

// Query executes a SQL query and returns the result
func (c *Connection) Query(sql string) (*Result, error) {
	var rawResult DuckDBResultRaw
	cQuery := ToCString(sql)
	defer FreeCString(cQuery)

	state := c.db.Query(c.handle, cQuery, &rawResult)
	if state != DuckDBSuccess {
		return nil, fmt.Errorf("Query failed: %s", sql)
	}

	internalResult := NewResult(c.db, rawResult)

	return internalResult, nil
}

// Execute runs a SQL statement that doesn't return a result
func (c *Connection) Execute(sql string) error {
	result, err := c.Query(sql)
	if err != nil {
		return err
	}
	result.Close()
	return nil
}

// Prepare creates a prepared statement for later execution
func (c *Connection) Prepare(query string) (*PreparedStatement, error) {
	// Check if our database instance has the prepare function
	if c.db.Prepare == nil {
		return nil, fmt.Errorf("Prepare function not available in this DuckDB build")
	}

	var stmt DuckDBPreparedStatement
	cQuery := ToCString(query)
	defer FreeCString(cQuery)

	// Call DuckDB's prepare function with correct pointer type
	state := c.db.Prepare(c.handle, cQuery, &stmt)
	if state != DuckDBSuccess {
		// Get error message from the prepared statement
		if c.db.PrepareError != nil && stmt != nil {
			errMsg := GoString(c.db.PrepareError(stmt))
			// Cleanup the failed prepared statement
			if c.db.DestroyPrepared != nil {
				c.db.DestroyPrepared(&stmt)
			}
			return nil, fmt.Errorf("failed to prepare statement: %s", errMsg)
		}
		return nil, fmt.Errorf("failed to prepare statement")
	}

	// Get the number of parameters
	var numParams int32 = 0
	if c.db.NumParams != nil {
		numParams = int32(c.db.NumParams(stmt))
	}

	// Create and return the prepared statement object
	return &PreparedStatement{
		handle:    stmt,
		conn:      c,
		numParams: numParams,
	}, nil
}

// Close closes the connection
func (c *Connection) Close() {
	c.db.Disconnect(&c.handle)
}
