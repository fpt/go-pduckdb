package pduckdb

// DuckDBConnection represents a connection to a DuckDB database
type DuckDBConnection struct {
	handle *byte
	db     *DuckDB
	query  func(*byte, string, *DuckDBResultRaw) DuckDBState
}

// Query executes a SQL query and returns the result
func (c *DuckDBConnection) Query(sql string) (*DuckDBResult, error) {
	result := &DuckDBResult{
		columnCount:    c.db.columnCount,
		rowCount:       c.db.rowCount,
		columnName:     c.db.columnName,
		valueString:    c.db.valueString,
		valueDate:      c.db.valueDate,
		valueTime:      c.db.valueTime,
		valueTimestamp: c.db.valueTimestamp,
		destroyResult:  c.db.destroyResult,
	}

	state := c.query(c.handle, sql, &result.raw)
	if state != DuckDBSuccess {
		return nil, ErrDuckDB{Message: "Query failed: " + sql}
	}

	return result, nil
}

// Execute runs a SQL statement that doesn't return a result
func (c *DuckDBConnection) Execute(sql string) error {
	result, err := c.Query(sql)
	if err != nil {
		return err
	}
	result.Close()
	return nil
}

// Close closes the connection
func (c *DuckDBConnection) Close() {
	// Note: The DuckDB API doesn't have connection close function
	// The connection is closed when the database is closed
}
