package pduckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"time"
)

// Initialize and register the driver
func init() {
	sql.Register("duckdb", &Driver{})
}

// Driver implements database/sql/driver.Driver
type Driver struct{}

// Open returns a new connection to the database.
// The dsn is a connection string for the database.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	db, err := NewDuckDB(dsn)
	if err != nil {
		return nil, err
	}

	conn, err := db.Connect()
	if err != nil {
		db.Close()
		return nil, err
	}

	return &Conn{
		db:   db,
		conn: conn,
	}, nil
}

// Conn implements database/sql/driver.Conn
type Conn struct {
	db   *DuckDB
	conn *DuckDBConnection
}

// Prepare returns a prepared statement, bound to this connection.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return &Stmt{
		conn:  c.conn,
		query: query,
	}, nil
}

// Close closes the connection.
func (c *Conn) Close() error {
	c.db.Close() // This will close the connection as well
	return nil
}

// Begin starts and returns a new transaction.
func (c *Conn) Begin() (driver.Tx, error) {
	// Execute BEGIN statement
	err := c.conn.Execute("BEGIN TRANSACTION")
	if err != nil {
		return nil, err
	}
	return &Tx{conn: c.conn}, nil
}

// Stmt implements database/sql/driver.Stmt
type Stmt struct {
	conn  *DuckDBConnection
	query string
}

// Close closes the statement.
func (s *Stmt) Close() error {
	return nil // Nothing to close for statements in DuckDB
}

// NumInput returns the number of placeholder parameters.
func (s *Stmt) NumInput() int {
	return -1 // Driver doesn't know how many parameters there are
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("not implemented, use ExecContext instead")
}

// Query executes a query that may return rows, such as a SELECT.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("not implemented, use QueryContext instead")
}

// Tx implements database/sql/driver.Tx
type Tx struct {
	conn *DuckDBConnection
}

// Commit commits the transaction.
func (tx *Tx) Commit() error {
	return tx.conn.Execute("COMMIT")
}

// Rollback aborts the transaction.
func (tx *Tx) Rollback() error {
	return tx.conn.Execute("ROLLBACK")
}

// Rows implements database/sql/driver.Rows
type Rows struct {
	result      *DuckDBResult
	columnCnt   int32
	rowCnt      int64
	currentRow  int64
	columnNames []string
}

// Columns returns the names of the columns.
func (r *Rows) Columns() []string {
	return r.columnNames
}

// Close closes the rows iterator.
func (r *Rows) Close() error {
	r.result.Close()
	return nil
}

// Next is called to populate the next row of data into the provided slice.
func (r *Rows) Next(dest []driver.Value) error {
	if r.currentRow >= r.rowCnt {
		return io.EOF
	}

	for i := int64(0); i < int64(r.columnCnt); i++ {
		// First try date/time types, then fall back to string

		// Try to get timestamp (most specific first)
		if val, ok := r.result.ValueTimestamp(i, int32(r.currentRow)); ok {
			dest[i] = val
			continue
		}

		// Try to get date
		if val, ok := r.result.ValueDate(i, int32(r.currentRow)); ok {
			dest[i] = val
			continue
		}

		// Try to get time
		if val, ok := r.result.ValueTime(i, int32(r.currentRow)); ok {
			dest[i] = val
			continue
		}

		// Fall back to string for other types
		if val, ok := r.result.ValueString(i, int32(r.currentRow)); ok {
			dest[i] = val
			continue
		}

		// If all attempts fail, set to nil
		dest[i] = nil
	}

	r.currentRow++
	return nil
}

// Additional interfaces to support context and named parameters

// ConnBeginTx implements driver.ConnBeginTx
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if ctx.Done() != nil {
		// If context is canceled, don't begin a transaction
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}

	// Use a simplified transaction begin statement that's compatible with DuckDB
	// Ignore isolation level and read-only settings for now as they might not be supported
	err := c.conn.Execute("BEGIN TRANSACTION")
	if err != nil {
		return nil, err
	}
	return &Tx{conn: c.conn}, nil
}

// StmtExecContext implements driver.StmtExecContext
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	// Convert named values to regular values
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	// Replace placeholders in query with actual values
	// Note: this is a simplified approach; a real implementation would use
	// parameter binding if supported by DuckDB
	query, err := replacePlaceholders(s.query, dargs)
	if err != nil {
		return nil, err
	}

	// Execute the query
	err = s.conn.Execute(query)
	if err != nil {
		return nil, err
	}

	// Return a result with no rows affected information
	return &Result{}, nil
}

// StmtQueryContext implements driver.StmtQueryContext
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	// Convert named values to regular values
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	// Replace placeholders in query with actual values
	query, err := replacePlaceholders(s.query, dargs)
	if err != nil {
		return nil, err
	}

	// Execute the query
	result, err := s.conn.Query(query)
	if err != nil {
		return nil, err
	}

	// Create column names slice
	columnCnt := result.ColumnCount()
	columnNames := make([]string, columnCnt)
	for i := int32(0); i < columnCnt; i++ {
		columnNames[i] = result.ColumnName(i)
	}

	// Create and return rows
	rows := &Rows{
		result:      result,
		columnCnt:   columnCnt,
		rowCnt:      result.RowCount(),
		currentRow:  0,
		columnNames: columnNames,
	}

	return rows, nil
}

// Result implements driver.Result
type Result struct{}

// LastInsertId returns the database's auto-generated ID.
func (r *Result) LastInsertId() (int64, error) {
	return 0, errors.New("LastInsertId is not supported by DuckDB")
}

// RowsAffected returns the number of rows affected.
func (r *Result) RowsAffected() (int64, error) {
	return 0, errors.New("RowsAffected is not supported in this implementation")
}

// Helper function to convert NamedValue slice to Value slice
func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	for i, nv := range named {
		dargs[i] = nv.Value
	}
	return dargs, nil
}

// Helper function to replace placeholders (?) with actual values
func replacePlaceholders(query string, args []driver.Value) (string, error) {
	// Simple implementation: replace ? with actual values
	var result []byte
	argIndex := 0

	for i := 0; i < len(query); i++ {
		if query[i] == '?' && argIndex < len(args) {
			// Found a placeholder, replace it with the argument value
			switch v := args[argIndex].(type) {
			case nil:
				result = append(result, []byte("NULL")...)
			case int64:
				result = append(result, []byte(fmt.Sprintf("%d", v))...)
			case float64:
				result = append(result, []byte(fmt.Sprintf("%g", v))...)
			case bool:
				if v {
					result = append(result, []byte("TRUE")...)
				} else {
					result = append(result, []byte("FALSE")...)
				}
			case []byte:
				result = append(result, '\'')
				// Escape single quotes in string
				for _, b := range v {
					if b == '\'' {
						result = append(result, '\'', '\'') // Escape ' as ''
					} else {
						result = append(result, b)
					}
				}
				result = append(result, '\'')
			case string:
				result = append(result, '\'')
				// Escape single quotes in string
				for j := 0; j < len(v); j++ {
					if v[j] == '\'' {
						result = append(result, '\'', '\'') // Escape ' as ''
					} else {
						result = append(result, v[j])
					}
				}
				result = append(result, '\'')
			case time.Time:
				result = append(result, []byte(fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05")))...)
			default:
				return "", fmt.Errorf("unsupported parameter type: %T", v)
			}
			argIndex++
		} else {
			// Regular character, just append it
			result = append(result, query[i])
		}
	}

	if argIndex < len(args) {
		return "", fmt.Errorf("too many parameters provided: expected %d, got %d", argIndex, len(args))
	}

	return string(result), nil
}

// Ensure our driver implements necessary interfaces
var (
	_ driver.Driver           = (*Driver)(nil)
	_ driver.Conn             = (*Conn)(nil)
	_ driver.Stmt             = (*Stmt)(nil)
	_ driver.StmtExecContext  = (*Stmt)(nil)
	_ driver.StmtQueryContext = (*Stmt)(nil)
	_ driver.Tx               = (*Tx)(nil)
	_ driver.ConnBeginTx      = (*Conn)(nil)
	_ driver.Result           = (*Result)(nil)
	_ driver.Rows             = (*Rows)(nil)
)
