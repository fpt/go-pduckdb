package pduckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
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
	// Create a new prepared statement using DuckDB's native prepare function
	preparedStmt, err := c.conn.Prepare(query)
	if err != nil {
		return nil, err
	}

	return &Stmt{
		conn:         c.conn,
		query:        query,
		preparedStmt: preparedStmt,
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
	conn         *DuckDBConnection
	query        string
	preparedStmt *PreparedStatement
}

// Close closes the statement.
func (s *Stmt) Close() error {
	if s.preparedStmt != nil {
		return s.preparedStmt.Close()
	}
	return nil
}

// NumInput returns the number of placeholder parameters.
func (s *Stmt) NumInput() int {
	if s.preparedStmt != nil {
		return int(s.preparedStmt.numParams)
	}
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
			// Check if this could be a JSON value, but keep as string for compatibility
			// We'll mark it as JSON in ColumnTypes, but provide as string for scanning
			dest[i] = val
			continue
		}

		// If all attempts fail, set to nil
		dest[i] = nil
	}

	r.currentRow++
	return nil
}

// ColumnTypes returns column type information.
func (r *Rows) ColumnTypes() ([]*ColumnType, error) {
	// Create a slice to hold the column types
	columnTypes := make([]*ColumnType, r.columnCnt)

	// We need to determine the types for each column
	for i := int32(0); i < r.columnCnt; i++ {
		// Default values
		scanType := reflect.TypeOf("")
		dbType := "VARCHAR"
		nullable := true
		length := int64(0)

		// Check if there's data to determine type from
		if r.rowCnt > 0 {
			// Try to determine type by checking different value getters
			col := int64(i)
			row := int32(0)

			// Check for timestamp/date/time types first
			if _, ok := r.result.ValueTimestamp(col, row); ok {
				scanType = reflect.TypeOf(time.Time{})
				dbType = "TIMESTAMP"
			} else if _, ok := r.result.ValueDate(col, row); ok {
				scanType = reflect.TypeOf(time.Time{})
				dbType = "DATE"
			} else if _, ok := r.result.ValueTime(col, row); ok {
				scanType = reflect.TypeOf(time.Time{})
				dbType = "TIME"
			} else {
				// For other types, we'd need to infer from the string value
				// This is a simple heuristic and could be improved
				if val, ok := r.result.ValueString(col, row); ok {
					// Try to determine type from string value
					if val == "true" || val == "false" {
						scanType = reflect.TypeOf(bool(false))
						dbType = "BOOLEAN"
					} else if isInteger(val) {
						// Could be INT, BIGINT, etc.
						scanType = reflect.TypeOf(int64(0))
						dbType = "INTEGER"
					} else if isFloat(val) {
						scanType = reflect.TypeOf(float64(0))
						dbType = "DOUBLE"
					} else if isJSON(val) {
						// Check if the value is JSON
						scanType = reflect.TypeOf(JSON{})
						dbType = "JSON"
					}

					// Set length for VARCHAR types
					if dbType == "VARCHAR" {
						length = int64(len(val))
					}
				}
			}
		}

		columnTypes[i] = &ColumnType{
			name:         r.columnNames[i],
			databaseType: dbType,
			length:       length,
			nullable:     nullable,
			scanType:     scanType,
		}
	}

	return columnTypes, nil
}

// Helper functions to infer types from string values
func isInteger(val string) bool {
	_, err := strconv.ParseInt(val, 10, 64)
	return err == nil
}

func isFloat(val string) bool {
	_, err := strconv.ParseFloat(val, 64)
	return err == nil
}

// isJSON checks if a string value is likely JSON
func isJSON(val string) bool {
	// Simple heuristic: JSON typically starts with { or [ and ends with } or ]
	val = strings.TrimSpace(val)
	if len(val) < 2 {
		return false
	}

	// Check if it starts with { or [ and ends with } or ]
	return (val[0] == '{' && val[len(val)-1] == '}') ||
		(val[0] == '[' && val[len(val)-1] == ']')
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
	// If the prepared statement is available, use it
	if s.preparedStmt != nil {
		// Check for context cancellation
		if ctx.Done() != nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}

		// Bind parameters
		for i, arg := range args {
			// Parameter indices in DuckDB are 1-based
			paramIdx := i + 1
			err := s.preparedStmt.BindParameter(paramIdx, arg.Value)
			if err != nil {
				return nil, err
			}
		}

		// Execute the prepared statement
		result, err := s.preparedStmt.Execute()
		if err != nil {
			return nil, err
		}
		defer result.Close()

		// Return the result
		return &Result{}, nil
	}

	// Fall back to the old implementation if native prepared statements aren't supported
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	query, err := replacePlaceholders(s.query, dargs)
	if err != nil {
		return nil, err
	}

	err = s.conn.Execute(query)
	if err != nil {
		return nil, err
	}

	return &Result{}, nil
}

// StmtQueryContext implements driver.StmtQueryContext
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	// If the prepared statement is available, use it
	if s.preparedStmt != nil {
		// Check for context cancellation
		if ctx.Done() != nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}

		// Bind parameters
		for i, arg := range args {
			// Parameter indices in DuckDB are 1-based
			paramIdx := i + 1
			err := s.preparedStmt.BindParameter(paramIdx, arg.Value)
			if err != nil {
				return nil, err
			}
		}

		// Execute the prepared statement
		result, err := s.preparedStmt.Execute()
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

	// Fall back to the old implementation if native prepared statements aren't supported
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	query, err := replacePlaceholders(s.query, dargs)
	if err != nil {
		return nil, err
	}

	result, err := s.conn.Query(query)
	if err != nil {
		return nil, err
	}

	columnCnt := result.ColumnCount()
	columnNames := make([]string, columnCnt)
	for i := int32(0); i < columnCnt; i++ {
		columnNames[i] = result.ColumnName(i)
	}

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
	// More robust implementation with better type handling
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
			case int32:
				result = append(result, []byte(fmt.Sprintf("%d", v))...)
			case int:
				result = append(result, []byte(fmt.Sprintf("%d", v))...)
			case int8:
				result = append(result, []byte(fmt.Sprintf("%d", v))...)
			case int16:
				result = append(result, []byte(fmt.Sprintf("%d", v))...)
			case uint64:
				result = append(result, []byte(fmt.Sprintf("%d", v))...)
			case uint32:
				result = append(result, []byte(fmt.Sprintf("%d", v))...)
			case uint:
				result = append(result, []byte(fmt.Sprintf("%d", v))...)
			case uint8:
				result = append(result, []byte(fmt.Sprintf("%d", v))...)
			case uint16:
				result = append(result, []byte(fmt.Sprintf("%d", v))...)
			case float64:
				result = append(result, []byte(fmt.Sprintf("%g", v))...)
			case float32:
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
				result = append(result, []byte(fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05.999999")))...)
			case Date:
				result = append(result, []byte(fmt.Sprintf("DATE '%s'", v.ToTime().Format("2006-01-02")))...)
			case Time:
				result = append(result, []byte(fmt.Sprintf("TIME '%s'", v.ToTime().Format("15:04:05.999999")))...)
			case Timestamp:
				result = append(result, []byte(fmt.Sprintf("TIMESTAMP '%s'", v.ToTime().Format("2006-01-02 15:04:05.999999")))...)
			case Interval:
				result = append(result, []byte(fmt.Sprintf("INTERVAL '%d months %d days %d microseconds'", v.Months, v.Days, v.Micros))...)
			case HugeInt:
				// Simple string representation for HugeInt
				result = append(result, []byte(fmt.Sprintf("%d%016x", v.Upper, v.Lower))...)
			case Decimal:
				// For decimal, we need a more sophisticated conversion
				// This is a simplified approach
				result = append(result, []byte(fmt.Sprintf("CAST(%d%016x AS DECIMAL(%d,%d))",
					v.Value.Upper, v.Value.Lower, v.Width, v.Scale))...)
			case JSON:
				// For JSON, we need to wrap it in the JSON() function
				result = append(result, []byte(fmt.Sprintf("JSON '%s'", v.Value))...)
			case map[string]interface{}, []interface{}:
				// Handle Go map/slice as JSON
				jsonBytes, err := json.Marshal(v)
				if err != nil {
					return "", fmt.Errorf("failed to marshal JSON: %v", err)
				}
				result = append(result, []byte(fmt.Sprintf("JSON '%s'", string(jsonBytes)))...)
			default:
				// For unsupported types, try to use String() if available
				if stringer, ok := v.(fmt.Stringer); ok {
					str := stringer.String()
					result = append(result, '\'')
					for j := 0; j < len(str); j++ {
						if str[j] == '\'' {
							result = append(result, '\'', '\'') // Escape ' as ''
						} else {
							result = append(result, str[j])
						}
					}
					result = append(result, '\'')
				} else {
					return "", fmt.Errorf("unsupported parameter type: %T", v)
				}
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
