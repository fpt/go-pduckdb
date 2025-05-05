package pduckdb

import (
	"fmt"

	"github.com/fpt/go-pduckdb/internal/duckdb"
)

// DuckDBConnection represents a connection to a DuckDB database
type DuckDBConnection struct {
	handle duckdb.DuckDBConnection
	db     *duckdb.DB
}

// PreparedStatement represents a DuckDB prepared statement
type PreparedStatement struct {
	handle    duckdb.DuckDBPreparedStatement
	conn      *DuckDBConnection
	numParams int32
}

// ParameterCount returns the number of parameters in the prepared statement
func (ps *PreparedStatement) ParameterCount() int32 {
	return ps.numParams
}

// ParameterName returns the name of the parameter at the given index
func (ps *PreparedStatement) ParameterName(paramIdx int) (string, error) {
	if ps.handle == nil {
		return "", ErrDuckDB{Message: "Prepared statement is closed"}
	}

	if ps.conn.db.ParameterName == nil {
		return "", ErrDuckDB{Message: "Parameter name function not available"}
	}

	// Parameter indices in DuckDB are 0-based for parameter_name
	idx := int64(paramIdx - 1)
	namePtr := ps.conn.db.ParameterName(ps.handle, idx)
	if namePtr == nil {
		return "", nil // No name for this parameter
	}

	return duckdb.GoString(namePtr), nil
}

// ParameterType returns the DuckDB type of the parameter at the given index
func (ps *PreparedStatement) ParameterType(paramIdx int) (duckdb.DuckDBType, error) {
	if ps.handle == nil {
		return duckdb.DuckDBTypeInvalid, ErrDuckDB{Message: "Prepared statement is closed"}
	}

	if ps.conn.db.ParamType == nil {
		return duckdb.DuckDBTypeInvalid, ErrDuckDB{Message: "Parameter type function not available"}
	}

	// Parameter indices in DuckDB are 0-based for param_type
	idx := int64(paramIdx - 1)
	typeCode := ps.conn.db.ParamType(ps.handle, idx)
	return duckdb.DuckDBType(typeCode), nil
}

// ClearBindings removes all parameter bindings from the prepared statement
func (ps *PreparedStatement) ClearBindings() error {
	if ps.handle == nil {
		return ErrDuckDB{Message: "Prepared statement is closed"}
	}

	if ps.conn.db.ClearBindings == nil {
		return ErrDuckDB{Message: "Clear bindings function not available"}
	}

	state := ps.conn.db.ClearBindings(ps.handle)
	if state != duckdb.DuckDBSuccess {
		return ErrDuckDB{Message: "Failed to clear bindings"}
	}

	return nil
}

// StatementType returns the type of SQL statement (SELECT, INSERT, etc.)
func (ps *PreparedStatement) StatementType() (duckdb.DuckDBStatementType, error) {
	typeCode := ps.conn.db.StatementType(ps.handle)
	return duckdb.DuckDBStatementType(typeCode), nil
}

// Query executes a SQL query and returns the result
func (c *DuckDBConnection) Query(sql string) (*DuckDBResult, error) {
	var rawResult duckdb.DuckDBResultRaw
	cQuery := duckdb.ToCString(sql)
	defer duckdb.FreeCString(cQuery)

	state := c.db.Query(c.handle, cQuery, &rawResult)
	if state != duckdb.DuckDBSuccess {
		return nil, ErrDuckDB{Message: "Query failed: " + sql}
	}

	internalResult := duckdb.NewResult(c.db, rawResult)

	result := &DuckDBResult{
		internal: internalResult,
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

// Prepare creates a prepared statement for later execution
func (c *DuckDBConnection) Prepare(query string) (*PreparedStatement, error) {
	// Check if our database instance has the prepare function
	if c.db.Prepare == nil {
		return nil, ErrDuckDB{Message: "Prepare function not available in this DuckDB build"}
	}

	var stmt duckdb.DuckDBPreparedStatement
	cQuery := duckdb.ToCString(query)
	defer duckdb.FreeCString(cQuery)

	// Call DuckDB's prepare function with correct pointer type
	state := c.db.Prepare(c.handle, cQuery, &stmt)
	if state != duckdb.DuckDBSuccess {
		// Get error message from the prepared statement
		if c.db.PrepareError != nil && stmt != nil {
			errMsg := duckdb.GoString(c.db.PrepareError(stmt))
			// Cleanup the failed prepared statement
			if c.db.DestroyPrepared != nil {
				c.db.DestroyPrepared(&stmt)
			}
			return nil, ErrDuckDB{Message: "Failed to prepare statement: " + errMsg}
		}
		return nil, ErrDuckDB{Message: "Failed to prepare statement"}
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
func (c *DuckDBConnection) Close() {
	c.db.Disconnect(&c.handle)
}

// Close releases resources associated with a prepared statement
func (ps *PreparedStatement) Close() error {
	if ps.handle == nil {
		return nil
	}

	if ps.conn.db.DestroyPrepared == nil {
		return ErrDuckDB{Message: "Destroy prepared function not available"}
	}

	// Convert handle to the format DuckDB expects for the destroy function
	handle := ps.handle
	ps.conn.db.DestroyPrepared(&handle)
	ps.handle = nil // Make sure we set the handle to nil after destroying
	return nil
}

// BindParameter binds a parameter value to a prepared statement
func (ps *PreparedStatement) BindParameter(paramIdx int, value any) error {
	if ps.handle == nil {
		return ErrDuckDB{Message: "Prepared statement is closed"}
	}

	// Ensure basic bind functions are available
	if ps.conn.db.BindNull == nil {
		return ErrDuckDB{Message: "Bind functions not available"}
	}

	// Get parameter type information if available
	paramType := duckdb.DuckDBTypeInvalid
	if ps.conn.db.ParamType != nil {
		// Parameter indices in DuckDB are 0-based for param_type
		idx := int64(paramIdx - 1)
		if idx >= 0 && idx < int64(ps.numParams) {
			paramType = duckdb.DuckDBType(ps.conn.db.ParamType(ps.handle, int64(paramIdx)))
		}
	}

	// Handle nil value (NULL) regardless of type
	if value == nil {
		state := ps.conn.db.BindNull(ps.handle, int32(paramIdx))
		if state != duckdb.DuckDBSuccess {
			return ErrDuckDB{Message: "Failed to bind NULL parameter"}
		}
		return nil
	}

	// Use DuckDB parameter type to guide binding if available
	if paramType == duckdb.DuckDBTypeInvalid {
		return ErrDuckDB{Message: "Parameter type is invalid"}
	}

	err := duckdb.BindParameter(ps.conn.db, ps.handle, paramIdx, value, paramType)
	if err != nil {
		return ErrDuckDB{Message: fmt.Sprintf("Failed to bind parameter: %v", err)}
	}

	return nil
}

// Execute executes a prepared statement with bound parameters
func (ps *PreparedStatement) Execute() (*DuckDBResult, error) {
	if ps.handle == nil {
		return nil, ErrDuckDB{Message: "Prepared statement is closed"}
	}

	if ps.conn.db.ExecutePrepared == nil {
		return nil, ErrDuckDB{Message: "Execute prepared function not available"}
	}

	var rawResult duckdb.DuckDBResultRaw
	state := ps.conn.db.ExecutePrepared(ps.handle, &rawResult)
	if state != duckdb.DuckDBSuccess {
		// Get error message if possible
		if ps.conn.db.ResultError != nil {
			errMsg := duckdb.GoString(ps.conn.db.ResultError(&rawResult))
			return nil, ErrDuckDB{Message: "Failed to execute prepared statement: " + errMsg}
		}
		return nil, ErrDuckDB{Message: "Failed to execute prepared statement"}
	}

	internalResult := duckdb.NewResult(ps.conn.db, rawResult)
	result := &DuckDBResult{
		internal: internalResult,
	}

	return result, nil
}
