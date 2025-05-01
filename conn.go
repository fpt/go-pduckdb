package pduckdb

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/fpt/go-pduckdb/internal/duckdb"
)

// DuckDBConnection represents a connection to a DuckDB database
type DuckDBConnection struct {
	handle *byte
	db     *duckdb.DB
}

// PreparedStatement represents a DuckDB prepared statement
type PreparedStatement struct {
	handle    unsafe.Pointer
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

	state := c.db.Query(c.handle, sql, &rawResult)
	if state != duckdb.DuckDBSuccess {
		return nil, ErrDuckDB{Message: "Query failed: " + sql}
	}

	internalResult := duckdb.CreateResult(c.db, rawResult)

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

	var stmt unsafe.Pointer
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
	// Note: The DuckDB API doesn't have connection close function
	// The connection is closed when the database is closed
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
	var paramType duckdb.DuckDBType = duckdb.DuckDBTypeInvalid
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
	if paramType != duckdb.DuckDBTypeInvalid {
		return ps.bindWithDuckDBType(paramIdx, value, paramType)
	}

	// Fall back to Go type-based binding if parameter type isn't available
	return ps.bindWithGoType(paramIdx, value)
}

// bindWithDuckDBType binds a parameter value using the DuckDB type information
func (ps *PreparedStatement) bindWithDuckDBType(paramIdx int, value any, paramType duckdb.DuckDBType) error {
	var state duckdb.DuckDBState
	idx := int32(paramIdx)

	switch paramType {
	case duckdb.DuckDBTypeBoolean:
		// Convert to boolean
		boolVal, err := convertToBoolean(value)
		if err != nil {
			return ErrDuckDB{Message: fmt.Sprintf("Failed to convert value to BOOLEAN: %v", err)}
		}
		if ps.conn.db.BindBoolean != nil {
			state = ps.conn.db.BindBoolean(ps.handle, idx, boolVal)
		} else if ps.conn.db.BindInt32 != nil {
			intVal := int32(0)
			if boolVal {
				intVal = 1
			}
			state = ps.conn.db.BindInt32(ps.handle, idx, intVal)
		} else if ps.conn.db.BindInt64 != nil {
			intVal := int64(0)
			if boolVal {
				intVal = 1
			}
			state = ps.conn.db.BindInt64(ps.handle, idx, intVal)
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for BOOLEAN"}
		}

	case duckdb.DuckDBTypeTinyint:
		// Convert to int8
		intVal, err := convertToInt8(value)
		if err != nil {
			return ErrDuckDB{Message: fmt.Sprintf("Failed to convert value to TINYINT: %v", err)}
		}
		if ps.conn.db.BindInt8 != nil {
			state = ps.conn.db.BindInt8(ps.handle, idx, intVal)
		} else if ps.conn.db.BindInt32 != nil {
			state = ps.conn.db.BindInt32(ps.handle, idx, int32(intVal))
		} else if ps.conn.db.BindInt64 != nil {
			state = ps.conn.db.BindInt64(ps.handle, idx, int64(intVal))
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for TINYINT"}
		}

	case duckdb.DuckDBTypeSmallint:
		// Convert to int16
		intVal, err := convertToInt16(value)
		if err != nil {
			return ErrDuckDB{Message: fmt.Sprintf("Failed to convert value to SMALLINT: %v", err)}
		}
		if ps.conn.db.BindInt16 != nil {
			state = ps.conn.db.BindInt16(ps.handle, idx, intVal)
		} else if ps.conn.db.BindInt32 != nil {
			state = ps.conn.db.BindInt32(ps.handle, idx, int32(intVal))
		} else if ps.conn.db.BindInt64 != nil {
			state = ps.conn.db.BindInt64(ps.handle, idx, int64(intVal))
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for SMALLINT"}
		}

	case duckdb.DuckDBTypeInteger:
		// Convert to int32
		intVal, err := convertToInt32(value)
		if err != nil {
			return ErrDuckDB{Message: fmt.Sprintf("Failed to convert value to INTEGER: %v", err)}
		}
		if ps.conn.db.BindInt32 != nil {
			state = ps.conn.db.BindInt32(ps.handle, idx, intVal)
		} else if ps.conn.db.BindInt64 != nil {
			state = ps.conn.db.BindInt64(ps.handle, idx, int64(intVal))
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for INTEGER"}
		}

	case duckdb.DuckDBTypeBigint:
		// Convert to int64
		intVal, err := convertToInt64(value)
		if err != nil {
			return ErrDuckDB{Message: fmt.Sprintf("Failed to convert value to BIGINT: %v", err)}
		}
		if ps.conn.db.BindInt64 != nil {
			state = ps.conn.db.BindInt64(ps.handle, idx, intVal)
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for BIGINT"}
		}

	case duckdb.DuckDBTypeUTinyint:
		// Convert to uint8
		uintVal, err := convertToUint8(value)
		if err != nil {
			return ErrDuckDB{Message: fmt.Sprintf("Failed to convert value to UTINYINT: %v", err)}
		}
		if ps.conn.db.BindUint8 != nil {
			state = ps.conn.db.BindUint8(ps.handle, idx, uintVal)
		} else if ps.conn.db.BindInt32 != nil {
			state = ps.conn.db.BindInt32(ps.handle, idx, int32(uintVal))
		} else if ps.conn.db.BindInt64 != nil {
			state = ps.conn.db.BindInt64(ps.handle, idx, int64(uintVal))
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for UTINYINT"}
		}

	case duckdb.DuckDBTypeUSmallint:
		// Convert to uint16
		uintVal, err := convertToUint16(value)
		if err != nil {
			return ErrDuckDB{Message: fmt.Sprintf("Failed to convert value to USMALLINT: %v", err)}
		}
		if ps.conn.db.BindUint16 != nil {
			state = ps.conn.db.BindUint16(ps.handle, idx, uintVal)
		} else if ps.conn.db.BindInt32 != nil && int32(uintVal) >= 0 {
			state = ps.conn.db.BindInt32(ps.handle, idx, int32(uintVal))
		} else if ps.conn.db.BindInt64 != nil {
			state = ps.conn.db.BindInt64(ps.handle, idx, int64(uintVal))
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for USMALLINT"}
		}

	case duckdb.DuckDBTypeUInteger:
		// Convert to uint32
		uintVal, err := convertToUint32(value)
		if err != nil {
			return ErrDuckDB{Message: fmt.Sprintf("Failed to convert value to UINTEGER: %v", err)}
		}
		if ps.conn.db.BindUint32 != nil {
			state = ps.conn.db.BindUint32(ps.handle, idx, uintVal)
		} else if ps.conn.db.BindInt64 != nil && int64(uintVal) >= 0 {
			state = ps.conn.db.BindInt64(ps.handle, idx, int64(uintVal))
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for UINTEGER"}
		}

	case duckdb.DuckDBTypeUBigint:
		// Convert to uint64
		uintVal, err := convertToUint64(value)
		if err != nil {
			return ErrDuckDB{Message: fmt.Sprintf("Failed to convert value to UBIGINT: %v", err)}
		}
		if ps.conn.db.BindUint64 != nil {
			state = ps.conn.db.BindUint64(ps.handle, idx, uintVal)
		} else if ps.conn.db.BindInt64 != nil && uintVal <= uint64(math.MaxInt64) {
			state = ps.conn.db.BindInt64(ps.handle, idx, int64(uintVal))
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for UBIGINT"}
		}

	case duckdb.DuckDBTypeFloat:
		// Convert to float32
		floatVal, err := convertToFloat32(value)
		if err != nil {
			return ErrDuckDB{Message: fmt.Sprintf("Failed to convert value to FLOAT: %v", err)}
		}
		if ps.conn.db.BindFloat != nil {
			state = ps.conn.db.BindFloat(ps.handle, idx, floatVal)
		} else if ps.conn.db.BindDouble != nil {
			state = ps.conn.db.BindDouble(ps.handle, idx, float64(floatVal))
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for FLOAT"}
		}

	case duckdb.DuckDBTypeDouble:
		// Convert to float64
		doubleVal, err := convertToFloat64(value)
		if err != nil {
			return ErrDuckDB{Message: fmt.Sprintf("Failed to convert value to DOUBLE: %v", err)}
		}
		if ps.conn.db.BindDouble != nil {
			state = ps.conn.db.BindDouble(ps.handle, idx, doubleVal)
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for DOUBLE"}
		}

	case duckdb.DuckDBTypeVarchar:
		// Convert to string
		strVal, err := convertToString(value)
		if err != nil {
			return ErrDuckDB{Message: fmt.Sprintf("Failed to convert value to VARCHAR: %v", err)}
		}
		if ps.conn.db.BindVarchar != nil {
			cStr := duckdb.ToCString(strVal)
			defer duckdb.FreeCString(cStr)
			state = ps.conn.db.BindVarchar(ps.handle, idx, cStr)
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for VARCHAR"}
		}

	case duckdb.DuckDBTypeBlob:
		// Convert to []byte
		blobVal, err := convertToBlob(value)
		if err != nil {
			return ErrDuckDB{Message: fmt.Sprintf("Failed to convert value to BLOB: %v", err)}
		}
		if ps.conn.db.BindBlob != nil {
			if len(blobVal) == 0 {
				state = ps.conn.db.BindBlob(ps.handle, idx, unsafe.Pointer(&[]byte{0}[0]), 0)
			} else {
				state = ps.conn.db.BindBlob(ps.handle, idx, unsafe.Pointer(&blobVal[0]), int64(len(blobVal)))
			}
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for BLOB"}
		}

	case duckdb.DuckDBTypeDate:
		// Convert to Date
		dateVal, err := convertToDate(value)
		if err != nil {
			return ErrDuckDB{Message: fmt.Sprintf("Failed to convert value to DATE: %v", err)}
		}
		if ps.conn.db.BindDate != nil {
			state = ps.conn.db.BindDate(ps.handle, idx, int32(dateVal.Days))
		} else if ps.conn.db.BindVarchar != nil {
			dateStr := dateVal.ToTime().Format("2006-01-02")
			cStr := duckdb.ToCString(dateStr)
			defer duckdb.FreeCString(cStr)
			state = ps.conn.db.BindVarchar(ps.handle, idx, cStr)
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for DATE"}
		}

	case duckdb.DuckDBTypeTime:
		// Convert to Time
		timeVal, err := convertToTime(value)
		if err != nil {
			return ErrDuckDB{Message: fmt.Sprintf("Failed to convert value to TIME: %v", err)}
		}
		if ps.conn.db.BindTime != nil {
			state = ps.conn.db.BindTime(ps.handle, idx, timeVal.Micros)
		} else if ps.conn.db.BindVarchar != nil {
			timeStr := timeVal.ToTime().Format("15:04:05.999999")
			cStr := duckdb.ToCString(timeStr)
			defer duckdb.FreeCString(cStr)
			state = ps.conn.db.BindVarchar(ps.handle, idx, cStr)
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for TIME"}
		}

	case duckdb.DuckDBTypeTimestamp:
		// Convert to timestamp (time.Time)
		timestampVal, err := convertToTimestamp(value)
		if err != nil {
			return ErrDuckDB{Message: fmt.Sprintf("Failed to convert value to TIMESTAMP: %v", err)}
		}
		if ps.conn.db.BindTimestamp != nil {
			// Convert to DuckDB timestamp (microseconds since epoch)
			micros := timestampVal.UnixNano() / 1000
			state = ps.conn.db.BindTimestamp(ps.handle, idx, micros)
		} else if ps.conn.db.BindVarchar != nil {
			// Fall back to string representation
			timestampStr := timestampVal.Format("2006-01-02 15:04:05.999999")
			cStr := duckdb.ToCString(timestampStr)
			defer duckdb.FreeCString(cStr)
			state = ps.conn.db.BindVarchar(ps.handle, idx, cStr)
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for TIMESTAMP"}
		}

	// DuckDBTypeInterval is not supported due to purego limitations

	// For types where we have limited support, fall back to string representation
	case duckdb.DuckDBTypeDecimal:
		// Convert to string for decimal
		strVal, err := convertToString(value)
		if err != nil {
			return ErrDuckDB{Message: fmt.Sprintf("Failed to convert value to DECIMAL string: %v", err)}
		}
		if ps.conn.db.BindVarchar != nil {
			cStr := duckdb.ToCString(strVal)
			defer duckdb.FreeCString(cStr)
			state = ps.conn.db.BindVarchar(ps.handle, idx, cStr)
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for DECIMAL"}
		}

	// For complex types, fall back to JSON representation
	case duckdb.DuckDBTypeMap, duckdb.DuckDBTypeList, duckdb.DuckDBTypeStruct:
		jsonBytes, err := json.Marshal(value)
		if err != nil {
			return ErrDuckDB{Message: fmt.Sprintf("Failed to marshal JSON: %v", err)}
		}
		if ps.conn.db.BindVarchar != nil {
			cStr := duckdb.ToCString(string(jsonBytes))
			defer duckdb.FreeCString(cStr)
			state = ps.conn.db.BindVarchar(ps.handle, idx, cStr)
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for JSON/complex types"}
		}

	default:
		// For unsupported or unknown types, fall back to the Go type-based binding
		return ps.bindWithGoType(paramIdx, value)
	}

	if state != duckdb.DuckDBSuccess {
		return ErrDuckDB{Message: fmt.Sprintf("Failed to bind parameter of type %s", paramType)}
	}

	return nil
}

// bindWithGoType binds a parameter value based on the Go type (original implementation)
func (ps *PreparedStatement) bindWithGoType(paramIdx int, value any) error {
	var state duckdb.DuckDBState
	idx := int32(paramIdx)

	switch v := value.(type) {
	case bool:
		if ps.conn.db.BindBoolean != nil {
			state = ps.conn.db.BindBoolean(ps.handle, idx, v)
		} else if ps.conn.db.BindInt32 != nil {
			val := int32(0)
			if v {
				val = 1
			}
			state = ps.conn.db.BindInt32(ps.handle, idx, val)
		} else if ps.conn.db.BindInt64 != nil {
			val := int64(0)
			if v {
				val = 1
			}
			state = ps.conn.db.BindInt64(ps.handle, idx, val)
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for boolean"}
		}
	case int8:
		if ps.conn.db.BindInt8 != nil {
			state = ps.conn.db.BindInt8(ps.handle, idx, v)
		} else if ps.conn.db.BindInt32 != nil {
			state = ps.conn.db.BindInt32(ps.handle, idx, int32(v))
		} else if ps.conn.db.BindInt64 != nil {
			state = ps.conn.db.BindInt64(ps.handle, idx, int64(v))
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for int8"}
		}
	case int16:
		if ps.conn.db.BindInt16 != nil {
			state = ps.conn.db.BindInt16(ps.handle, idx, v)
		} else if ps.conn.db.BindInt32 != nil {
			state = ps.conn.db.BindInt32(ps.handle, idx, int32(v))
		} else if ps.conn.db.BindInt64 != nil {
			state = ps.conn.db.BindInt64(ps.handle, idx, int64(v))
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for int16"}
		}
	case int32:
		if ps.conn.db.BindInt32 != nil {
			state = ps.conn.db.BindInt32(ps.handle, idx, v)
		} else if ps.conn.db.BindInt64 != nil {
			state = ps.conn.db.BindInt64(ps.handle, idx, int64(v))
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for int32"}
		}
	case int:
		// For standard int, use int32 if in range, otherwise int64
		if ps.conn.db.BindInt32 != nil && v >= math.MinInt32 && v <= math.MaxInt32 {
			state = ps.conn.db.BindInt32(ps.handle, idx, int32(v))
		} else if ps.conn.db.BindInt64 != nil {
			state = ps.conn.db.BindInt64(ps.handle, idx, int64(v))
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for int"}
		}
	case int64:
		if ps.conn.db.BindInt64 != nil {
			state = ps.conn.db.BindInt64(ps.handle, idx, v)
		} else {
			return ErrDuckDB{Message: "BindInt64 function not available"}
		}
	case uint8:
		if ps.conn.db.BindUint8 != nil {
			state = ps.conn.db.BindUint8(ps.handle, idx, v)
		} else if ps.conn.db.BindInt32 != nil {
			state = ps.conn.db.BindInt32(ps.handle, idx, int32(v))
		} else if ps.conn.db.BindInt64 != nil {
			state = ps.conn.db.BindInt64(ps.handle, idx, int64(v))
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for uint8"}
		}
	case uint16:
		if ps.conn.db.BindUint16 != nil {
			state = ps.conn.db.BindUint16(ps.handle, idx, v)
		} else if ps.conn.db.BindInt32 != nil && int32(v) >= 0 {
			state = ps.conn.db.BindInt32(ps.handle, idx, int32(v))
		} else if ps.conn.db.BindInt64 != nil {
			state = ps.conn.db.BindInt64(ps.handle, idx, int64(v))
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for uint16"}
		}
	case uint32:
		if ps.conn.db.BindUint32 != nil {
			state = ps.conn.db.BindUint32(ps.handle, idx, v)
		} else if ps.conn.db.BindInt64 != nil && int64(v) >= 0 {
			state = ps.conn.db.BindInt64(ps.handle, idx, int64(v))
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for uint32"}
		}
	case uint64:
		if ps.conn.db.BindUint64 != nil {
			state = ps.conn.db.BindUint64(ps.handle, idx, v)
		} else if ps.conn.db.BindInt64 != nil && v <= uint64(math.MaxInt64) {
			state = ps.conn.db.BindInt64(ps.handle, idx, int64(v))
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for uint64"}
		}
	case uint:
		if ps.conn.db.BindUint64 != nil {
			state = ps.conn.db.BindUint64(ps.handle, idx, uint64(v))
		} else if ps.conn.db.BindInt64 != nil && v <= uint(math.MaxInt64) {
			state = ps.conn.db.BindInt64(ps.handle, idx, int64(v))
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for uint"}
		}
	case float32:
		if ps.conn.db.BindFloat != nil {
			state = ps.conn.db.BindFloat(ps.handle, idx, v)
		} else if ps.conn.db.BindDouble != nil {
			state = ps.conn.db.BindDouble(ps.handle, idx, float64(v))
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for float32"}
		}
	case float64:
		if ps.conn.db.BindDouble != nil {
			state = ps.conn.db.BindDouble(ps.handle, idx, v)
		} else {
			return ErrDuckDB{Message: "BindDouble function not available"}
		}
	case string:
		if ps.conn.db.BindVarchar != nil {
			cStr := duckdb.ToCString(v)
			defer duckdb.FreeCString(cStr)
			state = ps.conn.db.BindVarchar(ps.handle, idx, cStr)
		} else {
			return ErrDuckDB{Message: "BindVarchar function not available"}
		}
	case []byte:
		if ps.conn.db.BindBlob != nil {
			if len(v) == 0 {
				// Special case for empty blob
				state = ps.conn.db.BindBlob(ps.handle, idx, unsafe.Pointer(&[]byte{0}[0]), 0)
			} else {
				state = ps.conn.db.BindBlob(ps.handle, idx, unsafe.Pointer(&v[0]), int64(len(v)))
			}
		} else {
			return ErrDuckDB{Message: "BindBlob function not available"}
		}
	case time.Time:
		if ps.conn.db.BindTimestamp != nil {
			// Convert to DuckDB timestamp (microseconds since epoch)
			micros := v.UnixNano() / 1000
			state = ps.conn.db.BindTimestamp(ps.handle, idx, micros)
		} else if ps.conn.db.BindVarchar != nil {
			// Fall back to string if timestamp binding not available
			cStr := duckdb.ToCString(v.Format("2006-01-02 15:04:05.999999"))
			defer duckdb.FreeCString(cStr)
			state = ps.conn.db.BindVarchar(ps.handle, idx, cStr)
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for time.Time"}
		}
	case Date:
		if ps.conn.db.BindDate != nil {
			state = ps.conn.db.BindDate(ps.handle, idx, int32(v.Days))
		} else if ps.conn.db.BindVarchar != nil {
			// Fall back to string
			cStr := duckdb.ToCString(v.ToTime().Format("2006-01-02"))
			defer duckdb.FreeCString(cStr)
			state = ps.conn.db.BindVarchar(ps.handle, idx, cStr)
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for Date"}
		}
	case Time:
		if ps.conn.db.BindTime != nil {
			state = ps.conn.db.BindTime(ps.handle, idx, v.Micros)
		} else if ps.conn.db.BindVarchar != nil {
			// Fall back to string
			cStr := duckdb.ToCString(v.ToTime().Format("15:04:05.999999"))
			defer duckdb.FreeCString(cStr)
			state = ps.conn.db.BindVarchar(ps.handle, idx, cStr)
		} else {
			return ErrDuckDB{Message: "No suitable bind function available for Time"}
		}
	// Interval/DuckDBInterval is not supported due to purego limitations
	case JSON:
		// Bind JSON value as a string with JSON type hint
		if ps.conn.db.BindVarchar != nil {
			cStr := duckdb.ToCString(v.Value)
			defer duckdb.FreeCString(cStr)
			state = ps.conn.db.BindVarchar(ps.handle, idx, cStr)
		} else {
			return ErrDuckDB{Message: "BindVarchar function not available for JSON"}
		}
	case map[string]interface{}, []interface{}:
		// Convert Go map/slice to JSON string
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return ErrDuckDB{Message: fmt.Sprintf("Failed to marshal JSON: %v", err)}
		}
		// Bind as string with JSON type hint
		if ps.conn.db.BindVarchar != nil {
			cStr := duckdb.ToCString(string(jsonBytes))
			defer duckdb.FreeCString(cStr)
			state = ps.conn.db.BindVarchar(ps.handle, idx, cStr)
		} else {
			return ErrDuckDB{Message: "BindVarchar function not available for JSON object"}
		}
	default:
		return ErrDuckDB{Message: fmt.Sprintf("Unsupported parameter type: %T", value)}
	}

	if state != duckdb.DuckDBSuccess {
		return ErrDuckDB{Message: "Failed to bind parameter"}
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

	internalResult := duckdb.CreateResult(ps.conn.db, rawResult)
	result := &DuckDBResult{
		internal: internalResult,
	}

	return result, nil
}
