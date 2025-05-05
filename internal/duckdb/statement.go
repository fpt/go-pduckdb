// Package duckdb provides internal implementation details for the go-pduckdb driver.
package duckdb

import (
	"encoding/json"
	"fmt"
	"math"
	"unsafe"

	"github.com/fpt/go-pduckdb/internal/convert"
	"github.com/fpt/go-pduckdb/types"
)

var ErrBindParameter = fmt.Errorf("failed to bind parameter")

// DuckDBPreparedStatement represents a DuckDB prepared statement
type DuckDBPreparedStatement unsafe.Pointer

func BindParameter(
	db *DB,
	ps DuckDBPreparedStatement,
	paramIdx int,
	value any,
	paramType DuckDBType,
) error {
	var state DuckDBState
	idx := int32(paramIdx)

	switch paramType {
	case DuckDBTypeBoolean:
		// Convert to boolean
		boolVal, err := convert.ToBoolean(value)
		if err != nil {
			return fmt.Errorf("failed to convert value to BOOLEAN: %v", err)
		}
		if db.BindBoolean != nil {
			state = db.BindBoolean(ps, idx, boolVal)
		} else if db.BindInt32 != nil {
			intVal := int32(0)
			if boolVal {
				intVal = 1
			}
			state = db.BindInt32(ps, idx, intVal)
		} else if db.BindInt64 != nil {
			intVal := int64(0)
			if boolVal {
				intVal = 1
			}
			state = db.BindInt64(ps, idx, intVal)
		} else {
			return fmt.Errorf("no suitable bind function available for BOOLEAN")
		}

	case DuckDBTypeTinyint:
		// Convert to int8
		intVal, err := convert.ToInt8(value)
		if err != nil {
			return fmt.Errorf("failed to convert value to TINYINT: %v", err)
		}
		if db.BindInt8 != nil {
			state = db.BindInt8(ps, idx, intVal)
		} else if db.BindInt32 != nil {
			state = db.BindInt32(ps, idx, int32(intVal))
		} else if db.BindInt64 != nil {
			state = db.BindInt64(ps, idx, int64(intVal))
		} else {
			return fmt.Errorf("no suitable bind function available for TINYINT")
		}

	case DuckDBTypeSmallint:
		// Convert to int16
		intVal, err := convert.ToInt16(value)
		if err != nil {
			return fmt.Errorf("failed to convert value to SMALLINT: %v", err)
		}
		if db.BindInt16 != nil {
			state = db.BindInt16(ps, idx, intVal)
		} else if db.BindInt32 != nil {
			state = db.BindInt32(ps, idx, int32(intVal))
		} else if db.BindInt64 != nil {
			state = db.BindInt64(ps, idx, int64(intVal))
		} else {
			return fmt.Errorf("no suitable bind function available for SMALLINT")
		}

	case DuckDBTypeInteger:
		// Convert to int32
		intVal, err := convert.ToInt32(value)
		if err != nil {
			return fmt.Errorf("failed to convert value to INTEGER: %v", err)
		}
		if db.BindInt32 != nil {
			state = db.BindInt32(ps, idx, intVal)
		} else if db.BindInt64 != nil {
			state = db.BindInt64(ps, idx, int64(intVal))
		} else {
			return fmt.Errorf("no suitable bind function available for INTEGER")
		}

	case DuckDBTypeBigint:
		// Convert to int64
		intVal, err := convert.ToInt64(value)
		if err != nil {
			return fmt.Errorf("failed to convert value to BIGINT: %v", err)
		}
		if db.BindInt64 != nil {
			state = db.BindInt64(ps, idx, intVal)
		} else {
			return fmt.Errorf("no suitable bind function available for BIGINT")
		}

	case DuckDBTypeUTinyint:
		// Convert to uint8
		uintVal, err := convert.ToUint8(value)
		if err != nil {
			return fmt.Errorf("failed to convert value to UTINYINT: %v", err)
		}
		if db.BindUint8 != nil {
			state = db.BindUint8(ps, idx, uintVal)
		} else if db.BindInt32 != nil {
			state = db.BindInt32(ps, idx, int32(uintVal))
		} else if db.BindInt64 != nil {
			state = db.BindInt64(ps, idx, int64(uintVal))
		} else {
			return fmt.Errorf("no suitable bind function available for UTINYINT")
		}

	case DuckDBTypeUSmallint:
		// Convert to uint16
		uintVal, err := convert.ToUint16(value)
		if err != nil {
			return fmt.Errorf("failed to convert value to USMALLINT: %v", err)
		}
		if db.BindUint16 != nil {
			state = db.BindUint16(ps, idx, uintVal)
		} else if db.BindInt32 != nil && int32(uintVal) >= 0 {
			state = db.BindInt32(ps, idx, int32(uintVal))
		} else if db.BindInt64 != nil {
			state = db.BindInt64(ps, idx, int64(uintVal))
		} else {
			return fmt.Errorf("no suitable bind function available for USMALLINT")
		}

	case DuckDBTypeUInteger:
		// Convert to uint32
		uintVal, err := convert.ToUint32(value)
		if err != nil {
			return fmt.Errorf("failed to convert value to UINTEGER: %v", err)
		}
		if db.BindUint32 != nil {
			state = db.BindUint32(ps, idx, uintVal)
		} else if db.BindInt64 != nil && int64(uintVal) >= 0 {
			state = db.BindInt64(ps, idx, int64(uintVal))
		} else {
			return fmt.Errorf("no suitable bind function available for UINTEGER")
		}

	case DuckDBTypeUBigint:
		// Convert to uint64
		uintVal, err := convert.ToUint64(value)
		if err != nil {
			return fmt.Errorf("failed to convert value to UBIGINT: %v", err)
		}
		if db.BindUint64 != nil {
			state = db.BindUint64(ps, idx, uintVal)
		} else if db.BindInt64 != nil && uintVal <= uint64(math.MaxInt64) {
			state = db.BindInt64(ps, idx, int64(uintVal))
		} else {
			return fmt.Errorf("no suitable bind function available for UBIGINT")
		}

	case DuckDBTypeFloat:
		// Convert to float32
		floatVal, err := convert.ToFloat32(value)
		if err != nil {
			return fmt.Errorf("failed to convert value to FLOAT: %v", err)
		}
		if db.BindFloat != nil {
			state = db.BindFloat(ps, idx, floatVal)
		} else if db.BindDouble != nil {
			state = db.BindDouble(ps, idx, float64(floatVal))
		} else {
			return fmt.Errorf("no suitable bind function available for FLOAT")
		}

	case DuckDBTypeDouble:
		// Convert to float64
		doubleVal, err := convert.ToFloat64(value)
		if err != nil {
			return fmt.Errorf("failed to convert value to DOUBLE: %v", err)
		}
		if db.BindDouble != nil {
			state = db.BindDouble(ps, idx, doubleVal)
		} else {
			return fmt.Errorf("no suitable bind function available for DOUBLE")
		}

	case DuckDBTypeVarchar:
		// Convert to string
		strVal, err := convert.ToString(value)
		if err != nil {
			return fmt.Errorf("failed to convert value to VARCHAR: %v", err)
		}
		if db.BindVarchar != nil {
			cStr := ToCString(strVal)
			defer FreeCString(cStr)
			state = db.BindVarchar(ps, idx, cStr)
		} else {
			return fmt.Errorf("no suitable bind function available for VARCHAR")
		}

	case DuckDBTypeBlob:
		// DuckDBTypeBlob is not supported.
		return fmt.Errorf("blob type is not supported")

	case DuckDBTypeDate:
		// Convert to Date
		dateVal, err := convert.ToDate(value)
		if err != nil {
			return fmt.Errorf("failed to convert value to DATE: %v", err)
		}
		if db.BindDate != nil {
			state = db.BindDate(ps, idx, int32(dateVal.Days))
		} else if db.BindVarchar != nil {
			dateStr := dateVal.ToTime().Format("2006-01-02")
			cStr := ToCString(dateStr)
			defer FreeCString(cStr)
			state = db.BindVarchar(ps, idx, cStr)
		} else {
			return fmt.Errorf("no suitable bind function available for DATE")
		}

	case DuckDBTypeTime:
		// Convert to Time
		timeVal, err := convert.ToTime(value)
		if err != nil {
			return fmt.Errorf("failed to convert value to TIME: %v", err)
		}
		if db.BindTime != nil {
			state = db.BindTime(ps, idx, timeVal.Micros)
		} else if db.BindVarchar != nil {
			timeStr := timeVal.ToTime().Format("15:04:05.999999")
			cStr := ToCString(timeStr)
			defer FreeCString(cStr)
			state = db.BindVarchar(ps, idx, cStr)
		} else {
			return fmt.Errorf("no suitable bind function available for TIME")
		}

	case DuckDBTypeTimestamp:
		// Convert to timestamp (time.Time)
		timestampVal, err := convert.ToTimestamp(value)
		if err != nil {
			return fmt.Errorf("failed to convert value to TIMESTAMP: %v", err)
		}
		if db.BindTimestamp != nil {
			// Convert to DuckDB timestamp (microseconds since epoch)
			micros := timestampVal.UnixNano() / 1000
			state = db.BindTimestamp(ps, idx, micros)
		} else if db.BindVarchar != nil {
			// Fall back to string representation
			timestampStr := timestampVal.Format("2006-01-02 15:04:05.999999")
			cStr := ToCString(timestampStr)
			defer FreeCString(cStr)
			state = db.BindVarchar(ps, idx, cStr)
		} else {
			return fmt.Errorf("no suitable bind function available for TIMESTAMP")
		}

	case DuckDBTypeInterval:
		// DuckDBTypeInterval is not supported due to purego limitations
		return fmt.Errorf("interval type is not supported")

	// For types where we have limited support, fall back to string representation
	case DuckDBTypeDecimal:
		// Convert to double - DuckDB uses double internally for DECIMAL
		doubleVal, err := convert.ToFloat64(value)
		if err != nil {
			return fmt.Errorf("failed to convert value to DECIMAL: %v", err)
		}

		if db.BindDouble != nil {
			state = db.BindDouble(ps, idx, doubleVal)
		} else if db.BindVarchar != nil {
			// If bind_double is not available, fall back to string representation
			// Format with high precision to preserve decimal places
			decimalStr := fmt.Sprintf("%.15g", doubleVal)
			cStr := ToCString(decimalStr)
			defer FreeCString(cStr)
			state = db.BindVarchar(ps, idx, cStr)
		} else {
			return fmt.Errorf("no suitable bind function available for DECIMAL")
		}

	// For complex types, fall back to JSON representation
	case DuckDBTypeMap:
		jsonObj, err := marshalToJSON(value)
		if err != nil {
			return fmt.Errorf("failed to marshal JSON: %v", err)
		}
		if db.BindVarchar != nil {
			cStr := ToCString(jsonObj.String())
			defer FreeCString(cStr)
			state = db.BindVarchar(ps, idx, cStr)
		} else {
			return fmt.Errorf("no suitable bind function available for JSON/complex types")
		}

	case DuckDBTypeList:
		return fmt.Errorf("list type is not supported")

	case DuckDBTypeStruct:
		return fmt.Errorf("struct type is not supported")

	default:
		return fmt.Errorf("unsupported parameter type: %s", paramType)
	}

	if state != DuckDBSuccess {
		return fmt.Errorf("failed to bind parameter of type %s", paramType)
	}

	return nil
}

// marshalToJSON converts any value to a JSON object
func marshalToJSON(value any) (*types.JSON, error) {
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return types.NewJSON(string(jsonBytes)), nil
}
