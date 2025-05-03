// Package duckdb provides internal implementation details for the go-pduckdb driver.
package duckdb

// DuckDBState represents the state returned by DuckDB operations
type DuckDBState int32

const (
	// DuckDBSuccess indicates a successful operation
	DuckDBSuccess DuckDBState = 0
	// DuckDBError indicates a failed operation
	DuckDBError DuckDBState = 1
)

type DuckDBErrorType int32

const (
	// DuckDBErrorInvalid represents an invalid error
	DuckDBErrorInvalid DuckDBErrorType = 0
	// DuckDBErrorOutOfRange represents an out of range error
	DuckDBErrorOutOfRange DuckDBErrorType = 1
	// DuckDBErrorConversion represents a conversion error
	DuckDBErrorConversion DuckDBErrorType = 2
	// DuckDBErrorUnknownType represents an unknown type error
	DuckDBErrorUnknownType DuckDBErrorType = 3
	// DuckDBErrorDecimal represents a decimal error
	DuckDBErrorDecimal DuckDBErrorType = 4
	// DuckDBErrorMismatchType represents a type mismatch error
	DuckDBErrorMismatchType DuckDBErrorType = 5
	// DuckDBErrorDivideByZero represents a divide by zero error
	DuckDBErrorDivideByZero DuckDBErrorType = 6
	// DuckDBErrorObjectSize represents an object size error
	DuckDBErrorObjectSize DuckDBErrorType = 7
	// DuckDBErrorInvalidType represents an invalid type error
	DuckDBErrorInvalidType DuckDBErrorType = 8
	// DuckDBErrorSerialization represents a serialization error
	DuckDBErrorSerialization DuckDBErrorType = 9
	// DuckDBErrorTransaction represents a transaction error
	DuckDBErrorTransaction DuckDBErrorType = 10
	// DuckDBErrorNotImplemented represents a not implemented error
	DuckDBErrorNotImplemented DuckDBErrorType = 11
	// DuckDBErrorExpression represents an expression error
	DuckDBErrorExpression DuckDBErrorType = 12
	// DuckDBErrorCatalog represents a catalog error
	DuckDBErrorCatalog DuckDBErrorType = 13
	// DuckDBErrorParser represents a parser error
	DuckDBErrorParser DuckDBErrorType = 14
	// DuckDBErrorPlanner represents a planner error
	DuckDBErrorPlanner DuckDBErrorType = 15
	// DuckDBErrorScheduler represents a scheduler error
	DuckDBErrorScheduler DuckDBErrorType = 16
	// DuckDBErrorExecutor represents an executor error
	DuckDBErrorExecutor DuckDBErrorType = 17
	// DuckDBErrorConstraint represents a constraint error
	DuckDBErrorConstraint DuckDBErrorType = 18
	// DuckDBErrorIndex represents an index error
	DuckDBErrorIndex DuckDBErrorType = 19
	// DuckDBErrorStat represents a stat error
	DuckDBErrorStat DuckDBErrorType = 20
	// DuckDBErrorConnection represents a connection error
	DuckDBErrorConnection DuckDBErrorType = 21
	// DuckDBErrorSyntax represents a syntax error
	DuckDBErrorSyntax DuckDBErrorType = 22
	// DuckDBErrorSettings represents a settings error
	DuckDBErrorSettings DuckDBErrorType = 23
	// DuckDBErrorBinder represents a binder error
	DuckDBErrorBinder DuckDBErrorType = 24
	// DuckDBErrorNetwork represents a network error
	DuckDBErrorNetwork DuckDBErrorType = 25
	// DuckDBErrorOptimizer represents an optimizer error
	DuckDBErrorOptimizer DuckDBErrorType = 26
	// DuckDBErrorNullPointer represents a null pointer error
	DuckDBErrorNullPointer DuckDBErrorType = 27
	// DuckDBErrorIO represents an IO error
	DuckDBErrorIO DuckDBErrorType = 28
	// DuckDBErrorInterrupt represents an interrupt error
	DuckDBErrorInterrupt DuckDBErrorType = 29
	// DuckDBErrorFatal represents a fatal error
	DuckDBErrorFatal DuckDBErrorType = 30
	// DuckDBErrorInternal represents an internal error
	DuckDBErrorInternal DuckDBErrorType = 31
	// DuckDBErrorInvalidInput represents an invalid input error
	DuckDBErrorInvalidInput DuckDBErrorType = 32
	// DuckDBErrorOutOfMemory represents an out of memory error
	DuckDBErrorOutOfMemory DuckDBErrorType = 33
	// DuckDBErrorPermission represents a permission error
	DuckDBErrorPermission DuckDBErrorType = 34
	// DuckDBErrorParameterNotResolved represents a parameter not resolved error
	DuckDBErrorParameterNotResolved DuckDBErrorType = 35
	// DuckDBErrorParameterNotAllowed represents a parameter not allowed error
	DuckDBErrorParameterNotAllowed DuckDBErrorType = 36
	// DuckDBErrorDependency represents a dependency error
	DuckDBErrorDependency DuckDBErrorType = 37
	// DuckDBErrorHTTP represents an HTTP error
	DuckDBErrorHTTP DuckDBErrorType = 38
	// DuckDBErrorMissingExtension represents a missing extension error
	DuckDBErrorMissingExtension DuckDBErrorType = 39
	// DuckDBErrorAutoload represents an autoload error
	DuckDBErrorAutoload DuckDBErrorType = 40
	// DuckDBErrorSequence represents a sequence error
	DuckDBErrorSequence DuckDBErrorType = 41
	// DuckDBInvalidConfiguration represents an invalid configuration error
	DuckDBInvalidConfiguration DuckDBErrorType = 42
)

// DuckDBResultRaw is the raw C structure for DuckDB query results
type DuckDBResultRaw struct {
	DeprecatedColumnCount  int64
	DeprecatedRowCount     int64
	DeprecatedRowsChanged  int64
	DeprecatedColumns      uintptr
	DeprecatedErrorMessage *byte
	InternalData           uintptr
}

// DuckDBType represents the type of a DuckDB value
type DuckDBType int32

const (
	DuckDBTypeInvalid        DuckDBType = 0
	DuckDBTypeBoolean        DuckDBType = 1
	DuckDBTypeTinyint        DuckDBType = 2
	DuckDBTypeSmallint       DuckDBType = 3
	DuckDBTypeInteger        DuckDBType = 4
	DuckDBTypeBigint         DuckDBType = 5
	DuckDBTypeUTinyint       DuckDBType = 6
	DuckDBTypeUSmallint      DuckDBType = 7
	DuckDBTypeUInteger       DuckDBType = 8
	DuckDBTypeUBigint        DuckDBType = 9
	DuckDBTypeFloat          DuckDBType = 10
	DuckDBTypeDouble         DuckDBType = 11
	DuckDBTypeTimestamp      DuckDBType = 12
	DuckDBTypeDate           DuckDBType = 13
	DuckDBTypeTime           DuckDBType = 14
	DuckDBTypeInterval       DuckDBType = 15
	DuckDBTypeHugeint        DuckDBType = 16
	DuckDBTypeUHugeint       DuckDBType = 32
	DuckDBTypeVarchar        DuckDBType = 17
	DuckDBTypeBlob           DuckDBType = 18
	DuckDBTypeDecimal        DuckDBType = 19
	DuckDBTypeTimestampS     DuckDBType = 20
	DuckDBTypeTimestampMS    DuckDBType = 21
	DuckDBTypeTimestampNS    DuckDBType = 22
	DuckDBTypeEnum           DuckDBType = 23
	DuckDBTypeList           DuckDBType = 24
	DuckDBTypeStruct         DuckDBType = 25
	DuckDBTypeMap            DuckDBType = 26
	DuckDBTypeArray          DuckDBType = 33
	DuckDBTypeUUID           DuckDBType = 27
	DuckDBTypeUnion          DuckDBType = 28
	DuckDBTypeBit            DuckDBType = 29
	DuckDBTypeTimeTZ         DuckDBType = 30
	DuckDBTypeTimestampTZ    DuckDBType = 31
	DuckDBTypeAny            DuckDBType = 34
	DuckDBTypeVarInt         DuckDBType = 35
	DuckDBTypeSQLNull        DuckDBType = 36
	DuckDBTypeStringLiteral  DuckDBType = 37
	DuckDBTypeIntegerLiteral DuckDBType = 38
)

// String returns a string representation of the DuckDB type
func (t DuckDBType) String() string {
	switch t {
	case DuckDBTypeInvalid:
		return "INVALID"
	case DuckDBTypeBoolean:
		return "BOOLEAN"
	case DuckDBTypeTinyint:
		return "TINYINT"
	case DuckDBTypeSmallint:
		return "SMALLINT"
	case DuckDBTypeInteger:
		return "INTEGER"
	case DuckDBTypeBigint:
		return "BIGINT"
	case DuckDBTypeUTinyint:
		return "UTINYINT"
	case DuckDBTypeUSmallint:
		return "USMALLINT"
	case DuckDBTypeUInteger:
		return "UINTEGER"
	case DuckDBTypeUBigint:
		return "UBIGINT"
	case DuckDBTypeFloat:
		return "FLOAT"
	case DuckDBTypeDouble:
		return "DOUBLE"
	case DuckDBTypeTimestamp:
		return "TIMESTAMP"
	case DuckDBTypeDate:
		return "DATE"
	case DuckDBTypeTime:
		return "TIME"
	case DuckDBTypeInterval:
		return "INTERVAL"
	case DuckDBTypeHugeint:
		return "HUGEINT"
	case DuckDBTypeUHugeint:
		return "UHUGEINT"
	case DuckDBTypeVarchar:
		return "VARCHAR"
	case DuckDBTypeBlob:
		return "BLOB"
	case DuckDBTypeDecimal:
		return "DECIMAL"
	case DuckDBTypeTimestampS:
		return "TIMESTAMP_S"
	case DuckDBTypeTimestampMS:
		return "TIMESTAMP_MS"
	case DuckDBTypeTimestampNS:
		return "TIMESTAMP_NS"
	case DuckDBTypeEnum:
		return "ENUM"
	case DuckDBTypeList:
		return "LIST"
	case DuckDBTypeStruct:
		return "STRUCT"
	case DuckDBTypeMap:
		return "MAP"
	case DuckDBTypeArray:
		return "ARRAY"
	case DuckDBTypeUUID:
		return "UUID"
	case DuckDBTypeUnion:
		return "UNION"
	case DuckDBTypeBit:
		return "BIT"
	case DuckDBTypeTimeTZ:
		return "TIME WITH TIME ZONE"
	case DuckDBTypeTimestampTZ:
		return "TIMESTAMP WITH TIME ZONE"
	case DuckDBTypeAny:
		return "ANY"
	case DuckDBTypeVarInt:
		return "VARINT"
	case DuckDBTypeSQLNull:
		return "SQLNULL"
	case DuckDBTypeStringLiteral:
		return "STRING_LITERAL"
	case DuckDBTypeIntegerLiteral:
		return "INTEGER_LITERAL"
	default:
		return "UNKNOWN"
	}
}

// DuckDBStatementType represents the type of a DuckDB statement
type DuckDBStatementType int32

const (
	DuckDBStatementTypeInvalid     DuckDBStatementType = 0
	DuckDBStatementTypeSelect      DuckDBStatementType = 1
	DuckDBStatementTypeInsert      DuckDBStatementType = 2
	DuckDBStatementTypeUpdate      DuckDBStatementType = 3
	DuckDBStatementTypeExplain     DuckDBStatementType = 4
	DuckDBStatementTypeDelete      DuckDBStatementType = 5
	DuckDBStatementTypePrepare     DuckDBStatementType = 6
	DuckDBStatementTypeCreate      DuckDBStatementType = 7
	DuckDBStatementTypeExecute     DuckDBStatementType = 8
	DuckDBStatementTypeAlter       DuckDBStatementType = 9
	DuckDBStatementTypeTransaction DuckDBStatementType = 10
	DuckDBStatementTypeCopy        DuckDBStatementType = 11
	DuckDBStatementTypeAnalyze     DuckDBStatementType = 12
	DuckDBStatementTypeVariableSet DuckDBStatementType = 13
	DuckDBStatementTypeCreateFunc  DuckDBStatementType = 14
	DuckDBStatementTypeDrop        DuckDBStatementType = 15
	DuckDBStatementTypeExport      DuckDBStatementType = 16
	DuckDBStatementTypePragma      DuckDBStatementType = 17
	DuckDBStatementTypeVacuum      DuckDBStatementType = 18
	DuckDBStatementTypeCall        DuckDBStatementType = 19
	DuckDBStatementTypeSet         DuckDBStatementType = 20
	DuckDBStatementTypeLoad        DuckDBStatementType = 21
	DuckDBStatementTypeRelation    DuckDBStatementType = 22
	DuckDBStatementTypeExtension   DuckDBStatementType = 23
	DuckDBStatementTypeLogicalPlan DuckDBStatementType = 24
	DuckDBStatementTypeAttach      DuckDBStatementType = 25
	DuckDBStatementTypeDetach      DuckDBStatementType = 26
	DuckDBStatementTypeMulti       DuckDBStatementType = 27
)

// String returns a string representation of the DuckDB statement type
func (t DuckDBStatementType) String() string {
	switch t {
	case DuckDBStatementTypeInvalid:
		return "INVALID"
	case DuckDBStatementTypeSelect:
		return "SELECT"
	case DuckDBStatementTypeInsert:
		return "INSERT"
	case DuckDBStatementTypeUpdate:
		return "UPDATE"
	case DuckDBStatementTypeExplain:
		return "EXPLAIN"
	case DuckDBStatementTypeDelete:
		return "DELETE"
	case DuckDBStatementTypePrepare:
		return "PREPARE"
	case DuckDBStatementTypeCreate:
		return "CREATE"
	case DuckDBStatementTypeExecute:
		return "EXECUTE"
	case DuckDBStatementTypeAlter:
		return "ALTER"
	case DuckDBStatementTypeTransaction:
		return "TRANSACTION"
	case DuckDBStatementTypeCopy:
		return "COPY"
	case DuckDBStatementTypeAnalyze:
		return "ANALYZE"
	case DuckDBStatementTypeVariableSet:
		return "VARIABLE_SET"
	case DuckDBStatementTypeCreateFunc:
		return "CREATE_FUNCTION"
	case DuckDBStatementTypeDrop:
		return "DROP"
	case DuckDBStatementTypeExport:
		return "EXPORT"
	case DuckDBStatementTypePragma:
		return "PRAGMA"
	case DuckDBStatementTypeVacuum:
		return "VACUUM"
	case DuckDBStatementTypeCall:
		return "CALL"
	case DuckDBStatementTypeSet:
		return "SET"
	case DuckDBStatementTypeLoad:
		return "LOAD"
	case DuckDBStatementTypeRelation:
		return "RELATION"
	case DuckDBStatementTypeExtension:
		return "EXTENSION"
	case DuckDBStatementTypeLogicalPlan:
		return "LOGICAL_PLAN"
	case DuckDBStatementTypeAttach:
		return "ATTACH"
	case DuckDBStatementTypeDetach:
		return "DETACH"
	case DuckDBStatementTypeMulti:
		return "MULTI"
	default:
		return "UNKNOWN"
	}
}
