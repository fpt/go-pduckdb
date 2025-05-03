package duckdb

import (
	"fmt"
	"unsafe"

	"github.com/ebitengine/purego"
)

// DB represents a DuckDB database instance with internal implementation details
type DB struct {
	Handle         *byte
	Lib            uintptr
	Connect        func(*byte, **byte) DuckDBState
	Close          func(**byte)
	Query          func(*byte, string, *DuckDBResultRaw) DuckDBState
	ColumnCount    func(*DuckDBResultRaw) int32
	RowCount       func(*DuckDBResultRaw) int64
	ColumnName     func(*DuckDBResultRaw, int32) *byte
	ValueString    func(*DuckDBResultRaw, int64, int32) *byte
	ValueDate      func(*DuckDBResultRaw, int64, int32) int32
	ValueTime      func(*DuckDBResultRaw, int64, int32) int64
	ValueTimestamp func(*DuckDBResultRaw, int64, int32) int64
	// Additional value functions
	ValueBoolean  func(*DuckDBResultRaw, int64, int32) bool
	ValueInt8     func(*DuckDBResultRaw, int64, int32) int8
	ValueInt16    func(*DuckDBResultRaw, int64, int32) int16
	ValueInt32    func(*DuckDBResultRaw, int64, int32) int32
	ValueInt64    func(*DuckDBResultRaw, int64, int32) int64
	ValueUint8    func(*DuckDBResultRaw, int64, int32) uint8
	ValueUint16   func(*DuckDBResultRaw, int64, int32) uint16
	ValueUint32   func(*DuckDBResultRaw, int64, int32) uint32
	ValueUint64   func(*DuckDBResultRaw, int64, int32) uint64
	ValueFloat    func(*DuckDBResultRaw, int64, int32) float32
	ValueDouble   func(*DuckDBResultRaw, int64, int32) float64
	ValueVarchar  func(*DuckDBResultRaw, int64, int32) *byte
	ValueNull     func(*DuckDBResultRaw, int64, int32) bool
	DestroyResult func(*DuckDBResultRaw)

	// Prepared statement functions
	Prepare         func(*byte, *byte, *unsafe.Pointer) DuckDBState
	DestroyPrepared func(*unsafe.Pointer)
	ExecutePrepared func(unsafe.Pointer, *DuckDBResultRaw) DuckDBState
	NumParams       func(unsafe.Pointer) int64
	PrepareError    func(unsafe.Pointer) *byte
	// Additional prepared statement functions
	ParameterName    func(unsafe.Pointer, int64) *byte
	ParamType        func(unsafe.Pointer, int64) DuckDBType
	ParamLogicalType func(unsafe.Pointer, int64) unsafe.Pointer
	ClearBindings    func(unsafe.Pointer) DuckDBState
	StatementType    func(unsafe.Pointer) int32

	// Parameter binding functions
	BindNull      func(unsafe.Pointer, int32) DuckDBState
	BindBoolean   func(unsafe.Pointer, int32, bool) DuckDBState
	BindInt8      func(unsafe.Pointer, int32, int8) DuckDBState
	BindInt16     func(unsafe.Pointer, int32, int16) DuckDBState
	BindInt32     func(unsafe.Pointer, int32, int32) DuckDBState
	BindInt64     func(unsafe.Pointer, int32, int64) DuckDBState
	BindUint8     func(unsafe.Pointer, int32, uint8) DuckDBState
	BindUint16    func(unsafe.Pointer, int32, uint16) DuckDBState
	BindUint32    func(unsafe.Pointer, int32, uint32) DuckDBState
	BindUint64    func(unsafe.Pointer, int32, uint64) DuckDBState
	BindFloat     func(unsafe.Pointer, int32, float32) DuckDBState
	BindDouble    func(unsafe.Pointer, int32, float64) DuckDBState
	BindVarchar   func(unsafe.Pointer, int32, *byte) DuckDBState
	BindBlob      func(unsafe.Pointer, int32, unsafe.Pointer, int64) DuckDBState
	BindDate      func(unsafe.Pointer, int32, int32) DuckDBState
	BindTime      func(unsafe.Pointer, int32, int64) DuckDBState
	BindTimestamp func(unsafe.Pointer, int32, int64) DuckDBState
	// BindInterval is not supported due to purego limitations

	// Error handling
	ResultError func(*DuckDBResultRaw) *byte
}

// NewDB creates a new internal database instance
func NewDB(path string) (*DB, error) {
	db := &DB{}

	// Load DuckDB library
	lib, err := LoadDuckDBLibrary()
	if err != nil {
		return nil, fmt.Errorf("failed to load DuckDB library: %w", err)
	}
	db.Lib = lib

	// Register DuckDB functions
	var open func(path string, out **byte) DuckDBState
	purego.RegisterLibFunc(&open, lib, "duckdb_open")
	purego.RegisterLibFunc(&db.Connect, lib, "duckdb_connect")
	purego.RegisterLibFunc(&db.Close, lib, "duckdb_close")
	purego.RegisterLibFunc(&db.Query, lib, "duckdb_query")
	purego.RegisterLibFunc(&db.ColumnCount, lib, "duckdb_column_count")
	purego.RegisterLibFunc(&db.RowCount, lib, "duckdb_row_count")
	purego.RegisterLibFunc(&db.ColumnName, lib, "duckdb_column_name")
	purego.RegisterLibFunc(&db.ValueString, lib, "duckdb_value_string")

	// Register date and time functions
	purego.RegisterLibFunc(&db.ValueDate, lib, "duckdb_value_date")
	purego.RegisterLibFunc(&db.ValueTime, lib, "duckdb_value_time")
	purego.RegisterLibFunc(&db.ValueTimestamp, lib, "duckdb_value_timestamp")

	// Register additional value functions
	purego.RegisterLibFunc(&db.ValueBoolean, lib, "duckdb_value_boolean")
	purego.RegisterLibFunc(&db.ValueInt8, lib, "duckdb_value_int8")
	purego.RegisterLibFunc(&db.ValueInt16, lib, "duckdb_value_int16")
	purego.RegisterLibFunc(&db.ValueInt32, lib, "duckdb_value_int32")
	purego.RegisterLibFunc(&db.ValueInt64, lib, "duckdb_value_int64")
	purego.RegisterLibFunc(&db.ValueUint8, lib, "duckdb_value_uint8")
	purego.RegisterLibFunc(&db.ValueUint16, lib, "duckdb_value_uint16")
	purego.RegisterLibFunc(&db.ValueUint32, lib, "duckdb_value_uint32")
	purego.RegisterLibFunc(&db.ValueUint64, lib, "duckdb_value_uint64")
	purego.RegisterLibFunc(&db.ValueFloat, lib, "duckdb_value_float")
	purego.RegisterLibFunc(&db.ValueDouble, lib, "duckdb_value_double")
	purego.RegisterLibFunc(&db.ValueVarchar, lib, "duckdb_value_varchar")
	// duckdb_value_blob is not supported due to purego limitations
	// duckdb_value_interval is not supported due to purego limitations
	// purego: struct return values only supported on darwin arm64 & amd64
	purego.RegisterLibFunc(&db.ValueNull, lib, "duckdb_value_is_null")

	purego.RegisterLibFunc(&db.DestroyResult, lib, "duckdb_destroy_result")

	// Register prepared statement functions
	purego.RegisterLibFunc(&db.Prepare, lib, "duckdb_prepare")
	purego.RegisterLibFunc(&db.DestroyPrepared, lib, "duckdb_destroy_prepare")
	purego.RegisterLibFunc(&db.ExecutePrepared, lib, "duckdb_execute_prepared")
	purego.RegisterLibFunc(&db.NumParams, lib, "duckdb_nparams")
	purego.RegisterLibFunc(&db.PrepareError, lib, "duckdb_prepare_error")
	purego.RegisterLibFunc(&db.ParameterName, lib, "duckdb_parameter_name")
	purego.RegisterLibFunc(&db.ParamType, lib, "duckdb_param_type")
	purego.RegisterLibFunc(&db.ParamLogicalType, lib, "duckdb_param_logical_type")
	purego.RegisterLibFunc(&db.ClearBindings, lib, "duckdb_clear_bindings")
	purego.RegisterLibFunc(&db.StatementType, lib, "duckdb_prepared_statement_type")

	// Register parameter binding functions
	purego.RegisterLibFunc(&db.BindNull, lib, "duckdb_bind_null")
	purego.RegisterLibFunc(&db.BindBoolean, lib, "duckdb_bind_boolean")
	purego.RegisterLibFunc(&db.BindInt8, lib, "duckdb_bind_int8")
	purego.RegisterLibFunc(&db.BindInt16, lib, "duckdb_bind_int16")
	purego.RegisterLibFunc(&db.BindInt32, lib, "duckdb_bind_int32")
	purego.RegisterLibFunc(&db.BindInt64, lib, "duckdb_bind_int64")
	purego.RegisterLibFunc(&db.BindUint8, lib, "duckdb_bind_uint8")
	purego.RegisterLibFunc(&db.BindUint16, lib, "duckdb_bind_uint16")
	purego.RegisterLibFunc(&db.BindUint32, lib, "duckdb_bind_uint32")
	purego.RegisterLibFunc(&db.BindUint64, lib, "duckdb_bind_uint64")
	purego.RegisterLibFunc(&db.BindFloat, lib, "duckdb_bind_float")
	purego.RegisterLibFunc(&db.BindDouble, lib, "duckdb_bind_double")
	purego.RegisterLibFunc(&db.BindVarchar, lib, "duckdb_bind_varchar")
	purego.RegisterLibFunc(&db.BindBlob, lib, "duckdb_bind_blob")
	purego.RegisterLibFunc(&db.BindDate, lib, "duckdb_bind_date")
	purego.RegisterLibFunc(&db.BindTime, lib, "duckdb_bind_time")
	purego.RegisterLibFunc(&db.BindTimestamp, lib, "duckdb_bind_timestamp")

	// Register error handling function
	purego.RegisterLibFunc(&db.ResultError, lib, "duckdb_result_error")

	// Open database
	var handle *byte
	state := open(path, &handle)
	if state != DuckDBSuccess {
		return nil, fmt.Errorf("failed to open database: %s", path)
	}
	db.Handle = handle

	return db, nil
}

// CloseDB closes the database and releases resources
func (db *DB) CloseDB() {
	db.Close(&db.Handle)
}
