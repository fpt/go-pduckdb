package duckdb

import (
	"fmt"
	"unsafe"

	"github.com/ebitengine/purego"
)

//! DuckDB's index type.
// typedef uint64_t idx_t;

//! Type used for the selection vector
// typedef uint32_t sel_t;

// DB represents a DuckDB database instance with internal implementation details
type DB struct {
	Handle            DuckDBDatabase
	Lib               uintptr
	Connect           func(DuckDBDatabase, *DuckDBConnection) DuckDBState
	Close             func(*DuckDBDatabase)
	Disconnect        func(*DuckDBConnection)
	LibraryVersion    func() *byte
	Query             func(DuckDBConnection, *byte, *DuckDBResultRaw) DuckDBState
	ColumnName        func(*DuckDBResultRaw, int64) *byte
	ColumnType        func(*DuckDBResultRaw, int64) DuckDBType
	ColumnLogicalType func(*DuckDBResultRaw, int64) DuckDBLogicalType
	ColumnCount       func(*DuckDBResultRaw) int64
	RowCount          func(*DuckDBResultRaw) int64
	RowsChanged       func(*DuckDBResultRaw) int64
	ValueString       func(*DuckDBResultRaw, int64, int32) *byte
	ValueDate         func(*DuckDBResultRaw, int64, int32) int32
	ValueTime         func(*DuckDBResultRaw, int64, int32) int64
	ValueTimestamp    func(*DuckDBResultRaw, int64, int32) int64
	// Additional value functions
	ValueBoolean         func(*DuckDBResultRaw, int64, int32) bool
	ValueInt8            func(*DuckDBResultRaw, int64, int32) int8
	ValueInt16           func(*DuckDBResultRaw, int64, int32) int16
	ValueInt32           func(*DuckDBResultRaw, int64, int32) int32
	ValueInt64           func(*DuckDBResultRaw, int64, int32) int64
	ValueUint8           func(*DuckDBResultRaw, int64, int32) uint8
	ValueUint16          func(*DuckDBResultRaw, int64, int32) uint16
	ValueUint32          func(*DuckDBResultRaw, int64, int32) uint32
	ValueUint64          func(*DuckDBResultRaw, int64, int32) uint64
	ValueFloat           func(*DuckDBResultRaw, int64, int32) float32
	ValueDouble          func(*DuckDBResultRaw, int64, int32) float64
	ValueVarchar         func(*DuckDBResultRaw, int64, int32) *byte
	ValueVarcharInternal func(*DuckDBResultRaw, int64, int32) *byte
	ValueNull            func(*DuckDBResultRaw, int64, int32) bool
	ValueListSize        func(*DuckDBResultRaw, int64, int32) int32
	ValueListChild       func(*DuckDBResultRaw, int64, int32) DuckDBValue
	DestroyResult        func(*DuckDBResultRaw)

	// Prepared statement functions
	Prepare         func(DuckDBConnection, *byte, *DuckDBPreparedStatement) DuckDBState
	DestroyPrepared func(*DuckDBPreparedStatement)
	ExecutePrepared func(DuckDBPreparedStatement, *DuckDBResultRaw) DuckDBState
	NumParams       func(DuckDBPreparedStatement) int64
	PrepareError    func(DuckDBPreparedStatement) *byte
	// Additional prepared statement functions
	ParameterName    func(DuckDBPreparedStatement, int64) *byte
	ParamType        func(DuckDBPreparedStatement, int64) DuckDBType
	ParamLogicalType func(DuckDBPreparedStatement, int64) DuckDBLogicalType
	ClearBindings    func(DuckDBPreparedStatement) DuckDBState
	StatementType    func(DuckDBPreparedStatement) int32

	// Parameter binding functions
	BindNull      func(DuckDBPreparedStatement, int32) DuckDBState
	BindBoolean   func(DuckDBPreparedStatement, int32, bool) DuckDBState
	BindInt8      func(DuckDBPreparedStatement, int32, int8) DuckDBState
	BindInt16     func(DuckDBPreparedStatement, int32, int16) DuckDBState
	BindInt32     func(DuckDBPreparedStatement, int32, int32) DuckDBState
	BindInt64     func(DuckDBPreparedStatement, int32, int64) DuckDBState
	BindUint8     func(DuckDBPreparedStatement, int32, uint8) DuckDBState
	BindUint16    func(DuckDBPreparedStatement, int32, uint16) DuckDBState
	BindUint32    func(DuckDBPreparedStatement, int32, uint32) DuckDBState
	BindUint64    func(DuckDBPreparedStatement, int32, uint64) DuckDBState
	BindFloat     func(DuckDBPreparedStatement, int32, float32) DuckDBState
	BindDouble    func(DuckDBPreparedStatement, int32, float64) DuckDBState
	BindVarchar   func(DuckDBPreparedStatement, int32, *byte) DuckDBState
	BindBlob      func(DuckDBPreparedStatement, int32, unsafe.Pointer, int64) DuckDBState
	BindDate      func(DuckDBPreparedStatement, int32, int32) DuckDBState
	BindTime      func(DuckDBPreparedStatement, int32, int64) DuckDBState
	BindTimestamp func(DuckDBPreparedStatement, int32, int64) DuckDBState
	// BindInterval is not supported due to purego limitations

	// Error handling
	ResultError func(*DuckDBResultRaw) *byte

	// Value interface functions
	DestroyValue    func(*DuckDBValue)
	CreateVarchar   func(string) DuckDBValue
	CreateInt32     func(int32) DuckDBValue
	CreateInt64     func(int64) DuckDBValue
	CreateDouble    func(float64) DuckDBValue
	CreateBool      func(bool) DuckDBValue
	CreateListValue func(DuckDBLogicalType, *DuckDBValue, int64) DuckDBValue
	GetListSize     func(DuckDBValue) int64
	GetListChild    func(DuckDBValue, int64) DuckDBValue
	IsNullValue     func(DuckDBValue) bool
	CreateNullValue func() DuckDBValue

	// Data Chunk interface functions
	FetchChunk              func(*DuckDBResultRaw) DuckDBDataChunk
	ResultGetChunk          func(*DuckDBResultRaw, int64) DuckDBDataChunk
	ResultChunkCount        func(*DuckDBResultRaw) int64
	ResultIsStreaming       func(*DuckDBResultRaw) bool
	CreateDataChunk         func(*DuckDBLogicalType, int64) DuckDBDataChunk
	DestroyDataChunk        func(*DuckDBDataChunk)
	DataChunkReset          func(DuckDBDataChunk)
	DataChunkGetColumnCount func(DuckDBDataChunk) int64
	DataChunkGetVector      func(DuckDBDataChunk, int64) DuckDBVector
	DataChunkGetSize        func(DuckDBDataChunk) int64
	DataChunkSetSize        func(DuckDBDataChunk, int64)

	// Vector interface functions
	VectorGetLogicalColumnType   func(DuckDBVector) DuckDBLogicalType
	VectorGetData                func(DuckDBVector) unsafe.Pointer
	VectorGetValidity            func(DuckDBVector) *uint64
	VectorEnsureValidityWritable func(DuckDBVector)
	VectorAssignStringElement    func(DuckDBVector, int64, *byte)
	VectorAssignStringElementLen func(DuckDBVector, int64, *byte, int64)
	ListVectorGetChild           func(DuckDBVector) DuckDBVector
	ListVectorGetSize            func(DuckDBVector) int64
	ListVectorSetSize            func(DuckDBVector, int64) DuckDBState
	ListVectorReserve            func(DuckDBVector, int64) DuckDBState
	StructVectorGetChild         func(DuckDBVector, int64) DuckDBVector
	ArrayVectorGetChild          func(DuckDBVector) DuckDBVector

	// Logical Type interface functions
	CreateLogicalType   func(DuckDBType) DuckDBLogicalType
	LogicalTypeGetAlias func(DuckDBLogicalType) *byte
	CreateListType      func(DuckDBLogicalType) DuckDBLogicalType
	ListTypeChildType   func(DuckDBLogicalType) DuckDBLogicalType
	GetTypeID           func(DuckDBLogicalType) DuckDBType
	DecimalWidth        func(DuckDBLogicalType) uint8
	DecimalScale        func(DuckDBLogicalType) uint8
	DestroyLogicalType  func(*DuckDBLogicalType)
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
	var open func(path string, out *DuckDBDatabase) DuckDBState
	purego.RegisterLibFunc(&open, lib, "duckdb_open")
	purego.RegisterLibFunc(&db.Connect, lib, "duckdb_connect")
	purego.RegisterLibFunc(&db.Close, lib, "duckdb_close")
	purego.RegisterLibFunc(&db.Disconnect, lib, "duckdb_disconnect")
	purego.RegisterLibFunc(&db.LibraryVersion, lib, "duckdb_library_version")
	purego.RegisterLibFunc(&db.Query, lib, "duckdb_query")
	purego.RegisterLibFunc(&db.ColumnName, lib, "duckdb_column_name")
	purego.RegisterLibFunc(&db.ColumnType, lib, "duckdb_column_type")
	purego.RegisterLibFunc(&db.ColumnLogicalType, lib, "duckdb_column_logical_type")
	purego.RegisterLibFunc(&db.ColumnCount, lib, "duckdb_column_count")
	purego.RegisterLibFunc(&db.RowCount, lib, "duckdb_row_count") // WARN: future deprecation
	purego.RegisterLibFunc(&db.RowsChanged, lib, "duckdb_rows_changed")

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
	purego.RegisterLibFunc(&db.ValueDate, lib, "duckdb_value_date")
	purego.RegisterLibFunc(&db.ValueTime, lib, "duckdb_value_time")
	purego.RegisterLibFunc(&db.ValueTimestamp, lib, "duckdb_value_timestamp")
	purego.RegisterLibFunc(&db.ValueVarchar, lib, "duckdb_value_varchar")
	purego.RegisterLibFunc(&db.ValueVarcharInternal, lib, "duckdb_value_varchar_internal")
	// duckdb_value_string is not supported due to purego limitations
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

	// Register Value interface functions
	purego.RegisterLibFunc(&db.DestroyValue, lib, "duckdb_destroy_value")
	purego.RegisterLibFunc(&db.CreateVarchar, lib, "duckdb_create_varchar")
	purego.RegisterLibFunc(&db.CreateInt32, lib, "duckdb_create_int32")
	purego.RegisterLibFunc(&db.CreateInt64, lib, "duckdb_create_int64")
	purego.RegisterLibFunc(&db.CreateDouble, lib, "duckdb_create_double")
	purego.RegisterLibFunc(&db.CreateBool, lib, "duckdb_create_bool")
	purego.RegisterLibFunc(&db.CreateListValue, lib, "duckdb_create_list_value")
	purego.RegisterLibFunc(&db.GetListSize, lib, "duckdb_get_list_size")
	purego.RegisterLibFunc(&db.GetListChild, lib, "duckdb_get_list_child")
	purego.RegisterLibFunc(&db.IsNullValue, lib, "duckdb_is_null_value")
	purego.RegisterLibFunc(&db.CreateNullValue, lib, "duckdb_create_null_value")

	// Register Data Chunk interface functions
	purego.RegisterLibFunc(&db.FetchChunk, lib, "duckdb_fetch_chunk")
	purego.RegisterLibFunc(&db.ResultGetChunk, lib, "duckdb_result_get_chunk")
	purego.RegisterLibFunc(&db.ResultChunkCount, lib, "duckdb_result_chunk_count")
	purego.RegisterLibFunc(&db.ResultIsStreaming, lib, "duckdb_result_is_streaming")
	purego.RegisterLibFunc(&db.CreateDataChunk, lib, "duckdb_create_data_chunk")
	purego.RegisterLibFunc(&db.DestroyDataChunk, lib, "duckdb_destroy_data_chunk")
	purego.RegisterLibFunc(&db.DataChunkReset, lib, "duckdb_data_chunk_reset")
	purego.RegisterLibFunc(&db.DataChunkGetColumnCount, lib, "duckdb_data_chunk_get_column_count")
	purego.RegisterLibFunc(&db.DataChunkGetVector, lib, "duckdb_data_chunk_get_vector")
	purego.RegisterLibFunc(&db.DataChunkGetSize, lib, "duckdb_data_chunk_get_size")
	purego.RegisterLibFunc(&db.DataChunkSetSize, lib, "duckdb_data_chunk_set_size")

	// Register Vector interface functions
	purego.RegisterLibFunc(&db.VectorGetLogicalColumnType, lib, "duckdb_vector_get_column_type")
	purego.RegisterLibFunc(&db.VectorGetData, lib, "duckdb_vector_get_data")
	purego.RegisterLibFunc(&db.VectorGetValidity, lib, "duckdb_vector_get_validity")
	purego.RegisterLibFunc(&db.VectorEnsureValidityWritable, lib, "duckdb_vector_ensure_validity_writable")
	purego.RegisterLibFunc(&db.VectorAssignStringElement, lib, "duckdb_vector_assign_string_element")
	purego.RegisterLibFunc(&db.VectorAssignStringElementLen, lib, "duckdb_vector_assign_string_element_len")
	purego.RegisterLibFunc(&db.ListVectorGetChild, lib, "duckdb_list_vector_get_child")
	purego.RegisterLibFunc(&db.ListVectorGetSize, lib, "duckdb_list_vector_get_size")
	purego.RegisterLibFunc(&db.ListVectorSetSize, lib, "duckdb_list_vector_set_size")
	purego.RegisterLibFunc(&db.ListVectorReserve, lib, "duckdb_list_vector_reserve")
	purego.RegisterLibFunc(&db.StructVectorGetChild, lib, "duckdb_struct_vector_get_child")
	purego.RegisterLibFunc(&db.ArrayVectorGetChild, lib, "duckdb_array_vector_get_child")

	// Register Logical Type interface functions
	purego.RegisterLibFunc(&db.CreateLogicalType, lib, "duckdb_create_logical_type")
	purego.RegisterLibFunc(&db.LogicalTypeGetAlias, lib, "duckdb_logical_type_get_alias")
	purego.RegisterLibFunc(&db.CreateListType, lib, "duckdb_create_list_type")
	purego.RegisterLibFunc(&db.ListTypeChildType, lib, "duckdb_list_type_child_type")
	purego.RegisterLibFunc(&db.GetTypeID, lib, "duckdb_get_type_id")
	purego.RegisterLibFunc(&db.DecimalWidth, lib, "duckdb_decimal_width")
	purego.RegisterLibFunc(&db.DecimalScale, lib, "duckdb_decimal_scale")
	purego.RegisterLibFunc(&db.DestroyLogicalType, lib, "duckdb_destroy_logical_type")

	// Print library version
	// version := db.LibraryVersion()
	// if version != nil {
	// 	fmt.Printf("DuckDB library version: %s\n", GoString(version))
	// } else {
	// 	fmt.Println("Failed to retrieve DuckDB library version")
	// }

	// Open database
	var handle DuckDBDatabase
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
