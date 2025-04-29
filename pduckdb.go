package pduckdb

import (
	"fmt"
	"runtime"
	"unsafe"

	"github.com/ebitengine/purego"
)

// DuckDB represents a DuckDB database instance
type DuckDB struct {
	handle         *byte
	lib            uintptr
	sysLib         uintptr
	connect        func(*byte, **byte) DuckDBState
	close          func(**byte)
	query          func(*byte, string, *DuckDBResultRaw) DuckDBState
	columnCount    func(*DuckDBResultRaw) int32
	rowCount       func(*DuckDBResultRaw) int64
	columnName     func(*DuckDBResultRaw, int32) *byte
	valueString    func(*DuckDBResultRaw, int64, int32) *byte
	valueDate      func(*DuckDBResultRaw, int64, int32) DuckDBDate
	valueTime      func(*DuckDBResultRaw, int64, int32) DuckDBTime
	valueTimestamp func(*DuckDBResultRaw, int64, int32) DuckDBTimestamp
	destroyResult  func(*DuckDBResultRaw)
	puts           func(string)
}

// NewDuckDB creates a new DuckDB instance
func NewDuckDB(path string) (*DuckDB, error) {
	db := &DuckDB{}

	// Load system library for debugging output
	sysLib, err := purego.Dlopen(getSystemLibrary(), purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		return nil, fmt.Errorf("failed to load system library: %w", err)
	}
	db.sysLib = sysLib

	// Load DuckDB library
	lib, err := purego.Dlopen(getDuckDBLibrary(), purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		return nil, fmt.Errorf("failed to load DuckDB library: %w", err)
	}
	db.lib = lib

	// Register system functions
	purego.RegisterLibFunc(&db.puts, sysLib, "puts")

	// Register DuckDB functions
	var open func(path string, out **byte) DuckDBState
	purego.RegisterLibFunc(&open, lib, "duckdb_open")
	purego.RegisterLibFunc(&db.connect, lib, "duckdb_connect")
	purego.RegisterLibFunc(&db.close, lib, "duckdb_close")
	purego.RegisterLibFunc(&db.query, lib, "duckdb_query")
	purego.RegisterLibFunc(&db.columnCount, lib, "duckdb_column_count")
	purego.RegisterLibFunc(&db.rowCount, lib, "duckdb_row_count")
	purego.RegisterLibFunc(&db.columnName, lib, "duckdb_column_name")
	purego.RegisterLibFunc(&db.valueString, lib, "duckdb_value_string")

	// Register date and time functions
	purego.RegisterLibFunc(&db.valueDate, lib, "duckdb_value_date")
	purego.RegisterLibFunc(&db.valueTime, lib, "duckdb_value_time")
	purego.RegisterLibFunc(&db.valueTimestamp, lib, "duckdb_value_timestamp")

	purego.RegisterLibFunc(&db.destroyResult, lib, "duckdb_destroy_result")

	// Open database
	var handle *byte
	state := open(path, &handle)
	if state != DuckDBSuccess {
		return nil, ErrDuckDB{Message: "Failed to open database: " + path}
	}
	db.handle = handle

	return db, nil
}

// Connect creates a new connection to the database
func (db *DuckDB) Connect() (*DuckDBConnection, error) {
	var handle *byte
	state := db.connect(db.handle, &handle)
	if state != DuckDBSuccess {
		return nil, ErrDuckDB{Message: "Failed to connect to database"}
	}

	conn := &DuckDBConnection{
		handle: handle,
		db:     db,
		query:  db.query,
	}

	return conn, nil
}

// Close closes the database and releases resources
func (db *DuckDB) Close() {
	db.close(&db.handle)
}

// Helper functions

func getSystemLibrary() string {
	switch runtime.GOOS {
	case "darwin":
		return "/usr/lib/libSystem.B.dylib"
	default:
		panic(fmt.Errorf("GOOS=%s is not supported", runtime.GOOS))
	}
}

func getDuckDBLibrary() string {
	switch runtime.GOOS {
	case "darwin":
		return "/opt/homebrew/lib/libduckdb.dylib"
	default:
		panic(fmt.Errorf("GOOS=%s is not supported", runtime.GOOS))
	}
}

func GoString(c *byte) string {
	if c == nil {
		return ""
	}

	p := uintptr(unsafe.Pointer(c))
	length := 0
	for {
		if *(*byte)(unsafe.Pointer(p)) == 0 {
			break
		}
		p++
		length++
	}

	bytes := make([]byte, length)
	for i := 0; i < length; i++ {
		bytes[i] = *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(c)) + uintptr(i)))
	}

	return string(bytes)
}
