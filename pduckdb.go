package pduckdb

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"unsafe"

	"github.com/ebitengine/purego"
)

// DuckDB represents a DuckDB database instance
type DuckDB struct {
	handle         *byte
	lib            uintptr
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

	// Load DuckDB library
	lib, err := loadDuckDBLibrary()
	if err != nil {
		return nil, fmt.Errorf("failed to load DuckDB library: %w", err)
	}
	db.lib = lib

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

// loadDuckDBLibrary attempts to load the DuckDB library from various locations based on the platform
func loadDuckDBLibrary() (uintptr, error) {
	// First check if the library path is specified via environment variable
	if envPath := os.Getenv("DUCKDB_LIBRARY_PATH"); envPath != "" {
		lib, err := purego.Dlopen(envPath, purego.RTLD_NOW|purego.RTLD_GLOBAL)
		if err == nil {
			return lib, nil
		}
		// If the explicitly provided path fails, return that error directly
		// as the user would expect the specified library to work
		return 0, fmt.Errorf("failed to load DuckDB library from DUCKDB_LIBRARY_PATH (%s): %w", envPath, err)
	}

	// Get platform-specific library paths
	locations := getLibraryPaths()

	// Try each location
	var lastErr error
	for _, location := range locations {
		lib, err := purego.Dlopen(location, purego.RTLD_NOW|purego.RTLD_GLOBAL)
		if err == nil {
			return lib, nil
		}
		lastErr = err
	}

	return 0, fmt.Errorf("failed to load DuckDB library from any standard location, last error: %w", lastErr)
}

// getLibraryPaths returns a list of paths to search for the DuckDB library based on the platform
func getLibraryPaths() []string {
	var locations []string

	switch runtime.GOOS {
	case "darwin":
		locations = getMacOSLibraryPaths()
	case "linux":
		locations = getLinuxLibraryPaths()
	case "windows":
		// Windows standard locations
		locations = []string{
			"duckdb.dll", // Current directory
			filepath.Join(os.Getenv("ProgramFiles"), "DuckDB", "duckdb.dll"),
			filepath.Join(os.Getenv("ProgramFiles(x86)"), "DuckDB", "duckdb.dll"),
		}
	}

	return locations
}

// getMacOSLibraryPaths returns a list of paths to search for the DuckDB library on macOS
func getMacOSLibraryPaths() []string {
	locations := []string{}

	// First check DYLD_LIBRARY_PATH
	if libPaths := os.Getenv("DYLD_LIBRARY_PATH"); libPaths != "" {
		for _, path := range filepath.SplitList(libPaths) {
			locations = append(locations, filepath.Join(path, "libduckdb.dylib"))
		}
	}

	// Then add standard macOS locations
	standardPaths := []string{
		"/opt/homebrew/lib/libduckdb.dylib",         // Apple Silicon Homebrew
		"/usr/local/lib/libduckdb.dylib",            // Intel Homebrew
		"/usr/local/opt/duckdb/lib/libduckdb.dylib", // Alternative Homebrew location
		"/usr/lib/libduckdb.dylib",                  // System location
		"./libduckdb.dylib",                         // Current directory
	}

	locations = append(locations, standardPaths...)

	return locations
}

// getLinuxLibraryPaths returns a list of paths to search for the DuckDB library on Linux
func getLinuxLibraryPaths() []string {
	locations := []string{}

	// First check LD_LIBRARY_PATH
	if libPaths := os.Getenv("LD_LIBRARY_PATH"); libPaths != "" {
		for _, path := range filepath.SplitList(libPaths) {
			locations = append(locations, filepath.Join(path, "libduckdb.so"))
		}
	}

	// Then add standard Linux locations
	standardPaths := []string{
		"/usr/lib/libduckdb.so",
		"/usr/local/lib/libduckdb.so",
		"/usr/lib/x86_64-linux-gnu/libduckdb.so",  // Debian/Ubuntu for amd64
		"/usr/lib/aarch64-linux-gnu/libduckdb.so", // Debian/Ubuntu for arm64
		"/usr/lib64/libduckdb.so",                 // Fedora/RHEL/CentOS
		"./libduckdb.so",                          // Current directory
	}

	locations = append(locations, standardPaths...)

	return locations
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
