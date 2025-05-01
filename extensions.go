package pduckdb

import (
	"fmt"
	"strconv"
)

// Extension represents a DuckDB extension
type Extension struct {
	Name string
}

// Common DuckDB extensions
var (
	ParquetExtension  = Extension{Name: "parquet"}
	JSONExtension     = Extension{Name: "json"}
	ICUExtension      = Extension{Name: "icu"}
	TPCDSExtension    = Extension{Name: "tpcds"}
	TPCHExtension     = Extension{Name: "tpch"}
	HTTPExtension     = Extension{Name: "httpfs"}
	SQLiteExtension   = Extension{Name: "sqlite"}
	PostgresExtension = Extension{Name: "postgres_scanner"}
)

// LoadExtension loads a DuckDB extension
func (conn *DuckDBConnection) LoadExtension(extension Extension) error {
	query := "LOAD '" + extension.Name + "'"
	return conn.Execute(query)
}

// InstallExtension installs a DuckDB extension from the extension repository
func (conn *DuckDBConnection) InstallExtension(extension Extension) error {
	query := "INSTALL '" + extension.Name + "'"
	return conn.Execute(query)
}

// MemoryLimit sets the memory limit for the database
func (conn *DuckDBConnection) MemoryLimit(limitInBytes int64) error {
	query := "SET memory_limit = '" + formatMemorySize(limitInBytes) + "'"
	return conn.Execute(query)
}

// FormatMemorySize converts bytes to human-readable format
func formatMemorySize(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)

	if bytes < MB {
		return fmt.Sprintf("%dKB", bytes/KB)
	} else if bytes < GB {
		return fmt.Sprintf("%dMB", bytes/MB)
	} else {
		return fmt.Sprintf("%dGB", bytes/GB)
	}
}

// ThreadLimit sets the maximum number of threads to use
func (conn *DuckDBConnection) ThreadLimit(numThreads int) error {
	query := "SET threads = " + strconv.Itoa(numThreads)
	return conn.Execute(query)
}
