package pduckdb

// DuckDBState represents the return status from DuckDB operations
type DuckDBState int

const (
	DuckDBSuccess DuckDBState = iota
	DuckDBError
)

// ErrDuckDB represents an error from DuckDB operations
type ErrDuckDB struct {
	Message string
}

func (e ErrDuckDB) Error() string {
	return e.Message
}
