package pduckdb

// ErrDuckDB represents an error from DuckDB operations
type ErrDuckDB struct {
	Message string
}

func (e ErrDuckDB) Error() string {
	return e.Message
}
