package pduckdb

import (
	"testing"
)

func TestErrDuckDB_Error(t *testing.T) {
	err := ErrDuckDB{Message: "test error"}
	if err.Error() != "test error" {
		t.Errorf("ErrDuckDB.Error() = %v, want %v", err.Error(), "test error")
	}
}
