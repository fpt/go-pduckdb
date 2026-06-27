//go:build !windows

package duckdb

import (
	"github.com/ebitengine/purego"
)

func loadLibrary(path string) (uintptr, error) {
	// Linux/macOS: use Dlopen
	return purego.Dlopen(path, purego.RTLD_NOW|purego.RTLD_GLOBAL)
}
