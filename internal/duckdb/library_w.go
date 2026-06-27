//go:build windows

package duckdb

import (
	"fmt"
	"syscall"
)

func loadLibrary(path string) (uintptr, error) {
	// Windows: use syscall.LoadLibrary
	handle, err := syscall.LoadLibrary(path)
	if err != nil {
		return 0, fmt.Errorf("failed to load DLL %s: %w", path, err)
	}
	return uintptr(handle), nil
}
