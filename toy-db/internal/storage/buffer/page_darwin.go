// page_darwin.go
//go:build darwin
// +build darwin

package buffer

import (
	"unsafe"

	"github.com/surajjagdev/ToyDB/internal/common"
)

func allocPageData() ([]byte, unsafe.Pointer, error) {
	// No alignment needed on darwin
	return make([]byte, common.PageSize), nil, nil
}

func freePageData(_ unsafe.Pointer) {
	// No-op on macOS
	return
}
