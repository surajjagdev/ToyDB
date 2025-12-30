// page_linux.go
//go:build linux
// +build linux

package page

/*
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"unsafe"

	"github.com/surajjagdev/ToyDB/internal/common"
)

func allocPageData() ([]byte, unsafe.Pointer, error) {
	var ptr unsafe.Pointer
	ret := C.posix_memalign(&ptr, C.size_t(common.PageSize), C.size_t(common.PageSize))
	if ret != 0 {
		return nil, nil, fmt.Errorf("posix_memalign failed: %d", ret)
	}
	return unsafe.Slice((*byte)(ptr), common.PageSize), ptr, nil
}

func freePageData(ptr unsafe.Pointer) {
	C.free(ptr)
}
