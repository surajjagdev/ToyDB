//go:build linux

package disk

import (
	"fmt"
	"os"
	"unsafe"

	"github.com/surajjagdev/ToyDB/internal/common"
	"golang.org/x/sys/unix"
)

func openDirect(path string, flags int) (*os.File, error) {
	fd, err := unix.Open(path, flags|unix.O_DIRECT, 0666)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}

func assertDirectIO(data []byte, offset int64) error {
	if len(data) != int(common.PageSize) {
		return fmt.Errorf("direct IO requires PageSize buffer")
	}
	if uintptr(unsafe.Pointer(&data[0]))%uintptr(common.PageSize) != 0 {
		return fmt.Errorf("unaligned direct IO buffer")
	}
	if offset%int64(common.PageSize) != 0 {
		return fmt.Errorf("unaligned direct IO offset")
	}
	return nil
}

func directSync(file *os.File) error {
	return file.Sync() // Linux needs explicit fsync with O_DIRECT
}
