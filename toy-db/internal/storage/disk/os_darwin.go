//go:build darwin

package disk

import (
	"os"

	"golang.org/x/sys/unix"
)

func openDirect(path string, flags int) (*os.File, error) {
	f, err := os.OpenFile(path, flags, 0666)
	if err != nil {
		return nil, err
	}

	if _, err := unix.FcntlInt(f.Fd(), unix.F_NOCACHE, 1); err != nil {
		f.Close()
		return nil, err
	}

	return f, nil
}

func assertDirectIO(_ []byte, _ int64) error {
	// No alignment requirements on darwin
	return nil
}

func directSync(file *os.File) error {
	return file.Sync()
}
