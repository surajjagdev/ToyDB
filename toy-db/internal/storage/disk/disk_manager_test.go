package disk

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/surajjagdev/ToyDB/internal/common"
)

func newTestDM(t *testing.T) (*DiskManager, string) {
	t.Helper()

	dir := t.TempDir()
	dm, err := NewDiskManager(dir, 10)
	if err != nil {
		t.Fatalf("NewDiskManager: %v", err)
	}
	return dm, dir
}

func writeFile(t *testing.T, path string, size int) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	buf := make([]byte, size)
	if _, err := f.Write(buf); err != nil {
		t.Fatal(err)
	}
}

func TestResolveLocation(t *testing.T) {
	dm, dir := newTestDM(t)

	tests := []struct {
		name     string
		page     common.PageID
		fork     common.ForkID
		wantPath string
		wantOff  int64
	}{
		{
			name:     "page 0 main fork",
			page:     0,
			fork:     common.ForkMain,
			wantPath: filepath.Join(dir, "1"),
			wantOff:  0,
		},
		{
			name:     "page 1 main fork",
			page:     1,
			fork:     common.ForkMain,
			wantPath: filepath.Join(dir, "1"),
			wantOff:  int64(common.PageSize),
		},
		{
			name:     "page in second segment",
			page:     common.PageID(common.MaxPagesPerSegment),
			fork:     common.ForkMain,
			wantPath: filepath.Join(dir, "1.1"),
			wantOff:  0,
		},
		{
			name:     "fsm fork segment 0",
			page:     0,
			fork:     common.ForkFSM,
			wantPath: filepath.Join(dir, "1_1"),
			wantOff:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, off := dm.resolveLocation(1, tt.fork, tt.page)
			if path != tt.wantPath {
				t.Fatalf("path = %s, want %s", path, tt.wantPath)
			}
			if off != tt.wantOff {
				t.Fatalf("offset = %d, want %d", off, tt.wantOff)
			}
		})
	}
}

func TestReadPage_Partial(t *testing.T) {
	dm, dir := newTestDM(t)

	path := filepath.Join(dir, "1")
	writeFile(t, path, int(common.PageSize/2))

	buf := make([]byte, common.PageSize)
	if err := dm.ReadPage(1, common.ForkMain, 0, buf); err == nil {
		t.Fatal("expected error on partial page read")
	}
}

func TestReadPage_FullRandom(t *testing.T) {
	dm, dir := newTestDM(t)

	path := filepath.Join(dir, "1")
	writeFile(t, path, int(common.PageSize))

	buf := make([]byte, common.PageSize)
	if err := dm.ReadPage(1, common.ForkMain, 0, buf); err != nil {
		t.Fatal("expected no error on full page read")
	}
}

func TestReadPage_MissingFile(t *testing.T) {
	dm, _ := newTestDM(t)

	buf := make([]byte, common.PageSize)
	if err := dm.ReadPage(1, common.ForkMain, 0, buf); err == nil {
		t.Fatal("expected error for missing file")
	}
}
