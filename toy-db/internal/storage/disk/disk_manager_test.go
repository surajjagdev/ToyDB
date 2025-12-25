package disk

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
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

func readPageFromDisk(
	t *testing.T,
	path string,
	offset int64,
) []byte {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file %s: %v", path, err)
	}
	defer f.Close()

	buf := make([]byte, common.PageSize)
	n, err := f.ReadAt(buf, offset)
	if err != nil {
		t.Fatalf("read file %s: %v", path, err)
	}
	if n != int(common.PageSize) {
		t.Fatalf("short read: %d/%d", n, common.PageSize)
	}

	return buf
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

// write file functional tests
func TestWritePageToDisk(t *testing.T) {
	dm, dir := newTestDM(t)

	rel := common.RelationID(1)
	fork := common.ForkMain
	page := common.PageID(0)

	data := bytes.Repeat([]byte{0xAB}, int(common.PageSize))

	// Write page
	if err := dm.WritePage(rel, fork, page, data); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}

	// Force data to disk
	if err := dm.vfd.CloseAll(false); err != nil {
		t.Fatalf("failed to close files: %v", err)
	}

	path := filepath.Join(dir, "1")

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected data file to exist: %v", err)
	}

	onDisk := readPageFromDisk(t, path, 0)

	if !bytes.Equal(data, onDisk) {
		t.Fatal("data on disk does not match written data")
	}
}

func TestWritePagePartialFail(t *testing.T) {
	dm, _ := newTestDM(t)

	rel := common.RelationID(1)
	fork := common.ForkMain
	page := common.PageID(0)

	data := bytes.Repeat([]byte{0xAB}, int(common.PageSize)/2)

	if err := dm.WritePage(rel, fork, page, data); err == nil {
		t.Fatalf("Expected an error writing partial page")
	}
}

// writing to same page and offset should not append
func TestWritePageOverwrite(t *testing.T) {
	dm, dir := newTestDM(t)

	data1 := bytes.Repeat([]byte{0xAA}, int(common.PageSize))
	data2 := bytes.Repeat([]byte{0xBB}, int(common.PageSize))

	if err := dm.WritePage(1, common.ForkMain, 0, data1); err != nil {
		t.Fatal(err)
	}
	if err := dm.WritePage(1, common.ForkMain, 0, data2); err != nil {
		t.Fatal(err)
	}

	_ = dm.vfd.CloseAll(false)

	path := filepath.Join(dir, "1")
	onDisk := readPageFromDisk(t, path, 0)

	if !bytes.Equal(data2, onDisk) {
		t.Fatal("overwrite did not replace page contents")
	}
}

func TestWritePageSegmentBoundary(t *testing.T) {
	dm, dir := newTestDM(t)

	page := common.PageID(common.MaxPagesPerSegment)
	data := bytes.Repeat([]byte{0xDD}, int(common.PageSize))

	if err := dm.WritePage(1, common.ForkMain, page, data); err != nil {
		t.Fatal(err)
	}

	_ = dm.vfd.CloseAll(false)

	path := filepath.Join(dir, "1.1")
	onDisk := readPageFromDisk(t, path, 0)

	if !bytes.Equal(data, onDisk) {
		t.Fatal("segment boundary write incorrect")
	}
}

func TestWritePageForkIsolation(t *testing.T) {
	dm, dir := newTestDM(t)

	data1 := bytes.Repeat([]byte{0xEE}, int(common.PageSize))
	data2 := bytes.Repeat([]byte{0xAA}, int(common.PageSize))

	if err := dm.WritePage(1, common.ForkFSM, 0, data1); err != nil {
		t.Fatal(err)
	}
	if err := dm.WritePage(1, common.ForkMain, 0, data2); err != nil {
		t.Fatal(err)
	}

	_ = dm.vfd.CloseAll(false)

	path1 := filepath.Join(dir, "1_1")
	path2 := filepath.Join(dir, "1")
	onDisk1 := readPageFromDisk(t, path1, 0)
	onDisk2 := readPageFromDisk(t, path2, 0)

	if !bytes.Equal(data1, onDisk1) {
		t.Fatal("FSM fork data mismatch")
	}
	if !bytes.Equal(data2, onDisk2) {
		t.Fatal("Main fork data mismatch")
	}
	if bytes.Equal(data1, data2) {
		t.Fatal("Main fork and FSM fork data should not match")
	}
}

func TestWriteReadRoundTrip(t *testing.T) {
	dm, _ := newTestDM(t)

	data := bytes.Repeat([]byte{0x5A}, int(common.PageSize))
	buf := make([]byte, common.PageSize)

	if err := dm.WritePage(1, common.ForkMain, 7, data); err != nil {
		t.Fatal(err)
	}
	if err := dm.ReadPage(1, common.ForkMain, 7, buf); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data, buf) {
		t.Fatal("read data does not match written data")
	}
}

func TestConcurrentWriteMonotonicPages(t *testing.T) {
	dm, dir := newTestDM(t)

	rel := common.RelationID(1)
	fork := common.ForkMain

	const writers = 8

	// Each entry is one page-sized byte slice
	dataArr := make([][]byte, writers)

	for i := 0; i < writers; i++ {
		// Fill each page with a unique byte pattern
		dataArr[i] = bytes.Repeat([]byte{byte(i)}, int(common.PageSize))
	}

	var wg sync.WaitGroup
	wg.Add(writers)

	for i := 0; i < writers; i++ {
		page := common.PageID(i)
		data := dataArr[i]

		go func(p common.PageID, d []byte) {
			defer wg.Done()
			if err := dm.WritePage(rel, fork, p, d); err != nil {
				t.Errorf("WritePage failed for page %d: %v", p, err)
			}
		}(page, data)
	}

	wg.Wait()

	// Force all files closed to ensure disk visibility
	if err := dm.vfd.CloseAll(false); err != nil {
		t.Fatalf("failed to close files: %v", err)
	}

	// Verify every page on disk
	path := filepath.Join(dir, "1")

	for i := 0; i < writers; i++ {
		offset := int64(i) * int64(common.PageSize)
		onDisk := readPageFromDisk(t, path, offset)

		if !bytes.Equal(onDisk, dataArr[i]) {
			t.Fatalf("page %d corrupted or mismatched", i)
		}
	}
}
