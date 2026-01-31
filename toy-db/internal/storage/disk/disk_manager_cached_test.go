package disk

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/surajjagdev/ToyDB/internal/common"
)

func newTestDM(t *testing.T, maxOpenFiles int, maxCachedRelations int) (*CachedManager, string) {
	t.Helper()

	dir := t.TempDir()

	dm, err := NewCachedManager(
		dir,
		maxOpenFiles,
		maxCachedRelations,
	)
	if err != nil {
		t.Fatalf("NewDiskManagerCached: %v", err)
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
	dm, dir := newTestDM(t, 10, 100)

	tests := []struct {
		name     string
		page     common.BlockID
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
			page:     common.BlockID(common.MaxPagesPerSegment),
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
	dm, dir := newTestDM(t, 10, 100)

	path := filepath.Join(dir, "1")
	writeFile(t, path, int(common.PageSize/2))

	buf := make([]byte, common.PageSize)
	if err := dm.ReadPage(1, common.ForkMain, 0, buf); err == nil {
		t.Fatal("expected error on partial page read")
	}
}

func TestReadPage_FullRandom(t *testing.T) {
	dm, dir := newTestDM(t, 10, 100)

	path := filepath.Join(dir, "1")
	writeFile(t, path, int(common.PageSize))

	buf := make([]byte, common.PageSize)
	if err := dm.ReadPage(1, common.ForkMain, 0, buf); err != nil {
		t.Fatal("expected no error on full page read")
	}
}

func TestReadPage_MissingFile(t *testing.T) {
	dm, _ := newTestDM(t, 10, 100)

	buf := make([]byte, common.PageSize)
	if err := dm.ReadPage(1, common.ForkMain, 0, buf); err == nil {
		t.Fatal("expected error for missing file")
	}
}

// write file functional tests
func TestWritePageToDisk(t *testing.T) {
	dm, dir := newTestDM(t, 10, 100)

	rel := common.RelationID(1)
	fork := common.ForkMain
	page := common.BlockID(0)

	data := bytes.Repeat([]byte{0xAB}, int(common.PageSize))

	// Write page
	if err := dm.WritePage(rel, fork, page, data, true); err != nil {
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

func TestWritePageToDiskPanicsWithoutFileExisting(t *testing.T) {
	dm, _ := newTestDM(t, 10, 100)

	rel := common.RelationID(1)
	fork := common.ForkMain
	page := common.BlockID(0)

	data := bytes.Repeat([]byte{0xAB}, int(common.PageSize))

	// Write page
	if err := dm.WritePage(rel, fork, page, data, false); err == nil {
		t.Fatalf("WritePage success, however should have failed")
	}
}

func TestWritePagePartialFail(t *testing.T) {
	dm, _ := newTestDM(t, 10, 100)

	rel := common.RelationID(1)
	fork := common.ForkMain
	page := common.BlockID(0)

	data := bytes.Repeat([]byte{0xAB}, int(common.PageSize)/2)

	if err := dm.WritePage(rel, fork, page, data, true); err == nil {
		t.Fatalf("Expected an error writing partial page")
	}
}

// writing to same page and offset should not append
func TestWritePageOverwrite(t *testing.T) {
	dm, dir := newTestDM(t, 10, 100)

	data1 := bytes.Repeat([]byte{0xAA}, int(common.PageSize))
	data2 := bytes.Repeat([]byte{0xBB}, int(common.PageSize))

	if err := dm.WritePage(1, common.ForkMain, 0, data1, true); err != nil {
		t.Fatal(err)
	}
	if err := dm.WritePage(1, common.ForkMain, 0, data2, true); err != nil {
		t.Fatal(err)
	}

	_ = dm.vfd.CloseAll(false)

	path := filepath.Join(dir, "1")
	onDisk := readPageFromDisk(t, path, 0)

	if !bytes.Equal(data2, onDisk) {
		t.Fatal("overwrite did not replace page contents")
	}
}

func TestWritePageSparse(t *testing.T) {
	dm, _ := newTestDM(t, 10, 100)

	page := common.BlockID(common.MaxPagesPerSegment)
	data := bytes.Repeat([]byte{0xDD}, int(common.PageSize))

	if err := dm.WritePage(1, common.ForkMain, page, data, true); err == nil {
		t.Fatal("expected error got success for write sparse pages")
	}
}

func TestWritePageForkIsolation(t *testing.T) {
	dm, dir := newTestDM(t, 10, 100)

	data1 := bytes.Repeat([]byte{0xEE}, int(common.PageSize))
	data2 := bytes.Repeat([]byte{0xAA}, int(common.PageSize))

	if err := dm.WritePage(1, common.ForkFSM, 0, data1, true); err != nil {
		t.Fatal(err)
	}
	if err := dm.WritePage(1, common.ForkMain, 0, data2, true); err != nil {
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

	// Verify page counts
	countFSM, err := dm.loadPageCountFromDisk(1, common.ForkFSM)
	if err != nil {
		t.Fatalf("failed to load page count for FSM: %v", err)
	}
	if countFSM != 1 {
		t.Fatalf("expected FSM fork page count 1, got %d", countFSM)
	}

	countMain, err := dm.loadPageCountFromDisk(1, common.ForkMain)
	if err != nil {
		t.Fatalf("failed to load page count for Main: %v", err)
	}
	if countMain != 1 {
		t.Fatalf("expected Main fork page count 1, got %d", countMain)
	}
}

func TestWriteReadRoundTrip(t *testing.T) {
	dm, _ := newTestDM(t, 10, 100)

	data := bytes.Repeat([]byte{0x5A}, int(common.PageSize))
	buf := make([]byte, common.PageSize)

	if err := dm.WritePage(1, common.ForkMain, 7, data, true); err != nil {
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
	dm, dir := newTestDM(t, 10, 100)

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
		page := common.BlockID(i)
		data := dataArr[i]

		go func(p common.BlockID, d []byte) {
			defer wg.Done()
			if err := dm.WritePage(rel, fork, p, d, true); err != nil {
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

func TestAllocateBlock(t *testing.T) {
	dm, _ := newTestDM(t, 10, 100)

	rel := common.RelationID(1)
	fork := common.ForkMain

	for i := 0; i < 10; i++ {
		pid, err := dm.AllocateBlock(rel, fork)
		if err != nil {
			t.Fatalf("AllocateBlock failed: %v", err)
		}
		if pid != common.BlockID(i) {
			t.Fatalf("expected page %d, got %d", i, pid)
		}
	}
}

func TestLRUEviction(t *testing.T) {
	dm, _ := newTestDM(t, 10, 5)

	// allocate more entries than maxEntries (5)
	for i := 0; i < 10; i++ {
		rel := common.RelationID(i)
		fork := common.ForkMain
		_, err := dm.AllocateBlock(rel, fork)
		if err != nil {
			t.Fatal(err)
		}
	}

	dm.countMu.Lock()
	defer dm.countMu.Unlock()
	if len(dm.pageCount) > dm.maxEntries {
		t.Fatalf("pageCount exceeded maxEntries: %d > %d", len(dm.pageCount), dm.maxEntries)
	}

	// Ensure LRU removed oldest entries
	for i := 0; i < 5; i++ {
		rf := RelationFork{Rel: common.RelationID(i), Fork: common.ForkMain}
		if _, ok := dm.pageCount[rf]; ok {
			t.Errorf("old relation %v should have been evicted", rf)
		}
	}
}

func TestGetNumPages_CachedAndDisk(t *testing.T) {
	dm, _ := newTestDM(t, 10, 100)

	rel := common.RelationID(1)
	fork := common.ForkMain

	// Initially no pages, disk empty
	num, err := dm.GetNumPages(rel, fork)
	if err != nil {
		t.Fatal(err)
	}
	if num != 0 {
		t.Fatalf("expected 0 pages initially, got %d", num)
	}

	// Write a page to disk
	data := bytes.Repeat([]byte{0xAB}, int(common.PageSize))
	if err := dm.WritePage(rel, fork, 0, data, true); err != nil {
		t.Fatal(err)
	}

	// Should update pageCount cache
	num, err = dm.GetNumPages(rel, fork)
	if err != nil {
		t.Fatal(err)
	}
	if num != 1 {
		t.Fatalf("expected 1 page after write, got %d", num)
	}

	// Write another page
	if err := dm.WritePage(rel, fork, 1, data, true); err != nil {
		t.Fatal(err)
	}

	// GetNumPages should reflect highest written page + 1
	num, err = dm.GetNumPages(rel, fork)
	if err != nil {
		t.Fatal(err)
	}
	if num != 2 {
		t.Fatalf("expected 2 page after writing page, got %d", num)
	}

	// Check LRU touched
	dm.countMu.Lock()
	df, exists := dm.pageCount[RelationFork{Rel: rel, Fork: fork}]
	dm.countMu.Unlock()
	if !exists || df.elem == nil {
		t.Fatal("expected dfEntry to exist and be in LRU list")
	}
}

func TestGetNumPages_LoadFromDisk(t *testing.T) {
	dm, dir := newTestDM(t, 10, 100)
	rel := common.RelationID(5)
	fork := common.ForkMain

	// Write directly to disk (bypass cache)
	path := filepath.Join(dir, "5")
	writeFile(t, path, int(common.PageSize*4)) // 4 pages

	num, err := dm.GetNumPages(rel, fork)
	if err != nil {
		t.Fatal(err)
	}
	if num != 4 {
		t.Fatalf("expected 4 pages from disk, got %d", num)
	}

	// Subsequent call should hit cache
	num2, err := dm.GetNumPages(rel, fork)
	if err != nil {
		t.Fatal(err)
	}
	if num2 != 4 {
		t.Fatalf("expected 4 pages from cache, got %d", num2)
	}
}

func TestGetNumPages_EmptyRelation(t *testing.T) {
	dm, _ := newTestDM(t, 10, 100)
	rel := common.RelationID(999)
	fork := common.ForkMain

	num, err := dm.GetNumPages(rel, fork)
	if err != nil {
		t.Fatal(err)
	}
	if num != 0 {
		t.Fatalf("expected 0 pages for empty relation, got %d", num)
	}
}

func TestSyncPage_PersistedData(t *testing.T) {
	dm, _ := newTestDM(t, 10, 100)

	rel := common.RelationID(1)
	fork := common.ForkMain
	page := common.BlockID(0)

	data := bytes.Repeat([]byte{0xAB}, int(common.PageSize))

	// 1. Write a page
	if err := dm.WritePage(rel, fork, page, data, true); err != nil {
		t.Fatal(err)
	}

	// 2. Sync the page
	if err := dm.SyncPage(rel, fork, page); err != nil {
		t.Fatal(err)
	}

	// 3. Bypass VFD to simulate fresh open (like after a crash)
	path, offset := dm.resolveLocation(rel, fork, page)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	buf := make([]byte, common.PageSize)
	n, err := f.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != int(common.PageSize) {
		t.Fatalf("short read: %d/%d", n, common.PageSize)
	}

	// 4. Check data matches
	if !bytes.Equal(data, buf) {
		t.Fatal("data on disk does not match after SyncPage")
	}
}

func TestAllocatePage_Basic(t *testing.T) {
	dm, _ := newTestDM(t, 10, 100)

	rel := common.RelationID(1)
	fork := common.ForkMain

	// Allocate a few pages
	for i := 0; i < 5; i++ {
		pid, err := dm.AllocateBlock(rel, fork)
		if err != nil {
			t.Fatalf("AllocateBlock failed: %v", err)
		}
		if pid != common.BlockID(i) {
			t.Fatalf("AllocateBlock returned %d, want %d", pid, i)
		}
	}

	// Verify page count
	numPages, err := dm.GetNumPages(rel, fork)
	if err != nil {
		t.Fatal(err)
	}
	if numPages != 5 {
		t.Fatalf("GetNumPages=%d, want 5", numPages)
	}
}

func TestAllocatePage_CreatesSegment(t *testing.T) {
	dm, _ := newTestDM(t, 10, 100)

	rel := common.RelationID(2)
	fork := common.ForkMain

	pid, err := dm.AllocateBlock(rel, fork)
	if err != nil {
		t.Fatal(err)
	}
	if pid != 0 {
		t.Fatalf("expected page 0, got %d", pid)
	}

	path := dm.segmentPath(rel, fork, 0)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("segment file not created: %v", err)
	}
}

func TestAllocatePage_SingleSegmentOnly(t *testing.T) {
	dm, dir := newTestDM(t, 10, 100)

	rel := common.RelationID(1)
	fork := common.ForkMain

	// Allocate multiple pages, but stay within first segment
	n := int(common.MaxPagesPerSegment - 1)

	for i := 0; i < n; i++ {
		pid, err := dm.AllocateBlock(rel, fork)
		if err != nil {
			t.Fatalf("AllocateBlock failed: %v", err)
		}
		if pid != common.BlockID(i) {
			t.Fatalf("blockid=%d, want %d", pid, i)
		}
	}

	// Segment 0 MUST exist
	seg0 := dm.segmentPath(rel, fork, 0)
	if _, err := os.Stat(seg0); err != nil {
		t.Fatalf("segment 0 missing: %v", err)
	}

	// Segment 1 MUST NOT exist
	seg1 := dm.segmentPath(rel, fork, 1)
	if _, err := os.Stat(seg1); !os.IsNotExist(err) {
		t.Fatalf("segment 1 should not exist")
	}

	// Ensure no extra files exist
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 file, found %d", len(entries))
	}
}

func TestAllocatePageCreatesNewSegment(t *testing.T) {
	dm, dir := newTestDM(t, 10, 100)

	rel := common.RelationID(1)
	fork := common.ForkMain

	data := bytes.Repeat([]byte{0xAB}, int(common.PageSize))

	// Fill first segment completely
	for i := common.BlockID(0); i < common.BlockID(common.MaxPagesPerSegment); i++ {
		block, err := dm.AllocateBlock(rel, fork)
		if err != nil {
			t.Fatalf("AllocateBlock failed: %v", err)
		}
		if block != i {
			t.Fatalf("Allocated block = %d, want %d", block, i)
		}

		if err := dm.WritePage(rel, fork, block, data, false); err != nil {
			t.Fatalf("WritePage failed: %v", err)
		}
	}

	// Allocate one more page → should create segment 1
	page, err := dm.AllocateBlock(rel, fork)
	if err != nil {
		t.Fatalf("AllocateBlock failed: %v", err)
	}
	if page != common.BlockID(common.MaxPagesPerSegment) {
		t.Fatalf("allocated page = %d, want %d", page, common.MaxPagesPerSegment)
	}

	if err := dm.WritePage(rel, fork, page, data, false); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}

	// test file with pages count
	actualCount, err := dm.GetNumPages(rel, fork)

	if err != nil {
		t.Fatalf("GetNumPages failed: %v", err)
	}

	if actualCount != common.BlockID(common.MaxPagesPerSegment+1) {
		t.Fatalf("page count = %d, want %d", actualCount, common.MaxPagesPerSegment+1)
	}

	// Force close all files
	_ = dm.vfd.CloseAll(false)

	// ---- Verify filesystem state ----

	seg0 := filepath.Join(dir, "1")
	seg1 := filepath.Join(dir, "1.1")
	seg2 := filepath.Join(dir, "1.2")

	if _, err := os.Stat(seg0); err != nil {
		t.Fatalf("segment 0 missing: %v", err)
	}
	if _, err := os.Stat(seg1); err != nil {
		t.Fatalf("segment 1 missing: %v", err)
	}
	if _, err := os.Stat(seg2); !os.IsNotExist(err) {
		t.Fatalf("unexpected segment 2 exists")
	}
}

func TestAllocatePageConcurrent(t *testing.T) {
	dm, dir := newTestDM(t, 50, 100)

	rel := common.RelationID(1)
	fork := common.ForkMain

	const workers = 64

	var wg sync.WaitGroup
	wg.Add(workers)

	results := make(chan common.BlockID, workers)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			page, err := dm.AllocateBlock(rel, fork)
			if err != nil {
				t.Errorf("AllocateBlock failed: %v", err)
				return
			}
			results <- page
		}()
	}

	wg.Wait()
	close(results)

	// ---- Verify uniqueness ----

	seen := make(map[common.BlockID]bool)
	for p := range results {
		if seen[p] {
			t.Fatalf("duplicate page ID allocated: %d", p)
		}
		seen[p] = true
	}

	if len(seen) != workers {
		t.Fatalf("allocated pages = %d, want %d", len(seen), workers)
	}

	// ---- Verify segments on disk ----
	_ = dm.vfd.CloseAll(false)

	expectedSegments := (workers + common.MaxPagesPerSegment - 1) / common.MaxPagesPerSegment

	for seg := 0; seg < int(expectedSegments); seg++ {
		path := filepath.Join(dir, fmt.Sprintf("1.%d", seg))
		if seg == 0 {
			path = filepath.Join(dir, "1")
		}

		if _, err := os.Stat(path); err != nil {
			t.Fatalf("missing segment file %s", path)
		}
	}

	// Ensure no extra segment was created
	extra := filepath.Join(dir, fmt.Sprintf("1.%d", expectedSegments))
	if _, err := os.Stat(extra); !os.IsNotExist(err) {
		t.Fatalf("unexpected extra segment file %s", extra)
	}
}
