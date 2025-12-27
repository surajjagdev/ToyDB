package disk

// read and write bytes to disk
// define endiness

import (
	"container/list"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/surajjagdev/ToyDB/internal/common"
	//"github.com/surajjagdev/ToyDB/internal/common"
)

type RelationFork struct {
	Rel  common.RelationID
	Fork common.ForkID
}

type dfEntry struct {
	count common.PageID
	elem  *list.Element // pointer to LRU element
	mu    sync.Mutex    // protects this relation fork's pageCount
}

type DiskManager struct {
	baseDir string
	vfd     *VFDCache

	pageCount  map[RelationFork]*dfEntry // per relation/fork
	lruList    *list.List
	maxEntries int
	countMu    sync.Mutex // protects map itself
}

// Creates a new instance for disk manager
func NewDiskManager(baseDir string, maxOpenFiles int, maxCachedRelations int) (*DiskManager, error) {
	// creates or does nothing. throws if base dir is not a directory
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create db directory: %w", err)
	}

	return &DiskManager{
		baseDir:    baseDir,
		vfd:        NewVFDCache(maxOpenFiles),
		pageCount:  make(map[RelationFork]*dfEntry),
		lruList:    list.New(),
		maxEntries: maxCachedRelations,
	}, nil
}

// helpers

// put rel to front of lru
func (d *DiskManager) touchEntry(rf RelationFork, e *dfEntry) {
	d.countMu.Lock()
	defer d.countMu.Unlock()

	if e.elem != nil {
		d.lruList.MoveToFront(e.elem)
	} else {
		e.elem = d.lruList.PushFront(rf)
	}

	if d.lruList.Len() > d.maxEntries {
		d.evictOldest()
	}
}

// should be done in mutex
func (d *DiskManager) evictOldest() {
	back := d.lruList.Back()
	if back == nil {
		return
	}

	rf := back.Value.(RelationFork)
	d.lruList.Remove(back)

	if e, exists := d.pageCount[rf]; exists {
		e.elem = nil
		delete(d.pageCount, rf)
	}
}

// Calculates the total number of pages for a relation by checking every file
// in the relation , every fork
func (d *DiskManager) loadPageCountFromDisk(rel common.RelationID, fork common.ForkID) (common.PageID, error) {
	var totalPages common.PageID = 0
	segment := common.PageID(0)

	for {
		path := d.segmentPath(rel, fork, segment)

		// Bypass VFD for this check to avoid polluting the cache with stats
		// or use os.Stat directly.
		info, err := os.Stat(path)
		if os.IsNotExist(err) {
			break // No more segments
		}
		if err != nil {
			return 0, err
		}

		size := info.Size()
		pages := common.PageID(size / int64(common.PageSize))
		totalPages += pages

		// If this segment is not full, it must be the last one
		if size < int64(common.MaxSegmentSize) {
			break
		}
		segment++
	}
	return totalPages, nil
}

func (d *DiskManager) Shutdown() error {
	return d.vfd.CloseAll(true)
}

// io ops

// read a page using the relationid + fork (multiple pages)
func (d *DiskManager) ReadPage(
	rel common.RelationID,
	fork common.ForkID,
	page common.PageID,
	data []byte) error {
	// verify data is same as page size
	if len(data) != int(common.PageSize) {
		return fmt.Errorf("buffer size %d != PageSize %d", len(data), int(common.PageSize))
	}

	// get path
	path, offset := d.resolveLocation(rel, fork, page)

	entry, err := d.vfd.GetOrOpen(FileNode{Path: path}, os.O_RDWR)

	if err != nil {
		return err
	}

	defer d.vfd.Release(entry)

	// read data at seek pos
	n, err := entry.file.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return err
	}

	if n < int(common.PageSize) {
		return fmt.Errorf("partial page read (%d/%d)", n, common.PageSize)
	}

	// update LRU
	rf := RelationFork{Rel: rel, Fork: fork}
	d.countMu.Lock()
	df, exists := d.pageCount[rf]
	d.countMu.Unlock()
	if exists {
		d.touchEntry(rf, df)
	}

	return nil
}

// write a page to disk
func (d *DiskManager) WritePage(
	rel common.RelationID,
	fork common.ForkID,
	page common.PageID,
	data []byte,
	allowCreate bool, // for testing, in practice should be false
) error {

	// check if correct page size
	if len(data) != int(common.PageSize) {
		return fmt.Errorf("buffer size %d != PageSize %d", len(data), common.PageSize)
	}

	// resolve path
	path, offset := d.resolveLocation(rel, fork, page)

	// acquire for vfd
	flags := os.O_RDWR
	if allowCreate {
		flags |= os.O_CREATE
	}

	entry, err := d.vfd.GetOrOpen(FileNode{Path: path}, flags)

	if err != nil {
		return err
	}

	defer d.vfd.Release(entry)

	n, err := entry.file.WriteAt(data, offset)
	if err != nil {
		return err
	}
	if n != int(common.PageSize) {
		return fmt.Errorf("partial write (%d/%d)", n, common.PageSize)
	}

	// update rel page count
	// add entry with quick lock
	rf := RelationFork{Rel: rel, Fork: fork}
	d.countMu.Lock()
	df, exists := d.pageCount[rf]
	if !exists {
		actualCount, err := d.loadPageCountFromDisk(rel, fork)
		if err != nil {
			d.countMu.Unlock()
			return err
		}
		df = &dfEntry{count: actualCount}
		d.pageCount[rf] = df
	}
	d.countMu.Unlock()

	df.mu.Lock()
	if page >= df.count {
		df.count = page + 1
	}
	df.mu.Unlock()

	d.touchEntry(rf, df)

	return nil
}

func (d *DiskManager) AllocatePage(rel common.RelationID, fork common.ForkID) (common.PageID, error) {
	rf := RelationFork{Rel: rel, Fork: fork}

	// 1. init page count
	d.countMu.Lock()
	df, exists := d.pageCount[rf]

	// if we didnt have a cached page count, get accurate
	// page count from disk and cache it
	if !exists {
		actualCount, err := d.loadPageCountFromDisk(rel, fork)

		if err != nil {
			d.countMu.Unlock()
			return common.InvalidPageID, err
		}
		df = &dfEntry{count: actualCount}
		d.pageCount[rf] = df
	}

	d.countMu.Unlock()

	// 2. Allocate page size
	df.mu.Lock()
	pageID := df.count
	df.count++
	df.mu.Unlock()

	// do we need new segment ?
	if pageID%common.PageID(common.MaxPagesPerSegment) == 0 {
		segment := pageID / common.PageID(common.MaxPagesPerSegment)
		path := d.segmentPath(rel, fork, segment)
		flags := os.O_RDWR | os.O_CREATE

		entry, err := d.vfd.GetOrOpen(
			FileNode{Path: path},
			flags,
		)

		if err != nil {
			// Fatal disk error: do NOT roll back pageID
			return common.InvalidPageID, fmt.Errorf("failed to create segment file: %w", err)
		}
		d.vfd.Release(entry)
	}

	d.touchEntry(rf, df)

	return pageID, nil
}

func (d *DiskManager) SyncPage(rel common.RelationID, fork common.ForkID, page common.PageID) error {
	path, _ := d.resolveLocation(rel, fork, page)

	// We need the file handle to call Sync()
	entry, err := d.vfd.GetOrOpen(FileNode{Path: path}, os.O_RDWR)
	if err != nil {
		return err
	}
	defer d.vfd.Release(entry)

	// Force OS to flush buffer to hardware
	return entry.file.Sync()
}

// get number of page counts
func (d *DiskManager) GetNumPages(rel common.RelationID, fork common.ForkID) (common.PageID, error) {
	rf := RelationFork{Rel: rel, Fork: fork}

	// 1. Check Cache
	d.countMu.Lock()
	df, exists := d.pageCount[rf]

	if exists {
		d.countMu.Unlock()
		d.touchEntry(rf, df)
		return df.count, nil
	}

	d.countMu.Unlock()

	// load count from disk
	actualCount, err := d.loadPageCountFromDisk(rel, fork)

	if err != nil {
		return 0, err
	}

	// update cache
	d.countMu.Lock()

	// check if someone else populated
	if existingDF, ok := d.pageCount[rf]; ok {
		d.countMu.Unlock()
		existingDF.mu.Lock()
		defer existingDF.mu.Unlock()
		return existingDF.count, nil
	}

	newDf := &dfEntry{count: actualCount}
	d.pageCount[rf] = newDf
	d.countMu.Unlock()

	return actualCount, nil
}

// -----------------------------------------------------------------------------
// Path Resolution
// -----------------------------------------------------------------------------

func (d *DiskManager) segmentPath(
	rel common.RelationID,
	fork common.ForkID,
	segment common.PageID,
) string {

	base := filepath.Join(d.baseDir, fmt.Sprintf("%d", rel))

	if fork != common.ForkMain {
		base = fmt.Sprintf("%s_%d", base, fork)
	}

	if segment == 0 {
		return base
	}
	return fmt.Sprintf("%s.%d", base, segment)
}

// returns path and offset
func (d *DiskManager) resolveLocation(
	rel common.RelationID,
	fork common.ForkID,
	page common.PageID,
) (string, int64) {

	segment := page / common.PageID(common.MaxPagesPerSegment)
	segPage := page % common.PageID(common.MaxPagesPerSegment)

	path := d.segmentPath(rel, fork, segment)
	offset := int64(segPage) * int64(common.PageSize)

	return path, offset
}
