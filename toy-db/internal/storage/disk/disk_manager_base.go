package disk

import (
	"container/list"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/surajjagdev/ToyDB/internal/common"
)

type DiskManager interface {
	ReadPage(rel common.RelationID, fork common.ForkID, page common.BlockID, data []byte) error
	WritePage(rel common.RelationID, fork common.ForkID, page common.BlockID, data []byte) error
	AllocateBlock(rel common.RelationID, fork common.ForkID) (common.BlockID, error)
	SyncPage(rel common.RelationID, fork common.ForkID, page common.BlockID) error
	SyncDir(rel common.RelationID, fork common.ForkID) error
	GetNumPages(rel common.RelationID, fork common.ForkID) (common.BlockID, error)
	Shutdown() error
}

type RelationFork struct {
	Rel  common.RelationID
	Fork common.ForkID
}

type dfEntry struct {
	count common.BlockID
	elem  *list.Element
	mu    sync.Mutex
}

type ManagerBase struct {
	baseDir string

	pageCount  map[RelationFork]*dfEntry
	lruList    *list.List
	maxEntries int

	countMu sync.Mutex
}

func newManagerBase(baseDir string, maxCachedRelations int) *ManagerBase {
	return &ManagerBase{
		baseDir:    baseDir,
		pageCount:  make(map[RelationFork]*dfEntry),
		lruList:    list.New(),
		maxEntries: maxCachedRelations,
	}
}

// -----------------------------------------------------------------------------
// LRU helpers
// -----------------------------------------------------------------------------

func (m *ManagerBase) touchEntry(rf RelationFork, e *dfEntry) {
	m.countMu.Lock()
	defer m.countMu.Unlock()

	if e.elem != nil {
		m.lruList.MoveToFront(e.elem)
	} else {
		e.elem = m.lruList.PushFront(rf)
	}

	if m.lruList.Len() > m.maxEntries {
		m.evictOldest()
	}
}

func (m *ManagerBase) evictOldest() {
	back := m.lruList.Back()
	if back == nil {
		return
	}

	rf := back.Value.(RelationFork)
	m.lruList.Remove(back)

	if e, ok := m.pageCount[rf]; ok {
		e.elem = nil
		delete(m.pageCount, rf)
	}
}

// others
func (d *ManagerBase) loadPageCountFromDisk(rel common.RelationID, fork common.ForkID) (common.BlockID, error) {
	var totalPages common.BlockID = 0
	segment := common.BlockID(0)

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
		pages := common.BlockID(size / int64(common.PageSize))
		totalPages += pages

		// If this segment is not full, it must be the last one
		if size < int64(common.MaxSegmentSize) {
			break
		}
		segment++
	}
	return totalPages, nil
}

func (d *ManagerBase) GetNumPages(rel common.RelationID, fork common.ForkID) (common.BlockID, error) {
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

// Sync directory on new seg creation
func (m *ManagerBase) SyncDir(rel common.RelationID, fork common.ForkID) error {
	// Any segment path works; we just need the directory
	segPath := m.segmentPath(rel, fork, 0)
	dirPath := filepath.Dir(segPath)

	// skip vfd
	dir, err := os.Open(dirPath)
	if err != nil {
		return err
	}
	defer dir.Close()

	// fsync directory metadata
	return dir.Sync()
}

// -----------------------------------------------------------------------------
// Path resolution (shared)
// -----------------------------------------------------------------------------

func (m *ManagerBase) segmentPath(
	rel common.RelationID,
	fork common.ForkID,
	segment common.BlockID,
) string {

	base := filepath.Join(m.baseDir, fmt.Sprintf("%d", rel))

	if fork != common.ForkMain {
		base = fmt.Sprintf("%s_%d", base, fork)
	}

	if segment == 0 {
		return base
	}
	return fmt.Sprintf("%s.%d", base, segment)
}

func (m *ManagerBase) resolveLocation(
	rel common.RelationID,
	fork common.ForkID,
	page common.BlockID,
) (string, int64) {

	segment := page / common.BlockID(common.MaxPagesPerSegment)
	segPage := page % common.BlockID(common.MaxPagesPerSegment)

	return m.segmentPath(rel, fork, segment),
		int64(segPage) * int64(common.PageSize)
}
