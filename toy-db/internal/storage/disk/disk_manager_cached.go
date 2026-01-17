package disk

import (
	"fmt"
	"io"
	"os"

	"github.com/surajjagdev/ToyDB/internal/common"
)

type CachedManager struct {
	*ManagerBase
	vfd *VFDCache
}

func NewCachedManager(
	baseDir string,
	maxOpenFiles int,
	maxCachedRelations int,
) (*CachedManager, error) {

	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}

	return &CachedManager{
		ManagerBase: newManagerBase(baseDir, maxCachedRelations),
		vfd:         NewCachedVFD(maxOpenFiles),
	}, nil
}

func (d *CachedManager) ReadPage(
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

func (d *CachedManager) WritePage(
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

func (d *CachedManager) AllocatePage(rel common.RelationID, fork common.ForkID) (common.PageID, error) {
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

func (m *CachedManager) SyncPage(rel common.RelationID, fork common.ForkID, page common.PageID) error {
	path, _ := m.resolveLocation(rel, fork, page)
	entry, err := m.vfd.GetOrOpen(FileNode{Path: path}, os.O_RDWR)
	if err != nil {
		return err
	}
	defer m.vfd.Release(entry)
	return entry.file.Sync()
}

func (m *CachedManager) Shutdown() error {
	return m.vfd.CloseAll(true)
}
