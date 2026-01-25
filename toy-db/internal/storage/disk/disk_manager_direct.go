package disk

import (
	"fmt"
	"io"
	"os"

	"github.com/surajjagdev/ToyDB/internal/common"
)

type DirectManager struct {
	*ManagerBase
	vfd *VFDCache
}

func NewDirectManager(
	baseDir string,
	maxOpenFiles int,
	maxCachedRelations int,
) (*DirectManager, error) {

	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}

	return &DirectManager{
		ManagerBase: newManagerBase(baseDir, maxCachedRelations),
		vfd:         NewDirectVFD(maxOpenFiles),
	}, nil
}

func (d *DirectManager) ReadPage(rel common.RelationID, fork common.ForkID, page common.BlockID, data []byte) error {
	// get path
	path, offset := d.resolveLocation(rel, fork, page)

	if err := assertDirectIO(data, offset); err != nil {
		return err
	}

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
	if _, exists := d.pageCount[rf]; exists {
		d.touchEntry(rf)
	}
	d.countMu.Unlock()

	return nil
}

func (d *DirectManager) WritePage(rel common.RelationID, fork common.ForkID, page common.BlockID, data []byte, allowCreate bool) error {
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

	if page >= df.count {
		df.count = page + 1
	}

	d.touchEntry(rf)

	d.countMu.Unlock()

	return nil
}

func (d *DirectManager) AllocateBlock(rel common.RelationID, fork common.ForkID) (common.BlockID, error) {
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
			return common.InvalidBlockID, err
		}
		df = &dfEntry{count: actualCount}
		d.pageCount[rf] = df
	}

	// 2. Allocate page size
	blockId := df.count
	df.count++
	d.touchEntry(rf)
	d.countMu.Unlock()

	// do we need new segment ?
	segment := blockId / common.BlockID(common.MaxPagesPerSegment)
	path := d.segmentPath(rel, fork, segment)
	entry, err := d.vfd.GetOrOpen(FileNode{Path: path}, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return common.InvalidBlockID, fmt.Errorf("failed to create segment: %w", err)
	}

	d.vfd.Release(entry)

	return blockId, nil
}

func (m *DirectManager) SyncPage(rel common.RelationID, fork common.ForkID, page common.BlockID) error {
	path, _ := m.resolveLocation(rel, fork, page)

	entry, err := m.vfd.GetOrOpen(FileNode{Path: path}, os.O_RDWR)
	if err != nil {
		return err
	}
	defer m.vfd.Release(entry)

	return directSync(entry.file)
}

func (m *DirectManager) Shutdown() error {
	return m.vfd.CloseAll(true)
}
