package disk

// read and write bytes to disk
// define endiness

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/surajjagdev/ToyDB/internal/common"
	//"github.com/surajjagdev/ToyDB/internal/common"
)

// Global endiness
var ByteOrder = binary.LittleEndian

type DiskManager struct {
	baseDir string
	vfd     *VFDCache
	mu      sync.Mutex
}

// Creates a new instance for disk manager
func NewDiskManager(baseDir string, maxOpenFiles int) (*DiskManager, error) {
	// creates or does nothing. throws if base dir is not a directory
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create db directory: %w", err)
	}

	return &DiskManager{
		baseDir: baseDir,
		vfd:     NewVFDCache(maxOpenFiles),
	}, nil
}

func (d *DiskManager) Shutdown() error {
	return d.vfd.CloseAll(true)
}

// io ops

// read a page
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

	d.vfd.Acquire(entry)
	defer d.vfd.Release(entry)

	// read data at seek pos
	n, err := entry.file.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return err
	}

	if n < int(common.PageSize) {
		return fmt.Errorf("partial page read (%d/%d)", n, common.PageSize)
	}

	return nil
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
