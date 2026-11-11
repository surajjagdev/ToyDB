package clog

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/disk"
)

const (
	PageSize      = 8192
	PageShift     = 13
	XidsPerByte   = 4 // 2 bits per xid = 4 xids per byte
	BitsPerXid    = 2
	XidsPerPage   = PageSize * XidsPerByte
	TxnIdsPerPage = XidsPerPage
)

type Status uint8

const (
	// InProgress is the default state of a slot that has never been written.
	// A transaction whose status is InProgress either is currently running
	// or was running when the process crashed.
	InProgress Status = 0b00

	// Committed means the transaction's changes are durable and visible.
	Committed Status = 0b01

	// Aborted means the transaction was rolled back and its changes must be
	// treated as if they never happened.
	Aborted Status = 0b10

	// SubCommitted is reserved for subtransaction semantics. Unused in v1.
	SubCommitted Status = 0b11
)

func (s Status) String() string {
	switch s {
	case InProgress:
		return "in-progress"
	case Committed:
		return "committed"
	case Aborted:
		return "aborted"
	case SubCommitted:
		return "sub-committed"
	}
	return fmt.Sprintf("unknown(%d)", uint8(s))
}

type Manager struct {
	mu sync.Mutex

	path  string
	vfd   *disk.VFDCache
	entry *disk.VfdEntry

	// pageCache holds the in-memory copy of every page the CLOG has touched.
	// Pages are loaded on first access and retained for the manager's
	// lifetime; there is no eviction. A page that has never been written on
	// disk is zero-filled on load, which correctly represents every slot in
	// it as InProgress.
	pageCache map[uint64][]byte

	// dirty tracks which cached pages have unsaved modifications. Flush
	// writes and fsyncs each dirty page, then clears the set.
	dirty map[uint64]struct{}

	// closed is set by Close. All public methods return ErrClosed afterwards.
	closed bool
}

var ErrClosed = errors.New("clog: manager is closed")

// Open opens or creates the CLOG at <dir>/clog. The directory is created if
// it does not exist.
func Open(vfd *disk.VFDCache, dir string) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("clog: mkdir %s: %w", dir, err)
	}
	path := filepath.Join(dir, "clog")
	entry, err := vfd.GetOrOpen(disk.FileNode{Path: path}, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, fmt.Errorf("clog: open %s: %w", path, err)
	}
	return &Manager{
		path:      path,
		vfd:       vfd,
		entry:     entry,
		pageCache: make(map[uint64][]byte),
		dirty:     make(map[uint64]struct{}),
	}, nil
}

// Close flushes all dirty pages and releases the underlying VFD entry.
// Idempotent; subsequent calls return nil.
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	// Mark closed under the lock so concurrent callers race deterministically.
	m.closed = true
	flushErr := m.flushLocked()
	m.mu.Unlock()

	// Release the VFD entry outside the lock: Release takes its own lock,
	// and holding both would create a lock-ordering hazard if the VFD ever
	// invokes a callback into the CLOG.

	m.vfd.Release(m.entry)

	if flushErr != nil {
		return flushErr
	}

	return nil
}

// GetStatus returns the status of a single transaction.
func (m *Manager) GetStatus(xid common.TransactionID) (Status, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, ErrClosed
	}
	return m.getStatusLocked(xid)
}

// check if txn is committed
func (m *Manager) IsCommitted(xid common.TransactionID) (bool, error) {
	s, err := m.GetStatus(xid)
	if err != nil {
		return false, err
	}
	return s == Committed, nil
}

// SetStatus updates the status of a single transaction.
func (m *Manager) SetStatus(xid common.TransactionID, s Status) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	return m.setStatusLocked(xid, s)
}

// GetStatusBatch returns the statuses of many transactions in one call.
// Faster than a loop over GetStatus when the batch is large, because it
// holds the lock once.
func (m *Manager) GetStatusBatch(xids []common.TransactionID) ([]Status, error) {
	out := make([]Status, len(xids))
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	for i, xid := range xids {
		s, err := m.getStatusLocked(xid)
		if err != nil {
			return nil, err
		}
		out[i] = s
	}
	return out, nil
}

// Flush writes all dirty pages to disk and fsyncs the file. Safe to call
// concurrently with Get/SetStatus.
func (m *Manager) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	return m.flushLocked()
}

// Path returns the file path of the CLOG. Useful for logging and tests.
func (m *Manager) Path() string { return m.path }

// -----------------------------------------------------------------------------
// Internal helpers (require m.mu held)
// -----------------------------------------------------------------------------

func (m *Manager) getStatusLocked(xid common.TransactionID) (Status, error) {
	pageID, byteOffset, shift := locate(xid)
	data, err := m.loadPageLocked(pageID)
	if err != nil {
		return InProgress, err
	}
	return Status((data[byteOffset] >> shift) & 0b11), nil
}

func (m *Manager) setStatusLocked(xid common.TransactionID, s Status) error {
	pageID, byteOffset, shift := locate(xid)
	data, err := m.loadPageLocked(pageID)
	if err != nil {
		return err
	}
	mask := byte(0b11) << shift
	data[byteOffset] = (data[byteOffset] &^ mask) | (byte(s) << shift)
	m.dirty[pageID] = struct{}{}
	return nil
}

// loadPageLocked returns the cached page, loading it from disk on first use.
// A short read (fewer than PageSize bytes) is not an error: the page has
// simply never been written past that point, and the missing bytes are
// correctly interpreted as zero — i.e. every slot is InProgress.
func (m *Manager) loadPageLocked(pageID uint64) ([]byte, error) {
	if data, ok := m.pageCache[pageID]; ok {
		return data, nil
	}
	data := make([]byte, PageSize)
	n, err := m.entry.ReadAt(data, int64(pageID)*PageSize)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("clog: read page %d: %w", pageID, err)
	}
	_ = n // short reads are fine; the tail stays zero
	m.pageCache[pageID] = data
	return data, nil
}

func (m *Manager) flushLocked() error {
	if len(m.dirty) == 0 {
		return nil
	}
	for pageID := range m.dirty {
		data := m.pageCache[pageID]
		if _, err := m.entry.WriteAt(data, int64(pageID)*PageSize); err != nil {
			return fmt.Errorf("clog: write page %d: %w", pageID, err)
		}
	}
	if err := m.entry.Sync(); err != nil {
		return fmt.Errorf("clog: fsync: %w", err)
	}
	m.dirty = make(map[uint64]struct{})
	return nil
}

// locate maps a transaction ID to (pageID, byteOffsetWithinPage, bitShift).
//
// Layout within a page:
//
//	byte 0:  xid 0 (bits 0-1), xid 1 (bits 2-3), xid 2 (bits 4-5), xid 3 (bits 6-7)
//	byte 1:  xid 4, xid 5, xid 6, xid 7
//	...
//	byte 8191: xid 32764, xid 32765, xid 32766, xid 32767
func locate(xid common.TransactionID) (pageID uint64, byteOffset int, shift uint) {
	byteInFile := uint64(xid) / XidsPerByte
	pageID = byteInFile >> PageShift
	byteOffset = int(byteInFile & (PageSize - 1))
	shift = uint(xid%XidsPerByte) * BitsPerXid
	return pageID, byteOffset, shift
}
