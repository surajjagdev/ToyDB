package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/disk"
)

const (
	WAL_FILE_EXT  = ".wal"
	fpwResetBytes = 1 << 30
)

type Options struct {
	Dir         string        // Directory to store wal files
	SegmentSize uint32        // Size of each seg file
	MaxBatch    int           // Max number of records per batch before we force a flush
	BatchWait   time.Duration // Max wait time to group records before flushing
	Fsync       bool          // Fsync or not - for tests
	Metrics     Metrics       // optional metrics reporting
}

// Default options
func WithDefaultOptions(o Options) Options {
	if o.SegmentSize == 0 {
		// default to 16 mb
		o.SegmentSize = 16 * 1024 * 1024
	}
	if o.MaxBatch == 0 {
		// default to 64 mib
		o.MaxBatch = 64 * 1024 * 1024
	}
	if o.BatchWait == 0 {
		// default to 2 milliseconds
		o.BatchWait = 2 * time.Millisecond
	}
	if o.Metrics == nil {
		o.Metrics = NopMetrics{}
	}

	return o
}

type syncWaiter struct {
	lsn  LSN
	done chan struct{}
}

type WAL struct {
	opts    Options
	vfd     *disk.VFDCache
	metrics Metrics

	// Append state for wal records
	mu              sync.Mutex
	currentSeg      uint32
	currentOffset   uint32
	nextLSN         LSN
	file            *disk.VfdEntry
	activeBuf       []byte
	flushBuf        []byte
	rotateCond      *sync.Cond
	rotateRequested bool

	fpwMu                 sync.Mutex
	fpwSeen               map[common.BlockID]struct{}
	walBytesSinceFPWReset uint32

	// Durability state, guarded by flushMu.
	flushMu        sync.Mutex
	lastFlushedLSN LSN
	syncWaiters    map[*syncWaiter]struct{}

	// Terminal state.
	fatalErr atomic.Pointer[error]
	closed   atomic.Bool

	wakeWriterCh chan struct{}
	closeCh      chan struct{}
	wg           sync.WaitGroup
}

// Open + recovery

func Open(vfd *disk.VFDCache, opts Options) (*WAL, error) {
	opts = WithDefaultOptions(opts)

	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}

	w := &WAL{
		opts:                  opts,
		vfd:                   vfd,
		metrics:               opts.Metrics,
		activeBuf:             make([]byte, 0, opts.MaxBatch*2),
		flushBuf:              make([]byte, 0, opts.MaxBatch*2),
		fpwSeen:               make(map[common.BlockID]struct{}),
		walBytesSinceFPWReset: 0,
		syncWaiters:           make(map[*syncWaiter]struct{}),
		wakeWriterCh:          make(chan struct{}, 1),
		closeCh:               make(chan struct{}),
	}

	// rotate seg
	w.rotateCond = sync.NewCond(&w.mu)

	seg, off, err := recoverFromDisk(vfd, opts.Dir, opts.SegmentSize)

	if err != nil {
		return nil, err
	}

	if off >= opts.SegmentSize {
		seg++
		off = 0
	}

	w.currentSeg = seg
	w.currentOffset = off
	w.lastFlushedLSN = MakeLSN(seg, off)
	w.nextLSN = MakeLSN(w.currentSeg, w.currentOffset)

	if err := w.openSegmentLocked(w.currentSeg); err != nil {
		return nil, err
	}

	w.wg.Add(1)
	go w.runWaiter()

	return w, nil
}

func listAllSegments(dir string) ([]uint32, error) {
	enteries, err := os.ReadDir(dir)

	if err != nil {
		return []uint32{}, err
	}

	var segs []uint32

	for _, entry := range enteries {
		name := entry.Name()

		if !strings.HasSuffix(name, WAL_FILE_EXT) {
			continue
		}

		var n uint32

		if _, err := fmt.Sscanf(name, "%08X.wal", &n); err != nil {
			continue
		}

		segs = append(segs, n)
	}

	sort.Slice(segs, func(i, j int) bool { return segs[i] < segs[j] })
	return segs, nil
}

// take seg number -> get hex numbered file
func segmentName(seg uint32) string { return fmt.Sprintf("%08X.wal", seg) }

// open a wal file, scan through all records, find first invalid or end
func scanSegment(vfd *disk.VFDCache, dir string, segNum uint32) (uint32, error) {
	path := filepath.Join(dir, segmentName(segNum))

	entry, err := vfd.GetOrOpen(
		disk.FileNode{Path: path},
		os.O_RDWR|os.O_CREATE,
	)

	if err != nil {
		return 0, err
	}

	defer vfd.Release(entry)

	var offset uint32
	header := make([]byte, RecordHeaderSize)

	// initially set to 4kb, can grow
	buff := make([]byte, 4096)

	for {
		// Read record header
		n, err := entry.ReadAt(header, int64(offset))

		// if end of file, end
		if err == io.EOF && n == 0 {
			break
		}
		if err != nil && err != io.EOF {
			return 0, err
		}
		// incomplete header
		if n < RecordHeaderSize {
			break
		}

		// extract header size
		totalLen, err := PeekRecordLen(header)

		if uint32(len(buff)) < totalLen {
			buff = make([]byte, totalLen)
		}

		if err != nil {
			break
		}

		// copy from disk to buff
		n, err = entry.ReadAt(buff[:totalLen], int64(offset))

		if err != nil && err != io.EOF {
			return 0, err
		}
		if n < int(totalLen) {
			break
		}

		var record Record

		m, err := record.Unmarshal(buff[:totalLen])

		if err != nil {
			break
		}

		if record.LSN != MakeLSN(segNum, offset) {
			break
		}

		offset += uint32(m)
	}

	// truncate a torn tail - corrupt last write
	currFile, err := entry.Stat()

	if err == nil && uint32(currFile.Size()) != offset {
		err := entry.Truncate(int64(offset))

		if err != nil {
			return 0, err
		}
	}

	return offset, nil
}

// Read all the files to find last segment and offset
func recoverFromDisk(vfd *disk.VFDCache, dir string, segmentSize uint32) (uint32, uint32, error) {
	// Get all segments
	segs, err := listAllSegments(dir)

	if err != nil {
		return 0, 0, err
	}

	if len(segs) == 0 {
		// no other segments, return 1st segment, 0th offset, no err
		return 1, 0, nil
	}

	var lastSeg, lastOffset uint32

	for i, segNum := range segs {

		offset, err := scanSegment(vfd, dir, segNum)

		if err != nil {
			return 0, 0, fmt.Errorf("scan segment %d: %w", segNum, err)
		}

		// set last valid segment and offset
		lastSeg, lastOffset = segNum, offset

		// check if segment is not full, but another one exist.
		// should not happen - corrupt
		if i < len(segs)-1 && offset < segmentSize {
			return 0, 0, fmt.Errorf(
				"segment %d short (%d/%d) but %d exists",
				segNum, offset, segmentSize, segs[i+1])
		}
	}

	return lastSeg, lastOffset, nil
}

// Opens segment for appends
func (w *WAL) openSegmentLocked(seg uint32) error {
	path := filepath.Join(w.opts.Dir, segmentName(seg))

	_, statErr := os.Stat(path)
	isNew := os.IsNotExist(statErr)

	entry, err := w.vfd.GetOrOpen(disk.FileNode{Path: path}, os.O_RDWR|os.O_CREATE)

	if err != nil {
		return err
	}

	if w.file != nil {
		w.vfd.Release(w.file)
	}

	w.file = entry

	if isNew {
		d, err := os.Open(w.opts.Dir)
		if err != nil {
			return err
		}
		_ = d.Sync()
		_ = d.Close()
	}

	return nil
}

// Serialize record into active buffer , assign LSN, return. Durability still requires sync
func (w *WAL) Append(rec *Record) (LSN, error) {
	start := time.Now()

	defer func() { w.metrics.ObserveAppendLatency(time.Since(start)) }()

	// if wal is closed, return err closed
	if w.closed.Load() {
		return 0, ErrClosed
	}

	// if wal has fatal err, return err
	if err := w.fatal(); err != nil {
		return 0, err
	}

	size := uint32(rec.EncodedSize())

	if size > w.opts.SegmentSize/2 {
		return 0, fmt.Errorf("%w: %d > %d", ErrRecordSize, size, w.opts.SegmentSize/2)
	}

	w.mu.Lock()

	for {
		if err := w.fatal(); err != nil {
			w.mu.Unlock()
			return 0, err
		}
		if w.closed.Load() {
			w.mu.Unlock()
			return 0, ErrClosed
		}

		if w.rotateRequested {
			// a rotate is in flight, wait for it to finish
			w.rotateCond.Wait()
			continue
		}

		if w.currentOffset+size <= w.opts.SegmentSize {
			break
		}

		// need rotation before record can fit
		w.rotateRequested = true
		w.signalWriter()
		w.rotateCond.Wait()
	}

	rec.LSN = MakeLSN(w.currentSeg, w.currentOffset)

	// add buffer size, and add to it
	begin := len(w.activeBuf)
	w.activeBuf = append(w.activeBuf, make([]byte, size)...)
	rec.MarshalTo(w.activeBuf[begin:])

	w.currentOffset += size
	w.nextLSN = MakeLSN(w.currentSeg, w.currentOffset)

	assigned := rec.LSN

	// check to signal waiters
	shouldWake := len(w.activeBuf) >= w.opts.MaxBatch

	w.walBytesSinceFPWReset += uint32(size)
	if w.walBytesSinceFPWReset >= fpwResetBytes {
		w.walBytesSinceFPWReset = 0
		w.ResetFPWSet()
	}

	w.mu.Unlock()

	if shouldWake {
		w.signalWriter()
	}

	return assigned, nil
}

func (w *WAL) signalWriter() {
	select {
	case w.wakeWriterCh <- struct{}{}:
	default:
	}
}

// -----------------------------------------------------------------------------
// Full-page writes
// -----------------------------------------------------------------------------

// checks if pageId needs full page write. First caller gets true, everything after
// gets false till ResetFPWSet is called
func (w *WAL) NeedsFPW(pageId common.BlockID) bool {
	w.fpwMu.Lock()
	defer w.fpwMu.Unlock()

	if _, ok := w.fpwSeen[pageId]; ok {
		return false
	}

	w.fpwSeen[pageId] = struct{}{}

	return true
}

// ResetFPWSet starts a new full-page-write map
func (w *WAL) ResetFPWSet() {
	w.fpwMu.Lock()
	defer w.fpwMu.Unlock()
	w.fpwSeen = make(map[common.BlockID]struct{})
}

// -----------------------------------------------------------------------------
// Sync
// -----------------------------------------------------------------------------

// Sync blocks until lastFlushedLSN >= lsn, ctx is cancelled, or WAL becomes poisoned
func (w *WAL) Sync(ctx context.Context, lsn LSN) error {
	w.flushMu.Lock()

	if w.lastFlushedLSN > lsn {
		w.flushMu.Unlock()

		return nil
	}

	if err := w.fatal(); err != nil {
		w.flushMu.Unlock()

		return err
	}

	waiter := &syncWaiter{lsn: lsn, done: make(chan struct{})}
	w.syncWaiters[waiter] = struct{}{}

	w.flushMu.Unlock()

	w.signalWriter()

	select {
	case <-waiter.done:
		return w.fatal()
	case <-ctx.Done():
		w.flushMu.Lock()
		delete(w.syncWaiters, waiter)
		w.flushMu.Unlock()
		return ctx.Err()
	case <-w.closeCh:
		if err := w.fatal(); err != nil {
			return err
		}

		return ErrClosed
	}
}

func (w *WAL) FlushUpTo(lsn LSN) error {
	return w.Sync(context.Background(), lsn)
}

// Return highest known LSN known to be fsynced
func (w *WAL) LastDurableLSN() LSN {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	return w.lastFlushedLSN
}

// Returns NextLSN that may not be fsynced
func (w *WAL) LastLSN() LSN {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.nextLSN
}

// -----------------------------------------------------------------------------
// Writer goroutine
// -----------------------------------------------------------------------------

// Wait the batch wait till we flush
func (w *WAL) runWaiter() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.opts.BatchWait)
	defer ticker.Stop()

	// Listen to channel
	for {
		select {
		case <-w.closeCh:
			w.finalFlush()
			return
		case <-w.wakeWriterCh:
			w.doWork()
		case <-ticker.C:
			w.doWork()
		}
	}
}

// handles at most one rotation
func (w *WAL) doWork() {
	if w.fatal() != nil {
		return
	}

	w.mu.Lock()

	if w.rotateRequested {
		toFlush := w.activeBuf
		// reset active buffer and place active
		// in flush buffer
		w.activeBuf = w.flushBuf[:0]
		w.flushBuf = toFlush

		file := w.file
		highLSN := w.nextLSN

		// The buffer's bytes belong at [highLSN.Offset()-len(toFlush), highLSN.Offset())
		flushOff := highLSN.GetOffset() - uint32(len(toFlush))
		w.mu.Unlock()

		if err := w.writeAndSync(file, toFlush, flushOff, highLSN); err != nil {
			w.poison(err)
			return
		}

		w.mu.Lock()

		if w.currentSeg == math.MaxUint32 {
			w.poison(errors.New("wal: segment number overflow"))
			w.mu.Unlock()

			return
		}

		if err := w.openSegmentLocked(w.currentSeg + 1); err != nil {
			w.mu.Unlock()
			w.poison(err)
			return
		}

		w.currentSeg++
		w.currentOffset = 0
		w.nextLSN = MakeLSN(w.currentSeg, 0)
		w.rotateRequested = false
		w.rotateCond.Broadcast()
		w.mu.Unlock()
		return
	}

	if len(w.activeBuf) == 0 {
		w.mu.Unlock()
		return
	}

	toFlush := w.activeBuf
	w.activeBuf = w.flushBuf[:0]
	w.flushBuf = toFlush
	file := w.file
	highLSN := w.nextLSN
	flushOff := highLSN.GetOffset() - uint32(len(toFlush))
	w.mu.Unlock()

	if err := w.writeAndSync(file, toFlush, flushOff, highLSN); err != nil {
		w.poison(err)
		return
	}
}

// writeAndSync writes buf at offset off and fsyncs if configured.
// Runs OUTSIDE w.mu.
func (w *WAL) writeAndSync(file *disk.VfdEntry, buf []byte, off uint32, highLSN LSN) error {
	if len(buf) == 0 {
		w.publishDurable(highLSN)
		return nil
	}

	n, err := file.WriteAt(buf, int64(off))

	if err != nil || n != len(buf) {
		w.metrics.IncFsyncError()
		if err == nil {
			err = fmt.Errorf("short write: %d/%d", n, len(buf))
		}
		return fmt.Errorf("wal write: %w", err)
	}

	if w.opts.Fsync {
		t0 := time.Now()
		if err := file.Sync(); err != nil {
			w.metrics.IncFsyncError()
			return fmt.Errorf("%w: %v", ErrFsync, err)
		}
		w.metrics.ObserveFsyncLatency(time.Since(t0))
	}
	w.metrics.AddBytesWritten(int64(len(buf)))
	w.publishDurable(highLSN)

	return nil
}

// finalFlush runs once after closeCh is closed. Unblocks waiters so they
// can observe closed/poison, then performs a final write.
func (w *WAL) finalFlush() {
	w.mu.Lock()
	w.rotateRequested = false
	w.rotateCond.Broadcast()

	toFlush := w.activeBuf
	w.activeBuf = w.flushBuf[:0]
	w.flushBuf = toFlush
	file := w.file
	highLSN := w.nextLSN
	flushOff := highLSN.GetOffset() - uint32(len(toFlush))
	w.mu.Unlock()

	if err := w.writeAndSync(file, toFlush, flushOff, highLSN); err != nil {
		w.poison(err)
	}
}

// publishDurable advances lastFlushedLSN and wakes any waiters whose LSN
// is now covered.
func (w *WAL) publishDurable(highLSN LSN) {
	w.flushMu.Lock()
	if highLSN > w.lastFlushedLSN {
		w.lastFlushedLSN = highLSN
	}
	for waiter := range w.syncWaiters {
		if waiter.lsn < w.lastFlushedLSN {
			close(waiter.done)
			delete(w.syncWaiters, waiter)
		}
	}

	w.flushMu.Unlock()
}

// -----------------------------------------------------------------------------
// Errors and shutdown
// -----------------------------------------------------------------------------

func (w *WAL) fatal() error {
	if p := w.fatalErr.Load(); p != nil {
		return *p
	}
	return nil
}

func (w *WAL) poison(err error) {
	wrapped := fmt.Errorf("wal fatal: %w", err)
	if w.fatalErr.CompareAndSwap(nil, &wrapped) {
		w.flushMu.Lock()
		for waiter := range w.syncWaiters {
			close(waiter.done)
			delete(w.syncWaiters, waiter)
		}
		w.flushMu.Unlock()

		w.mu.Lock()
		w.rotateCond.Broadcast()
		w.mu.Unlock()
	}
}

// Close flushes and shuts down. Idempotent.
func (w *WAL) Close() error {
	if w.closed.Swap(true) {
		return nil
	}
	close(w.closeCh)
	w.wg.Wait()

	// Wake appenders waiting on a rotation that will never complete.
	w.mu.Lock()
	w.rotateCond.Broadcast()
	w.mu.Unlock()

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		w.vfd.Release(w.file)
		w.file = nil
	}
	return w.fatal()
}
