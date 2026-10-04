package buffer

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/disk"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

const (
	INVALID_IDX = ^(common.FrameIndex(0))

	evictionWaitTimeout = 1 * time.Second
)

// To reduce contention, a frame partition holds a subset of frames
type FramePartition struct {
	mu sync.RWMutex // lock for the partition's map and clock hand

	frames []*Frame

	numFrames common.FrameIndex // number of frames in the partition

	frameMap map[BufferTag]common.FrameIndex // identifier to index in the frames

	freeFrames []common.FrameIndex // indices of free frames

	clockHand common.FrameIndex // index of the current frame in the frames slice

	// evictable is closed and replaced whenever a frame's pin count or
	// IO flag drops. Waiters capture the current channel under fp.mu,
	// release, and select on it plus a timer.
	evictable chan struct{}
}

type LogFlusher interface {
	FlushUpTo(lsn wal.LSN) error
}

// we hold a number of different frame partitions
type BufferPool struct {
	diskManager disk.DiskManager

	wal LogFlusher

	numPartitions common.PartitionIndex // number of partitions in the buffer pool

	partitions []*FramePartition

	isShuttingDown atomic.Bool
}

func NewFramePartition(numFrames common.FrameIndex) (*FramePartition, error) {
	frames := make([]*Frame, numFrames)
	freeFrames := make([]common.FrameIndex, numFrames)

	fp := &FramePartition{
		mu:         sync.RWMutex{},
		frames:     frames,
		numFrames:  numFrames,
		frameMap:   make(map[BufferTag]common.FrameIndex),
		freeFrames: freeFrames,
		clockHand:  0,
		evictable:  make(chan struct{}),
	}

	for i := common.FrameIndex(0); i < numFrames; i++ {
		frame, err := NewFrame()
		if err != nil {
			return nil, fmt.Errorf("failed to create frame: %v", err)
		}
		frame.onEvictable = fp.notifyEvictable
		frames[i] = frame
		freeFrames[i] = i
	}

	return fp, nil
}

func NewBufferPool(diskManager disk.DiskManager,
	wal LogFlusher,
	numPartitions common.PartitionIndex,
	numFrames common.FrameIndex) (*BufferPool, error) {

	if numPartitions <= 0 {
		return nil, fmt.Errorf("numPartitions must be greater than 0")
	}

	if diskManager == nil {
		return nil, fmt.Errorf("diskManager must be not nil")
	}

	partitions := make([]*FramePartition, numPartitions)

	for i := common.PartitionIndex(0); i < numPartitions; i++ {
		partition, err := NewFramePartition(numFrames)

		if err != nil {
			return nil, fmt.Errorf("failed to create frame partition: %v", err)
		}

		partitions[i] = partition
	}

	return &BufferPool{
		diskManager:    diskManager,
		wal:            wal,
		numPartitions:  numPartitions,
		partitions:     partitions,
		isShuttingDown: atomic.Bool{},
	}, nil
}

// Internal methods

// writePageOut flushes the frame's page to disk and clears the dirty bit.
// It acquires the frame's write latch for the whole operation, so callers
// must NOT already hold the frame latch, and it clears the dirty bit under
// the same latch that observed the bytes (otherwise a concurrent writer
// could re-dirty between the write and the clear).
func (bp *BufferPool) writePageOut(frame *Frame) error {
	frame.WLatch()
	defer frame.WUnlatch()

	// Write-ahead rule: the WAL must be durable up to the page's pageLSN
	// before the page is written to disk. Otherwise a crash could leave
	// the page on disk reflecting a WAL record that never made it.
	pageLSN := frame.Page.GetLSN()
	if pageLSN != 0 {
		if err := bp.wal.FlushUpTo(pageLSN); err != nil {
			return fmt.Errorf("write-ahead flush failed: %w", err)
		}
	}

	frame.Page.UpdateChecksum()

	if err := bp.diskManager.WritePage(
		frame.GetFrameIdentity().RelationID,
		frame.GetFrameIdentity().ForkID,
		frame.GetFrameIdentity().BlockID,
		frame.Page,
		false,
	); err != nil {
		return err
	}

	frame.ClearDirty()
	return nil
}

// notifyEvictable wakes anyone blocked in claimVictimFrame.
// Must be called WITHOUT fp.mu held.
// closing and re-opening channel notifies all watchers
// and we have a bounded timeout
func (fp *FramePartition) notifyEvictable() {
	fp.mu.Lock()
	close(fp.evictable)
	fp.evictable = make(chan struct{})
	fp.mu.Unlock()
}

// From internet, seems to be a good hash function for this use case
// Given buffer tag, get a consistient index to the frame partition in buffer pool
func (bp *BufferPool) getPartitionIndex(tag BufferTag) common.PartitionIndex {
	h := common.PartitionIndex(tag.RelationID) ^ common.PartitionIndex(tag.ForkID) ^ common.PartitionIndex(tag.BlockID)
	// Mix the bits thoroughly
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16

	return h % bp.numPartitions
}

// Assumes you have a write lock on the frame partition
func (fp *FramePartition) freeFramesAvail() bool {
	return len(fp.freeFrames) > 0
}

// Assumes you have a write lock on the frame partition
func (fp *FramePartition) popFreeFrame() (common.FrameIndex, bool) {
	if len(fp.freeFrames) == 0 {
		return 0, false
	}

	last := len(fp.freeFrames) - 1
	frameIdx := fp.freeFrames[last]
	fp.freeFrames = fp.freeFrames[:last]

	return frameIdx, true
}

// claimVictimFrame runs the clock sweep to find a frame with no pins,
// no IO in progress, and zero usage. If no victim is available, it waits
// (bounded by evictionWaitTimeout) for an Unpin/StopIO to make one
// available, then re-checks whether the tag we want appeared in the map
// while we were blocked. Returns (frame, idx, true, nil) on success, or
// (nil, INVALID_IDX, false, nil) if the caller should retry the fast path
//
// Caller must hold fp.mu as a WRITE lock.
func (bp *BufferPool) claimVictimFrame(
	fp *FramePartition,
	tag BufferTag,
) (*Frame, common.FrameIndex, bool, error) {

	n := fp.numFrames
	deadline := time.Now().Add(evictionWaitTimeout)

	for {
		// Capture evictable BEFORE sweeping, so a signal that fires
		// during the sweep is not lost.
		ch := fp.evictable

		for i := common.FrameIndex(0); i < (n * 2); i++ {
			idx := fp.clockHand
			frame := fp.frames[idx]
			fp.clockHand = (fp.clockHand + 1) % n

			if frame.IsPinned() {
				continue
			}
			if !frame.TryStartIO() {
				continue
			}

			if frame.GetUsage() > 0 {
				frame.DecrementUsage()
				// StopIO notifies eviction waiters and takes fp.mu,
				// so it must run with fp.mu released.
				fp.mu.Unlock()
				frame.StopIO()
				fp.mu.Lock()
				continue
			}

			// We own the victim. recheck
			if _, exists := fp.frameMap[tag]; exists {
				fp.mu.Unlock()
				frame.StopIO()
				fp.mu.Lock()
				return nil, INVALID_IDX, false, nil
			}

			return frame, idx, true, nil
		}

		if time.Now().After(deadline) {
			return nil, INVALID_IDX, false, fmt.Errorf(
				"no victim frames found within %s", evictionWaitTimeout)
		}

		fp.mu.Unlock()

		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-ch:
			timer.Stop()
		case <-timer.C:
		}

		fp.mu.Lock()

		if time.Now().After(deadline) {
			return nil, INVALID_IDX, false, fmt.Errorf(
				"no victim frames found within %s", evictionWaitTimeout)
		}
	}
}

// evictFrame picks a victim frame (free list first, then clock sweep),
// re-checks the map for `tag` after a wait, flushes the old contents if
// dirty, and unhooks the old identity. The returned frame has its IO flag
// set and is ready for the caller to install the new tag.
//
// Returns (frame, oldTag, idx, retryNeeded, err). If retryNeeded is true,
// the caller must release fp.mu and retry the fast path: `tag` was
// installed by another goroutine while we were waiting.
//
// Caller must hold fp.mu as a WRITE lock.
func (bp *BufferPool) evictFrame(
	framePartition common.PartitionIndex,
	tag BufferTag,
) (*Frame, BufferTag, common.FrameIndex, bool, error) {

	fp := bp.partitions[framePartition]

	var frame *Frame
	var idx common.FrameIndex
	var oldTag BufferTag

	// ----------------------------
	// 1. pick a frame
	// ----------------------------
	if fp.freeFramesAvail() {
		var popOk bool
		idx, popOk = fp.popFreeFrame()
		if !popOk {
			return nil, BufferTag{}, INVALID_IDX, false,
				fmt.Errorf("free frame pop failed")
		}
		frame = fp.frames[idx]

		if _, exists := fp.frameMap[tag]; exists {
			fp.freeFrames = append(fp.freeFrames, idx)
			return nil, BufferTag{}, INVALID_IDX, true, nil
		}

		// Fresh frame — claim IO now, since claimVictimFrame (which
		// does it on the other path) isn't called here.
		if !frame.TryStartIO() {
			fp.freeFrames = append(fp.freeFrames, idx)
			return nil, BufferTag{}, INVALID_IDX, false,
				fmt.Errorf("failed to claim IO for free frame")
		}

		// Free frames carry the zero tag and are not valid/dirty.
		oldTag = frame.GetFrameIdentity()

		// Publish the tag and clear valid, same as the victim path.
		frame.ClearValid()
		fp.frameMap[tag] = idx

		return frame, oldTag, idx, false, nil
	}

	// ----------------------------
	// 2. victim path
	// ----------------------------
	var claimOk bool
	var err error
	frame, idx, claimOk, err = bp.claimVictimFrame(fp, tag)
	if err != nil {
		return nil, BufferTag{}, INVALID_IDX, false, err
	}
	if !claimOk {
		return nil, BufferTag{}, INVALID_IDX, true, nil
	}

	// claimVictimFrame guarantees the IO flag is set.
	oldTag = frame.GetFrameIdentity()
	oldDirty := frame.IsDirty() && frame.IsValid()

	// Publish BEFORE releasing fp.mu for the flush. This is the whole
	// fix: concurrent GetPage(tag) will find frameMap[tag] and WaitIO
	// instead of claiming a second victim.
	delete(fp.frameMap, oldTag)
	fp.frameMap[tag] = idx
	frame.ClearValid()

	if oldDirty {
		fp.mu.Unlock()

		// writePageOut uses frame.GetFrameIdentity() → still oldTag
		// (we haven't called SetFrameIdentity yet), so it flushes the
		// old contents to the old location. Correct.
		flushErr := bp.writePageOut(frame)

		fp.mu.Lock()
		if flushErr != nil {
			// Roll back the install. frame.bufferTag was never changed.
			delete(fp.frameMap, tag)
			frame.StopIO()
			return nil, BufferTag{}, INVALID_IDX, false,
				fmt.Errorf("failed to flush dirty page: %w", flushErr)
		}
	}

	return frame, oldTag, idx, false, nil
}

func (bp *BufferPool) getPageFromCache(framePartition *FramePartition, tag BufferTag) (*Frame, error, bool) {

	framePartition.mu.RLock()

	frameIdx, ok := framePartition.frameMap[tag]
	if !ok {
		framePartition.mu.RUnlock()
		return nil, nil, false
	}

	frame := framePartition.frames[frameIdx]

	// must pin first
	if !frame.Pin() {
		framePartition.mu.RUnlock()
		return nil, nil, false
	}

	// safe to release partition lock
	framePartition.mu.RUnlock()

	// wait until eviction/IO finishes
	frame.WaitIO()

	// now safe to increase usage
	frame.IncrementUsage()

	if !frame.IsValid() {
		frame.Unpin()
		return nil, fmt.Errorf("frame invalid after IO completion"), false
	}

	return frame, nil, true
}

// Public methods

// We need to get a page by its tag and load into a frame. If it already exists,
// great. otherwise we will need to load from disk
// Page is returned pinned, so you must unpin it
func (bp *BufferPool) GetPage(tag BufferTag) (*Frame, error, bool) {
	if bp.isShuttingDown.Load() {
		return nil, fmt.Errorf("buffer pool is shutting down"), false
	}

	partitionIdx := bp.getPartitionIndex(tag)
	framePartition := bp.partitions[partitionIdx]

	// ----------------------------
	// 1. fast path
	// ----------------------------
	frame, err, ok := bp.getPageFromCache(framePartition, tag)

	if err != nil {
		return nil, err, false
	}
	if ok {
		return frame, nil, true
	}

retry:

	framePartition.mu.Lock()

	// ----------------------------
	// 2. double check (Postgres: recheck after lock)
	// ----------------------------
	if frameIdx, ok := framePartition.frameMap[tag]; ok {
		frame := framePartition.frames[frameIdx]

		if !frame.Pin() {
			framePartition.mu.Unlock()
			runtime.Gosched()
			goto retry
		}

		framePartition.mu.Unlock()

		frame.WaitIO()
		frame.IncrementUsage()

		if !frame.IsValid() {
			frame.Unpin()
			return nil, fmt.Errorf("frame invalid after IO completion"), false
		}

		return frame, nil, true
	}

	// ----------------------------
	// 3. eviction (may block, bounded)
	// ----------------------------
	frame, _, _, retryNeeded, err := bp.evictFrame(partitionIdx, tag)
	if err != nil {
		framePartition.mu.Unlock()
		return nil, err, false
	}
	if retryNeeded {
		// Another goroutine installed `tag` while we were waiting
		// for a victim. Go back to the double-check and take the
		// map path — do NOT install a second frame for the same tag.
		framePartition.mu.Unlock()
		goto retry
	}

	// ----------------------------
	// 4. initialize frame (fp.mu still held — install is atomic)
	// ----------------------------
	frame.WLatch()
	frame.SetFrameIdentity(tag.BlockID, tag.ForkID, tag.RelationID)
	frame.WUnlatch()

	if !frame.Pin() {
		framePartition.mu.Unlock()
		return nil, fmt.Errorf("failed to pin newly allocated frame"), false
	}
	frame.IncrementUsage()

	// unlock before io
	framePartition.mu.Unlock()

	// ----------------------------
	// 5. disk read (no partition lock held)
	// ----------------------------
	err = bp.diskManager.ReadPage(
		tag.RelationID,
		tag.ForkID,
		tag.BlockID,
		frame.Page,
	)

	if err == nil {
		if !frame.Page.ValidateIntegrity() {
			err = fmt.Errorf("page corruption detected: invalid checksum on Block %d", tag.BlockID)
		}
	}

	// ----------------------------
	// 6. finalize
	// ----------------------------
	framePartition.mu.Lock()
	if err != nil {
		delete(framePartition.frameMap, tag)
	} else {
		frame.SetValid()
	}
	framePartition.mu.Unlock()

	// StopIO outside the partition lock: it notifies eviction waiters.
	frame.StopIO()

	if err != nil {
		// Error path: we own the pin we took in step 4, so release it.
		frame.Unpin()
		return nil, err, false
	}

	// Success path: frame stays pinned for the caller.
	return frame, nil, false
}

func (bp *BufferPool) GracefulShutdown(timeout time.Duration) error {
	bp.isShuttingDown.Store(true)
	// wait for active pins to drop to 0, or timeout
	deadline := time.Now().Add(timeout)

	for {
		allUnpinned := true

		// Check every frame in every partition
		for _, partition := range bp.partitions {
			partition.mu.RLock()
			for _, frame := range partition.frames {
				if frame.IsPinned() {
					allUnpinned = false
					break
				}
			}
			partition.mu.RUnlock()

			if !allUnpinned {
				break // skip other partitions in this loop
			}
		}

		if allUnpinned {
			break
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("graceful shutdown timed out: active queries refused to unpin frames")
		}

		time.Sleep(10 * time.Millisecond)
	}

	return bp.Shutdown()
}

// This is a hard shutdown, no graceful shutdown
func (bp *BufferPool) Shutdown() error {
	var firstErr error

	for _, partition := range bp.partitions {
		func(p *FramePartition) {
			p.mu.Lock()
			defer p.mu.Unlock()

			for _, frame := range p.frames {
				// 1. Only flush if the frame actually holds valid data and was modified
				if frame.IsValid() && frame.IsDirty() {
					tag := frame.GetFrameIdentity()

					// 2. Write page out, expected that page exists on disk
					err := bp.writePageOut(frame)

					// 3. Record the first error we see, but keep going!
					if err != nil && firstErr == nil {
						firstErr = fmt.Errorf("failed to flush page %v during shutdown: %w", tag, err)
					}
				}

				// 4. Free the physical OS memory
				frame.Free()
			}
		}(partition)
	}

	// 5. Shut down the disk manager (closes all VFD OS files)
	diskErr := bp.diskManager.Shutdown()

	// 6. Return errors if any occurred
	if firstErr != nil {
		return firstErr
	}
	return diskErr
}

// Allocate page
// page is returned pinned, so unpin it
func (bp *BufferPool) AllocatePage(tag BufferTag) (*Frame, error) {
	// 1. Check if buffer pool is active
	if bp.isShuttingDown.Load() {
		return nil, fmt.Errorf("buffer pool is shutting down")
	}

	// 2. Get the partition idx
	partionId := bp.getPartitionIndex(tag)
	framePartition := bp.partitions[partionId]

	// Lock partition
	framePartition.mu.Lock()

	// Double check: Make sure it's not somehow already in memory
	if _, ok := framePartition.frameMap[tag]; ok {
		framePartition.mu.Unlock()
		return nil, fmt.Errorf("page already exists in buffer pool")
	}

	frame, _, _, retryNeeded, err := bp.evictFrame(partionId, tag)
	if err != nil {
		framePartition.mu.Unlock()
		return nil, err
	}
	if retryNeeded {
		framePartition.mu.Unlock()
		return nil, fmt.Errorf("page already exists in buffer pool")
	}

	// 3. Initialize the frame for the new block
	// evictFrame already installed tag in frameMap and cleared valid.
	// Just initialize in place.
	frame.WLatch()
	frame.SetFrameIdentity(tag.BlockID, tag.ForkID, tag.RelationID)
	frame.Page.ResetPage(0)
	frame.SetValid()
	frame.SetDirty()
	frame.WUnlatch()

	if !frame.Pin() {
		framePartition.mu.Unlock()
		return nil, fmt.Errorf("failed to pin newly allocated frame")
	}

	frame.IncrementUsage()
	framePartition.mu.Unlock()

	// StopIO notifies eviction waiters — must be outside the partition lock.
	frame.StopIO()

	return frame, nil
}

// Flush Single Page
func (bp *BufferPool) FlushPage(tag BufferTag) error {
	// 1. get frame partition
	partitionIdx := bp.getPartitionIndex(tag)
	framePartition := bp.partitions[partitionIdx]

	// 2. lock frame partition
	framePartition.mu.Lock()

	// check if frame is in frame partition
	frameIdx, ok := framePartition.frameMap[tag]

	if !ok {
		framePartition.mu.Unlock()
		return nil // nothing to do, page not in buffer pool
	}

	// frame exists, check if dirty
	frame := framePartition.frames[frameIdx]

	if !frame.IsDirty() {
		framePartition.mu.Unlock()
		return nil // nothing to do, frame not dirty
	}

	// pin the frame
	if !frame.Pin() {
		framePartition.mu.Unlock()
		return fmt.Errorf("failed to pin frame")
	}

	// unlock the frame partition
	framePartition.mu.Unlock()

	// wait for IO to complete
	frame.WaitIO()
	frame.StartIO()

	var err error = nil

	if frame.IsDirty() {
		// writePageOut now clears the dirty bit under its own latch.
		err = bp.writePageOut(frame)
	}

	// release io and unpin (outside the partition lock: both notify waiters)
	frame.StopIO()
	frame.Unpin()

	return err
}

// Flush all pages in the buffer pool
func (bp *BufferPool) FlushAllPages() error {
	var firstError error = nil

	for i := common.PartitionIndex(0); i < bp.numPartitions; i++ {
		fp := bp.partitions[i]

		// Snapshot dirty frames under the partition lock so a concurrent
		// map mutation cannot invalidate our iteration.
		fp.mu.RLock()
		dirty := make([]*Frame, 0, len(fp.frameMap))
		for _, frameIdx := range fp.frameMap {
			frame := fp.frames[frameIdx]
			if frame.IsDirty() {
				dirty = append(dirty, frame)
			}
		}
		fp.mu.RUnlock()

		for _, frame := range dirty {
			if !frame.Pin() {
				if firstError == nil {
					firstError = fmt.Errorf("failed to pin frame during flush")
				}
				continue
			}

			frame.WaitIO()
			frame.StartIO()

			if frame.IsDirty() {
				// writePageOut clears the dirty bit under its own latch.
				if err := bp.writePageOut(frame); err != nil && firstError == nil {
					firstError = err
				}
			}

			frame.StopIO()
			frame.Unpin()
		}
	}

	return firstError
}

// Get buffer pool instance
func (bp *BufferPool) GetDiskManager() (disk.DiskManager, error) {
	if bp.isShuttingDown.Load() {
		return nil, fmt.Errorf("Buffer pool is shutting down")
	}

	return bp.diskManager, nil
}
