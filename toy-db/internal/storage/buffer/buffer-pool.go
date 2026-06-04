package buffer

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/disk"
)

const (
	INVALID_IDX = ^(common.FrameIndex(0))
)

// To reduce contention, a frame partition holds a subset of frames
type FramePartition struct {
	mu sync.RWMutex // lock for the partition's map and clock hand

	frames []*Frame

	numFrames common.FrameIndex // number of frames in the partition

	frameMap map[BufferTag]common.FrameIndex // identifier to index in the frames

	freeFrames []common.FrameIndex // indices of free frames

	clockHand common.FrameIndex // index of the current frame in the frames slice
}

// we hold a number of different frame partitions
type BufferPool struct {
	diskManager disk.DiskManager

	numPartitions common.PartitionIndex // number of partitions in the buffer pool

	partitions []*FramePartition

	isShuttingDown atomic.Bool
}

func NewFramePartition(numFrames common.FrameIndex) (*FramePartition, error) {
	frames := make([]*Frame, numFrames)
	freeFrames := make([]common.FrameIndex, numFrames)

	for i := common.FrameIndex(0); i < numFrames; i++ {
		frame, err := NewFrame()
		if err != nil {
			return nil, fmt.Errorf("failed to create frame: %v", err)
		}
		frames[i] = frame
		freeFrames[i] = i
	}

	return &FramePartition{
		mu:         sync.RWMutex{},
		frames:     frames,
		numFrames:  numFrames,
		frameMap:   make(map[BufferTag]common.FrameIndex),
		freeFrames: freeFrames,
		clockHand:  0,
	}, nil
}

func NewBufferPool(diskManager disk.DiskManager, numPartitions common.PartitionIndex, numFrames common.FrameIndex) (*BufferPool, error) {

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
		numPartitions:  numPartitions,
		partitions:     partitions,
		isShuttingDown: atomic.Bool{},
	}, nil
}

// Internal methods

// Assume you have a write lock on the frame
// Write the page out to disk
func (bp *BufferPool) writePageOut(frame *Frame) error {
	return bp.diskManager.WritePage(
		frame.GetFrameIdentity().RelationID,
		frame.GetFrameIdentity().ForkID,
		frame.GetFrameIdentity().BlockID,
		frame.Page,
		false,
	)
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
func (fp *FramePartition) isFrameInFreeFrameList() bool {
	if len(fp.freeFrames) > 0 {
		return true
	}

	return false
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

// Assume you have a write lock on the frame partition and
// that you have called `isFrameInFreeFrameList`
func (fp *FramePartition) getLastFreeFrame() *Frame {
	idx := fp.freeFrames[len(fp.freeFrames)-1]

	return fp.frames[idx]
}

// Assume you have a write lock on the frame partition
// get a victim frame
func (fp *FramePartition) getVictimFrame() (*Frame, common.FrameIndex, error) {
	n := fp.numFrames

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

		usage := frame.GetUsage()
		if usage > 0 {
			frame.DecrementUsage()
			frame.StopIO()
			continue
		}

		// we own frame now
		return frame, idx, nil
	}

	return nil, INVALID_IDX, fmt.Errorf("no victim frames found")
}

// Assume you have write lock on frame partition
// evict a frame for reuse, looks in free list first
func (bp *BufferPool) evictFrame(framePartition common.PartitionIndex) (*Frame, BufferTag, common.FrameIndex, error) {
	fp := bp.partitions[framePartition]

	var frame *Frame
	var idx common.FrameIndex
	var tag BufferTag

	// ----------------------------
	// 1. pick frame
	// ----------------------------
	if fp.isFrameInFreeFrameList() {
		frame = fp.getLastFreeFrame()
		var ok bool
		idx, ok = fp.popFreeFrame()
		if !ok {
			return nil, BufferTag{}, INVALID_IDX, fmt.Errorf("free frame pop failed")
		}
	} else {
		resFrame, frameIdx, err := fp.getVictimFrame()
		if err != nil {
			return nil, BufferTag{}, INVALID_IDX, err
		}
		frame = resFrame
		idx = frameIdx
	}

	// ----------------------------
	// 2. MUST already own IO
	// ----------------------------
	// (victim already has IO; free-frame must also have it)
	if !frame.IsIOInProgress() {
		if !frame.TryStartIO() {
			return nil, BufferTag{}, INVALID_IDX, fmt.Errorf("failed to claim IO for eviction")
		}
	}

	// ----------------------------
	// 3. read identity
	// ----------------------------
	frame.RLatch()
	tag = frame.GetFrameIdentity()
	frame.RUnlatch()

	// IMPORTANT: remove from map BEFORE IO
	delete(fp.frameMap, tag)

	// ----------------------------
	// 4. flush if needed
	// ----------------------------
	if frame.IsDirty() && frame.IsValid() {
		fp.mu.Unlock()

		err := bp.writePageOut(frame)

		fp.mu.Lock()

		if err != nil {
			frame.StopIO()
			return nil, BufferTag{}, INVALID_IDX, fmt.Errorf("failed to flush dirty page: %w", err)
		}

		frame.ClearDirty()
	}

	frame.ClearValid()

	// caller still owns IO lock + frame
	return frame, tag, idx, nil
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
	// 2. double check
	// ----------------------------
	frameIdx, ok := framePartition.frameMap[tag]
	if ok {
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
	// 3. eviction
	// ----------------------------
	frame, _, frameIdx, err = bp.evictFrame(partitionIdx)
	if err != nil {
		framePartition.mu.Unlock()
		return nil, err, false
	}

	// ----------------------------
	// 4. initialize frame
	// ----------------------------
	frame.WLatch()
	frame.SetFrameIdentity(tag.BlockID, tag.ForkID, tag.RelationID)
	frame.WUnlatch()

	if !frame.Pin() {
		framePartition.mu.Unlock()
		return nil, fmt.Errorf("failed to pin newly allocated frame"), false
	}

	frame.StartIO()
	frame.IncrementUsage()

	framePartition.frameMap[tag] = frameIdx
	framePartition.mu.Unlock()

	// ----------------------------
	// 5. disk read
	// ----------------------------
	err = bp.diskManager.ReadPage(
		tag.RelationID,
		tag.ForkID,
		tag.BlockID,
		frame.Page,
	)

	// ----------------------------
	// 6. finalize
	// ----------------------------
	framePartition.mu.Lock()
	defer framePartition.mu.Unlock()

	frame.StopIO()

	if err != nil {
		frame.ClearValid()
		frame.Unpin()

		delete(framePartition.frameMap, tag)
		framePartition.freeFrames = append(framePartition.freeFrames, frameIdx)

		return nil, err, false
	}

	frame.SetValid()

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
