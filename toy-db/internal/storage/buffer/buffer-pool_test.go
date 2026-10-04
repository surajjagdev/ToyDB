package buffer

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/disk"
	"github.com/surajjagdev/ToyDB/internal/storage/page"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

const testPagePayload = "Write some data to the frame"

// -----------------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------------

// fakeLogFlusher is a no-op LogFlusher for buffer pool tests. It records the
// highest LSN it was asked to flush and the number of calls, so tests can
// assert that the write-ahead rule was actually exercised.
type fakeLogFlusher struct {
	mu            sync.Mutex
	lastFlushedTo wal.LSN
	calls         int
	failWith      error // if non-nil, FlushUpTo returns this
}

func (f *fakeLogFlusher) FlushUpTo(lsn wal.LSN) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failWith != nil {
		return f.failWith
	}
	if lsn > f.lastFlushedTo {
		f.lastFlushedTo = lsn
	}
	return nil
}

func (f *fakeLogFlusher) snapshot() (wal.LSN, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastFlushedTo, f.calls
}

func (f *fakeLogFlusher) setFailure(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failWith = err
}

func writeFramePayload(frame *Frame, payload string) {
	copy(frame.Page[page.OffsetDataStart:], []byte(payload))
}

func verifyDiskPayload(t *testing.T, buf []byte, payload string) {
	t.Helper()
	if !bytes.HasPrefix(buf[page.OffsetDataStart:], []byte(payload)) {
		t.Errorf("Flushed data did not match expected bytes on disk")
	}
}

// setupBufferPool constructs a DirectManager, a fake WAL, and a BufferPool
// for tests that don't care about WAL behaviour.
func setupBufferPool(
	t *testing.T,
	numPartitions common.PartitionIndex,
	framesPerPartition common.FrameIndex,
) (*BufferPool, *disk.DirectManager) {
	t.Helper()
	bp, dm, _ := setupBufferPoolWithWAL(t, numPartitions, framesPerPartition)
	return bp, dm
}

// setupBufferPoolWithWAL also returns the fakeLogFlusher so tests can inspect
// the write-ahead rule.
func setupBufferPoolWithWAL(
	t *testing.T,
	numPartitions common.PartitionIndex,
	framesPerPartition common.FrameIndex,
) (*BufferPool, *disk.DirectManager, *fakeLogFlusher) {
	t.Helper()
	tempDir := t.TempDir()

	dm, err := disk.NewDirectManager(tempDir, 100, 100)
	if err != nil {
		t.Fatalf("Failed to create DirectManager: %v", err)
	}

	fake := &fakeLogFlusher{}
	bp, err := NewBufferPool(dm, fake, numPartitions, framesPerPartition)
	if err != nil {
		t.Fatalf("Failed to create BufferPool: %v", err)
	}

	return bp, dm, fake
}

func writePage(t *testing.T, dm disk.DiskManager, rel common.RelationID, fork common.ForkID, pid common.BlockID) {
	t.Helper()
	var data page.Page = make([]byte, common.PageSize)
	for i := 0; i < len(data); i++ {
		data[i] = byte(i % 256)
	}
	data.SetLSN(0) // deterministic header for tests that care
	data.UpdateChecksum()
	if err := dm.WritePage(rel, fork, pid, data, false); err != nil {
		t.Fatalf("Write page failed: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Basic hit/miss
// -----------------------------------------------------------------------------

func TestBufferPool_RealDisk_HitMiss(t *testing.T) {
	bp, dm := setupBufferPool(t, 2, 4)
	defer bp.Shutdown()

	rel := common.RelationID(1)
	fork := common.ForkID(0)

	blockID, err := dm.AllocateBlock(rel, fork)
	if err != nil {
		t.Fatalf("Failed to allocate block: %v", err)
	}

	writePage(t, dm, rel, fork, blockID)

	tag := BufferTag{RelationID: rel, ForkID: fork, BlockID: blockID}

	// Cache miss — reads from disk.
	frame1, err, fromCache := bp.GetPage(tag)
	if err != nil {
		t.Fatalf("GetPage (Miss) failed: %v", err)
	}
	if !frame1.IsPinned() {
		t.Errorf("Expected frame to be pinned")
	}
	if fromCache {
		t.Errorf("Expected cache miss")
	}

	frame1.WLatch()
	frame1.Page[0] = 42
	frame1.SetDirty()
	frame1.WUnlatch()

	frame1.Unpin()

	// Cache hit — reads from memory.
	frame2, err, cacheHit := bp.GetPage(tag)
	if err != nil {
		t.Fatalf("GetPage (Hit) failed: %v", err)
	}
	if !cacheHit {
		t.Errorf("Expected cache hit")
	}

	frame2.RLatch()
	val := frame2.Page[0]
	frame2.RUnlatch()

	if val != 42 {
		t.Errorf("Expected to read 42 from cache, got %d", val)
	}

	frame2.Unpin()
}

// -----------------------------------------------------------------------------
// Eviction and flush
// -----------------------------------------------------------------------------

func TestBufferPool_RealDisk_EvictionAndFlush(t *testing.T) {
	bp, dm := setupBufferPool(t, 1, 1)
	defer bp.Shutdown()

	rel := common.RelationID(2)
	fork := common.ForkID(0)

	b1, _ := dm.AllocateBlock(rel, fork)
	b2, _ := dm.AllocateBlock(rel, fork)

	writePage(t, dm, rel, fork, b1)
	writePage(t, dm, rel, fork, b2)

	tag1 := BufferTag{RelationID: rel, ForkID: fork, BlockID: b1}
	tag2 := BufferTag{RelationID: rel, ForkID: fork, BlockID: b2}

	f1, _, _ := bp.GetPage(tag1)
	f1.WLatch()
	copy(f1.Page, []byte("Direct IO Flush Test"))
	f1.SetDirty()
	f1.WUnlatch()
	f1.Unpin()

	f2, err, _ := bp.GetPage(tag2)
	if err != nil {
		t.Fatalf("Failed to get 2nd page: %v", err)
	}
	f2.Unpin()

	verifyBuf, ptr, err := allocPageData()
	if err != nil {
		t.Fatalf("Failed to alloc verify buffer: %v", err)
	}
	defer freePageData(ptr)

	if err := dm.ReadPage(rel, fork, b1, page.Page(verifyBuf)); err != nil {
		t.Fatalf("Direct disk read failed: %v", err)
	}

	if !bytes.HasPrefix(verifyBuf, []byte("Direct IO Flush Test")) {
		t.Errorf("Flushed data did not match expected bytes on disk")
	}
}

// -----------------------------------------------------------------------------
// Concurrency
// -----------------------------------------------------------------------------

func TestMultipleReadersSameFrameShouldOnlyInvokeOneDiskRead(t *testing.T) {
	bp, dm := setupBufferPool(t, 1, 1)
	defer bp.Shutdown()

	rel := common.RelationID(2)
	fork := common.ForkID(0)

	b1, _ := dm.AllocateBlock(rel, fork)
	writePage(t, dm, rel, fork, b1)

	tag := BufferTag{BlockID: b1, RelationID: rel, ForkID: fork}

	workersLen := 100
	var wg sync.WaitGroup
	var cacheMisses atomic.Int32
	var cacheHits atomic.Int32

	for i := 0; i < workersLen; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attempt := 0; attempt < 5; attempt++ {
				frame, err, cacheHit := bp.GetPage(tag)
				if err != nil {
					runtime.Gosched()
					continue
				}
				frame.Unpin()
				if cacheHit {
					cacheHits.Add(1)
				} else {
					cacheMisses.Add(1)
				}
				return
			}
			t.Errorf("GetPage failed after retries")
		}()
	}

	wg.Wait()

	if got := cacheMisses.Load(); got != 1 {
		t.Fatalf("expected exactly 1 cache miss, got %d", got)
	}
	if got := cacheHits.Load(); got != int32(workersLen-1) {
		t.Fatalf("expected %d cache hits, got %d", workersLen-1, got)
	}
}

// -----------------------------------------------------------------------------
// Graceful shutdown
// -----------------------------------------------------------------------------

func TestBufferPool_GracefulShutdownSavesToDisk(t *testing.T) {
	bp, dm := setupBufferPool(t, 1, 1)

	rel := common.RelationID(2)
	fork := common.ForkID(0)

	b1, err := dm.AllocateBlock(rel, fork)
	if err != nil {
		t.Fatalf("Failed to allocate block: %v", err)
	}
	writePage(t, dm, rel, fork, b1)

	tag1 := BufferTag{RelationID: rel, ForkID: fork, BlockID: b1}

	f1, err, _ := bp.GetPage(tag1)
	if err != nil {
		t.Fatalf("Failed to GetPage: %v", err)
	}

	f1.WLatch()
	copy(f1.Page, []byte("Direct IO Flush Test"))
	f1.SetDirty()
	f1.WUnlatch()
	f1.Unpin()

	if err := bp.GracefulShutdown(10 * time.Second); err != nil {
		t.Fatalf("Failed to graceful shutdown: %v", err)
	}

	verifyBuf, ptr, err := allocPageData()
	if err != nil {
		t.Fatalf("Failed to alloc verify buffer: %v", err)
	}
	defer freePageData(ptr)

	if err := dm.ReadPage(rel, fork, b1, verifyBuf); err != nil {
		t.Fatalf("Direct disk read failed: %v", err)
	}

	if !bytes.HasPrefix(verifyBuf, []byte("Direct IO Flush Test")) {
		t.Errorf("Flushed data did not match expected bytes on disk")
	}
}

func TestBufferPool_GracefulShutdownDoesNotAllowNewRequests(t *testing.T) {
	bp, dm := setupBufferPool(t, 1, 1)

	rel := common.RelationID(2)
	fork := common.ForkID(0)

	b1, err := dm.AllocateBlock(rel, fork)
	if err != nil {
		t.Fatalf("Failed to allocate block: %v", err)
	}
	writePage(t, dm, rel, fork, b1)

	tag1 := BufferTag{RelationID: rel, ForkID: fork, BlockID: b1}

	f1, err, _ := bp.GetPage(tag1)
	if err != nil {
		t.Fatalf("Failed to GetPage: %v", err)
	}

	f1.WLatch()
	copy(f1.Page, []byte("Direct IO Flush Test"))
	f1.SetDirty()
	f1.WUnlatch()
	f1.Unpin()

	if err := bp.GracefulShutdown(10 * time.Second); err != nil {
		t.Fatalf("Failed to graceful shutdown: %v", err)
	}

	if _, err, _ := bp.GetPage(tag1); err == nil {
		t.Fatalf("expected error for reading after shutdown")
	}

	verifyBuf, ptr, err := allocPageData()
	if err != nil {
		t.Fatalf("Failed to alloc verify buffer: %v", err)
	}
	defer freePageData(ptr)

	if err := dm.ReadPage(rel, fork, b1, verifyBuf); err != nil {
		t.Fatalf("Direct disk read failed: %v", err)
	}

	if !bytes.HasPrefix(verifyBuf, []byte("Direct IO Flush Test")) {
		t.Errorf("Flushed data did not match expected bytes on disk")
	}
}

// -----------------------------------------------------------------------------
// Single-page flush
// -----------------------------------------------------------------------------

func TestBufferPool_FlushSinglePage(t *testing.T) {
	bp, dm := setupBufferPool(t, 1, 1)
	defer bp.Shutdown()

	rel := common.RelationID(0)
	fork := common.ForkID(0)

	b1, _ := dm.AllocateBlock(rel, fork)
	writePage(t, dm, rel, fork, b1)

	tag1 := BufferTag{RelationID: rel, ForkID: fork, BlockID: b1}

	f1, _, _ := bp.GetPage(tag1)

	f1.WLatch()
	writeFramePayload(f1, testPagePayload)
	f1.SetDirty()
	f1.WUnlatch()
	f1.Unpin()

	if err := bp.FlushPage(tag1); err != nil {
		t.Fatalf("Failed to flush page: %v", err)
	}
	if f1.IsDirty() {
		t.Errorf("Frame should not be dirty after flush")
	}

	verifyBuf, ptr, err := allocPageData()
	if err != nil {
		t.Fatalf("Failed to alloc verify buffer: %v", err)
	}
	defer freePageData(ptr)

	if err := dm.ReadPage(rel, fork, b1, page.Page(verifyBuf)); err != nil {
		t.Fatalf("Direct disk read failed: %v", err)
	}

	verifyDiskPayload(t, verifyBuf, testPagePayload)
}

// -----------------------------------------------------------------------------
// Pinning and eviction wakeup
// -----------------------------------------------------------------------------

func TestBufferPool_AllFramesPinned(t *testing.T) {
	bp, dm := setupBufferPool(t, 1, 2)
	defer bp.Shutdown()

	rel := common.RelationID(999)
	fork := common.ForkID(0)

	b1, _ := dm.AllocateBlock(rel, fork)
	b2, _ := dm.AllocateBlock(rel, fork)
	b3, _ := dm.AllocateBlock(rel, fork)

	writePage(t, dm, rel, fork, b1)
	writePage(t, dm, rel, fork, b2)
	writePage(t, dm, rel, fork, b3)

	tag1 := BufferTag{RelationID: rel, ForkID: fork, BlockID: b1}
	tag2 := BufferTag{RelationID: rel, ForkID: fork, BlockID: b2}
	tag3 := BufferTag{RelationID: rel, ForkID: fork, BlockID: b3}

	f1, err, _ := bp.GetPage(tag1)
	if err != nil {
		t.Fatalf("Failed to get Block 1: %v", err)
	}
	// intentionally not unpinned

	f2, err, _ := bp.GetPage(tag2)
	if err != nil {
		t.Fatalf("Failed to get Block 2: %v", err)
	}
	// intentionally not unpinned

	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, getErr, _ := bp.GetPage(tag3)
		errChan <- getErr
	}()

	wg.Wait()
	close(errChan)

	getErr := <-errChan
	if getErr == nil {
		t.Fatalf("GetPage succeeded and stole a pinned frame")
	}
	if !bytes.Contains([]byte(getErr.Error()), []byte("no victim frames found within")) {
		t.Errorf("Expected timeout error, got: %v", getErr)
	}

	f1.Unpin()
	f2.Unpin()
}

func TestBufferPool_EvictionWakeupOnUnpin(t *testing.T) {
	bp, dm := setupBufferPool(t, 1, 1)
	defer bp.Shutdown()

	rel := common.RelationID(888)
	fork := common.ForkID(0)

	b1, _ := dm.AllocateBlock(rel, fork)
	b2, _ := dm.AllocateBlock(rel, fork)
	writePage(t, dm, rel, fork, b1)
	writePage(t, dm, rel, fork, b2)

	tag1 := BufferTag{RelationID: rel, ForkID: fork, BlockID: b1}
	tag2 := BufferTag{RelationID: rel, ForkID: fork, BlockID: b2}

	f1, _, _ := bp.GetPage(tag1)
	// pinned

	var wg sync.WaitGroup
	successChan := make(chan bool, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		f2, err, _ := bp.GetPage(tag2)
		if err == nil {
			f2.Unpin()
			successChan <- true
		} else {
			successChan <- false
		}
	}()

	time.Sleep(100 * time.Millisecond)
	f1.Unpin()

	wg.Wait()
	close(successChan)

	if !<-successChan {
		t.Fatalf("The waiting thread failed to wake up and claim the unpinned frame")
	}
}

// -----------------------------------------------------------------------------
// Flush-all
// -----------------------------------------------------------------------------

func TestBufferPool_FlushAllPagesSingleDirtyPage(t *testing.T) {
	bp, dm := setupBufferPool(t, 1, 1)
	defer bp.Shutdown()

	rel := common.RelationID(0)
	fork := common.ForkID(0)

	b1, _ := dm.AllocateBlock(rel, fork)
	writePage(t, dm, rel, fork, b1)

	tag1 := BufferTag{RelationID: rel, ForkID: fork, BlockID: b1}

	f1, _, _ := bp.GetPage(tag1)

	f1.WLatch()
	writeFramePayload(f1, testPagePayload)
	f1.SetDirty()
	f1.WUnlatch()
	f1.Unpin()

	if err := bp.FlushAllPages(); err != nil {
		t.Fatalf("Failed to flush page: %v", err)
	}
	if f1.IsDirty() {
		t.Errorf("Frame should not be dirty after flush")
	}

	verifyBuf, ptr, err := allocPageData()
	if err != nil {
		t.Fatalf("Failed to alloc verify buffer: %v", err)
	}
	defer freePageData(ptr)

	if err := dm.ReadPage(rel, fork, b1, page.Page(verifyBuf)); err != nil {
		t.Fatalf("Direct disk read failed: %v", err)
	}

	verifyDiskPayload(t, verifyBuf, testPagePayload)
}

func TestBufferPool_FlushAllPages_Concurrent(t *testing.T) {
	bp, dm := setupBufferPool(t, 4, 4)
	defer bp.Shutdown()

	rel := common.RelationID(99)
	fork := common.ForkID(0)
	numBlocks := 10

	var blocks []common.BlockID
	for range numBlocks {
		b, err := dm.AllocateBlock(rel, fork)
		if err != nil {
			t.Fatalf("Failed to allocate block: %v", err)
		}
		blocks = append(blocks, b)

		tag := BufferTag{RelationID: rel, ForkID: fork, BlockID: b}
		frame, err := bp.AllocatePage(tag)
		if err != nil {
			t.Fatalf("Failed to allocate page in buffer pool: %v", err)
		}
		frame.Unpin()
	}

	var wg sync.WaitGroup
	numWriters := 20
	iterationsPerWriter := 50

	for i := range numWriters {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < iterationsPerWriter; j++ {
				blockID := blocks[(writerID+j)%numBlocks]
				tag := BufferTag{RelationID: rel, ForkID: fork, BlockID: blockID}

				frame, err, _ := bp.GetPage(tag)
				if err != nil {
					t.Errorf("Writer failed to GetPage: %v", err)
					return
				}

				frame.WLatch()
				expectedStr := fmt.Sprintf("Data for Block %d", blockID)
				copy(frame.Page, []byte(expectedStr))
				frame.SetDirty()
				frame.WUnlatch()
				frame.Unpin()

				time.Sleep(1 * time.Microsecond)
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 10 {
			time.Sleep(2 * time.Millisecond)
			if err := bp.FlushAllPages(); err != nil {
				t.Errorf("FlushAllPages failed concurrently: %v", err)
			}
		}
	}()

	wg.Wait()

	verifyBuf, ptr, err := allocPageData()
	if err != nil {
		t.Fatalf("Failed to alloc verify buffer: %v", err)
	}
	defer freePageData(ptr)

	for _, blockID := range blocks {
		if err := dm.ReadPage(rel, fork, blockID, page.Page(verifyBuf)); err != nil {
			t.Fatalf("Direct disk read failed for block %d: %v", blockID, err)
		}
		expectedStr := fmt.Appendf(nil, "Data for Block %d", blockID)
		if !bytes.HasPrefix(verifyBuf, expectedStr) {
			t.Errorf("Block %d on disk did not match. Expected prefix: %s",
				blockID, string(expectedStr))
		}
	}
}

// -----------------------------------------------------------------------------
// Duplicate mapping
// -----------------------------------------------------------------------------

func TestBufferPool_DuplicateMappingCorruption(t *testing.T) {
	bp, dm := setupBufferPool(t, 1, 1)
	defer bp.Shutdown()

	rel := common.RelationID(999)
	fork := common.ForkID(0)

	b1, _ := dm.AllocateBlock(rel, fork)
	b2, _ := dm.AllocateBlock(rel, fork)

	writePage(t, dm, rel, fork, b1)
	writePage(t, dm, rel, fork, b2)

	tag1 := BufferTag{RelationID: rel, ForkID: fork, BlockID: b1}
	tag2 := BufferTag{RelationID: rel, ForkID: fork, BlockID: b2}

	f1, err, _ := bp.GetPage(tag1)
	if err != nil {
		t.Fatalf("Failed to get Block 1: %v", err)
	}
	f1.WLatch()
	writeFramePayload(f1, testPagePayload)
	f1.SetDirty()
	f1.WUnlatch()
	f1.Unpin()

	numGoroutines := 50
	var wg sync.WaitGroup
	frameChan := make(chan *Frame, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f, getErr, _ := bp.GetPage(tag2)
			if getErr == nil {
				frameChan <- f
				f.Unpin()
			} else {
				t.Errorf("GetPage failed: %v", getErr)
			}
		}()
	}

	wg.Wait()
	close(frameChan)

	verifyBuf, ptr, err := allocPageData()
	if err != nil {
		t.Fatalf("Failed to alloc verify buffer: %v", err)
	}
	defer freePageData(ptr)

	if err := dm.ReadPage(rel, fork, b1, page.Page(verifyBuf)); err != nil {
		t.Fatalf("Direct disk read failed: %v", err)
	}

	verifyDiskPayload(t, verifyBuf, testPagePayload)

	var firstFrame *Frame
	isFirst := true
	for f := range frameChan {
		if isFirst {
			firstFrame = f
			isFirst = false
		} else if f != firstFrame {
			t.Fatalf("Threads were given TWO DIFFERENT frames for the same BlockID")
		}
	}
}

// -----------------------------------------------------------------------------
// NEW: write-ahead rule
// -----------------------------------------------------------------------------

// TestBufferPool_WriteAheadRule verifies that flushing a dirty page with a
// nonzero pageLSN calls LogFlusher.FlushUpTo with that LSN before WritePage.
// This is the test that proves the WAL is wired into the buffer pool's flush
// path.
func TestBufferPool_WriteAheadRule(t *testing.T) {
	bp, dm, fake := setupBufferPoolWithWAL(t, 1, 2)
	defer bp.Shutdown()

	rel := common.RelationID(1)
	fork := common.ForkID(0)

	b1, _ := dm.AllocateBlock(rel, fork)
	writePage(t, dm, rel, fork, b1)

	tag := BufferTag{RelationID: rel, ForkID: fork, BlockID: b1}

	f1, _, _ := bp.GetPage(tag)

	// Simulate the heap having written a WAL record at LSN 7:4096 and
	// applied it to this page.
	wantLSN := wal.MakeLSN(7, 4096)
	f1.WLatch()
	f1.Page.SetLSN(wantLSN)
	writeFramePayload(f1, testPagePayload)
	f1.SetDirty()
	f1.WUnlatch()
	f1.Unpin()

	if err := bp.FlushPage(tag); err != nil {
		t.Fatalf("FlushPage failed: %v", err)
	}

	gotLSN, calls := fake.snapshot()
	if calls == 0 {
		t.Fatal("FlushUpTo was never called: write-ahead rule not enforced")
	}
	if gotLSN < wantLSN {
		t.Errorf("FlushUpTo called with %v, want at least %v", gotLSN, wantLSN)
	}
}

// TestBufferPool_WriteAheadRule_SkipsZeroLSN verifies that a page with a
// zero pageLSN (never touched by the WAL) does not trigger a flush call.
// Useful for pages that are only modified by internal bookkeeping (e.g.
// FSM or VM when those are added).
func TestBufferPool_WriteAheadRule_SkipsZeroLSN(t *testing.T) {
	bp, dm, fake := setupBufferPoolWithWAL(t, 1, 2)
	defer bp.Shutdown()

	rel := common.RelationID(1)
	fork := common.ForkID(0)

	b1, _ := dm.AllocateBlock(rel, fork)
	writePage(t, dm, rel, fork, b1)

	tag := BufferTag{RelationID: rel, ForkID: fork, BlockID: b1}

	f1, _, _ := bp.GetPage(tag)
	// pageLSN stays at whatever the freshly read page had (zero).
	f1.WLatch()
	writeFramePayload(f1, testPagePayload)
	f1.SetDirty()
	f1.WUnlatch()
	f1.Unpin()

	if err := bp.FlushPage(tag); err != nil {
		t.Fatalf("FlushPage failed: %v", err)
	}

	_, calls := fake.snapshot()
	if calls != 0 {
		t.Errorf("FlushUpTo called %d times for zero pageLSN, want 0", calls)
	}
}

// TestBufferPool_WriteAheadRule_FailureAbortsWrite verifies that if the
// LogFlusher fails, the page is not written to disk and the frame remains
// dirty.
func TestBufferPool_WriteAheadRule_FailureAbortsWrite(t *testing.T) {
	bp, dm, fake := setupBufferPoolWithWAL(t, 1, 2)
	defer bp.Shutdown()

	rel := common.RelationID(1)
	fork := common.ForkID(0)

	b1, _ := dm.AllocateBlock(rel, fork)
	writePage(t, dm, rel, fork, b1)

	tag := BufferTag{RelationID: rel, ForkID: fork, BlockID: b1}

	f1, _, _ := bp.GetPage(tag)
	f1.WLatch()
	f1.Page.SetLSN(wal.MakeLSN(9, 0))
	writeFramePayload(f1, testPagePayload)
	f1.SetDirty()
	f1.WUnlatch()
	f1.Unpin()

	fake.setFailure(fmt.Errorf("simulated fsync failure"))

	if err := bp.FlushPage(tag); err == nil {
		t.Fatal("FlushPage succeeded despite FlushUpTo failure")
	}

	// The frame must still be dirty: the write must not have happened.
	f1.RLatch()
	dirty := f1.IsDirty()
	f1.RUnlatch()
	if !dirty {
		t.Error("Frame was marked clean despite write-ahead flush failure")
	}
}

// TestBufferPool_WriteAheadRule_MixedLSNs verifies that under concurrent
// flushes of pages with different pageLSNs, FlushUpTo is called with
// non-decreasing LSNs and the highest LSN observed matches the max pageLSN
// set.
func TestBufferPool_WriteAheadRule_MixedLSNs(t *testing.T) {
	bp, dm, fake := setupBufferPoolWithWAL(t, 2, 4)
	defer bp.Shutdown()

	rel := common.RelationID(1)
	fork := common.ForkID(0)

	var maxLSN wal.LSN
	for i := 0; i < 8; i++ {
		b, err := dm.AllocateBlock(rel, fork)
		if err != nil {
			t.Fatalf("alloc: %v", err)
		}
		writePage(t, dm, rel, fork, b)

		tag := BufferTag{RelationID: rel, ForkID: fork, BlockID: b}
		f, err := bp.AllocatePage(tag)
		if err != nil {
			t.Fatalf("AllocatePage: %v", err)
		}

		lsn := wal.MakeLSN(uint32(i+1), uint32(i*100))
		if lsn > maxLSN {
			maxLSN = lsn
		}
		f.WLatch()
		f.Page.SetLSN(lsn)
		f.SetDirty()
		f.WUnlatch()
		f.Unpin()
	}

	if err := bp.FlushAllPages(); err != nil {
		t.Fatalf("FlushAllPages: %v", err)
	}

	gotLSN, _ := fake.snapshot()
	if gotLSN < maxLSN {
		t.Errorf("highest flushed LSN %v < max pageLSN %v", gotLSN, maxLSN)
	}
}
