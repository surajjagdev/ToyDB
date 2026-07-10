package buffer

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/disk"
	"github.com/surajjagdev/ToyDB/internal/storage/page"
)

func setupBufferPool(t *testing.T, numPartitions common.PartitionIndex, framesPerPartition common.FrameIndex) (*BufferPool, *disk.DirectManager) {
	tempDir := t.TempDir()

	// dm is of type *disk.DirectManager
	dm, err := disk.NewDirectManager(tempDir, 100, 100)
	if err != nil {
		t.Fatalf("Failed to create DirectManager: %v", err)
	}

	bp, err := NewBufferPool(dm, numPartitions, framesPerPartition)
	if err != nil {
		t.Fatalf("Failed to create BufferPool: %v", err)
	}

	return bp, dm
}

func writePage(t *testing.T, dm disk.DiskManager, rel common.RelationID, fork common.ForkID, pid common.BlockID) {
	data := make([]byte, common.PageSize)
	for i := 0; i < len(data); i++ {
		data[i] = byte(i % 256)
	}

	err := dm.WritePage(rel, fork, pid, data, false)

	if err != nil {
		t.Fatalf("Write page failed: %v", err)
	}
}

func TestBufferPool_RealDisk_HitMiss(t *testing.T) {
	bp, dm := setupBufferPool(t, 2, 4)
	defer bp.Shutdown()

	rel := common.RelationID(1)
	fork := common.ForkID(0)

	// 1. Allocate a real block on disk so it exists for GetPage
	blockID, err := dm.AllocateBlock(rel, fork)
	if err != nil {
		t.Fatalf("Failed to allocate block: %v", err)
	}

	writePage(t, dm, rel, fork, blockID)

	tag := BufferTag{RelationID: rel, ForkID: fork, BlockID: blockID}

	// 2. Cache Miss (Reads from real disk)
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

	// Write some dummy data to the frame
	frame1.WLatch()
	frame1.Page[0] = 42
	frame1.SetDirty() // Mark it dirty so we test flushing later
	frame1.WUnlatch()

	frame1.Unpin()

	// 3. Cache Hit (Reads from memory, no disk I/O)
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

func TestBufferPool_RealDisk_EvictionAndFlush(t *testing.T) {
	// Tiny buffer pool: 1 partition, 2 frames total
	bp, dm := setupBufferPool(t, 1, 1)
	defer bp.Shutdown()

	rel := common.RelationID(2)
	fork := common.ForkID(0)

	// write 3 blocks on disk
	b1, _ := dm.AllocateBlock(rel, fork)
	b2, _ := dm.AllocateBlock(rel, fork)

	writePage(t, dm, rel, fork, b1)
	writePage(t, dm, rel, fork, b2)

	tag1 := BufferTag{RelationID: rel, ForkID: fork, BlockID: b1}
	tag2 := BufferTag{RelationID: rel, ForkID: fork, BlockID: b2}

	// Fill the pool
	f1, _, _ := bp.GetPage(tag1)

	// Modify frame 1
	f1.WLatch()
	copy(f1.Page, []byte("Direct IO Flush Test"))
	f1.SetDirty()
	f1.WUnlatch()

	// Unpin both so they can be evicted
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

	err = dm.ReadPage(rel, fork, b1, page.Page(verifyBuf))
	if err != nil {
		t.Fatalf("Direct disk read failed: %v", err)
	}

	if !bytes.HasPrefix(verifyBuf, []byte("Direct IO Flush Test")) {
		t.Errorf("Flushed data did not match expected bytes on disk")
	}
}

func TestMultipleReadersSameFrameShouldOnlyInvokeOneDiskRead(t *testing.T) {
	bp, dm := setupBufferPool(t, 1, 1)
	defer bp.Shutdown()

	rel := common.RelationID(2)
	fork := common.ForkID(0)

	b1, _ := dm.AllocateBlock(rel, fork)

	writePage(t, dm, rel, fork, b1)

	tag := BufferTag{
		BlockID:    b1,
		RelationID: rel,
		ForkID:     fork,
	}

	workersLen := 100

	var wg sync.WaitGroup

	var cacheMisses atomic.Int32
	var cacheHits atomic.Int32

	for i := 0; i < workersLen; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_, err, cacheHit := bp.GetPage(tag)
			if err != nil {
				t.Errorf("GetPage failed: %v", err)
				return
			}

			if cacheHit {
				cacheHits.Add(1)
			} else {
				cacheMisses.Add(1)
			}
		}()
	}

	wg.Wait()

	if got := cacheMisses.Load(); got != 1 {
		t.Fatalf("expected exactly 1 cache miss, got %d", got)
	}

	if got := cacheHits.Load(); got != int32(workersLen-1) {
		t.Fatalf(
			"expected %d cache hits, got %d",
			workersLen-1,
			got,
		)
	}
}

func TestBufferPool_GracefulShutdownSavesToDisk(t *testing.T) {
	// Tiny buffer pool: 1 partition, 1 frame total
	bp, dm := setupBufferPool(t, 1, 1)

	rel := common.RelationID(2)
	fork := common.ForkID(0)

	// write 1 block on disk
	b1, err := dm.AllocateBlock(rel, fork)
	if err != nil {
		t.Fatalf("Failed to allocate block: %v", err)
	}
	writePage(t, dm, rel, fork, b1)

	tag1 := BufferTag{RelationID: rel, ForkID: fork, BlockID: b1}

	// 1. Fill the pool (FIX: GetPage only returns frame and error)
	f1, err, _ := bp.GetPage(tag1)
	if err != nil {
		t.Fatalf("Failed to GetPage: %v", err)
	}

	// 2. Modify frame 1
	f1.WLatch()
	copy(f1.Page, []byte("Direct IO Flush Test"))
	f1.SetDirty()
	f1.WUnlatch()

	// 3. Unpin so it can be safely flushed during shutdown
	f1.Unpin()

	// 4. Graceful shutdown
	err = bp.GracefulShutdown(10 * time.Second)
	if err != nil {
		// No need to call bp.Shutdown() here, GracefulShutdown already attempts it
		t.Fatalf("Failed to graceful shutdown: %v", err)
	}

	time.Sleep(10 * time.Second) // wait for the page to be flushed

	// 5. Verify the page made it to the disk
	verifyBuf, ptr, err := allocPageData()
	if err != nil {
		t.Fatalf("Failed to alloc verify buffer: %v", err)
	}
	defer freePageData(ptr)

	// Note: GracefulShutdown closed all the files in the VFD cache.
	// However, because of your excellent VFD design, calling ReadPage here
	// will simply cause the VFD to safely reopen the file descriptor!
	err = dm.ReadPage(rel, fork, b1, verifyBuf) // FIX: Removed page.Page() cast
	if err != nil {
		t.Fatalf("Direct disk read failed: %v", err)
	}

	if !bytes.HasPrefix(verifyBuf, []byte("Direct IO Flush Test")) {
		t.Errorf("Flushed data did not match expected bytes on disk")
	}
}

func TestBufferPool_GracefulShutdownDoesNotAllNewRequests(t *testing.T) {
	// Tiny buffer pool: 1 partition, 1 frame total
	bp, dm := setupBufferPool(t, 1, 1)

	rel := common.RelationID(2)
	fork := common.ForkID(0)

	// write 1 block on disk
	b1, err := dm.AllocateBlock(rel, fork)
	if err != nil {
		t.Fatalf("Failed to allocate block: %v", err)
	}
	writePage(t, dm, rel, fork, b1)

	tag1 := BufferTag{RelationID: rel, ForkID: fork, BlockID: b1}

	// 1. Fill the pool (FIX: GetPage only returns frame and error)
	f1, err, _ := bp.GetPage(tag1)
	if err != nil {
		t.Fatalf("Failed to GetPage: %v", err)
	}

	// 2. mod frame 1
	f1.WLatch()
	copy(f1.Page, []byte("Direct IO Flush Test"))
	f1.SetDirty()
	f1.WUnlatch()

	// 3. Unpin so it can be safely flushed during shutdown
	f1.Unpin()

	// 4. Graceful shutdown
	err = bp.GracefulShutdown(10 * time.Second)
	if err != nil {
		t.Fatalf("Failed to graceful shutdown: %v", err)
	}

	_, err, _ = bp.GetPage(tag1)
	if err == nil {
		t.Fatalf("expected error for reading after shutdown")
	}

	time.Sleep(10 * time.Second) // wait for the page to be flushed

	// 5. Verify the page made it to the disk
	verifyBuf, ptr, err := allocPageData()
	if err != nil {
		t.Fatalf("Failed to alloc verify buffer: %v", err)
	}
	defer freePageData(ptr)
	err = dm.ReadPage(rel, fork, b1, verifyBuf)
	if err != nil {
		t.Fatalf("Direct disk read failed: %v", err)
	}

	if !bytes.HasPrefix(verifyBuf, []byte("Direct IO Flush Test")) {
		t.Errorf("Flushed data did not match expected bytes on disk")
	}
}

func TestBufferPool_FlushSinglePage(t *testing.T) {
	// Tiny buffer pool: 1 partition, 1 frames total
	bp, dm := setupBufferPool(t, 1, 1)
	defer bp.Shutdown()

	rel := common.RelationID(0)
	fork := common.ForkID(0)

	// write 1 block on disk
	b1, _ := dm.AllocateBlock(rel, fork)

	writePage(t, dm, rel, fork, b1)

	tag1 := BufferTag{RelationID: rel, ForkID: fork, BlockID: b1}

	// Fill the pool
	f1, _, _ := bp.GetPage(tag1)

	// Modify frame 1
	f1.WLatch()
	copy(f1.Page, []byte("Write some data to the frame"))
	f1.SetDirty()
	f1.WUnlatch()

	// Unpin
	f1.Unpin()

	// flush page
	err := bp.FlushPage(tag1)
	if err != nil {
		t.Fatalf("Failed to flush page: %v", err)
	}
	// verify page is not dirty
	if f1.IsDirty() {
		t.Errorf("Frame should not be dirty after flush")
	}

	verifyBuf, ptr, err := allocPageData()
	if err != nil {
		t.Fatalf("Failed to alloc verify buffer: %v", err)
	}
	defer freePageData(ptr)

	err = dm.ReadPage(rel, fork, b1, page.Page(verifyBuf))
	if err != nil {
		t.Fatalf("Direct disk read failed: %v", err)
	}

	if !bytes.HasPrefix(verifyBuf, []byte("Write some data to the frame")) {
		t.Errorf("Flushed data did not match expected bytes on disk")
	}
}

func TestBufferPool_FlushAllPagesSingleDirtyPage(t *testing.T) {
	// Tiny buffer pool: 1 partition, 1 frames total
	bp, dm := setupBufferPool(t, 1, 1)
	defer bp.Shutdown()

	rel := common.RelationID(0)
	fork := common.ForkID(0)

	// write 1 block on disk
	b1, _ := dm.AllocateBlock(rel, fork)

	writePage(t, dm, rel, fork, b1)

	tag1 := BufferTag{RelationID: rel, ForkID: fork, BlockID: b1}

	// Fill the pool
	f1, _, _ := bp.GetPage(tag1)

	// Modify frame 1
	f1.WLatch()
	copy(f1.Page, []byte("Write some data to the frame"))
	f1.SetDirty()
	f1.WUnlatch()

	// Unpin
	f1.Unpin()

	// flush page
	err := bp.FlushAllPages()
	if err != nil {
		t.Fatalf("Failed to flush page: %v", err)
	}

	// verify page is not dirty
	if f1.IsDirty() {
		t.Errorf("Frame should not be dirty after flush")
	}

	verifyBuf, ptr, err := allocPageData()
	if err != nil {
		t.Fatalf("Failed to alloc verify buffer: %v", err)
	}

	defer freePageData(ptr)

	err = dm.ReadPage(rel, fork, b1, page.Page(verifyBuf))
	if err != nil {
		t.Fatalf("Direct disk read failed: %v", err)
	}

	if !bytes.HasPrefix(verifyBuf, []byte("Write some data to the frame")) {
		t.Errorf("Flushed data did not match expected bytes on disk")
	}
}

func TestBufferPool_FlushAllPages_Concurrent(t *testing.T) {
	// Buffer pool with 4 partitions, 4 frames each (16 frames total)
	bp, dm := setupBufferPool(t, 4, 4)
	defer bp.Shutdown()

	rel := common.RelationID(99)
	fork := common.ForkID(0)
	numBlocks := 10 // Less than total frames so we get cache hits, but enough to spread out

	// 1. Allocate blocks on disk
	var blocks []common.BlockID
	for range numBlocks {
		b, err := dm.AllocateBlock(rel, fork)
		if err != nil {
			t.Fatalf("Failed to allocate block: %v", err)
		}
		blocks = append(blocks, b)

		// FIX: Tell the BufferPool to initialize this new page in memory
		// without trying to read it from the empty disk file!
		tag := BufferTag{RelationID: rel, ForkID: fork, BlockID: b}
		frame, err := bp.AllocatePage(tag)
		if err != nil {
			t.Fatalf("Failed to allocate page in buffer pool: %v", err)
		}

		// We just needed to initialize it. Let it go so the concurrent writers can use it.
		// AllocatePage automatically marked it dirty, so if it gets evicted, it will physically write to disk!
		frame.Unpin()
	}

	var wg sync.WaitGroup
	numWriters := 20
	iterationsPerWriter := 50

	// 2. Start Concurrent Writers
	// These threads constantly fetch pages, dirty them, and unpin them.
	for i := range numWriters {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < iterationsPerWriter; j++ {
				// Pick a block deterministically but spread out
				blockID := blocks[(writerID+j)%numBlocks]
				tag := BufferTag{RelationID: rel, ForkID: fork, BlockID: blockID}

				frame, err, _ := bp.GetPage(tag)
				if err != nil {
					t.Errorf("Writer failed to GetPage: %v", err)
					return
				}

				// Modify the page safely
				frame.WLatch()
				expectedStr := fmt.Sprintf("Data for Block %d", blockID)
				copy(frame.Page, []byte(expectedStr))
				frame.SetDirty()
				frame.WUnlatch()

				// Unpin it so the background flusher can grab it
				frame.Unpin()

				// Tiny sleep to force interleaving
				time.Sleep(1 * time.Microsecond)
			}
		}(i)
	}

	// 3. Start Concurrent Flusher (Simulates Checkpointer / WAL)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 10 {
			time.Sleep(2 * time.Millisecond) // Wait for writers to dirty some pages

			err := bp.FlushAllPages()
			if err != nil {
				t.Errorf("FlushAllPages failed concurrently: %v", err)
			}
		}
	}()

	// 4. Wait for all threads to finish their chaos
	wg.Wait()

	// 5. Verify the physical disk contents directly
	verifyBuf, ptr, err := allocPageData() // Must be aligned for DirectIO!
	if err != nil {
		t.Fatalf("Failed to alloc verify buffer: %v", err)
	}
	defer freePageData(ptr)

	for _, blockID := range blocks {
		err = dm.ReadPage(rel, fork, blockID, page.Page(verifyBuf))
		if err != nil {
			t.Fatalf("Direct disk read failed for block %d: %v", blockID, err)
		}

		expectedStr := fmt.Appendf(nil, "Data for Block %d", blockID)
		if !bytes.HasPrefix(verifyBuf, expectedStr) {
			t.Errorf("Block %d on disk did not match. Expected prefix: %s", blockID, string(expectedStr))
		}
	}
}
