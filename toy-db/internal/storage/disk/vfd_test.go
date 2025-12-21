package disk

import (
	"os"
	"path/filepath"
	"testing"
)

func newTempFileNode(t *testing.T, dir, name string) FileNode {
	t.Helper()
	path := filepath.Join(dir, name)
	return FileNode{Path: path}
}

func mustStatFail(t *testing.T, f *os.File) {
	t.Helper()
	_, err := f.Stat()
	if err == nil {
		t.Fatalf("expected file to be closed")
	}
}

// test if we get an accurate struct
func TestNewVFDCache(t *testing.T) {
	cache := NewVFDCache(10)
	if cache == nil {
		t.Fatal("NewVFDCache returned nil")
	}
	if cache.capacity != 10 {
		t.Errorf("capacity = %d, want 10", cache.capacity)
	}
	if cache.cache == nil {
		t.Fatal("cache map is nil")
	}
	if cache.lruList == nil {
		t.Fatal("lruList is nil")
	}
	if cache.lruList.Len() != 0 {
		t.Errorf("lruList length = %d, want 0", cache.lruList.Len())
	}
	if len(cache.cache) != 0 {
		t.Errorf("cache length = %d, want 0", len(cache.cache))
	}
}

// test if file can be created
func TestGetOrOpen_NewFile(t *testing.T) {
	dir := t.TempDir()
	cache := NewVFDCache(10)
	fn := newTempFileNode(t, dir, "test1.txt")

	entry, err := cache.GetOrOpen(fn, os.O_RDWR|os.O_CREATE)
	if err != nil {
		t.Fatalf("GetOrOpen failed: %v", err)
	}
	if entry == nil {
		t.Fatal("entry is nil")
	}
	if entry.fileNode.Path != fn.Path {
		t.Errorf("fileNode.Path = %s, want %s", entry.fileNode.Path, fn.Path)
	}
	if entry.file == nil {
		t.Fatal("file is nil")
	}
	if entry.refCount != 0 {
		t.Errorf("refCount = %d, want 0", entry.refCount)
	}
	if cache.lruList.Len() != 1 {
		t.Errorf("lruList length = %d, want 1", cache.lruList.Len())
	}
	if len(cache.cache) != 1 {
		t.Errorf("cache length = %d, want 1", len(cache.cache))
	}

	// Verify file exists and can be accessed
	if _, err := entry.file.Stat(); err != nil {
		t.Errorf("file.Stat() failed: %v", err)
	}
}

// See if we hit cache
func TestGetOrOpen_Cached(t *testing.T) {
	dir := t.TempDir()
	cache := NewVFDCache(10)
	fn := newTempFileNode(t, dir, "test1.txt")

	entry1, err := cache.GetOrOpen(fn, os.O_RDWR|os.O_CREATE)
	if err != nil {
		t.Fatalf("GetOrOpen failed: %v", err)
	}

	// Get the same file again - should return cached entry
	entry2, err := cache.GetOrOpen(fn, os.O_RDWR|os.O_CREATE)
	if err != nil {
		t.Fatalf("GetOrOpen failed: %v", err)
	}

	if entry1 != entry2 {
		t.Error("GetOrOpen returned different entries for same file")
	}
	if cache.lruList.Len() != 1 {
		t.Errorf("lruList length = %d, want 1", cache.lruList.Len())
	}
	if len(cache.cache) != 1 {
		t.Errorf("cache length = %d, want 1", len(cache.cache))
	}
}

func TestGetOrOpen_LRU_Eviction(t *testing.T) {
	dir := t.TempDir()
	cache := NewVFDCache(2) // Small capacity for testing
	fn1 := newTempFileNode(t, dir, "test1.txt")
	fn2 := newTempFileNode(t, dir, "test2.txt")
	fn3 := newTempFileNode(t, dir, "test3.txt")

	// Open file1
	entry1, err := cache.GetOrOpen(fn1, os.O_RDWR|os.O_CREATE)
	if err != nil {
		t.Fatalf("GetOrOpen failed: %v", err)
	}
	file1 := entry1.file

	// Open file2
	entry2, err := cache.GetOrOpen(fn2, os.O_RDWR|os.O_CREATE)
	if err != nil {
		t.Fatalf("GetOrOpen failed: %v", err)
	}
	file2 := entry2.file

	// Open file3 - should evict file1 (least recently used)
	entry3, err := cache.GetOrOpen(fn3, os.O_RDWR|os.O_CREATE)
	if err != nil {
		t.Fatalf("GetOrOpen failed: %v", err)
	}

	// Verify file1 was closed (evicted)
	mustStatFail(t, file1)

	// Verify file2 and file3 are still open
	if _, err := file2.Stat(); err != nil {
		t.Errorf("file2 should still be open: %v", err)
	}
	if _, err := entry3.file.Stat(); err != nil {
		t.Errorf("file3 should still be open: %v", err)
	}

	// Verify cache still has 2 entries
	if cache.lruList.Len() != 2 {
		t.Errorf("lruList length = %d, want 2", cache.lruList.Len())
	}
}

func TestGetOrOpen_LRU_MoveToFront(t *testing.T) {
	dir := t.TempDir()
	cache := NewVFDCache(3)

	fn1 := newTempFileNode(t, dir, "test1.txt")
	fn2 := newTempFileNode(t, dir, "test2.txt")
	fn3 := newTempFileNode(t, dir, "test3.txt")
	fn4 := newTempFileNode(t, dir, "test4.txt")

	// Open files in order: fn1, fn2, fn3
	e1, _ := cache.GetOrOpen(fn1, os.O_RDWR|os.O_CREATE)
	e2, _ := cache.GetOrOpen(fn2, os.O_RDWR|os.O_CREATE)
	e3, _ := cache.GetOrOpen(fn3, os.O_RDWR|os.O_CREATE)

	// Access fn1 again → should move fn1 to front
	cache.GetOrOpen(fn1, os.O_RDWR|os.O_CREATE)

	// LRU order should now be: fn1 (MRU), fn3, fn2 (LRU)
	front := cache.lruList.Front().Value.(*vfdEntry)
	if front.fileNode.Path != fn1.Path {
		t.Fatalf("front entry = %s, want %s", front.fileNode.Path, fn1.Path)
	}

	back := cache.lruList.Back().Value.(*vfdEntry)
	if back.fileNode.Path != fn2.Path {
		t.Fatalf("back entry = %s, want %s", back.fileNode.Path, fn2.Path)
	}

	// Open fn4 → should evict fn2 (least recently used)
	_, err := cache.GetOrOpen(fn4, os.O_RDWR|os.O_CREATE)
	if err != nil {
		t.Fatalf("GetOrOpen(fn4) failed: %v", err)
	}

	// fn2 should be evicted and closed
	if e2.elem != nil {
		t.Fatalf("fn2 should have been evicted from LRU list")
	}
	mustStatFail(t, e2.file)

	// fn1 and fn3 should still be present and open
	if _, err := e1.file.Stat(); err != nil {
		t.Fatalf("fn1 should still be open: %v", err)
	}
	if _, err := e3.file.Stat(); err != nil {
		t.Fatalf("fn3 should still be open: %v", err)
	}

	// Cache size must remain at capacity
	if cache.lruList.Len() != 3 {
		t.Fatalf("cache size = %d, want 3", cache.lruList.Len())
	}
}

func TestAcquire(t *testing.T) {
	dir := t.TempDir()
	cache := NewVFDCache(10)
	fn := newTempFileNode(t, dir, "test1.txt")

	entry, err := cache.GetOrOpen(fn, os.O_RDWR|os.O_CREATE)
	if err != nil {
		t.Fatalf("GetOrOpen failed: %v", err)
	}

	if entry.refCount != 0 {
		t.Errorf("initial refCount = %d, want 0", entry.refCount)
	}

	// Acquire entry
	cache.Acquire(entry)
	if entry.refCount != 1 {
		t.Errorf("refCount after Acquire = %d, want 1", entry.refCount)
	}

	// Acquire again
	cache.Acquire(entry)
	if entry.refCount != 2 {
		t.Errorf("refCount after second Acquire = %d, want 2", entry.refCount)
	}

	// Verify entry moved to front
	frontEntry := cache.lruList.Front().Value.(*vfdEntry)
	if frontEntry != entry {
		t.Error("entry should be at front after Acquire")
	}
}

func TestRelease(t *testing.T) {
	dir := t.TempDir()
	cache := NewVFDCache(10)
	fn := newTempFileNode(t, dir, "test1.txt")

	entry, err := cache.GetOrOpen(fn, os.O_RDWR|os.O_CREATE)
	if err != nil {
		t.Fatalf("GetOrOpen failed: %v", err)
	}

	// Acquire twice
	cache.Acquire(entry)
	cache.Acquire(entry)
	if entry.refCount != 2 {
		t.Errorf("refCount = %d, want 2", entry.refCount)
	}

	// Release once
	cache.Release(entry)
	if entry.refCount != 1 {
		t.Errorf("refCount after Release = %d, want 1", entry.refCount)
	}

	// Release again
	cache.Release(entry)
	if entry.refCount != 0 {
		t.Errorf("refCount after second Release = %d, want 0", entry.refCount)
	}

	// Release when already at 0 - should not go negative
	cache.Release(entry)
	if entry.refCount != 0 {
		t.Errorf("refCount after Release at 0 = %d, want 0", entry.refCount)
	}
}

func TestEvictOne_WithRefCount(t *testing.T) {
	dir := t.TempDir()
	cache := NewVFDCache(2)
	fn1 := newTempFileNode(t, dir, "test1.txt")
	fn2 := newTempFileNode(t, dir, "test2.txt")

	entry1, _ := cache.GetOrOpen(fn1, os.O_RDWR|os.O_CREATE)
	entry2, _ := cache.GetOrOpen(fn2, os.O_RDWR|os.O_CREATE)

	file1 := entry1.file
	file2 := entry2.file

	// Acquire entry1 - it should not be evicted
	cache.Acquire(entry1)

	// Try to add a third file - should evict entry2 (not entry1 due to refCount > 0)
	fn3 := newTempFileNode(t, dir, "test3.txt")
	_, err := cache.GetOrOpen(fn3, os.O_RDWR|os.O_CREATE)
	if err != nil {
		t.Fatalf("GetOrOpen failed: %v", err)
	}

	// Verify file2 was closed (evicted)
	mustStatFail(t, file2)

	// Verify file1 is still open (not evicted due to refCount > 0)
	if _, err := file1.Stat(); err != nil {
		t.Errorf("file1 should still be open (has refCount > 0): %v", err)
	}
}

func TestEvictOne_AllHaveRefCount(t *testing.T) {
	dir := t.TempDir()
	cache := NewVFDCache(2)
	fn1 := newTempFileNode(t, dir, "test1.txt")
	fn2 := newTempFileNode(t, dir, "test2.txt")

	entry1, _ := cache.GetOrOpen(fn1, os.O_RDWR|os.O_CREATE)
	entry2, _ := cache.GetOrOpen(fn2, os.O_RDWR|os.O_CREATE)

	// Acquire both entries
	cache.Acquire(entry1)
	cache.Acquire(entry2)

	// Try to add a third file - should not evict any (all have refCount > 0)
	fn3 := newTempFileNode(t, dir, "test3.txt")
	entry3, err := cache.GetOrOpen(fn3, os.O_RDWR|os.O_CREATE)
	if err != nil {
		t.Fatalf("GetOrOpen failed: %v", err)
	}

	// Verify all files are still open
	if _, err := entry1.file.Stat(); err != nil {
		t.Errorf("file1 should still be open: %v", err)
	}
	if _, err := entry2.file.Stat(); err != nil {
		t.Errorf("file2 should still be open: %v", err)
	}
	if entry3 == nil || entry3.file == nil {
		t.Error("file3 should exist in cache")
	} else if _, err := entry3.file.Stat(); err != nil {
		t.Errorf("file3 should still be open: %v", err)
	}

	// Cache should have 3 entries even though capacity is 2
	if cache.lruList.Len() != 3 {
		t.Errorf("lruList length = %d, want 3 (can exceed capacity when all have refCount > 0)", cache.lruList.Len())
	}
}

func TestCloseAll_WithoutForce(t *testing.T) {
	dir := t.TempDir()
	cache := NewVFDCache(10)
	fn1 := newTempFileNode(t, dir, "test1.txt")
	fn2 := newTempFileNode(t, dir, "test2.txt")

	entry1, _ := cache.GetOrOpen(fn1, os.O_RDWR|os.O_CREATE)
	entry2, _ := cache.GetOrOpen(fn2, os.O_RDWR|os.O_CREATE)

	file1 := entry1.file
	file2 := entry2.file

	// Acquire entry1 - it should not be closed
	cache.Acquire(entry1)

	// Close all without force
	err := cache.CloseAll(false)
	if err != nil {
		t.Fatalf("CloseAll failed: %v", err)
	}

	// Verify file1 is still open (has refCount > 0)
	if _, err := file1.Stat(); err != nil {
		t.Errorf("file1 should still be open (has refCount > 0): %v", err)
	}

	// Verify file2 was closed (refCount == 0)
	mustStatFail(t, file2)

	// Verify entry1 is still in cache
	if _, exists := cache.cache[fn1]; !exists {
		t.Error("entry1 should still be in cache")
	}

	// Verify entry2 was removed
	if _, exists := cache.cache[fn2]; exists {
		t.Error("entry2 should be removed from cache")
	}

	// Release entry1
	cache.Release(entry1)

	// Close all again - now file1 should be closed
	err = cache.CloseAll(false)
	if err != nil {
		t.Fatalf("CloseAll failed: %v", err)
	}

	mustStatFail(t, file1)
}

func TestCloseAll_WithForce(t *testing.T) {
	dir := t.TempDir()
	cache := NewVFDCache(10)
	fn1 := newTempFileNode(t, dir, "test1.txt")
	fn2 := newTempFileNode(t, dir, "test2.txt")

	entry1, _ := cache.GetOrOpen(fn1, os.O_RDWR|os.O_CREATE)
	entry2, _ := cache.GetOrOpen(fn2, os.O_RDWR|os.O_CREATE)

	file1 := entry1.file
	file2 := entry2.file

	// Acquire both entries
	cache.Acquire(entry1)
	cache.Acquire(entry2)

	// Close all with force - should close all files regardless of refCount
	err := cache.CloseAll(true)
	if err != nil {
		t.Fatalf("CloseAll failed: %v", err)
	}

	// Verify all files are closed
	mustStatFail(t, file1)
	mustStatFail(t, file2)

	// Verify cache is empty
	if cache.lruList.Len() != 0 {
		t.Errorf("lruList length = %d, want 0", cache.lruList.Len())
	}
	if len(cache.cache) != 0 {
		t.Errorf("cache length = %d, want 0", len(cache.cache))
	}
}

func TestCloseAll_EmptyCache(t *testing.T) {
	cache := NewVFDCache(10)

	err := cache.CloseAll(false)
	if err != nil {
		t.Fatalf("CloseAll on empty cache failed: %v", err)
	}

	err = cache.CloseAll(true)
	if err != nil {
		t.Fatalf("CloseAll(force) on empty cache failed: %v", err)
	}
}

func TestGetOrOpen_NonExistentFile_ReadOnly(t *testing.T) {
	dir := t.TempDir()
	cache := NewVFDCache(10)
	fn := newTempFileNode(t, dir, "nonexistent.txt")

	// Try to open non-existent file without O_CREATE
	_, err := cache.GetOrOpen(fn, os.O_RDONLY)
	if err == nil {
		t.Fatal("GetOrOpen should fail for non-existent file without O_CREATE")
	}
}

func TestConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	cache := NewVFDCache(10)
	fn := newTempFileNode(t, dir, "test1.txt")

	// Test concurrent GetOrOpen
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			_, err := cache.GetOrOpen(fn, os.O_RDWR|os.O_CREATE)
			if err != nil {
				t.Errorf("GetOrOpen failed: %v", err)
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify only one entry exists
	if cache.lruList.Len() != 1 {
		t.Errorf("lruList length = %d, want 1", cache.lruList.Len())
	}
	if len(cache.cache) != 1 {
		t.Errorf("cache length = %d, want 1", len(cache.cache))
	}
}

func TestAcquireRelease_Concurrent(t *testing.T) {
	dir := t.TempDir()
	cache := NewVFDCache(10)
	fn := newTempFileNode(t, dir, "test1.txt")

	entry, _ := cache.GetOrOpen(fn, os.O_RDWR|os.O_CREATE)

	// Concurrent acquire/release
	done := make(chan bool, 20)
	for i := 0; i < 10; i++ {
		go func() {
			cache.Acquire(entry)
			done <- true
		}()
	}
	for i := 0; i < 10; i++ {
		go func() {
			cache.Release(entry)
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 20; i++ {
		<-done
	}

	// Final refCount should be 0 (10 acquires - 10 releases)
	if entry.refCount != 0 {
		t.Errorf("final refCount = %d, want 0", entry.refCount)
	}
}

func TestAccessCountNeverBelowZero(t *testing.T) {
	dir := t.TempDir()
	cache := NewVFDCache(10)
	fn := newTempFileNode(t, dir, "test1.txt")

	entry, _ := cache.GetOrOpen(fn, os.O_RDWR|os.O_CREATE)
	cache.Acquire(entry)

	if entry.refCount != 1 {
		t.Errorf("Acquire refCount = %d, want 1", entry.refCount)
	}

	// Concurrent release
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			cache.Release(entry)
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	if entry.refCount != 0 {
		t.Errorf("Acquire refCount = %d, want 0", entry.refCount)
	}
}
