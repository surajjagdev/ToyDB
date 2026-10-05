package disk

import (
	"container/list"
	"fmt"
	"os"
	"sync"
)

// virtual file descriptor so we don't hit os limit of open files

// entry for single file open
type FileNode struct {
	Path string
}

type OpenDiskFileFunc func(path string, flags int) (*os.File, error)

type VfdEntry struct {
	fileNode FileNode
	flags    int
	file     *os.File
	refCount uint32
	elem     *list.Element

	mu sync.RWMutex
}

type VFDCache struct {
	mu sync.Mutex

	capacity int

	// map of elements that are doubly linked
	cache map[FileNode]*VfdEntry

	// Ordering: Front = Most Recently Used, Back = Least Recently Used
	lruList *list.List

	openFile OpenDiskFileFunc
}

func NewCachedVFD(capacity int) *VFDCache {
	if capacity <= 0 {
		panic("VFD capacity must be > 0")
	}

	return &VFDCache{
		capacity: capacity,
		cache:    make(map[FileNode]*VfdEntry),
		lruList:  list.New(),
		openFile: func(path string, flags int) (*os.File, error) {
			return os.OpenFile(path, flags, 0666)
		},
	}
}

func NewDirectVFD(capacity int) *VFDCache {
	return &VFDCache{
		capacity: capacity,
		cache:    make(map[FileNode]*VfdEntry),
		lruList:  list.New(),
		openFile: openDirect, // OS-specific
	}
}

func (e *VfdEntry) ReadAt(p []byte, off int64) (int, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.file.ReadAt(p, off)
}

func (e *VfdEntry) Stat() (os.FileInfo, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.file.Stat()
}

func (e *VfdEntry) Truncate(sz int64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.file.Truncate(sz); err != nil {
		return err
	}

	return nil
}

// WriteAt serializes writes against reads, writes, and closes.
func (e *VfdEntry) WriteAt(p []byte, off int64) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.file.WriteAt(p, off)
}

// Sync flushes the underlying file.
func (e *VfdEntry) Sync() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.file.Sync()
}

func (e *VfdEntry) closeFile() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.file == nil {
		return nil
	}
	err := e.file.Close()
	e.file = nil
	return err
}

// evict least recently used element
func (v *VFDCache) evictOne() (error, bool) {
	lastElement := v.lruList.Back()

	for {
		if lastElement == nil {
			return nil, false
		}

		entry := lastElement.Value.(*VfdEntry)

		if entry.refCount > 0 {
			lastElement = lastElement.Prev()
			continue
		}

		// Close under the entry lock so we don't race an in-flight I/O.
		if err := entry.closeFile(); err != nil {
			return err, false
		}

		// remove from lru cache
		v.lruList.Remove(entry.elem)
		entry.elem = nil
		delete(v.cache, entry.fileNode)

		return nil, true
	}
}

// GetOrOpen retrieves a file handle.
// If it's cached, it moves to the front (most recently used).
// If not, it opens the file and evicts the LRU file if at capacity.
func (v *VFDCache) GetOrOpen(fn FileNode, flags int) (*VfdEntry, error) {
	// Lock so we don't have a double entry
	v.mu.Lock()
	defer v.mu.Unlock()

	// 1. check cache if we can find the entry
	if entry, exists := v.cache[fn]; exists {
		// move to front of list
		v.lruList.MoveToFront(entry.elem)
		entry.refCount++
		return entry, nil
	}

	// evict if exceeds capacity
	if v.lruList.Len() >= v.capacity {

		err, hasEvicted := v.evictOne()

		if err != nil {
			return nil, err
		}

		if !hasEvicted {
			return nil, fmt.Errorf("failed to evict any entries for %s", fn.Path)
		}
	}

	// 2. not in cache so grab and put in cache
	// have as read write file, create file if not exists
	// O_RDWR: Read/Write
	// O_CREATE: Create file if it doesn't exist
	// 0666: Standard RW permissions
	file, err := v.openFile(fn.Path, flags)

	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", fn.Path, err)
	}

	newEntry := &VfdEntry{
		fileNode: fn,
		file:     file,
		refCount: 1,
		flags:    flags,
	}

	newEntry.elem = v.lruList.PushFront(newEntry)
	v.cache[fn] = newEntry

	return newEntry, nil
}

// on db shut down close all files
func (v *VFDCache) CloseAll(force bool) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	var firstError error

	for ele := v.lruList.Back(); ele != nil; {
		next := ele.Prev()
		entry := ele.Value.(*VfdEntry)

		if entry.refCount == 0 || force {
			if err := entry.closeFile(); err != nil && firstError == nil {
				firstError = err
			}

			v.lruList.Remove(ele)
			delete(v.cache, entry.fileNode)
			entry.elem = nil
		}

		ele = next
	}

	return firstError
}

// acquire a entry
func (v *VFDCache) Acquire(e *VfdEntry) {
	v.mu.Lock()
	defer v.mu.Unlock()

	e.refCount++
	v.lruList.MoveToFront(e.elem)
}

// release a entry
func (v *VFDCache) Release(e *VfdEntry) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if e.refCount == 0 {
		panic("VFDCache: Release without Acquire")
	}

	e.refCount--
}
