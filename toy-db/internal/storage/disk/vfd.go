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

type vfdEntry struct {
	fileNode FileNode
	flags    int
	file     *os.File
	refCount uint32
	elem     *list.Element
}

type VFDCache struct {
	mu sync.Mutex

	capacity int

	// map of elements that are doubly linked
	cache map[FileNode]*vfdEntry

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
		cache:    make(map[FileNode]*vfdEntry),
		lruList:  list.New(),
		openFile: func(path string, flags int) (*os.File, error) {
			return os.OpenFile(path, flags, 0666)
		},
	}
}

func NewDirectVFD(capacity int) *VFDCache {
	return &VFDCache{
		capacity: capacity,
		cache:    make(map[FileNode]*vfdEntry),
		lruList:  list.New(),
		openFile: openDirect, // OS-specific
	}
}

// evict least recently used element
func (v *VFDCache) evictOne() (error, bool) {
	// evict the first entry that is not used
	lastElement := v.lruList.Back()

	for {
		if lastElement == nil {
			return nil, false
		}

		entry := lastElement.Value.(*vfdEntry)

		if entry.refCount > 0 {
			lastElement = lastElement.Prev()
			continue
		}

		// close the os file
		if entry.file != nil {
			if err := entry.file.Close(); err != nil {
				return err, false
			}
		}

		// remove from lru cache
		v.lruList.Remove(entry.elem)
		entry.elem = nil
		delete(v.cache, entry.fileNode)

		// entry in cache can remain
		return nil, true
	}
}

// GetOrOpen retrieves a file handle.
// If it's cached, it moves to the front (most recently used).
// If not, it opens the file and evicts the LRU file if at capacity.
func (v *VFDCache) GetOrOpen(fn FileNode, flags int) (*vfdEntry, error) {
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
			return nil, fmt.Errorf("failed to evict any enteries for %s", fn.Path)
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

	newEntry := &vfdEntry{
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
		entry := ele.Value.(*vfdEntry)

		if entry.refCount == 0 || force {
			if entry.file != nil {
				err := entry.file.Close()
				if err != nil && firstError == nil {
					firstError = err
				}

				entry.file = nil
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
func (v *VFDCache) Acquire(e *vfdEntry) {
	v.mu.Lock()
	defer v.mu.Unlock()

	e.refCount++
	v.lruList.MoveToFront(e.elem)
}

// release a entry
func (v *VFDCache) Release(e *vfdEntry) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if e.refCount == 0 {
		panic("VFDCache: Release without Acquire")
	}

	e.refCount--
}
