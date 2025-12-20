package disk

import (
	"container/list"
	"fmt"
	"os"
	"sync"
)

// virtual file descriptor so we don't hit os limit of open files

// entry for single file open
type fileNode struct {
	path string
	file *os.File
}

type VFDCache struct {
	mu sync.Mutex

	capacity int

	// map of elements that are doubly linked
	cache map[string]*list.Element

	// Ordering: Front = Most Recently Used, Back = Least Recently Used
	lruList *list.List
}

func NewVFDCache(capacity int) *VFDCache {
	return &VFDCache{
		capacity: capacity,
		cache:    make(map[string]*list.Element),
		lruList:  list.New(),
	}
}

// evict least recently used element
func (v *VFDCache) evict() {
	elem := v.lruList.Back()

	if elem != nil {
		node := elem.Value.(*fileNode)

		node.file.Close()

		delete(v.cache, node.path)
		v.lruList.Remove(elem)
	}
}

// GetOrOpen retrieves a file handle.
// If it's cached, it moves to the front (most recently used).
// If not, it opens the file and evicts the LRU file if at capacity.
func (v *VFDCache) GetOrOpen(path string) (*os.File, error) {
	// Lock so we don't have a double entry
	v.mu.Lock()
	defer v.mu.Unlock()

	// 1. check cache if we can find the entry
	if ele, exists := v.cache[path]; exists {
		// move to front of list
		v.lruList.MoveToFront(ele)
		return ele.Value.(*fileNode).file, nil
	}

	// 2. not in cache so grab and put in cache
	// have as read write file, create file if not exists
	// O_RDWR: Read/Write
	// O_CREATE: Create file if it doesn't exist
	// 0666: Standard RW permissions
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)

	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}

	// evict if exceeds capacity
	if v.lruList.Len() >= v.capacity {
		// evict
		v.evict()
	}

	node := &fileNode{
		path: path,
		file: file,
	}

	elem := v.lruList.PushFront(node)
	v.cache[path] = elem

	return file, nil
}

// on db shut down close all files
func (v *VFDCache) CloseAll() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	var closeErr error

	for e := v.lruList.Front(); e != nil; e.Next() {
		node := e.Value.(*fileNode)

		if err := node.file.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}

	// reset lru
	v.lruList.Init()
	v.cache = make(map[string]*list.Element)

	return closeErr
}
