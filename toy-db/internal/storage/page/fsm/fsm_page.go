package fsm

import (
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/page"
)

// FSM tracks the free space in pages
// It is a max heap implementation per page

// Since each page is 8192 bytes, we can instead save as
// 1 byte per page. This byte is between 0-255
// so its divided into categories as approx mem available

// 0   -> 0 bytes avail
// 1 -> 32 bytes avail
// 10 -> 320 bytes
// 255 → 8160 bytes avail

const LeavesPerPage int = 2048
const MaxCategoryBytes int = 255
const BytesPerCategory = int(common.PageSize) / MaxCategoryBytes

type FSMPage struct {
	page.Page
}

// Init FSM page
func InitFSMPage(p page.Page) *FSMPage {
	page.InitBasePage(p, page.PageFlagFSM)

	// FSM pages dont need to use headers of page
	data := p[page.OffsetDataStart:]

	for i := range data {
		data[i] = 0
	}

	return &FSMPage{p}
}

func FreeBytesToCategory(freeBytes int) uint8 {
	if freeBytes <= 0 {
		return 0
	}

	category := freeBytes / BytesPerCategory

	if category > MaxCategoryBytes {
		return uint8(MaxCategoryBytes)
	}

	return uint8(category)
}

func CategoryToBytes(category uint8) int {
	return int(category) * BytesPerCategory
}

// FSM is a max-heap implementation

// Bytes after header is a flat binary tree array
func (f *FSMPage) getTreeArray() []byte {
	return f.Page[page.OffsetDataStart:]
}

func (f *FSMPage) getMaxNodes() int {
	capacity := len(f.getTreeArray())

	nodes := 1
	for (nodes*2)+1 <= capacity {
		nodes = (nodes * 2) + 1
	}
	return nodes
}

// The bottom half of the array are the leaves.
// Each leaf represents 1 physical block.
func (f *FSMPage) getNumLeaves() int {
	return (f.getMaxNodes() + 1) / 2
}

func (f *FSMPage) getNonLeafNodes() int {
	return f.getMaxNodes() / 2
}

// Gets max available space anywhere in tree
func (f *FSMPage) GetMaxAvailable() uint8 {
	tree := f.getTreeArray()

	return tree[0]
}

// Get the leaf offset that has enough space. Return -1, if nothing available
func (f *FSMPage) SearchAvailable(minCategory uint8) int {
	tree := f.getTreeArray()
	maxNodes := f.getMaxNodes()

	// root is at index 0, if its less than minCategory required
	// then page is full
	if maxNodes == 0 || tree[0] < minCategory {
		return -1
	}

	index := 0
	nonLeafNodes := f.getNonLeafNodes()

	// continue till end of tree for the 0-indexed max heap
	for index < nonLeafNodes {
		left := (2 * index) + 1
		right := (2 * index) + 2

		// We prefer the left subtree
		if left < maxNodes && tree[left] >= minCategory {
			index = left
		} else if right < maxNodes && tree[right] >= minCategory {
			index = right
		} else {
			return -1
		}
	}

	// found an index -> return logical offset
	return index - nonLeafNodes
}

// Given offset update available space, and update so max heap still works
func (f *FSMPage) UpdateAvailable(offset int, category uint8) {
	tree := f.getTreeArray()
	nonLeafNodes := f.getNonLeafNodes()

	// converting leaf offset to array index
	index := nonLeafNodes + offset
	if index >= f.getMaxNodes() {
		panic("FSM leaf offset out of bounds")
	}

	// Nothing changed
	if tree[index] == category {
		return
	}

	// update the tree at index
	tree[index] = category

	// percolate up
	for index > 0 {
		parent := (index - 1) / 2
		left := (2 * parent) + 1 // first ptr is current index
		right := (2 * parent) + 2

		maxCategory := tree[left]

		if right < f.getMaxNodes() && tree[right] > maxCategory {
			maxCategory = tree[right]
		}

		// if tree[parent] == maxCategory {
		// 	break
		// }

		tree[parent] = maxCategory
		index = parent
	}

}
