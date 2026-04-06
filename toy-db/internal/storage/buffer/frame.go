package buffer

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/page"
)

/*
*
bits 0–17   : pin count (lower)
bit  18     : dirty
bit  19     : valid
bit  20     : IO in progress
bits 21–24  : usage count
bits 25-31 : Free not used
*
*/
const (
	// ---- pin count ----
	PinCountBits = 18 // allows for abt 250, 000 pins
	PinCountMask = uint32((1 << PinCountBits) - 1)

	// ---- page flags ----
	PageFlagDirty = uint32(1) << PinCountBits
	PageFlagValid = uint32(1) << (PinCountBits + 1)
	PageFlagIO    = uint32(1) << (PinCountBits + 2)

	PageFlagBits = 3

	// ---- usage count (clock sweep) ----
	UsageBits  = 4 // 0–15 max val
	UsageShift = PinCountBits + PageFlagBits
	UsageMask  = uint32((1<<UsageBits)-1) << UsageShift
)

// Page struct, OS-independent
type Frame struct {
	// block id + fork id + relation id uniquely identifies a page
	BlockID    common.BlockID
	ForkID     common.ForkID
	RelationID common.RelationID

	Page page.Page // data for the page

	rwLatch sync.RWMutex // latch for the page data, not the frame itself

	// keep as uint32 to read as atomic if needed
	// (lower ) pin count | dirty | valid | io | usage
	state uint32

	ptr unsafe.Pointer // only used on Linux for aligned pages
}

// NewFrame creates a new frame for a page
func NewFrame() (*Frame, error) {
	data, ptr, err := allocPageData()
	if err != nil {
		return nil, err
	}

	return &Frame{
		BlockID:    common.InvalidBlockID,
		ForkID:     common.InvalidForkID,
		RelationID: common.InvalidRelationID,
		Page:       page.Page(data), // cast []byte → page.Page
		ptr:        ptr,
		state:      uint32(0),
	}, nil
}

// Free the frame and the page data
func (f *Frame) Free() {
	// acquire write latch
	f.WLatch()
	defer f.WUnlatch()

	// free the page data
	if f.ptr != nil {
		freePageData(f.ptr)
		f.ptr = nil
	}
}

// CopyData returns a copy of the page's data as buffer.
// Does not copy frame meta data like relation id, block id or fork id
// pin count and dirty will start as 0
// Works for both normal and aligned pages.
func (f *Frame) CopyPageData() (page.Page, unsafe.Pointer) {
	f.RLatch()
	defer f.RUnlatch()

	// Allocate a new page buffer (aligned on Linux, normal slice on Darwin)
	buf, ptr, err := allocPageData()

	if err != nil {
		// fallback: panic or return normal slice
		panic("failed to allocate aligned page: " + err.Error())
	}

	// Copy the data
	copy(buf, f.Page[:common.PageSize])

	return buf, ptr
}

// Copy the current frames data to the given frame's data
// only page data, not meta data
// Latches source first, then destination
func (f *Frame) CopyDataToExistingFrame(dst *Frame) {
	f.RLatch()
	defer f.RUnlatch()

	dst.WLatch()
	defer dst.WUnlatch()
	copy(dst.Page[:common.PageSize], f.Page)
}

// Helper function to get the frame identity
// used for testing. No locks are acquired.
func (f *Frame) GetFrameIdentity() (common.BlockID, common.ForkID, common.RelationID) {
	return f.BlockID, f.ForkID, f.RelationID
}

// Set the page identity
// Buffer pool should be in charge of meta data lifecycle
// once this is set, the frame should be invisible for reuse, till it can be reused
// panics if pinned
func (f *Frame) SetFrameIdentity(blockId common.BlockID, forkId common.ForkID, rel common.RelationID) {
	if f.IsPinned() {
		panic("Setting identity on pinned frame")
	}

	f.RelationID = rel
	f.BlockID = blockId
	f.ForkID = forkId
}

// Returns pin count
// uses atomic load
func (f *Frame) GetPinCount() uint32 {
	stateData := atomic.LoadUint32(&f.state)

	// read pinned info bits
	return stateData & PinCountMask
}

// Returns true if page is modified
// uses atomic load
func (f *Frame) IsDirty() bool {
	stateData := atomic.LoadUint32(&f.state)

	// read dirty flag
	return (stateData & PageFlagDirty) != 0
}

// Returns true if page is pinned
// uses atomic load
func (f *Frame) IsPinned() bool {
	return f.GetPinCount() != 0
}

// utility functions

// Pin the frame, add 1 to pin count
// atomic op
func (f *Frame) Pin() {
	for {
		old := atomic.LoadUint32(&f.state)
		pins := old & PinCountMask
		newState := (old & ^PinCountMask) | (pins + 1)
		if atomic.CompareAndSwapUint32(&f.state, old, newState) {
			return
		}
	}
}

// unpin the frame, subtract 1 from pin count
// atomic op
// panics if pin count underflows
func (f *Frame) Unpin() {
	for {
		old := atomic.LoadUint32(&f.state)
		pins := old & PinCountMask
		if pins == 0 {
			panic("unpin on zero pin count")
		}
		newState := (old & ^PinCountMask) | (pins - 1)
		if atomic.CompareAndSwapUint32(&f.state, old, newState) {
			return
		}
	}
}

// mark frame as dirty
// uses atomic op
func (f *Frame) SetDirty() {
	atomic.OrUint32(&f.state, uint32(PageFlagDirty))
}

// clear dirty flag
// uses atomic op
func (f *Frame) ClearDirty() {
	atomic.AndUint32(&f.state, ^uint32(PageFlagDirty))
}

// GetUsage returns the frame's usage count
func (f *Frame) GetUsage() uint32 {
	state := atomic.LoadUint32(&f.state)
	return (state & UsageMask) >> UsageShift
}

// IncrementUsage increases usage count (max 15)
func (f *Frame) IncrementUsage() {
	for {
		old := atomic.LoadUint32(&f.state)
		usage := (old & UsageMask) >> UsageShift
		if usage == (1<<UsageBits - 1) {
			return // maxed out
		}
		newState := (old & ^UsageMask) | ((usage + 1) << UsageShift)
		if atomic.CompareAndSwapUint32(&f.state, old, newState) {
			return
		}
	}
}

// DecrementUsage decreases usage count (min 0)
func (f *Frame) DecrementUsage() {
	for {
		old := atomic.LoadUint32(&f.state)
		usage := (old & UsageMask) >> UsageShift
		if usage == 0 {
			return
		}
		newState := (old & ^UsageMask) | ((usage - 1) << UsageShift)
		if atomic.CompareAndSwapUint32(&f.state, old, newState) {
			return
		}
	}
}

func (f *Frame) IsValid() bool {
	state := atomic.LoadUint32(&f.state)
	return (state & PageFlagValid) != 0
}

func (f *Frame) SetValid() {
	atomic.OrUint32(&f.state, uint32(PageFlagValid))
}

func (f *Frame) ClearValid() {
	atomic.AndUint32(&f.state, ^uint32(PageFlagValid))
}

func (f *Frame) IsIOInProgress() bool {
	state := atomic.LoadUint32(&f.state)
	return (state & PageFlagIO) != 0
}

func (f *Frame) SetIOInProgress() {
	atomic.OrUint32(&f.state, uint32(PageFlagIO))
}

func (f *Frame) ClearIOInProgress() {
	atomic.AndUint32(&f.state, ^uint32(PageFlagIO))
}

// -----------------------------------------------------------------------------
// Reset
// -----------------------------------------------------------------------------

// Reset the frame to its initial state
// flags are the page flags to set
func (f *Frame) Reset(flags page.PageFlags) {
	// acquire write latch
	// reset page data
	// reset pin count and dirty flag
	f.WLatch()
	defer f.WUnlatch()

	f.Page.ResetPage(flags)

	atomic.StoreUint32(&f.state, uint32(0))
}

// -----------------------------------------------------------------------------
// Latches
// -----------------------------------------------------------------------------

func (f *Frame) WLatch()   { f.rwLatch.Lock() }
func (f *Frame) WUnlatch() { f.rwLatch.Unlock() }
func (f *Frame) RLatch()   { f.rwLatch.RLock() }
func (f *Frame) RUnlatch() { f.rwLatch.RUnlock() }
