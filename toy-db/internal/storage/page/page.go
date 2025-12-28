package page

import (
	"hash/crc32"
	"sync"

	"github.com/surajjagdev/ToyDB/internal/common"
)

/**
Page
------

------
Page Header (Defined by offset rather than struct bc of packing)
00-08 LSN
08-12 PageId
12-14 OffsetLower
14-16 OffsetUpper
16-18 OffsetSpecial
18-20 OffsetFlags
20-24 checksum // after this is page type and etc
PageHeaderSize = 24 bytes
**/

const (
	OffsetPageLSN   = 0  // uint64, 8 bytes -> what modified this page?
	OffsetPageID    = 8  // uint32, 4 bytes -> unique identifier
	OffsetLower     = 12 // uint16, 2 bytes -> start of free space
	OffsetUpper     = 14 // uint16, 2 bytes -> end of free space. Tuples insert from upper to lower
	OffsetSpecial   = 16 // uint16, 2 bytes -> offset to special metadata if needed
	OffsetFlags     = 18 // uint16, 2 bytes -> page status, leaf, root, deleted page
	OffsetChecksum  = 20 // uint32, 4 bytes  -> data integrity
	OffsetDataStart = 24 // Start of actual tuple data -> first avail
	PageHeaderSize  = 24 // total header size
)

type PageFlags uint16

const (
	// Page lifecycle
	PageFlagInitialized PageFlags = 1 << iota // page has been formatted

	// Page kind (exactly ONE should be set)
	PageFlagHeap
	PageFlagIndex
	PageFlagFSM
	PageFlagVM

	// Index-specific structure
	PageFlagLeaf
	PageFlagInternal
	PageFlagRoot

	// Page state
	PageFlagDeleted // page is logically dead but not reclaimed
)

// To be used by buffer pool
type Page struct {
	PageID common.PageID
	Rel    common.RelationID
	Fork   common.ForkID

	PinCount uint32
	IsDirty  bool

	// protect rw latch
	rwLatch sync.RWMutex

	// contents
	Data [common.PageSize]byte
}

// Creates empty Page
func NewPage() *Page {
	return &Page{
		PageID:   common.InvalidPageID,
		Rel:      common.InvalidRelationID,
		Fork:     common.InvalidForkId,
		PinCount: 0,
		IsDirty:  false,
	}
}

// Reset a page
func (p *Page) Reset() {
	p.PageID = common.InvalidPageID
	p.Rel = common.InvalidRelationID
	p.Fork = common.InvalidForkId
	p.PinCount = 0
	p.IsDirty = false
	// no need to reset bytes
}

// -----------------------------------------------------------------------------
// Latches
// -----------------------------------------------------------------------------

// write latch
func (p *Page) WLatch() {
	p.rwLatch.Lock()
}

// write unlock latch
func (p *Page) WUnlatch() {
	p.rwLatch.Unlock()
}

// read latch
func (p *Page) RLatch() {
	p.rwLatch.RLock()
}

// read unlock latch
func (p *Page) RUnlatch() {
	p.rwLatch.RUnlock()
}

// -----------------------------------------------------------------------------
// Header Accessors (Using common.ByteOrder)
// -----------------------------------------------------------------------------

// Read LSN sequence
// Reads first 8 bytes from header
func (p *Page) GetLSN() common.LogSeqNumber {
	val := common.ByteOrder.Uint64(p.Data[OffsetPageLSN : OffsetPageLSN+8])

	return common.LogSeqNumber(val)
}

// Set LSN sequence in header
func (p *Page) SetLSN(lsn common.LogSeqNumber) {
	common.ByteOrder.PutUint64(p.Data[OffsetPageLSN:OffsetPageLSN+8], uint64(lsn))
}

// Read PageId sequence
// Reads byte 8-12 from header
func (p *Page) GetPageID() common.PageID {
	val := common.ByteOrder.Uint32(p.Data[OffsetPageID : OffsetPageID+4])

	return common.PageID(val)
}

// Set page id in header
func (p *Page) SetPageId(pageID common.PageID) {
	common.ByteOrder.PutUint32(p.Data[OffsetPageID:OffsetPageID+4], uint32(pageID))
}

// Get flags in header. Bytes 18-20
func (p *Page) GetFlags() PageFlags {
	val := PageFlags(common.ByteOrder.Uint16(p.Data[OffsetFlags : OffsetFlags+2]))
	return val
}

func (p *Page) SetFlags(flags PageFlags) {
	common.ByteOrder.PutUint16(p.Data[OffsetFlags:OffsetFlags+2], uint16(flags))
}

func (p *Page) AddFlags(flags PageFlags) {
	p.SetFlags(p.GetFlags() | flags)
}

func (p *Page) ClearFlags(flags PageFlags) {
	p.SetFlags(p.GetFlags() &^ flags)
}

func (p *Page) HasFlag(flag PageFlags) bool {
	return p.GetFlags()&flag != 0
}

func (p *Page) GetLower() uint16 {
	return common.ByteOrder.Uint16(p.Data[OffsetLower : OffsetLower+2])
}

func (p *Page) SetLower(v uint16) {
	common.ByteOrder.PutUint16(p.Data[OffsetLower:OffsetLower+2], v)
}

func (p *Page) GetUpper() uint16 {
	return common.ByteOrder.Uint16(p.Data[OffsetUpper : OffsetUpper+2])
}

func (p *Page) SetUpper(v uint16) {
	common.ByteOrder.PutUint16(p.Data[OffsetUpper:OffsetUpper+2], v)
}

func (p *Page) GetSpecial() uint16 {
	return common.ByteOrder.Uint16(p.Data[OffsetSpecial : OffsetSpecial+2])
}

func (p *Page) SetSpecial(v uint16) {
	common.ByteOrder.PutUint16(p.Data[OffsetSpecial:OffsetSpecial+2], v)
}

// -----------------------------------------------------------------------------
// Checksum Logic
// -----------------------------------------------------------------------------

func (p *Page) calculateChecksum() uint32 {
	crc := crc32.NewIEEE()

	// Checksum the header except checksum itself
	crc.Write(p.Data[0:OffsetChecksum])

	// Checksum the Payload (Bytes 24-4096)
	crc.Write(p.Data[OffsetDataStart:])

	return crc.Sum32()
}

// UpdateChecksum calculates the CRC32 of the current data and writes it to the header.
// This MUST be called by the BufferPool before writing to disk.
func (p *Page) UpdateChecksum() {
	sum := p.calculateChecksum()
	common.ByteOrder.PutUint32(p.Data[OffsetChecksum:OffsetChecksum+4], sum)
}

// ValidateIntegrity calculates the CRC32 of the data and compares it to the stored checksum.
func (p *Page) ValidateIntegrity() bool {
	// Read the checksum stored in the page
	stored := common.ByteOrder.Uint32(p.Data[OffsetChecksum : OffsetChecksum+4])

	// Calculate what it *should* be
	calculated := p.calculateChecksum()

	return stored == calculated
}

// -----------------------------------------------------------------------------
// util fns
// -----------------------------------------------------------------------------

// Get slice of raw bytes
func (p *Page) GetData() []byte {
	return p.Data[:]
}

// Get full copy of the bytes in page
func (p *Page) CopyData() []byte {
	newData := make([]byte, common.PageSize)
	copy(newData, p.Data[:])

	return newData
}
