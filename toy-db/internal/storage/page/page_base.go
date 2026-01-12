package page

import (
	"hash/crc32"

	"github.com/surajjagdev/ToyDB/internal/common"
)

const (
	OffsetPageLSN   = 0
	OffsetPageID    = 8
	OffsetLower     = 12
	OffsetUpper     = 14
	OffsetSpecial   = 16
	OffsetFlags     = 18
	OffsetChecksum  = 20
	OffsetDataStart = 24
	PageHeaderSize  = 24
)

type PageFlags uint16

const (
	PageFlagInitialized PageFlags = 1 << iota
	PageFlagHeap
	PageFlagIndex
	PageFlagFSM
	PageFlagVM
	PageFlagLeaf
	PageFlagInternal
	PageFlagRoot
	PageFlagDeleted
)

type Page []byte

// init
func Init(p Page, flags PageFlags) {
	if uint64(len(p)) != common.PageSize {
		panic("page.Init: invalid page size")
	}

	p.ResetPage(flags)
}

func (p Page) ResetPage(flags PageFlags) {
	p.zeroPageMemory()

	p.SetPageId(common.InvalidPageID)

	// LSN starts invalid / zero
	p.SetLSN(0)

	// empty
	p.SetLower(PageHeaderSize)
	p.SetUpper(uint16(common.PageSize))
	p.SetSpecial(uint16(common.PageSize))

	// Set page type flags (heap, index, etc.)
	p.SetFlags(flags | PageFlagInitialized)
}

// Zero the entire page
func (p Page) zeroPageMemory() {
	for i := range p {
		p[i] = 0
	}
}

// -----------------------------------------------------------------------------
// Header Accessors
// -----------------------------------------------------------------------------

func (p Page) GetLSN() common.LogSeqNumber {
	return common.LogSeqNumber(common.ByteOrder.Uint64(p[OffsetPageLSN : OffsetPageLSN+8]))
}

func (p Page) SetLSN(lsn common.LogSeqNumber) {
	common.ByteOrder.PutUint64(p[OffsetPageLSN:OffsetPageLSN+8], uint64(lsn))
}

func (p Page) GetPageID() common.PageID {
	return common.PageID(common.ByteOrder.Uint32(p[OffsetPageID : OffsetPageID+4]))
}

func (p Page) SetPageId(pageID common.PageID) {
	common.ByteOrder.PutUint32(p[OffsetPageID:OffsetPageID+4], uint32(pageID))
}

func (p Page) GetFlags() PageFlags {
	return PageFlags(common.ByteOrder.Uint16(p[OffsetFlags : OffsetFlags+2]))
}

func (p Page) SetFlags(flags PageFlags) {
	common.ByteOrder.PutUint16(p[OffsetFlags:OffsetFlags+2], uint16(flags))
}

func (p Page) AddFlags(flags PageFlags) {
	p.SetFlags(p.GetFlags() | flags)
}

func (p Page) ClearFlags(flags PageFlags) {
	p.SetFlags(p.GetFlags() &^ flags)
}

func (p Page) HasFlag(flag PageFlags) bool {
	return p.GetFlags()&flag != 0
}

// Lower/Upper/Special
func (p Page) GetLower() uint16  { return common.ByteOrder.Uint16(p[OffsetLower : OffsetLower+2]) }
func (p Page) SetLower(v uint16) { common.ByteOrder.PutUint16(p[OffsetLower:OffsetLower+2], v) }
func (p Page) GetUpper() uint16  { return common.ByteOrder.Uint16(p[OffsetUpper : OffsetUpper+2]) }
func (p Page) SetUpper(v uint16) { common.ByteOrder.PutUint16(p[OffsetUpper:OffsetUpper+2], v) }
func (p Page) GetSpecial() uint16 {
	return common.ByteOrder.Uint16(p[OffsetSpecial : OffsetSpecial+2])
}
func (p Page) SetSpecial(v uint16) {
	common.ByteOrder.PutUint16(p[OffsetSpecial:OffsetSpecial+2], v)
}

// -----------------------------------------------------------------------------
// Checksum
// -----------------------------------------------------------------------------

func (p Page) calculateChecksum() uint32 {
	crc := crc32.NewIEEE()
	crc.Write(p[0:OffsetChecksum])
	crc.Write(p[OffsetDataStart:])
	return crc.Sum32()
}

func (p Page) UpdateChecksum() {
	common.ByteOrder.PutUint32(p[OffsetChecksum:OffsetChecksum+4], p.calculateChecksum())
}

func (p Page) ValidateIntegrity() bool {
	stored := common.ByteOrder.Uint32(p[OffsetChecksum : OffsetChecksum+4])
	return stored == p.calculateChecksum()
}
