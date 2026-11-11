package page

import (
	"hash/crc32"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

const (
	OffsetPageLSN            = 0  // 64 bit
	OffsetPageIDReserved     = 8  // 32 bit
	OffsetLower              = 12 // 16 bit
	OffsetUpper              = 14 // 16 bit
	OffsetSpecial            = 16 // 16 bit
	OffsetFlags              = 18 // 16 bit
	OffsetPageVersion        = 20 // 16 bit
	OffsetPageVersionPadding = 22 // 16 bit padding
	OffsetChecksum           = 24 // 32 bit
	OffsetReservedPadding    = 28 // 32 bit padding
	OffsetDataStart          = 32
	PageHeaderSize           = 32
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
func InitBasePage(p Page, flags PageFlags) {
	if uint64(len(p)) != common.PageSize {
		panic("page.Init: invalid page size")
	}

	p.ResetPage(flags)
}

func (p Page) ResetPage(flags PageFlags) {
	p.zeroPageMemory()

	p.SetPageId(^uint32(0) - 1)

	// LSN starts invalid / zero
	p.SetLSN(0)

	// empty
	// p.SetLower(PageHeaderSize)
	// p.SetUpper(uint16(common.PageSize))
	// p.SetSpecial(uint16(common.PageSize))
	p.SetPageVersion()

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

func (p Page) GetLSN() wal.LSN {
	return wal.LSN(common.ByteOrder.Uint64(p[OffsetPageLSN : OffsetPageLSN+8]))
}

func (p Page) SetLSN(lsn wal.LSN) {
	common.ByteOrder.PutUint64(p[OffsetPageLSN:OffsetPageLSN+8], uint64(lsn))
}

func (p Page) getPageID() uint32 {
	return common.ByteOrder.Uint32(p[OffsetPageIDReserved : OffsetPageIDReserved+4])
}

func (p Page) SetPageId(v uint32) {
	common.ByteOrder.PutUint32(p[OffsetPageIDReserved:OffsetPageIDReserved+4], v)
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
func (p Page) SetPageVersion() {
	// take size of page (e.g.8192) / 512 to fit into 12 bits
	pg_size_code := uint16(common.PageSize / common.PageSectorSize)
	// take the remainder shift 12 bits left, upper 12 bits are size and xor with page version
	// to get 16 bits
	v := (pg_size_code << common.PageSizeShift) | common.CurrentPageVersion

	common.ByteOrder.PutUint16(
		p[OffsetPageVersion:OffsetPageVersion+2],
		v,
	)
}

func (p Page) GetPageVersion() uint16 {
	v := common.ByteOrder.Uint16(p[OffsetPageVersion : OffsetPageVersion+2])

	// and with page version mask which is 4 bits as 1. e.g. 1111

	return v & common.PageVersionMask
}

func (p Page) GetPageSize() uint64 {
	v := common.ByteOrder.Uint16(p[OffsetPageVersion : OffsetPageVersion+2])

	// PageSizeMask is 12 bits as 1, shifted by 4 (page version)
	// 1111 1111 1111 0000, then shift by 4 bits, bc value is 16 times larger without it
	pgSz := (v & common.PageSizeMask)
	sizeCode := pgSz >> common.PageSizeShift
	return uint64(sizeCode) * common.PageSectorSize
}

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

// general function to align. Returns smallest next multiple of alignment
func AlignTo(sz int, alignment int) int {
	// e.g. sz = 13, alignment = 8
	// boundary = 13 + 8 - 1 = 20
	// alignmentMask = ^(8 - 1) = ^(7) = ^(0b00000111) = (11111000). Clears lower bits
	// boundary & alignmentMask
	// 00010100
	// 11111000
	// 00010000 = 16
	boundary := (sz + alignment - 1)
	alignmentMask := ^(alignment - 1)

	return boundary & alignmentMask
}
