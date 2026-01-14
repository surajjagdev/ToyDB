package common

const (
	PageSize = uint64(8192) // Max Page size 8 KB

	PageVersionBits = 4
	PageSizeBits    = 12

	// Shifts
	PageVersionShift = 0
	PageSizeShift    = PageVersionBits // 4

	// Masks
	PageVersionMask uint16 = (1 << PageVersionBits) - 1     // 0x000F
	PageSizeMask    uint16 = ((1 << PageSizeBits) - 1) << 4 // 0xFFF0

	PageSectorSize     uint64 = 512
	CurrentPageVersion uint16 = 1

	MaxSegmentSize     = uint64(1024 * 1024 * 1024) // Max File Size, 1 GB
	DataFileExtension  = ".data"                    // File extension constant
	MaxPagesPerSegment = MaxSegmentSize / PageSize  // Max number of pages allowed per file
)

const (
	InvalidPageID        PageID        = PageID(^uint32(0))
	MaxPageID            PageID        = InvalidPageID - 1
	InvalidForkId        ForkID        = ^ForkID(0)
	InvalidRelationID    RelationID    = ^RelationID(0)
	InvalidTransactionID TransactionID = TransactionID(^uint64(0))
	MaxTransactionID     TransactionID = InvalidTransactionID - 1
	InvalidLogSeqNumber  LogSeqNumber  = LogSeqNumber(^uint64(0))
	MaxLogSeqNumber      LogSeqNumber  = InvalidLogSeqNumber - 1
)

// const (
// 	ReservedTableCatalog          TableID = 0 // Metadata for tables
// 	ReservedTableColumns          TableID = 1 // Metadata for columns
// 	ReservedTableIndexes          TableID = 2 // Metadata for indexes
// 	ReservedTableTypes            TableID = 3 // Metadata for types
// 	ReservedTableConstraints      TableID = 4
// 	ReservedTableSequences        TableID = 5
// 	ReservedTableUserDefinedStart TableID = 100 // First ID for user tables
// )
