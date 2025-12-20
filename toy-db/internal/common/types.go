package common

// Page Id is a u32 (4 bytes) number.
// Defined in: internal/common/types.go
type PageID uint32

// SegmentID identifies a segment (file) of a table.
// Default segment size is 1 GB. Starts at 0.
// Pages are stored in files (for each table). Each page has a page id, but we have
// a max number of pages per file per table. So we divide the table into segments.
// Defined in: internal/common/types.go
type SegmentID uint32

// TableID identifies a table  in the db.
// combination of table id + segment id + page id will
// uniquely identify a page
// Defined in: internal/common/types.go
type TableID uint32

// transaction number is u64 (8 bytes).
// Defined in: internal/common/types.go
type TransactionID uint64

// Log seq number is u64 (8 bytes).
// Defined in: internal/common/types.go
type LogSeqNumber uint64
