package common

// Page Id is a u32 (4 bytes) number.
// Defined in: internal/common/types.go
type PageID uint32

// Relation identifies a table, index, etc  in the db.
// uniquely identify a page
// Defined in: internal/common/types.go
type RelationID uint32

// Logical relations can have multiple physical files ie. forks
// Defined in: internal/common/types.go
// small number of unique file types
type ForkID uint8

const (
	// ForkMain is the primary data fork of a relation.
	ForkMain ForkID = iota

	// ForkFSM stores free space metadata.
	ForkFSM

	// ForkVM stores visibility metadata.
	ForkVM
)

// transaction number is u64 (8 bytes).
// Defined in: internal/common/types.go
type TransactionID uint64

// Log seq number is u64 (8 bytes).
// Defined in: internal/common/types.go
type LogSeqNumber uint64
