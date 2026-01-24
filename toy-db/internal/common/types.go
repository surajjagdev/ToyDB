package common

import "encoding/binary"

// Global endiness. For reading and writing data
// not for storing on disk (disk doesnt need to know)
var ByteOrder = binary.LittleEndian

// Block Id is a u32 (4 bytes) number.
// Defined in: internal/common/types.go
type BlockID uint32

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

// transaction number is u32 (4 bytes).
// Defined in: internal/common/types.go
type TransactionID uint32

// command id (cid) number is u32 (4 bytes).
// Defined in: internal/common/types.go
type CommandID uint32

// Log seq number is u64 (8 bytes).
// Defined in: internal/common/types.go
type LogSeqNumber uint64
