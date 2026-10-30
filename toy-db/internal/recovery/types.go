package recovery

import (
	"sort"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

// State of the txn
type TxStatus uint8

const (
	TxActive TxStatus = iota
	TxCommited
	TxAborted
)

func (s TxStatus) String() string {
	switch s {
	case TxActive:
		return "active"
	case TxCommited:
		return "committed"
	case TxAborted:
		return "aborted"
	}

	return "unknown"
}

// Single row of txn table
type TxEntry struct {
	TxId      common.TransactionID
	Status    TxStatus
	FirstLSN  wal.LSN
	LastLSN   wal.LSN
	CommitLSN wal.LSN
	AbortLSN  wal.LSN
}

// Dirty Page entry
type DirtyPageEntry struct {
	Row    common.RowId
	RecLSN wal.LSN
}

// set of pages modified since scan began, key is rowid.
// keeps earliest LSN found per key, since scan is monotonically
// increasing
type DirtyPageTable struct {
	m map[common.RowId]wal.LSN
}

func NewDirtyPageTable() *DirtyPageTable {
	return &DirtyPageTable{
		m: make(map[common.RowId]wal.LSN),
	}
}

// Add row to map if it does not already exist
func (t *DirtyPageTable) Add(row common.RowId, recLSN wal.LSN) {
	if _, exists := t.m[row]; !exists {
		t.m[row] = recLSN
	}
}

// Return length of dirty page table built
func (t *DirtyPageTable) Len() int {
	return len(t.m)
}

func (t *DirtyPageTable) MinRecLSN() wal.LSN {
	if t.Len() == 0 {
		return 0
	}

	var min wal.LSN = wal.InvalidLSN

	for _, lsn := range t.m {
		if lsn < min {
			min = lsn
		}
	}

	return min
}

func lessRowId(a, b common.RowId) bool {
	if a.RelationID != b.RelationID {
		return a.RelationID < b.RelationID
	}

	if a.BlockID != b.BlockID {
		return a.BlockID < b.BlockID
	}

	return a.Slot < b.Slot
}

func (t *DirtyPageTable) Enteries() []DirtyPageEntry {
	out := make([]DirtyPageEntry, 0, t.Len())

	for row, lsn := range t.m {
		out = append(out, DirtyPageEntry{Row: row, RecLSN: lsn})
	}

	sort.Slice(out, func(i, j int) bool {
		return lessRowId(out[i].Row, out[j].Row)
	})

	return out
}

// Analysis result for recovery
type AnalysisResult struct {
	TxTable    map[common.TransactionID]*TxEntry
	DirtyPages *DirtyPageTable
	Losers     []common.TransactionID
	RedoStart  wal.LSN
	MaxLSN     wal.LSN
	MaxTxID    common.TransactionID
}

// Get the commited count
func (a *AnalysisResult) CommittedCount() int {
	c := 0

	for _, e := range a.TxTable {
		if e.Status == TxCommited {
			c++
		}
	}

	return c
}

// Get the Aborted count
func (a *AnalysisResult) AbortedCount() int {
	c := 0

	for _, e := range a.TxTable {
		if e.Status == TxAborted {
			c++
		}
	}

	return c
}
