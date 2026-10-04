package transaction

import (
	"sort"

	"github.com/surajjagdev/ToyDB/internal/common"
)

// Snapshot is the transaction's view of committed history at Begin time.
//
// Anything with a transaction ID below XMin is committed and visible.
// Anything at or above XMax is not yet visible. InProgress contains the ids
// of transactions that were running when the snapshot was taken; their
// effects must be hidden.
type Snapshot struct {
	XMin common.TransactionID
	XMax common.TransactionID

	// record in progress transactions at this time
	InProgress []common.TransactionID
}

// Visible reports whether a tuple version with the given xmin/xmax is visible
// to this snapshot.
func (s *Snapshot) IsVisible(xmin, xmax common.TransactionID) bool {
	// Deleted by a transaction that is committed in our view → invisible.
	if xmax != 0 && s.committed(xmax) {
		return false
	}

	// non-committed txn -> should be invisible
	if !s.committed(xmin) {
		return false
	}

	return true
}

// committed reports whether txid is committed from the snapshot's perspective.
// A txid is committed if it is below XMax and not in the InProgress set.
func (s *Snapshot) committed(txid common.TransactionID) bool {
	// current txn is after xmax of snapshot
	if txid >= s.XMax {
		return false
	}

	// search for txn in-progress, and if there return false
	// bc its still possibly in-progress
	if s.InProgressContains(txid) {
		return false
	}

	return true
}

// InProgressContains reports whether txid was in-flight at snapshot time.
func (s *Snapshot) InProgressContains(txid common.TransactionID) bool {
	i := sort.Search(len(s.InProgress), func(i int) bool {
		return s.InProgress[i] >= txid
	})

	return i < len(s.InProgress) && s.InProgress[i] == txid
}
