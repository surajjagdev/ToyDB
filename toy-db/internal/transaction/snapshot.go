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
func (s *Snapshot) IsVisible(
	xmin, xmax common.TransactionID,
	cid common.CommandID,
	currentTxID common.TransactionID,
	currentCID common.CommandID,
	isoLevel IsolationLevel,
	tm *TransactionManager,
) bool {
	// ==========================================================
	// RULE 1: Was the tuple created by a visible transaction?
	// ==========================================================
	if xmin == currentTxID {
		if cid >= currentCID {
			return false
		}
	} else {
		committedSnap := s.committedInSnapshot(xmin)
		if !committedSnap {
			if isoLevel == ReadCommitted {
				state := tm.GetTxnState(xmin)
				if state != Committed {
					return false
				}
			} else {
				return false
			}
		} else {
			state := tm.GetTxnState(xmin)
			if state == Aborted {
				return false
			}
		}
	}

	// ==========================================================
	// RULE 2: Was the tuple deleted/updated by a visible transaction?
	// ==========================================================
	if xmax == 0 {
		return true
	}

	if xmax == currentTxID {
		if cid >= currentCID {
			return true
		}
		return false
	}

	deleterCommitted := s.committedInSnapshot(xmax)
	if !deleterCommitted && isoLevel == ReadCommitted {
		deleterCommitted = tm.GetTxnState(xmax) == Committed
	}
	if !deleterCommitted {
		return true
	}

	deleterState := tm.GetTxnState(xmax)
	if deleterState == Aborted {
		return true
	}
	return false
}

func (s *Snapshot) committedInSnapshot(txid common.TransactionID) bool {
	if txid < s.XMin {
		return true
	}
	if txid >= s.XMax {
		return false
	}
	return !s.InProgressContains(txid)
}

// InProgressContains reports whether txid was in-flight at snapshot time.
func (s *Snapshot) InProgressContains(txid common.TransactionID) bool {
	i := sort.Search(len(s.InProgress), func(i int) bool {
		return s.InProgress[i] >= txid
	})

	return i < len(s.InProgress) && s.InProgress[i] == txid
}
