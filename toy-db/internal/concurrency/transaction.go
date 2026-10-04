package concurrency

import "github.com/surajjagdev/ToyDB/internal/common"

// snapshots of txns

// Type of transaction states
type TxnState uint8

const (
	TxnInProgress TxnState = iota
	TxnCommitted
	TxnAborted
)

// Represents a single user's work
type Transaction struct {
	TxnId common.TransactionID
	State TxnState
}

// Creates a new transaction
func NewTransaction(txnId common.TransactionID) *Transaction {
	return &Transaction{
		TxnId: txnId,
		State: TxnInProgress,
	}
}

// State of the database at specific ms in time
// helps to determine which queries are visibile
type Snapshot struct {
	// Xmin is the lowest active Transaction ID when the snapshot was taken.
	// Any tuple with xmax < Xmin is completely dead and invisible.
	Xmin common.TransactionID

	// Xmax is the next available Transaction ID (the highest ID + 1).
	// Any tuple with xmin >= Xmax was created AFTER our snapshot and is invisible.
	Xmax common.TransactionID

	// ActiveTxns is a map of all transactions that were in progress during this query
	ActiveTxns map[common.TransactionID]bool
}

// Check if a transaction is active in the snapshot
func (s *Snapshot) isTxnActiveInSnapshot(txnId common.TransactionID) bool {
	// This txn id finished much before
	if txnId < s.Xmin {
		return false
	}

	// txn started after this snapshot, so it is active for us
	if txnId >= s.Xmax {
		return true
	}

	// check map
	_, ok := s.ActiveTxns[txnId]

	return ok
}
