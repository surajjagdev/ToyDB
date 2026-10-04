package concurrency

import (
	"sync"

	"github.com/surajjagdev/ToyDB/internal/common"
)

type TransactionManager struct {
	mu sync.RWMutex

	// Next txn id - global
	nextTxnId common.TransactionID

	// map of currently running txns
	activeTxns map[common.TransactionID]*Transaction
}
