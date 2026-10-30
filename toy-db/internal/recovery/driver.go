package recovery

import (
	"fmt"
	"time"

	"github.com/surajjagdev/ToyDB/internal/clog"
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/transaction"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

// RecoveryApplier bundles the two applier interfaces. Implementations must
// satisfy both — during recovery, one applier instance handles both redo and
// undo.
type RecoveryApplier interface {
	RedoApplier
	UndoApplier
}

// Deps is everything recovery needs from the rest of the system.
type Deps struct {
	WAL   *wal.WAL
	TM    *transaction.TransactionManager
	CLOG  *clog.Manager
	Apply RecoveryApplier
}

// Options tunes recovery. Currently only diagnostics; future versions will
// accept checkpoint-related knobs.
type Options struct {
	// LogProgress, if true, prints to stdout at the start and end of each
	// phase. Useful in crash tests and demos.
	LogProgress bool
}

// RecoveryResult bundles the outcome of a recovery run.
type RecoveryResult struct {
	Analysis    *AnalysisResult
	RedoStats   RedoStats
	UndoStats   UndoStats
	CLOGUpdates int
	NextTxID    common.TransactionID
	Duration    time.Duration
}

// Recover runs the full three-pass recovery algorithm and returns the
// database to a consistent state.
//
// On success:
//   - All committed transactions' effects are on disk (redo).
//   - All in-flight transactions' effects are reversed (undo).
//   - The transaction manager's CLOG reflects every transaction ever seen.
//   - The transaction manager's active set is empty and nextTxID is set so
//     new transactions don't collide with recovered ones.
//
// Callers must invoke this once at startup, before accepting any client
// traffic. It is not safe to call Recover concurrently with normal
// operation.
func Recover(deps Deps, opts Options) (*RecoveryResult, error) {
	start := time.Now()

	if deps.WAL == nil || deps.TM == nil || deps.CLOG == nil || deps.Apply == nil {
		return nil, fmt.Errorf("recovery: Recover requires non-nil WAL, TM, CLOG, and Apply")
	}

	res := &RecoveryResult{}

	// --- 1. Analyze -------------------------------------------------------
	if opts.LogProgress {
		fmt.Println("[recovery] analyze: scanning WAL")
	}
	analysis, err := Analyze(deps.WAL, 0, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("recovery: analyze: %w", err)
	}
	res.Analysis = analysis

	if opts.LogProgress {
		fmt.Printf("[recovery] analyze: %d txns (%d committed, %d aborted, %d losers), %d dirty pages, redo from %v\n",
			len(analysis.TxTable),
			analysis.CommittedCount(),
			analysis.AbortedCount(),
			len(analysis.Losers),
			analysis.DirtyPages.Len(),
			analysis.RedoStart,
		)
	}

	// --- 2. Redo ----------------------------------------------------------
	if opts.LogProgress {
		fmt.Println("[recovery] redo: replaying changes")
	}
	redoStats, err := Redo(deps.WAL, analysis, deps.Apply)
	if err != nil {
		return nil, fmt.Errorf("recovery: redo: %w", err)
	}
	res.RedoStats = redoStats

	if opts.LogProgress {
		fmt.Printf("[recovery] redo: %d records scanned, %d applied\n",
			redoStats.RecordsScanned, redoStats.RecordsApplied)
	}

	// --- 3. Undo ----------------------------------------------------------
	if opts.LogProgress {
		fmt.Printf("[recovery] undo: rolling back %d losers\n", len(analysis.Losers))
	}
	undoStats, err := Undo(deps.WAL, analysis, deps.Apply)
	if err != nil {
		return nil, fmt.Errorf("recovery: undo: %w", err)
	}
	res.UndoStats = undoStats

	if opts.LogProgress {
		fmt.Printf("[recovery] undo: %d losers, %d records undone, %d CLRs\n",
			undoStats.Losers, undoStats.RecordsUndone, undoStats.CLRsWritten)
	}

	// --- 4. Populate CLOG -------------------------------------------------
	//
	// Every transaction in the analysis result has a final state now:
	//   - TxCommitted   → CLOG Committed
	//   - TxAborted     → CLOG Aborted (was aborted before the crash)
	//   - TxActive      → CLOG Aborted (we just undid it above)
	//
	// We call TM.RecordState for each so the CLOG is complete. This is the
	// durability contract for IsCommitted: after Recover returns, every xid
	// that ever existed has an answer in CLOG.
	for txid, entry := range analysis.TxTable {
		var state transaction.State
		switch entry.Status {
		case TxCommited:
			state = transaction.Committed
		case TxAborted, TxActive:
			// TxActive losers were just aborted by the Undo pass.
			state = transaction.Aborted
		default:
			return nil, fmt.Errorf(
				"recovery: unexpected tx state %v for tx %d", entry.Status, txid)
		}
		if err := deps.TM.RecordState(txid, state); err != nil {
			return nil, fmt.Errorf(
				"recovery: CLOG update for tx %d: %w", txid, err)
		}
		res.CLOGUpdates++
	}

	// --- 5. Reset transaction manager -------------------------------------
	//
	// The TM's active map must be empty (no client traffic yet) and
	// nextTxID must be past any ID we saw, so new transactions don't
	// collide with existing tuples' xmin/xmax.
	nextTxID := analysis.MaxTxID + 1
	if nextTxID < 3 { // 0=Invalid, 1=Bootstrap, 2=Frozen
		nextTxID = 3
	}
	deps.TM.ResetForRecovery(nextTxID)
	res.NextTxID = nextTxID

	// --- 6. Flush CLOG ----------------------------------------------------
	//
	// Make CLOG durable before accepting traffic. If we crash between here
	// and the first client query, the next recovery rebuilds CLOG from the
	// WAL — so this flush isn't strictly necessary for correctness, but it
	// bounds the re-scan work on repeated crashes.
	if err := deps.CLOG.Flush(); err != nil {
		return nil, fmt.Errorf("recovery: CLOG flush: %w", err)
	}

	res.Duration = time.Since(start)
	if opts.LogProgress {
		fmt.Printf("[recovery] complete in %v (next txid %d)\n",
			res.Duration, res.NextTxID)
	}

	return res, nil
}
