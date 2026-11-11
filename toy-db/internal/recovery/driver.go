package recovery

import (
	"errors"
	"fmt"
	"time"

	"github.com/surajjagdev/ToyDB/internal/clog"
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/control"
	"github.com/surajjagdev/ToyDB/internal/storage/disk"
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
	LogProgress bool

	// ControlDir, if non-empty, enables checkpoint-based recovery. The
	// driver reads the master record from this directory to find the last
	// checkpoint. If empty (or the file is missing/corrupt), recovery
	// falls back to a full scan.
	ControlDir string

	// VFD is used for control file I/O. Required if ControlDir is set.
	VFD *disk.VFDCache
}

// RecoveryResult bundles the outcome of a recovery run.
type RecoveryResult struct {
	Analysis       *AnalysisResult
	RedoStats      RedoStats
	UndoStats      UndoStats
	CLOGUpdates    int
	NextTxID       common.TransactionID
	Duration       time.Duration
	UsedCheckpoint bool
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

	// Try to locate a checkpoint via the master record.
	var (
		startLSN          wal.LSN
		initialTxTable    map[common.TransactionID]*TxEntry
		initialDirtyPages []DirtyPageEntry
	)

	if opts.ControlDir != "" && opts.VFD != nil {
		ctrl, err := control.Read(opts.VFD, opts.ControlDir)
		switch {
		case err == nil:
			cpRec, rerr := deps.WAL.ReadAt(ctrl.CheckpointLSN)
			if rerr == nil && cpRec.Type == wal.RecCheckpoint {
				decoded, derr := wal.DecodeCheckpoint(cpRec.Data)
				if derr == nil {
					res.UsedCheckpoint = true
					startLSN = decoded.RedoLSN

					initialTxTable = make(map[common.TransactionID]*TxEntry, len(decoded.ActiveTxIDs))
					for _, txid := range decoded.ActiveTxIDs {
						initialTxTable[txid] = &TxEntry{
							TxId:   txid,
							Status: TxActive,
						}
					}
					for _, dp := range decoded.DirtyPages {
						initialDirtyPages = append(initialDirtyPages, DirtyPageEntry{
							Row:    dp.Row,
							RecLSN: dp.RecLSN,
						})
					}
					if opts.LogProgress {
						fmt.Printf("[recovery] checkpoint at %v, redo from %v\n",
							ctrl.CheckpointLSN, decoded.RedoLSN)
					}
				}
			}
		case errors.Is(err, control.ErrNotFound):
			// Fresh DB. Full scan.
		case errors.Is(err, control.ErrCorrupt):
			if opts.LogProgress {
				fmt.Printf("[recovery] control corrupt, full scan: %v\n", err)
			}
		default:
			return nil, fmt.Errorf("recovery: control read: %w", err)
		}
	}

	// --- 1. Analyze ---
	analysis, err := Analyze(deps.WAL, startLSN, initialTxTable, initialDirtyPages)
	if err != nil {
		return nil, fmt.Errorf("recovery: analyze: %w", err)
	}
	res.Analysis = analysis

	// --- 2. Redo ---
	redoStats, err := Redo(deps.WAL, analysis, deps.Apply)
	if err != nil {
		return nil, fmt.Errorf("recovery: redo: %w", err)
	}
	res.RedoStats = redoStats

	// --- 3. Undo ---
	undoStats, err := Undo(deps.WAL, analysis, deps.Apply)
	if err != nil {
		return nil, fmt.Errorf("recovery: undo: %w", err)
	}
	res.UndoStats = undoStats

	// --- 4. Populate CLOG ---
	for txid, entry := range analysis.TxTable {
		var state transaction.State
		switch entry.Status {
		case TxCommited:
			state = transaction.Committed
		case TxAborted, TxActive:
			state = transaction.Aborted
		default:
			return nil, fmt.Errorf("recovery: unexpected tx state %v for tx %d", entry.Status, txid)
		}
		if err := deps.TM.RecordState(txid, state); err != nil {
			return nil, fmt.Errorf("recovery: CLOG update for tx %d: %w", txid, err)
		}
		res.CLOGUpdates++
	}

	// --- 5. Reset TM ---
	nextTxID := analysis.MaxTxID + 1
	if nextTxID < 3 {
		nextTxID = 3
	}
	deps.TM.ResetForRecovery(nextTxID)
	res.NextTxID = nextTxID

	// --- 6. Flush CLOG ---
	if err := deps.CLOG.Flush(); err != nil {
		return nil, fmt.Errorf("recovery: CLOG flush: %w", err)
	}

	res.Duration = time.Since(start)
	return res, nil
}
