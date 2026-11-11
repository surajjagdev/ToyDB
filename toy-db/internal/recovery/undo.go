package recovery

import (
	"fmt"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

// UndoApplier applies before-images from the WAL to storage. The same
// implementation as RedoApplier, extended with ApplyBeforeImage.
//
// Implementations MUST be idempotent under repeated application — undo may
// be interrupted by a crash and restarted, in which case records that were
// already undone are marked by a CLR in the WAL and skipped on the next
// pass. In practice, undo never repeats a record because the CLR chain
// short-circuits it; idempotence is a safety net.
type UndoApplier interface {
	ApplyBeforeImage(rec *wal.Record) error
}

type UndoStats struct {
	Losers        int
	RecordsUndone int
	CLRsWritten   int
}

// Undo rolls back every loser transaction by walking its PrevLSN chain
// backwards, applying before-images and writing a CLR for each undone
// record. After the walk, it writes a RecAbort for the transaction, which
// marks it as terminally aborted for any subsequent recovery pass.
//
// The chain may already contain CLRs (from a crash mid-abort before this
// recovery). Walking PrevLSN from LastLSN naturally skips over those records
// because each CLR's PrevLSN points at the record that came BEFORE the one
// it compensates for. That is the ARIES invariant: undo is restartable.
func Undo(
	w *wal.WAL,
	result *AnalysisResult,
	applier UndoApplier,
) (UndoStats, error) {
	var stats UndoStats
	if w == nil || result == nil || applier == nil {
		return stats, fmt.Errorf("recovery: Undo requires non-nil WAL, result, and applier")
	}

	for _, txid := range result.Losers {
		entry, ok := result.TxTable[txid]
		if !ok {
			return stats, fmt.Errorf("recovery: loser %d missing from tx table", txid)
		}
		stats.Losers++

		lsn := entry.LastLSN
		for lsn != 0 {
			rec, err := w.ReadAt(lsn)
			if err != nil {
				return stats, fmt.Errorf(
					"recovery: undo read at %v for tx %d: %w", lsn, txid, err)
			}

			// Apply the before-image if the record carries one. Insert
			// records have FlagHasAfter only; undo of an insert is
			// structurally handled by the applier (remove the tuple).
			// Delete records have FlagHasBefore; undo restores the tuple.
			// Other records (Begin, FPW) have neither and are skipped by
			// the applier.
			if err := applier.ApplyBeforeImage(&rec); err != nil {
				return stats, fmt.Errorf(
					"recovery: apply before-image at %v for tx %d: %w",
					lsn, txid, err)
			}
			stats.RecordsUndone++

			// Write a CLR. Its PrevLSN is the record's PrevLSN, which
			// skips the record we just undid. A crash mid-undo resumes
			// at that pointer, so no record is undone twice.
			clrLSN, err := w.Append(&wal.Record{
				TxID:    txid,
				PrevLSN: rec.PrevLSN,
				Type:    wal.RecClear,
				Data:    encodeCLRPayload(rec.LSN),
			})
			if err != nil {
				return stats, fmt.Errorf(
					"recovery: undo write CLR for tx %d at %v: %w", txid, lsn, err)
			}
			stats.CLRsWritten++
			lsn = rec.PrevLSN
			_ = clrLSN // not otherwise used; LastLSN would advance on a live tx
		}

		// Write the RecAbort record. After this, the loser's state is
		// terminally aborted. A subsequent recovery pass that encounters
		// this record will mark the transaction TxAborted rather than
		// TxActive, and Undo will skip it.
		abortLSN, err := w.Append(&wal.Record{
			TxID:    txid,
			PrevLSN: entry.LastLSN,
			Type:    wal.RecAbort,
		})
		if err != nil {
			return stats, fmt.Errorf("recovery: undo write Abort for tx %d: %w", txid, err)
		}

		// Flush so the abort is durable. A crash after this point cannot
		// resurrect the transaction.
		if err := w.FlushUpTo(abortLSN); err != nil {
			return stats, fmt.Errorf("recovery: undo flush Abort for tx %d: %w", txid, err)
		}
	}

	return stats, nil
}

// encodeCLRPayload encodes a CLR's payload: the LSN of the record being
// undone. Also duplicated in the transaction package; keeping a local copy
// avoids the import.
func encodeCLRPayload(undoneLSN wal.LSN) []byte {
	buf := make([]byte, 8)
	common.ByteOrder.PutUint64(buf, uint64(undoneLSN))
	return buf
}
