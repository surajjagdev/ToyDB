package recovery

import (
	"fmt"
	"io"

	"github.com/surajjagdev/ToyDB/internal/wal"
)

// RedoApplier applies after-images from the WAL to storage. Implementations
// must be idempotent: check page.PageLSN() >= rec.LSN before applying, and
// skip if the page already reflects the change.
type RedoApplier interface {
	ApplyAfterImage(rec *wal.Record) error
}

type RedoStats struct {
	RecordsScanned int
	RecordsApplied int
}

// Redo replays every page-modifying WAL record from result.RedoStart to
// result.MaxLSN, calling applier.ApplyAfterImage for each.
//
// Non-page-modifying records (Begin/Commit/Abort/CLR/Checkpoint) are also
// dispatched to the applier — it's responsible for ignoring them.
func Redo(
	w *wal.WAL,
	result *AnalysisResult,
	applier RedoApplier,
) (RedoStats, error) {
	var stats RedoStats
	if w == nil || result == nil || applier == nil {
		return stats, fmt.Errorf("recovery: Redo requires non-nil WAL, result, and applier")
	}
	if result.RedoStart == 0 {
		return stats, nil
	}

	it, err := w.IterateFrom(result.RedoStart)
	if err != nil {
		return stats, fmt.Errorf("recovery: open iterator at %v: %w", result.RedoStart, err)
	}
	defer it.Close()

	for {
		rec, err := it.Next()
		if err == io.EOF {
			break
		}
		if err == wal.ErrShortRead || err == wal.ErrCorrupt {
			break
		}
		if err != nil {
			return stats, fmt.Errorf("recovery: redo read: %w", err)
		}
		if rec.LSN > result.MaxLSN {
			break
		}

		stats.RecordsScanned++
		if err := applier.ApplyAfterImage(&rec); err != nil {
			return stats, fmt.Errorf("recovery: redo record at %v (type %d): %w",
				rec.LSN, rec.Type, err)
		}
		stats.RecordsApplied++
	}
	return stats, nil
}
