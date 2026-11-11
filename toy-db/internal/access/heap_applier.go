package access

import (
	"fmt"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/buffer"
	"github.com/surajjagdev/ToyDB/internal/storage/page/heap"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

// HeapApplier implements recovery.RecoveryApplier for a set of TableHeaps,
// dispatching records by relation and applying the appropriate change to the
// underlying page.
type HeapApplier struct {
	heaps map[common.RelationID]*TableHeap
}

func NewHeapApplier(heaps map[common.RelationID]*TableHeap) *HeapApplier {
	return &HeapApplier{heaps: heaps}
}

// ApplyAfterImage applies a WAL record's after-image (redo).
func (a *HeapApplier) ApplyAfterImage(rec *wal.Record) error {
	switch rec.Type {
	case wal.RecInsert:
		row, tupleBytes, err := wal.DecodeInsert(rec.Data)
		if err != nil {
			return fmt.Errorf("decode insert: %w", err)
		}
		h, ok := a.heaps[row.RelationID]
		if !ok {
			return fmt.Errorf("redo: unknown relation %d in insert at %v",
				row.RelationID, rec.LSN)
		}
		return h.replayWithPage(row.BlockID, rec.LSN, func(hp *heap.HeapPage) error {
			return hp.ReplayInsert(row.Slot, tupleBytes)
		})

	case wal.RecDelete:
		row, _, err := wal.DecodeDelete(rec.Data)
		if err != nil {
			return fmt.Errorf("decode delete: %w", err)
		}
		h, ok := a.heaps[row.RelationID]
		if !ok {
			return fmt.Errorf("redo: unknown relation %d in delete at %v",
				row.RelationID, rec.LSN)
		}
		return h.replayWithPage(row.BlockID, rec.LSN, func(hp *heap.HeapPage) error {
			return hp.ReplayDelete(row.Slot, rec.TxID)
		})

	case wal.RecFPW:
		rel, fork, blockID, pageBytes, err := wal.DecodeFPW(rec.Data)
		if err != nil {
			return fmt.Errorf("decode fpw: %w", err)
		}
		h, ok := a.heaps[rel]
		if !ok {
			return fmt.Errorf("redo: unknown relation %d in fpw at %v", rel, rec.LSN)
		}
		return h.replayFPW(fork, blockID, pageBytes, rec.LSN)

	default:
		return nil
	}
}

// ApplyBeforeImage reverses a WAL record's effect (undo).
//
// Semantics per record type:
//   - RecInsert → remove the tuple at row.Slot (undo of insert is delete)
//   - RecDelete → restore the tuple from the before-image
//   - RecFPW    → no-op (system record; no txid ever owns it)
//   - others    → no-op (no page change)
func (a *HeapApplier) ApplyBeforeImage(rec *wal.Record) error {
	switch rec.Type {
	case wal.RecInsert:
		row, _, err := wal.DecodeInsert(rec.Data)
		if err != nil {
			return fmt.Errorf("decode insert for undo: %w", err)
		}
		h, ok := a.heaps[row.RelationID]
		if !ok {
			return fmt.Errorf("undo: unknown relation %d in insert at %v",
				row.RelationID, rec.LSN)
		}
		return h.replayWithPage(row.BlockID, rec.LSN, func(hp *heap.HeapPage) error {
			return hp.ReplayRemoveTuple(row.Slot)
		})

	case wal.RecDelete:
		row, beforeBytes, err := wal.DecodeDelete(rec.Data)
		if err != nil {
			return fmt.Errorf("decode delete for undo: %w", err)
		}
		h, ok := a.heaps[row.RelationID]
		if !ok {
			return fmt.Errorf("undo: unknown relation %d in delete at %v",
				row.RelationID, rec.LSN)
		}
		return h.replayWithPage(row.BlockID, rec.LSN, func(hp *heap.HeapPage) error {
			// Restore the entire pre-delete tuple bytes, including the
			// original xmax (which was 0 in the before-image).
			return hp.ReplayRestoreTuple(row.Slot, beforeBytes)
		})

	default:
		return nil
	}
}

// -----------------------------------------------------------------------------
// Per-relation redo/undo handlers on TableHeap
// -----------------------------------------------------------------------------

// replayWithPage fetches, latches, applies, and marks-dirty a page, with the
// standard pageLSN < rec.LSN idempotence check.
func (th *TableHeap) replayWithPage(
	blockID common.BlockID,
	lsn wal.LSN,
	apply func(*heap.HeapPage) error,
) error {
	tag := buffer.BufferTag{
		RelationID: th.rel,
		ForkID:     th.fork,
		BlockID:    blockID,
	}
	frame, err, _ := th.bp.GetPage(tag)
	if err != nil {
		return fmt.Errorf("replay: get page %d: %w", blockID, err)
	}
	defer frame.Unpin()

	frame.WLatch()
	defer frame.WUnlatch()

	// Idempotence check: if the page is already at or past this record,
	// skip. This is what makes redo/undo restartable after a crash.
	if frame.Page.GetLSN() >= lsn {
		return nil
	}

	hp := &heap.HeapPage{Page: frame.Page}
	if err := apply(hp); err != nil {
		return err
	}

	frame.Page.SetLSN(lsn)
	th.bp.MarkDirtyWithLSN(frame, lsn)
	return nil
}

func (th *TableHeap) replayFPW(
	fork common.ForkID,
	blockID common.BlockID,
	pageBytes []byte,
	lsn wal.LSN,
) error {
	if len(pageBytes) != int(common.PageSize) {
		return fmt.Errorf("replay fpw: page size %d != %d",
			len(pageBytes), common.PageSize)
	}
	tag := buffer.BufferTag{RelationID: th.rel, ForkID: fork, BlockID: blockID}
	frame, err, _ := th.bp.GetPage(tag)

	if err != nil {
		// The page may not exist on disk yet: it was allocated in memory
		// but never flushed before the crash. This is precisely the case
		// the FPW record exists to handle — recovery needs to install the
		// page from scratch.
		//
		// AllocateBlock (called before the crash) has already reserved the
		// block ID and created the file. Only the bytes were never written.
		// AllocatePage here creates a fresh zeroed frame for the same tag;
		// when it's later flushed via WritePage, the file will grow to
		// cover the block correctly (sparse writes extend files).
		var allocErr error
		frame, allocErr = th.bp.AllocatePage(tag)
		if allocErr != nil {
			return fmt.Errorf(
				"replay fpw: page %d: read failed (%v), allocate failed (%w)",
				blockID, err, allocErr)
		}
	}
	defer frame.Unpin()

	frame.WLatch()
	defer frame.WUnlatch()

	// Idempotence: if the page is already at or past this LSN, skip.
	// After AllocatePage, pageLSN is 0, so this passes for any nonzero lsn.
	if frame.Page.GetLSN() >= lsn {
		return nil
	}

	copy(frame.Page, pageBytes)
	frame.Page.SetLSN(lsn)
	th.bp.MarkDirtyWithLSN(frame, lsn)
	return nil
}
