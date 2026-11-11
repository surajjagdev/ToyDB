package recovery

import (
	"fmt"
	"io"
	"sort"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

// Scans from WAL forward from startLSN and builds transaction table and dirty page table.
// It is first pass of recovery
// Without checkpoint caller should call 0, nil, nil
// With checkpoints, pass in last checkpoint's redoLSN, ActiveTxIds, DirtyPages
func Analyze(
	w *wal.WAL,
	startLSN wal.LSN,
	initialTxTable map[common.TransactionID]*TxEntry,
	initialDirtyPages []DirtyPageEntry,
) (*AnalysisResult, error) {
	if w == nil {
		return nil, ErrWalNil
	}

	res := &AnalysisResult{
		TxTable:    make(map[common.TransactionID]*TxEntry),
		DirtyPages: NewDirtyPageTable(),
		RedoStart:  startLSN,
	}

	// make copy
	for txid, entry := range initialTxTable {
		cp := *entry
		res.TxTable[txid] = &cp
	}

	for _, e := range initialDirtyPages {
		res.DirtyPages.Add(e.Row, e.RecLSN)
	}

	it, err := w.IterateFrom(startLSN)

	if err != nil {
		return nil, fmt.Errorf("recovery: open iterator at %v: %w", startLSN, err)
	}

	defer it.Close()

	for {
		rec, err := it.Next()
		if err == io.EOF {
			break
		}
		if err == wal.ErrShortRead || err == wal.ErrCorrupt {
			// Torn tail: normal after a crash. Treat as end-of-log.
			break
		}
		if err != nil {
			return nil, fmt.Errorf("recovery: read record: %w", err)
		}

		if rec.LSN > res.MaxLSN {
			res.MaxLSN = rec.LSN
		}
		if rec.TxID > res.MaxTxID {
			res.MaxTxID = rec.TxID
		}

		if err := applyTxRecord(res, &rec); err != nil {
			return nil, err
		}
		applyDirtyPageRecord(res, &rec)
	}

	// Populate the loser list.
	for txid, entry := range res.TxTable {
		if entry.Status == TxActive {
			res.Losers = append(res.Losers, txid)
		}
	}

	sort.Slice(res.Losers, func(i, j int) bool {
		return res.Losers[i] < res.Losers[j]
	})

	// Narrow RedoStart if the DPT says redo must begin earlier.
	if minRec := res.DirtyPages.MinRecLSN(); minRec != 0 {
		if res.RedoStart == 0 || minRec < res.RedoStart {
			res.RedoStart = minRec
		}
	}

	return res, nil
}

// applyTxRecord folds one WAL record into the transaction table.
func applyTxRecord(res *AnalysisResult, rec *wal.Record) error {
	// System records (allocation FPWs, checkpoints) use TxID 0.
	if rec.TxID == 0 {
		return nil
	}

	entry, ok := res.TxTable[rec.TxID]
	if !ok {
		entry = &TxEntry{
			TxId:     rec.TxID,
			Status:   TxActive,
			FirstLSN: rec.LSN,
			LastLSN:  rec.LSN,
		}
		res.TxTable[rec.TxID] = entry
	}
	entry.LastLSN = rec.LSN

	switch rec.Type {
	case wal.RecCommit:
		entry.Status = TxCommited
		entry.CommitLSN = rec.LSN
	case wal.RecAbort:
		entry.Status = TxAborted
		entry.AbortLSN = rec.LSN
	case wal.RecClear:
		// CLR advances the undo chain; status unchanged.
	}
	return nil
}

// applyDirtyPageRecord records that a page may need redo.
func applyDirtyPageRecord(res *AnalysisResult, rec *wal.Record) {
	switch rec.Type {
	case wal.RecInsert:
		row, _, err := wal.DecodeInsert(rec.Data)
		if err != nil {
			return
		}
		res.DirtyPages.Add(row, rec.LSN)

	case wal.RecDelete:
		row, _, err := wal.DecodeDelete(rec.Data)
		if err != nil {
			return
		}
		res.DirtyPages.Add(row, rec.LSN)

	case wal.RecFPW:
		rel, _, blockID, _, err := wal.DecodeFPW(rec.Data)
		if err != nil {
			return
		}
		res.DirtyPages.Add(common.RowId{
			RelationID: rel,
			BlockID:    blockID,
			Slot:       0,
		}, rec.LSN)
	}
}
