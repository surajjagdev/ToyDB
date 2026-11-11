package transaction

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/lock"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

// -----------------------------------------------------------------------------
// Transaction construction — invariants that Begin must guarantee
// -----------------------------------------------------------------------------

func TestTransactionFieldsInitialized(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	defer tm.Commit(tx)

	if tx.Id == 0 {
		t.Error("Id is zero after Begin")
	}
	if tx.State != Active {
		t.Errorf("State = %v, want Active", tx.State)
	}
	if tx.manager != tm {
		t.Error("manager not set")
	}
	if tx.ctx != ctx {
		t.Error("ctx not set")
	}
	if tx.done == nil {
		t.Error("done channel not initialized")
	}
	if tx.LastLSN == 0 {
		t.Error("LastLSN not set (RecBegin not logged)")
	}
	if tx.FirstLSN != tx.LastLSN {
		t.Errorf("FirstLSN = %v, LastLSN = %v, want equal at Begin",
			tx.FirstLSN, tx.LastLSN)
	}
	if tx.CurrentCommandID() != 0 {
		t.Errorf("initial commandID = %d, want 0", tx.CurrentCommandID())
	}
}

// -----------------------------------------------------------------------------
// Log
// -----------------------------------------------------------------------------

func TestLogAssignsLSN(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	defer tm.Commit(tx)

	beginLSN := tx.LastLSN

	rec := &wal.Record{
		Type: wal.RecInsert,
		Data: []byte("test payload"),
	}
	lsn, err := tx.Log(rec)
	if err != nil {
		t.Fatalf("Log: %v", err)
	}

	if lsn == 0 {
		t.Error("Log returned zero LSN")
	}
	if lsn <= beginLSN {
		t.Errorf("Log LSN %v not greater than Begin LSN %v", lsn, beginLSN)
	}
	if tx.LastLSN != lsn {
		t.Errorf("tx.LastLSN = %v, want %v", tx.LastLSN, lsn)
	}
}

func TestLogSetsTxIDAndPrevLSN(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	defer tm.Commit(tx)

	beginLSN := tx.LastLSN

	// Caller leaves TxID and PrevLSN zero; Log overwrites them.
	rec := &wal.Record{
		Type: wal.RecInsert,
		Data: []byte("payload"),
	}
	if _, err := tx.Log(rec); err != nil {
		t.Fatal(err)
	}

	if rec.TxID != tx.Id {
		t.Errorf("rec.TxID = %d, want %d", rec.TxID, tx.Id)
	}
	if rec.PrevLSN != beginLSN {
		t.Errorf("rec.PrevLSN = %v, want %v (Begin LSN)", rec.PrevLSN, beginLSN)
	}
}

func TestLogBuildsUndoChain(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	defer tm.Commit(tx)

	// Log three records. Each PrevLSN must be the previous record's LSN.
	var lsns []wal.LSN
	for i := 0; i < 3; i++ {
		rec := &wal.Record{
			Type: wal.RecInsert,
			Data: []byte{byte(i)},
		}
		lsn, err := tx.Log(rec)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if rec.PrevLSN != tx.FirstLSN {
				t.Errorf("record 0 PrevLSN = %v, want %v (FirstLSN)",
					rec.PrevLSN, tx.FirstLSN)
			}
		} else {
			if rec.PrevLSN != lsns[i-1] {
				t.Errorf("record %d PrevLSN = %v, want %v",
					i, rec.PrevLSN, lsns[i-1])
			}
		}
		lsns = append(lsns, lsn)
	}
}

func TestLogLSNMonotonic(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	defer tm.Commit(tx)

	var prev wal.LSN
	for i := 0; i < 10; i++ {
		rec := &wal.Record{
			Type: wal.RecInsert,
			Data: []byte{byte(i)},
		}
		lsn, err := tx.Log(rec)
		if err != nil {
			t.Fatal(err)
		}
		if lsn <= prev {
			t.Errorf("LSN %v not greater than previous %v", lsn, prev)
		}
		prev = lsn
	}
}

// -----------------------------------------------------------------------------
// NextCommandID / CurrentCommandID
// -----------------------------------------------------------------------------

func TestCurrentCommandIDStartsAtZero(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	defer tm.Commit(tx)

	if got := tx.CurrentCommandID(); got != 0 {
		t.Errorf("CurrentCommandID = %d, want 0", got)
	}
}

func TestNextCommandIDIncrements(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	defer tm.Commit(tx)

	for want := common.CommandID(1); want <= 5; want++ {
		got := tx.NextCommandID()
		if got != want {
			t.Errorf("NextCommandID #%d = %d, want %d", want, got, want)
		}
		if tx.CurrentCommandID() != want {
			t.Errorf("CurrentCommandID after Next = %d, want %d",
				tx.CurrentCommandID(), want)
		}
	}
}

func TestCommandIDConcurrent(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	defer tm.Commit(tx)

	const goroutines = 8
	const perGoroutine = 100

	var wg sync.WaitGroup
	results := make(chan common.CommandID, goroutines*perGoroutine)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				results <- tx.NextCommandID()
			}
		}()
	}
	wg.Wait()
	close(results)

	// Every returned value must be unique and in [1, total].
	total := goroutines * perGoroutine
	seen := make(map[common.CommandID]bool, total)
	for cid := range results {
		if cid == 0 {
			t.Error("NextCommandID returned 0")
		}
		if cid > common.CommandID(total) {
			t.Errorf("NextCommandID returned %d > %d", cid, total)
		}
		if seen[cid] {
			t.Errorf("duplicate CommandID: %d", cid)
		}
		seen[cid] = true
	}
	if len(seen) != total {
		t.Errorf("got %d unique IDs, want %d", len(seen), total)
	}
}

// -----------------------------------------------------------------------------
// LockRow / HoldsLock (no-ops until the lock manager is wired)
// -----------------------------------------------------------------------------

func TestLockRowNoOp(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	defer tm.Commit(tx)

	row := common.RowId{RelationID: 1, BlockID: 5, Slot: 3}
	if err := tx.LockRow(row, lock.LockExclusive); err != nil {
		t.Errorf("LockRow returned error: %v", err)
	}
	if err := tx.LockRow(row, lock.LockShared); err != nil {
		t.Errorf("LockRow (shared) returned error: %v", err)
	}
	// A second exclusive lock on the same row is also a no-op.
	if err := tx.LockRow(row, lock.LockExclusive); err != nil {
		t.Errorf("LockRow (re-acquire) returned error: %v", err)
	}
}

func TestHoldsLockAlwaysFalse(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	defer tm.Commit(tx)

	row := common.RowId{RelationID: 1, BlockID: 5, Slot: 3}
	_ = tx.LockRow(row, lock.LockExclusive)

	if tx.HoldsLock(row) {
		t.Error("HoldsLock = true, want false (no-op lock manager)")
	}
	if tx.HoldsLock(common.RowId{RelationID: 99, BlockID: 0, Slot: 0}) {
		t.Error("HoldsLock on unrelated row = true, want false")
	}
}

// -----------------------------------------------------------------------------
// String
// -----------------------------------------------------------------------------

func TestStringContainsKeyFields(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	defer tm.Commit(tx)

	s := tx.String()

	// The exact format is not part of the contract, but the important
	// fields should appear so logs are useful.
	if !strings.Contains(s, "tx{") {
		t.Errorf("String() = %q, missing 'tx{'", s)
	}
	if !strings.Contains(s, "id=") {
		t.Errorf("String() = %q, missing 'id='", s)
	}
	if !strings.Contains(s, "state=") {
		t.Errorf("String() = %q, missing 'state='", s)
	}
}

func TestStringReflectsState(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})

	// Active state.
	if !strings.Contains(tx.String(), "state=1") {
		t.Errorf("before commit: String = %q, want state=1 (Active)", tx.String())
	}

	_ = tm.Commit(tx)

	// Committed state.
	if !strings.Contains(tx.String(), "state=3") {
		t.Errorf("after commit: String = %q, want state=3 (Committed)", tx.String())
	}
}

// -----------------------------------------------------------------------------
// done channel — the "wait for this tx to finish" primitive
// -----------------------------------------------------------------------------

func TestDoneChannelClosedOnCommit(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})

	// Not yet closed.
	select {
	case <-tx.done:
		t.Fatal("done channel closed before commit")
	default:
	}

	if err := tm.Commit(tx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Now closed.
	select {
	case <-tx.done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("done channel not closed after commit")
	}
}

func TestDoneChannelClosedOnAbort(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})

	_ = tm.Abort(tx, nil)

	select {
	case <-tx.done:
	case <-timeAfter(500):
		t.Fatal("done channel not closed after abort")
	}
}

// -----------------------------------------------------------------------------
// State transitions via the manager
// -----------------------------------------------------------------------------

func TestStateTransitions(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	// Begin -> Active
	tx, _ := tm.Begin(ctx, BeginOptions{})
	if tx.State != Active {
		t.Errorf("after Begin: %v, want Active", tx.State)
	}

	// Commit -> Committed
	_ = tm.Commit(tx)
	if tx.State != Committed {
		t.Errorf("after Commit: %v, want Committed", tx.State)
	}

	// Begin -> Abort -> Aborted
	tx2, _ := tm.Begin(ctx, BeginOptions{})
	if tx2.State != Active {
		t.Errorf("after Begin: %v, want Active", tx2.State)
	}
	_ = tm.Abort(tx2, nil)
	if tx2.State != Aborted {
		t.Errorf("after Abort: %v, want Aborted", tx2.State)
	}
}

// -----------------------------------------------------------------------------
// Multiple transactions — independence
// -----------------------------------------------------------------------------

func TestMultipleTransactionsIndependent(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx1, _ := tm.Begin(ctx, BeginOptions{})
	tx2, _ := tm.Begin(ctx, BeginOptions{})

	// Distinct IDs.
	if tx1.Id == tx2.Id {
		t.Errorf("tx1 and tx2 have the same ID: %d", tx1.Id)
	}

	// Command IDs are per-transaction.
	tx1.NextCommandID()
	tx1.NextCommandID()
	tx2.NextCommandID()
	if tx1.CurrentCommandID() != 2 {
		t.Errorf("tx1 CurrentCommandID = %d, want 2", tx1.CurrentCommandID())
	}
	if tx2.CurrentCommandID() != 1 {
		t.Errorf("tx2 CurrentCommandID = %d, want 1 (independent from tx1)",
			tx2.CurrentCommandID())
	}

	// Undo chains are per-transaction.
	if tx1.LastLSN == tx2.LastLSN {
		t.Error("tx1 and tx2 share LastLSN; should be independent")
	}

	// done channels are per-transaction.
	_ = tm.Commit(tx1)
	select {
	case <-tx1.done:
		// expected
	default:
		t.Fatal("tx1.done not closed after tx1 commit")
	}
	select {
	case <-tx2.done:
		t.Fatal("tx2.done closed when only tx1 committed")
	default:
	}

	_ = tm.Commit(tx2)
}

// -----------------------------------------------------------------------------
// Log LSNs across multiple transactions
// -----------------------------------------------------------------------------

func TestLogLSNsMonotonicAcrossTransactions(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx1, _ := tm.Begin(ctx, BeginOptions{})
	tx2, _ := tm.Begin(ctx, BeginOptions{})

	l1, _ := tx1.Log(&wal.Record{Type: wal.RecInsert, Data: []byte("a")})
	l2, _ := tx2.Log(&wal.Record{Type: wal.RecInsert, Data: []byte("b")})
	l3, _ := tx1.Log(&wal.Record{Type: wal.RecInsert, Data: []byte("c")})

	// The LSN counter is global (shared WAL). Records from different
	// transactions interleave in the log, but LSNs always increase.
	if !(l1 < l2 && l2 < l3) {
		t.Errorf("LSNs not globally increasing: l1=%v l2=%v l3=%v", l1, l2, l3)
	}

	// PrevLSN is per-transaction. tx1's third log has PrevLSN = l1.
	// tx2's single log has PrevLSN = tx2.FirstLSN.
	// (Implicitly verified by ordering: l1 < l2 doesn't mean
	// PrevLSN of l2 is l1, because they're different transactions.)
	//
	// The actual chain assertion: log again in tx1, verify PrevLSN = l3.
	rec := &wal.Record{Type: wal.RecInsert, Data: []byte("d")}
	if _, err := tx1.Log(rec); err != nil {
		t.Fatal(err)
	}
	if rec.PrevLSN != l3 {
		t.Errorf("tx1 second record PrevLSN = %v, want %v", rec.PrevLSN, l3)
	}

	_ = tm.Commit(tx1)
	_ = tm.Commit(tx2)
}

// -----------------------------------------------------------------------------
// Timestamp / age — sanity check for TxEntry fields
// -----------------------------------------------------------------------------

func TestFirstLSNStable(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	defer tm.Commit(tx)

	first := tx.FirstLSN
	for i := 0; i < 5; i++ {
		tx.Log(&wal.Record{Type: wal.RecInsert, Data: []byte{byte(i)}})
	}

	if tx.FirstLSN != first {
		t.Errorf("FirstLSN mutated: was %v, now %v", first, tx.FirstLSN)
	}
}
