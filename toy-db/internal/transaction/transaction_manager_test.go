package transaction

import (
	"context"
	"testing"
	"time"

	"github.com/surajjagdev/ToyDB/internal/common"
)

// -----------------------------------------------------------------------------
// Basic lifecycle
// -----------------------------------------------------------------------------

func TestBeginAllocatesIncreasingXIDs(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx1, err := tm.Begin(ctx, BeginOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := tm.Begin(ctx, BeginOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tx3, err := tm.Begin(ctx, BeginOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if tx2.Id <= tx1.Id || tx3.Id <= tx2.Id {
		t.Errorf("xids not increasing: %d, %d, %d", tx1.Id, tx2.Id, tx3.Id)
	}

	_ = tm.Commit(tx1)
	_ = tm.Commit(tx2)
	_ = tm.Commit(tx3)
}

func TestBeginStartsActive(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, err := tm.Begin(ctx, BeginOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer tm.Abort(tx, nil)

	if tx.State != Active {
		t.Errorf("State = %v, want Active", tx.State)
	}
	if !tm.IsInProgress(tx.Id) {
		t.Error("IsInProgress = false after Begin")
	}
	if tm.ActiveCount() != 1 {
		t.Errorf("ActiveCount = %d, want 1", tm.ActiveCount())
	}
}

func TestCommitTransitionsState(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	id := tx.Id

	if err := tm.Commit(tx); err != nil {
		t.Fatal(err)
	}

	if tx.State != Committed {
		t.Errorf("State = %v, want Committed", tx.State)
	}
	if tm.IsInProgress(id) {
		t.Error("IsInProgress = true after commit")
	}
	if tm.ActiveCount() != 0 {
		t.Errorf("ActiveCount = %d, want 0", tm.ActiveCount())
	}

	ok, err := tm.IsCommitted(id)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("IsCommitted = false after commit")
	}
}

func TestAbortTransitionsState(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	id := tx.Id

	if err := tm.Abort(tx, nil); err != nil {
		t.Fatal(err)
	}

	if tx.State != Aborted {
		t.Errorf("State = %v, want Aborted", tx.State)
	}
	if tm.IsInProgress(id) {
		t.Error("IsInProgress = true after abort")
	}

	ok, err := tm.IsCommitted(id)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("IsCommitted = true after abort")
	}
}

func TestCommitAfterCommitFails(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	if err := tm.Commit(tx); err != nil {
		t.Fatal(err)
	}
	if err := tm.Commit(tx); err != ErrTxNotActive {
		t.Errorf("second commit: got %v, want ErrTxNotActive", err)
	}
}

func TestAbortAfterCommitFails(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	if err := tm.Commit(tx); err != nil {
		t.Fatal(err)
	}
	if err := tm.Abort(tx, nil); err != ErrTxNotActive {
		t.Errorf("abort after commit: got %v, want ErrTxNotActive", err)
	}
}

// -----------------------------------------------------------------------------
// Snapshot construction
// -----------------------------------------------------------------------------

func TestSnapshotNoConcurrent(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	defer tm.Commit(tx)

	// No other active transactions: XMin = own id, XMax = next id, empty
	// InProgress.
	if tx.Snapshot.XMin != tx.Id {
		t.Errorf("XMin = %d, want %d (own id)", tx.Snapshot.XMin, tx.Id)
	}
	if tx.Snapshot.XMax != tx.Id+1 {
		t.Errorf("XMax = %d, want %d (own id + 1)", tx.Snapshot.XMax, tx.Id+1)
	}
	if len(tx.Snapshot.InProgress) != 0 {
		t.Errorf("InProgress = %v, want empty", tx.Snapshot.InProgress)
	}
}

func TestSnapshotCapturesActiveSet(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx1, _ := tm.Begin(ctx, BeginOptions{})
	tx2, _ := tm.Begin(ctx, BeginOptions{})
	tx3, _ := tm.Begin(ctx, BeginOptions{})

	// tx3's snapshot should include tx1 and tx2 as in-progress.
	if !tx3.Snapshot.InProgressContains(tx1.Id) {
		t.Error("tx3 snapshot missing tx1 in InProgress")
	}
	if !tx3.Snapshot.InProgressContains(tx2.Id) {
		t.Error("tx3 snapshot missing tx2 in InProgress")
	}
	if tx3.Snapshot.InProgressContains(tx3.Id) {
		t.Error("tx3 snapshot incorrectly includes itself")
	}

	_ = tm.Commit(tx1)
	_ = tm.Commit(tx2)
	_ = tm.Commit(tx3)
}

func TestSnapshotXMinTracksOldestActive(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx1, _ := tm.Begin(ctx, BeginOptions{})
	tx2, _ := tm.Begin(ctx, BeginOptions{})

	// tx2's snapshot XMin should be tx1.Id (oldest active at its Begin).
	if tx2.Snapshot.XMin != tx1.Id {
		t.Errorf("tx2.XMin = %d, want %d (tx1.Id)", tx2.Snapshot.XMin, tx1.Id)
	}

	_ = tm.Commit(tx1)
	_ = tm.Commit(tx2)
}

func TestSnapshotFrozenAfterBegin(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx1, _ := tm.Begin(ctx, BeginOptions{})
	tx2, _ := tm.Begin(ctx, BeginOptions{})

	// Snapshot at tx2's Begin captured tx1 in InProgress.
	before := len(tx2.Snapshot.InProgress)

	// Commit tx1 — tx2's snapshot must be unchanged.
	if err := tm.Commit(tx1); err != nil {
		t.Fatal(err)
	}

	if len(tx2.Snapshot.InProgress) != before {
		t.Errorf("snapshot mutated after other tx committed: %d -> %d",
			before, len(tx2.Snapshot.InProgress))
	}
	if !tx2.Snapshot.InProgressContains(tx1.Id) {
		t.Error("snapshot no longer contains tx1 after tx1 committed")
	}

	_ = tm.Commit(tx2)
}

// -----------------------------------------------------------------------------
// OldestActiveSnapshot
// -----------------------------------------------------------------------------

func TestOldestActiveSnapshotEmpty(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	// No active txns: boundary is nextTxID.
	want := tm.NextTxID()
	if got := tm.OldestActiveSnapshot(); got != want {
		t.Errorf("empty: got %d, want %d", got, want)
	}

	// Begin and commit, should remain nextTxID.
	tx, _ := tm.Begin(ctx, BeginOptions{})
	_ = tm.Commit(tx)
	want = tm.NextTxID()
	if got := tm.OldestActiveSnapshot(); got != want {
		t.Errorf("after commit: got %d, want %d", got, want)
	}
}

func TestOldestActiveSnapshotSingle(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	defer tm.Commit(tx)

	if got := tm.OldestActiveSnapshot(); got != tx.Snapshot.XMin {
		t.Errorf("single: got %d, want %d", got, tx.Snapshot.XMin)
	}
}

func TestOldestActiveSnapshotMultiple(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	// tx1 begins with nothing active; its snapshot XMin = tx1.Id.
	tx1, _ := tm.Begin(ctx, BeginOptions{})
	// tx2 begins with tx1 active; its snapshot XMin = min(tx1.Id, tx1 in-progress).
	// Actually snapshot.XMin = tx1.Id (oldest active at tx2's Begin).
	tx2, _ := tm.Begin(ctx, BeginOptions{})
	// tx3 begins with tx1+tx2 active; snapshot.XMin = tx1.Id.
	tx3, _ := tm.Begin(ctx, BeginOptions{})

	// Oldest active snapshot boundary = min of XMins = tx1.Id.
	if got := tm.OldestActiveSnapshot(); got != tx1.Id {
		t.Errorf("all active: got %d, want %d", got, tx1.Id)
	}

	// Commit tx1. tx2's and tx3's snapshots still have XMin = tx1.Id
	// (their snapshots were frozen before tx1 committed). Boundary stays.
	if err := tm.Commit(tx1); err != nil {
		t.Fatal(err)
	}
	if got := tm.OldestActiveSnapshot(); got != tx1.Id {
		t.Errorf("after tx1 commit: got %d, want %d", got, tx1.Id)
	}

	// Commit tx2. tx3's snapshot still has XMin = tx1.Id.
	if err := tm.Commit(tx2); err != nil {
		t.Fatal(err)
	}
	if got := tm.OldestActiveSnapshot(); got != tx1.Id {
		t.Errorf("after tx2 commit: got %d, want %d", got, tx1.Id)
	}

	// Commit tx3. All done; boundary reverts to nextTxID.
	if err := tm.Commit(tx3); err != nil {
		t.Fatal(err)
	}
	want := tm.NextTxID()
	if got := tm.OldestActiveSnapshot(); got != want {
		t.Errorf("after all commit: got %d, want %d", got, want)
	}
}

// TestOldestActiveSnapshotStaleLongRunTx is the key correctness case: a
// long-running transaction holds a snapshot whose XMin is low, even after
// all the transactions that were active at its Begin have since committed.
// The boundary must not advance past that snapshot's XMin.
func TestOldestActiveSnapshotStaleLongRunTx(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	// Long-running transaction L begins first.
	long, _ := tm.Begin(ctx, BeginOptions{})

	// Several short-lived transactions begin and commit while L is active.
	for i := 0; i < 5; i++ {
		tx, _ := tm.Begin(ctx, BeginOptions{})
		if err := tm.Commit(tx); err != nil {
			t.Fatal(err)
		}
	}

	// Now L is the only active transaction, but its snapshot.XMin equals
	// its own Id (nothing was active when L began). Boundary = L.Snapshot.XMin.
	wantBoundary := long.Snapshot.XMin
	if got := tm.OldestActiveSnapshot(); got != wantBoundary {
		t.Errorf("got %d, want %d (long tx's snapshot XMin)", got, wantBoundary)
	}

	_ = tm.Commit(long)
}

// TestOldestActiveSnapshotWithInProgressFromStart covers the scenario where
// a long-running reader began while another transaction was active. That
// other transaction has since committed. The reader's snapshot still has the
// old XMin.
func TestOldestActiveSnapshotWithInProgressFromStart(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	txA, _ := tm.Begin(ctx, BeginOptions{})
	txID_A := txA.Id

	// Long reader begins while txA is active. Its snapshot XMin = txA.Id.
	reader, _ := tm.Begin(ctx, BeginOptions{})

	if reader.Snapshot.XMin != txID_A {
		t.Fatalf("reader.Snapshot.XMin = %d, want %d (txA.Id)",
			reader.Snapshot.XMin, txID_A)
	}

	// txA commits. Reader is now the only active txn, but its snapshot
	// still claims XMin = txA.Id.
	_ = tm.Commit(txA)

	if got := tm.OldestActiveSnapshot(); got != txID_A {
		t.Errorf("got %d, want %d (reader's frozen XMin)", got, txID_A)
	}

	_ = tm.Commit(reader)
}

// -----------------------------------------------------------------------------
// IsCommitted / GetTxnState
// -----------------------------------------------------------------------------

func TestIsCommittedSpecialXIDs(t *testing.T) {
	tm := testManager(t)

	// InvalidXID=0, BootstrapXID=1, FrozenXID=2 — all "committed" for
	// visibility purposes.
	for _, xid := range []uint32{0, 1, 2} {
		ok, err := tm.IsCommitted(common.TransactionID(xid))
		if err != nil {
			t.Fatalf("IsCommitted(%d): %v", xid, err)
		}
		if !ok {
			t.Errorf("IsCommitted(%d) = false, want true", xid)
		}
	}
}

func TestIsCommittedActive(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	defer tm.Commit(tx)

	ok, err := tm.IsCommitted(tx.Id)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("active tx should not be committed")
	}
}

func TestIsCommittedCommitted(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	id := tx.Id
	_ = tm.Commit(tx)

	ok, err := tm.IsCommitted(id)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("committed tx should report committed")
	}
}

func TestIsCommittedAborted(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	id := tx.Id
	_ = tm.Abort(tx, nil)

	ok, err := tm.IsCommitted(id)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("aborted tx should not report committed")
	}
}

func TestIsCommittedNilCLOGFailsOpen(t *testing.T) {
	// Without a CLOG, IsCommitted falls back to "assume committed for
	// finished transactions."
	tm := testManagerNoCLOG(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	id := tx.Id
	_ = tm.Commit(tx)

	ok, err := tm.IsCommitted(id)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("nil CLOG: finished tx should report committed")
	}
}

func TestGetTxnState(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	active, _ := tm.Begin(ctx, BeginOptions{})
	activeID := active.Id

	committed, _ := tm.Begin(ctx, BeginOptions{})
	committedID := committed.Id
	_ = tm.Commit(committed)

	aborted, _ := tm.Begin(ctx, BeginOptions{})
	abortedID := aborted.Id
	_ = tm.Abort(aborted, nil)

	if got := tm.GetTxnState(activeID); got != Active {
		t.Errorf("active: got %v, want Active", got)
	}
	if got := tm.GetTxnState(committedID); got != Committed {
		t.Errorf("committed: got %v, want Committed", got)
	}
	if got := tm.GetTxnState(abortedID); got != Aborted {
		t.Errorf("aborted: got %v, want Aborted", got)
	}

	_ = tm.Commit(active)
}

// -----------------------------------------------------------------------------
// ActiveTxIDs / NextTxID
// -----------------------------------------------------------------------------

func TestActiveTxIDs(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx1, _ := tm.Begin(ctx, BeginOptions{})
	tx2, _ := tm.Begin(ctx, BeginOptions{})
	tx3, _ := tm.Begin(ctx, BeginOptions{})

	ids := tm.ActiveTxIDs()
	if len(ids) != 3 {
		t.Fatalf("ActiveTxIDs len = %d, want 3", len(ids))
	}
	// Sorted ascending.
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Errorf("ids not sorted: %v", ids)
			break
		}
	}

	_ = tm.Commit(tx1)
	ids = tm.ActiveTxIDs()
	if len(ids) != 2 {
		t.Errorf("after tx1 commit: len = %d, want 2", len(ids))
	}

	_ = tm.Commit(tx2)
	_ = tm.Commit(tx3)
	if len(tm.ActiveTxIDs()) != 0 {
		t.Error("ActiveTxIDs not empty after all commits")
	}
}

func TestNextTxIDAdvances(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	first := tm.NextTxID()

	tx1, _ := tm.Begin(ctx, BeginOptions{})
	second := tm.NextTxID()
	if second <= first {
		t.Errorf("NextTxID did not advance: %d -> %d", first, second)
	}
	_ = tm.Commit(tx1)
}

// -----------------------------------------------------------------------------
// WaitForTxn
// -----------------------------------------------------------------------------

func TestWaitForTxnAlreadyFinished(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	id := tx.Id
	_ = tm.Commit(tx)

	// Should return immediately.
	if err := tm.WaitForTxn(ctx, id); err != nil {
		t.Errorf("WaitForTxn on finished tx: %v", err)
	}
}

func TestWaitForTxnBlocksUntilCommit(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	id := tx.Id

	done := make(chan error, 1)
	go func() {
		done <- tm.WaitForTxn(context.Background(), id)
	}()

	// Should not return yet.
	select {
	case err := <-done:
		t.Fatalf("WaitForTxn returned before commit: %v", err)
	default:
	}

	_ = tm.Commit(tx)

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("WaitForTxn returned error: %v", err)
		}
	case <-timeAfter(500 * time.Millisecond):
		t.Fatal("WaitForTxn did not wake after commit")
	}
}

func TestWaitForTxnBlocksUntilAbort(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	id := tx.Id

	done := make(chan error, 1)
	go func() {
		done <- tm.WaitForTxn(context.Background(), id)
	}()

	_ = tm.Abort(tx, nil)

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("WaitForTxn returned error: %v", err)
		}
	case <-timeAfter(500 * time.Millisecond):
		t.Fatal("WaitForTxn did not wake after abort")
	}
}

func TestWaitForTxnContextCancel(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	defer tm.Commit(tx)

	cancelCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- tm.WaitForTxn(cancelCtx, tx.Id)
	}()

	// Give the goroutine a moment to enter the wait.
	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("got %v, want context.Canceled", err)
		}
	case <-timeAfter(500 * time.Millisecond):
		t.Fatal("WaitForTxn did not honor context cancellation")
	}
}

// -----------------------------------------------------------------------------
// Recovery-facing methods
// -----------------------------------------------------------------------------

func TestRecordState(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	tx, _ := tm.Begin(ctx, BeginOptions{})
	id := tx.Id
	_ = tm.Commit(tx)

	// RecordState should be a no-op (state already recorded by Commit).
	// But it must not error.
	if err := tm.RecordState(id, Committed); err != nil {
		t.Errorf("RecordState(Committed): %v", err)
	}

	// Recording Aborted on a committed tx is a bug in the caller, but
	// the API doesn't validate it. Confirm the current behavior is
	// "last write wins" so the API's behavior is documented by test.
	if err := tm.RecordState(id, Aborted); err != nil {
		t.Errorf("RecordState(Aborted): %v", err)
	}

	ok, _ := tm.IsCommitted(id)
	if ok {
		t.Error("RecordState(Aborted) should have overwritten Committed")
	}
}

func TestRecordStateNilCLOG(t *testing.T) {
	tm := testManagerNoCLOG(t)
	if err := tm.RecordState(42, Committed); err == nil {
		t.Error("expected error with nil CLOG")
	}
}

func TestResetForRecovery(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	// Create some active transactions.
	tx1, _ := tm.Begin(ctx, BeginOptions{})
	tx2, _ := tm.Begin(ctx, BeginOptions{})
	_ = tx1
	_ = tx2

	if tm.ActiveCount() != 2 {
		t.Fatalf("ActiveCount = %d, want 2", tm.ActiveCount())
	}

	// Reset — should clear active set and set nextTxID.
	tm.ResetForRecovery(100)

	if tm.ActiveCount() != 0 {
		t.Errorf("ActiveCount after reset = %d, want 0", tm.ActiveCount())
	}
	if tm.NextTxID() != 100 {
		t.Errorf("NextTxID = %d, want 100", tm.NextTxID())
	}

	// New tx should get id 100.
	newTx, _ := tm.Begin(ctx, BeginOptions{})
	if newTx.Id != 100 {
		t.Errorf("new tx id = %d, want 100", newTx.Id)
	}
	_ = tm.Commit(newTx)
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

// Aliases avoid an extra import in tests that don't otherwise need common.
// (The types are identical.)
func timeAfter(d time.Duration) <-chan time.Time {
	return time.After(d)
}
