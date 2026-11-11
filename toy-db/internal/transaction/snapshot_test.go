package transaction

import (
	"context"
	"testing"

	"github.com/surajjagdev/ToyDB/internal/common"
)

// A snapshot with XMin=100, XMax=200 has:
//   xid 50  -> committed before the snapshot
//   xid 150 -> in-progress-at-snapshot if present in InProgress, else committed
//   xid 250 -> began after the snapshot

// -----------------------------------------------------------------------------
// Rule 1: creator visibility
// -----------------------------------------------------------------------------

func TestIsVisible_CreatorCommittedBefore(t *testing.T) {
	tm := testManager(t)
	s := fakeSnapshot(100, 200)

	if !s.IsVisible(50, 0, 0, 999, 0, SnapshotIsolation, tm) {
		t.Error("creator committed before snapshot: should be visible")
	}
}

func TestIsVisible_CreatorInProgressAtSnapshot(t *testing.T) {
	tm := testManager(t)
	s := fakeSnapshot(100, 200, 150)

	if s.IsVisible(150, 0, 0, 999, 0, SnapshotIsolation, tm) {
		t.Error("creator in-progress at snapshot: should be invisible")
	}
}

func TestIsVisible_CreatorAfterSnapshot(t *testing.T) {
	tm := testManager(t)
	s := fakeSnapshot(100, 200)

	if s.IsVisible(250, 0, 0, 999, 0, SnapshotIsolation, tm) {
		t.Error("creator after snapshot: should be invisible")
	}
}

func TestIsVisible_CreatorIsMe(t *testing.T) {
	tm := testManager(t)
	s := fakeSnapshot(100, 200)
	const me = common.TransactionID(500)
	const currentCID = common.CommandID(5)

	// Earlier command: visible.
	if !s.IsVisible(me, 0, 3, me, currentCID, SnapshotIsolation, tm) {
		t.Error("my own write from earlier command: should be visible")
	}

	// Same command: Halloween problem, invisible.
	if s.IsVisible(me, 0, 5, me, currentCID, SnapshotIsolation, tm) {
		t.Error("my own write in current command: should be invisible (Halloween)")
	}

	// Future command: shouldn't happen, guard catches it.
	if s.IsVisible(me, 0, 7, me, currentCID, SnapshotIsolation, tm) {
		t.Error("my own write from future command: should be invisible")
	}
}

// -----------------------------------------------------------------------------
// Rule 2: deleter visibility — no delete
// -----------------------------------------------------------------------------

func TestIsVisible_NotDeleted(t *testing.T) {
	tm := testManager(t)
	s := fakeSnapshot(100, 200)

	if !s.IsVisible(50, 0, 0, 999, 0, SnapshotIsolation, tm) {
		t.Error("live tuple: should be visible")
	}
}

// -----------------------------------------------------------------------------
// Rule 2: deleter visibility — committed before snapshot
// -----------------------------------------------------------------------------

func TestIsVisible_DeleterCommittedBefore(t *testing.T) {
	tm := testManager(t)
	s := fakeSnapshot(100, 200)

	if s.IsVisible(50, 75, 0, 999, 0, SnapshotIsolation, tm) {
		t.Error("deleter committed before snapshot: should be invisible")
	}
}

// -----------------------------------------------------------------------------
// Rule 2: deleter visibility — in-progress at snapshot
// -----------------------------------------------------------------------------

func TestIsVisible_DeleterInProgressAtSnapshot(t *testing.T) {
	tm := testManager(t)
	s := fakeSnapshot(100, 200, 150)

	if !s.IsVisible(50, 150, 0, 999, 0, SnapshotIsolation, tm) {
		t.Error("deleter in-progress at snapshot: should be visible")
	}
}

// -----------------------------------------------------------------------------
// Rule 2: deleter visibility — began after snapshot
// -----------------------------------------------------------------------------

func TestIsVisible_DeleterAfterSnapshot(t *testing.T) {
	tm := testManager(t)
	s := fakeSnapshot(100, 200)

	if !s.IsVisible(50, 250, 0, 999, 0, SnapshotIsolation, tm) {
		t.Error("deleter after snapshot: should be visible")
	}
}

// -----------------------------------------------------------------------------
// Rule 2: deleter is me
// -----------------------------------------------------------------------------

func TestIsVisible_DeleterIsMe(t *testing.T) {
	tm := testManager(t)
	s := fakeSnapshot(100, 200)
	const me = common.TransactionID(500)
	const currentCID = common.CommandID(5)

	// Deleted by me, earlier command: invisible.
	if s.IsVisible(50, me, 3, me, currentCID, SnapshotIsolation, tm) {
		t.Error("my own delete from earlier command: should be invisible")
	}

	// Deleted by me, this command: visible (I can see what I'm modifying).
	if !s.IsVisible(50, me, 5, me, currentCID, SnapshotIsolation, tm) {
		t.Error("my own delete in current command: should be visible")
	}
}

// -----------------------------------------------------------------------------
// SnapshotIsolation vs ReadCommitted divergence
// -----------------------------------------------------------------------------

// TestIsVisible_ReadCommittedSeesRecentCommit verifies that RC sees a tuple
// whose creator committed after the snapshot was taken, while SI does not.
//
// We construct the fixture by deriving the snapshot bounds from the actual
// creatorXID so the snapshot is well-formed regardless of how many xids
// have been allocated before this test.
func TestIsVisible_ReadCommittedSeesRecentCommit(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	creator, err := tm.Begin(ctx, BeginOptions{})
	if err != nil {
		t.Fatal(err)
	}
	creatorXID := creator.Id
	if err := tm.Commit(creator); err != nil {
		t.Fatal(err)
	}

	// Snapshot taken *before* creator committed: XMin is well below
	// creatorXID, XMax is above it, and creatorXID appears in InProgress
	// (which is what makes it "in progress at snapshot time").
	snap := fakeSnapshot(creatorXID-1, creatorXID+1, creatorXID)

	// SI: creator was in-progress at snapshot -> invisible.
	if snap.IsVisible(creatorXID, 0, 0, 999, 0, SnapshotIsolation, tm) {
		t.Error("SI: creator in-progress at snapshot should be invisible")
	}

	// RC: creator committed in real time -> visible.
	if !snap.IsVisible(creatorXID, 0, 0, 999, 0, ReadCommitted, tm) {
		t.Error("RC: creator committed in real time should be visible")
	}
}

// TestIsVisible_ReadCommittedSeesRecentDelete is the inverse.
//
// The fixture must be valid: XMin <= deleterXID < XMax. Since deleterXID
// depends on how many xids have already been allocated, we derive the
// snapshot bounds from the actual value.
func TestIsVisible_ReadCommittedSeesRecentDelete(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	deleter, err := tm.Begin(ctx, BeginOptions{})
	if err != nil {
		t.Fatal(err)
	}
	deleterXID := deleter.Id
	if err := tm.Commit(deleter); err != nil {
		t.Fatal(err)
	}

	// Snapshot taken *before* deleter committed. XMin is a couple below
	// deleterXID so that the tuple's creator (deleterXID-1, a hypothetical
	// committed xid) is above XMin; XMax is above deleterXID.
	// deleterXID appears in InProgress, making it "in progress at
	// snapshot time."
	minBound := deleterXID - 1
	snap := fakeSnapshot(minBound, deleterXID+1, deleterXID)

	creatorXID := deleterXID - 1
	if creatorXID < 1 {
		creatorXID = 1 // defensive; xids start at 3 so this shouldn't fire
	}

	// SI: delete was in-progress at snapshot -> tuple still visible.
	if !snap.IsVisible(creatorXID, deleterXID, 0, 999, 0, SnapshotIsolation, tm) {
		t.Error("SI: delete in-progress at snapshot should not hide tuple")
	}

	// RC: delete committed in real time -> tuple invisible.
	if snap.IsVisible(creatorXID, deleterXID, 0, 999, 0, ReadCommitted, tm) {
		t.Error("RC: delete committed in real time should hide tuple")
	}
}

// -----------------------------------------------------------------------------
// Committed vs aborted creator — CLOG consultation
// -----------------------------------------------------------------------------

// TestIsVisible_AbortedCreatorHidesTuple verifies that a tuple whose creator
// aborted is invisible, even though the xid is below XMin and the pure-
// snapshot predicate would call it "committed."
//
// This test can only pass if IsVisible consults tm.GetTxnState for the
// creator, not just the frozen snapshot state.
func TestIsVisible_AbortedCreatorHidesTuple(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	creator, _ := tm.Begin(ctx, BeginOptions{})
	creatorXID := creator.Id
	if err := tm.Abort(creator, nil); err != nil {
		t.Fatal(err)
	}

	// Snapshot where creatorXID is below XMin. To the pure-snapshot
	// predicate this looks committed.
	snap := fakeSnapshot(creatorXID+10, creatorXID+20)

	if snap.IsVisible(creatorXID, 0, 0, 999, 0, SnapshotIsolation, tm) {
		t.Error("aborted creator should produce invisible tuple even at SI")
	}
}

// TestIsVisible_AbortedDeleterShowsTuple is the inverse: a delete by an
// aborted transaction must not hide the tuple.
func TestIsVisible_AbortedDeleterShowsTuple(t *testing.T) {
	tm := testManager(t)
	ctx := context.Background()

	// Real committed creator. No arithmetic; the xid comes from Begin.
	creator, _ := tm.Begin(ctx, BeginOptions{})
	creatorXID := creator.Id
	if err := tm.Commit(creator); err != nil {
		t.Fatal(err)
	}

	// Real aborted deleter.
	deleter, _ := tm.Begin(ctx, BeginOptions{})
	deleterXID := deleter.Id
	if err := tm.Abort(deleter, nil); err != nil {
		t.Fatal(err)
	}

	// Snapshot from much later. Both xids are below XMin, so the pure-
	// snapshot predicate would call both "committed." Only CLOG distinguishes
	// the aborted deleter.
	snap := fakeSnapshot(deleterXID+10, deleterXID+20)

	t.Logf("creatorXID=%d state=%v, deleterXID=%d state=%v",
		creatorXID, tm.GetTxnState(creatorXID),
		deleterXID, tm.GetTxnState(deleterXID))

	got := snap.IsVisible(creatorXID, deleterXID, 0, 999, 0, SnapshotIsolation, tm)
	if !got {
		t.Errorf("aborted deleter should not hide tuple: IsVisible = %v", got)
	}
}

// -----------------------------------------------------------------------------
// committedInSnapshot / InProgressContains
// -----------------------------------------------------------------------------

func TestCommittedInSnapshot(t *testing.T) {
	s := fakeSnapshot(100, 200, 150, 175)

	cases := []struct {
		xid  common.TransactionID
		want bool
		desc string
	}{
		{50, true, "below XMin"},
		{99, true, "just below XMin"},
		{100, true, "== XMin, not in InProgress => committed"},
		{150, false, "in InProgress"},
		{175, false, "in InProgress"},
		{199, true, "below XMax, not in progress => committed"},
		{200, false, "== XMax (not yet visible)"},
		{250, false, "above XMax"},
	}
	for _, c := range cases {
		if got := s.committedInSnapshot(c.xid); got != c.want {
			t.Errorf("committedInSnapshot(%d) [%s] = %v, want %v",
				c.xid, c.desc, got, c.want)
		}
	}
}

func TestInProgressContains(t *testing.T) {
	s := fakeSnapshot(0, 0, 3, 5, 7, 9)

	for _, xid := range []common.TransactionID{3, 5, 7, 9} {
		if !s.InProgressContains(xid) {
			t.Errorf("InProgressContains(%d) = false, want true", xid)
		}
	}
	for _, xid := range []common.TransactionID{1, 2, 4, 6, 8, 10, 100} {
		if s.InProgressContains(xid) {
			t.Errorf("InProgressContains(%d) = true, want false", xid)
		}
	}
}

func TestInProgressContainsEmpty(t *testing.T) {
	s := fakeSnapshot(100, 200)
	if s.InProgressContains(150) {
		t.Error("empty InProgress should never contain anything")
	}
}
