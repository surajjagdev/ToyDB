package lock

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/surajjagdev/ToyDB/internal/common"
)

func testRow() common.RowId {
	return common.RowId{RelationID: 1, BlockID: 1, Slot: 1}
}

func TestExclusiveExcludesSharedAndExclusive(t *testing.T) {
	m := NewLockManager(time.Second, nil)
	row := testRow()

	if err := m.Acquire(context.Background(), 1, row, LockExclusive); err != nil {
		t.Fatal(err)
	}

	// Second tx times out waiting.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := m.Acquire(ctx, 2, row, LockExclusive)
	if err == nil {
		t.Fatal("expected conflict")
	}
	err = m.Acquire(context.Background(), 3, row, LockShared)
	if err == nil {
		t.Fatal("expected shared to conflict with exclusive")
	}
	m.ReleaseAll(1)
}

func TestSharedCoexist(t *testing.T) {
	m := NewLockManager(time.Second, nil)
	row := testRow()

	if err := m.Acquire(context.Background(), 1, row, LockShared); err != nil {
		t.Fatal(err)
	}
	if err := m.Acquire(context.Background(), 2, row, LockShared); err != nil {
		t.Fatal(err)
	}
	m.ReleaseAll(1)
	m.ReleaseAll(2)
}

func TestFIFOFairness(t *testing.T) {
	m := NewLockManager(2*time.Second, nil)
	row := testRow()

	// Tx 1 holds exclusive.
	if err := m.Acquire(context.Background(), 1, row, LockExclusive); err != nil {
		t.Fatal(err)
	}

	var order []common.TransactionID
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Tx 2, 3, 4 queue in order.
	for _, id := range []common.TransactionID{2, 3, 4} {
		wg.Add(1)
		go func(id common.TransactionID) {
			defer wg.Done()
			if err := m.Acquire(context.Background(), id, row, LockExclusive); err != nil {
				t.Errorf("tx %d: %v", id, err)
				return
			}
			mu.Lock()
			order = append(order, id)
			mu.Unlock()
			// Hold briefly so the next one has to wait.
			time.Sleep(20 * time.Millisecond)
			m.Release(id, row)
		}(id)
	}

	// Let the queue fill.
	time.Sleep(30 * time.Millisecond)
	m.Release(1, row)
	wg.Wait()

	want := []common.TransactionID{2, 3, 4}
	if len(order) != len(want) {
		t.Fatalf("got %v want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("FIFO violated: got %v want %v", order, want)
		}
	}
}

func TestUpgradeSoleHolder(t *testing.T) {
	m := NewLockManager(time.Second, nil)
	row := testRow()

	if err := m.Acquire(context.Background(), 1, row, LockShared); err != nil {
		t.Fatal(err)
	}
	// Sole holder may upgrade without waiting.
	if err := m.Acquire(context.Background(), 1, row, LockExclusive); err != nil {
		t.Fatal(err)
	}
	m.ReleaseAll(1)
}

func TestUpgradeQueuesWhenContended(t *testing.T) {
	m := NewLockManager(time.Second, nil)
	row := testRow()

	if err := m.Acquire(context.Background(), 1, row, LockShared); err != nil {
		t.Fatal(err)
	}
	if err := m.Acquire(context.Background(), 2, row, LockShared); err != nil {
		t.Fatal(err)
	}

	// Tx 1 wants to upgrade but tx 2 holds shared. Must block.
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if err := m.Acquire(ctx, 1, row, LockExclusive); err == nil {
		t.Fatal("expected upgrade to block while another shared holder exists")
	}

	m.ReleaseAll(1)
	m.ReleaseAll(2)
}

func TestReleaseAllCleansUp(t *testing.T) {
	m := NewLockManager(time.Second, nil)
	row1 := testRow()
	row2 := common.RowId{RelationID: 1, BlockID: 2, Slot: 1}

	m.Acquire(context.Background(), 1, row1, LockExclusive)
	m.Acquire(context.Background(), 1, row2, LockExclusive)

	// Second tx now blocks on row1.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := m.Acquire(ctx, 2, row1, LockShared); err == nil {
		t.Fatal("expected block before release")
	}

	m.ReleaseAll(1)

	// After release, tx 2 should succeed.
	if err := m.Acquire(context.Background(), 2, row1, LockShared); err != nil {
		t.Fatal(err)
	}
	m.ReleaseAll(2)
}

func TestHeldBy(t *testing.T) {
	m := NewLockManager(time.Second, nil)
	row := testRow()

	if m.HeldBy(1, row) {
		t.Fatal("nothing should be held yet")
	}
	m.Acquire(context.Background(), 1, row, LockExclusive)
	if !m.HeldBy(1, row) {
		t.Fatal("expected HeldBy to return true")
	}
	if m.HeldBy(2, row) {
		t.Fatal("tx 2 should not hold")
	}
	m.ReleaseAll(1)
}
