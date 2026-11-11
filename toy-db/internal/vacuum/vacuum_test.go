package vacuum

import (
	"context"
	"testing"
	"time"

	"github.com/surajjagdev/ToyDB/internal/access"
	"github.com/surajjagdev/ToyDB/internal/catalog"
	"github.com/surajjagdev/ToyDB/internal/clog"
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/buffer"
	"github.com/surajjagdev/ToyDB/internal/storage/disk"
	"github.com/surajjagdev/ToyDB/internal/transaction"
	"github.com/surajjagdev/ToyDB/internal/types"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

type env struct {
	dir  string
	vfd  *disk.VFDCache
	wal  *wal.WAL
	bp   *buffer.BufferPool
	cl   *clog.Manager
	tm   *transaction.TransactionManager
	heap *access.TableHeap
	vac  *Vacuumer
}

func setup(t *testing.T) *env {
	t.Helper()
	e := &env{dir: t.TempDir()}
	e.vfd = disk.NewCachedVFD(16)
	t.Cleanup(func() { e.vfd.CloseAll(true) })

	var err error
	e.wal, err = wal.Open(e.vfd, wal.Options{
		Dir:         e.dir + "/wal",
		SegmentSize: 1 << 20,
		Fsync:       false,
		Metrics:     wal.NopMetrics{},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.wal.Close() })

	dm, _ := disk.NewDirectManager(e.dir, 100, 100)
	e.bp, _ = buffer.NewBufferPool(dm, e.wal, 2, 8)
	t.Cleanup(func() { e.bp.Shutdown() })

	e.cl, _ = clog.Open(e.vfd, e.dir+"/clog")
	t.Cleanup(func() { e.cl.Close() })

	e.tm = transaction.NewManager(e.wal, e.cl)

	rel := common.RelationID(1)
	e.heap = access.NewTableHeap(e.bp, access.NewFSM(e.bp), rel, e.tm, e.wal)
	heaps := map[common.RelationID]*access.TableHeap{rel: e.heap}

	e.vac = New(Options{Interval: time.Hour}, e.tm, heaps)
	return e
}

func intSchema(t *testing.T) *catalog.Schema {
	t.Helper()
	s, err := catalog.NewSchema([]catalog.Column{
		catalog.NewColumn("id", types.IntegerType, false),
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func insert(t *testing.T, e *env, tx *transaction.Transaction, schema *catalog.Schema, id int32) common.RecordId {
	t.Helper()
	tx.NextCommandID()
	rid, err := e.heap.InsertTuple(tx,
		access.NewTuple([]types.Value{types.NewInteger(id)}), schema)
	if err != nil {
		t.Fatal(err)
	}
	return rid
}

// -----------------------------------------------------------------------------
// Basic behavior
// -----------------------------------------------------------------------------

func TestVacuumEmpty(t *testing.T) {
	e := setup(t)
	if err := e.vac.Take(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := e.vac.LastStats().RelationsHit; got != 0 {
		t.Errorf("RelationsHit = %d, want 0 (nothing to vacuum)", got)
	}
}

func TestVacuumSkipsNonBloated(t *testing.T) {
	e := setup(t)
	schema := intSchema(t)
	ctx := context.Background()

	// Insert 10 rows. Delete 2. Ratio is 16%, below the default 20%.
	tx0, _ := e.tm.Begin(ctx, transaction.BeginOptions{})
	rids := make([]common.RecordId, 10)
	for i := 0; i < 10; i++ {
		rids[i] = insert(t, e, tx0, schema, int32(i))
	}
	e.tm.Commit(tx0)

	for i := 0; i < 2; i++ {
		tx, _ := e.tm.Begin(ctx, transaction.BeginOptions{})
		tx.NextCommandID()
		if err := e.heap.MarkTupleAsDeleted(tx, rids[i]); err != nil {
			t.Fatal(err)
		}
		e.tm.Commit(tx)
	}

	if err := e.vac.Take(ctx); err != nil {
		t.Fatal(err)
	}

	// Should have skipped — ratio below threshold.
	if got := e.vac.LastStats().RelationsHit; got != 0 {
		t.Errorf("RelationsHit = %d, want 0 (below bloat threshold)", got)
	}

	// 10 inserted versions + 2 delete-marks = 12 physical versions.
	// None were reclaimed (vacuum skipped), so the count is unchanged.
	live, dead := e.heap.TupleCounts()
	if live+dead != 12 {
		t.Errorf("live+dead = %d, want 12", live+dead)
	}
	if dead != 2 {
		t.Errorf("dead = %d, want 2", dead)
	}
	if live != 10 {
		t.Errorf("live = %d, want 10", live)
	}
}

func TestVacuumThresholdOverride(t *testing.T) {
	e := setup(t)
	e.vac.opts.DeadTupleMin = 1
	e.vac.opts.DeadRatio = 0.01

	schema := intSchema(t)
	ctx := context.Background()

	// Insert 10, delete 1. Ratio 10%, above the (overridden) 1%.
	tx0, _ := e.tm.Begin(ctx, transaction.BeginOptions{})
	rids := make([]common.RecordId, 10)
	for i := 0; i < 10; i++ {
		rids[i] = insert(t, e, tx0, schema, int32(i))
	}
	e.tm.Commit(tx0)

	tx, _ := e.tm.Begin(ctx, transaction.BeginOptions{})
	tx.NextCommandID()
	e.heap.MarkTupleAsDeleted(tx, rids[0])
	e.tm.Commit(tx)

	if err := e.vac.Take(ctx); err != nil {
		t.Fatal(err)
	}

	if got := e.vac.LastStats().RelationsHit; got != 1 {
		t.Errorf("RelationsHit = %d, want 1", got)
	}
	if got := e.vac.LastStats().TuplesRemoved; got == 0 {
		t.Error("no tuples removed despite exceeding threshold")
	}
}

// -----------------------------------------------------------------------------
// Correctness — the two tests that matter
// -----------------------------------------------------------------------------

// TestVacuumReclaimsSupersededVersions is the canonical case: many updates
// to the same row create many dead versions. Vacuum must reclaim them
// without touching the live version.
func TestVacuumReclaimsSupersededVersions(t *testing.T) {
	e := setup(t)
	e.vac.opts.DeadTupleMin = 1
	e.vac.opts.DeadRatio = 0.01

	schema := intSchema(t)
	ctx := context.Background()

	// Insert one row.
	tx0, _ := e.tm.Begin(ctx, transaction.BeginOptions{})
	rid := insert(t, e, tx0, schema, 1)
	e.tm.Commit(tx0)

	// Update it 20 times, committing each update.
	for update := 0; update < 20; update++ {
		tx, _ := e.tm.Begin(ctx, transaction.BeginOptions{})
		tx.NextCommandID()
		newRid, err := e.heap.UpdateTuple(tx, rid,
			access.NewTuple([]types.Value{types.NewInteger(int32(update + 2))}), schema)
		if err != nil {
			t.Fatalf("update %d: %v", update, err)
		}
		if err := e.tm.Commit(tx); err != nil {
			t.Fatal(err)
		}
		rid = newRid
	}

	live, dead := e.heap.TupleCounts()
	t.Logf("before vacuum: live=%d dead=%d", live, dead)
	if dead < 20 {
		t.Fatalf("dead = %d, want >= 20 (20 updates superseded 20 versions)", dead)
	}

	if err := e.vac.Take(ctx); err != nil {
		t.Fatal(err)
	}

	live2, dead2 := e.heap.TupleCounts()
	t.Logf("after vacuum: live=%d dead=%d", live2, dead2)

	if dead2 >= dead {
		t.Errorf("vacuum didn't reduce dead: %d -> %d", dead, dead2)
	}

	// The live row must still be readable.
	txr, _ := e.tm.Begin(ctx, transaction.BeginOptions{})
	defer e.tm.Commit(txr)
	tuple, err := e.heap.GetTuple(txr, rid, schema)
	if err != nil {
		t.Fatalf("live row lost after vacuum: %v", err)
	}
	if v, ok := tuple.Values[0].(types.Fixed4ByteValue); !ok || int32(v.Value) != 21 {
		t.Errorf("live row value = %v, want 21", tuple.Values[0])
	}
}

// TestVacuumRespectsOldestSnapshot is the correctness-critical case: a
// long-running reader must keep seeing old versions until it ends.
func TestVacuumRespectsOldestSnapshot(t *testing.T) {
	e := setup(t)
	e.vac.opts.DeadTupleMin = 1
	e.vac.opts.DeadRatio = 0.01

	schema := intSchema(t)
	ctx := context.Background()

	// Insert + commit a row.
	tx0, _ := e.tm.Begin(ctx, transaction.BeginOptions{})
	rid := insert(t, e, tx0, schema, 42)
	e.tm.Commit(tx0)

	// Long-running reader opens its snapshot.
	reader, _ := e.tm.Begin(ctx, transaction.BeginOptions{})

	// Delete + commit the row.
	txd, _ := e.tm.Begin(ctx, transaction.BeginOptions{})
	txd.NextCommandID()
	e.heap.MarkTupleAsDeleted(txd, rid)
	e.tm.Commit(txd)

	// Vacuum must NOT reclaim the deleted version — the reader's snapshot
	// still sees it.
	if err := e.vac.Take(ctx); err != nil {
		t.Fatal(err)
	}

	// The reader must see the row.
	tuple, err := e.heap.GetTuple(reader, rid, schema)
	if err != nil {
		t.Fatalf("reader lost visible row after vacuum: %v", err)
	}
	if v, ok := tuple.Values[0].(types.Fixed4ByteValue); !ok || int32(v.Value) != 42 {
		t.Errorf("reader got value %v, want 42", tuple.Values[0])
	}
	e.tm.Commit(reader)

	// After the reader exits, vacuum can reclaim.
	if err := e.vac.Take(ctx); err != nil {
		t.Fatal(err)
	}

	// A new transaction must not see the deleted row.
	txn, _ := e.tm.Begin(ctx, transaction.BeginOptions{})
	defer e.tm.Commit(txn)
	if _, err := e.heap.GetTuple(txn, rid, schema); err == nil {
		t.Error("deleted row still visible after vacuum reclaimed it")
	}
}

// -----------------------------------------------------------------------------
// Background loop
// -----------------------------------------------------------------------------

func TestVacuumBackgroundLoop(t *testing.T) {
	e := setup(t)
	e.vac.opts.Interval = 20 * time.Millisecond
	e.vac.opts.tickInterval = 10 * time.Millisecond
	e.vac.opts.DeadTupleMin = 1
	e.vac.opts.DeadRatio = 0.01

	schema := intSchema(t)
	ctx := context.Background()

	// Create some dead versions.
	tx0, _ := e.tm.Begin(ctx, transaction.BeginOptions{})
	rid := insert(t, e, tx0, schema, 1)
	e.tm.Commit(tx0)

	txd, _ := e.tm.Begin(ctx, transaction.BeginOptions{})
	txd.NextCommandID()
	e.heap.MarkTupleAsDeleted(txd, rid)
	e.tm.Commit(txd)

	e.vac.Start()
	defer e.vac.Stop()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e.vac.LastStats().TuplesRemoved > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("background vacuum did not reclaim within timeout")
}

func TestVacuumStopIdempotent(t *testing.T) {
	e := setup(t)
	e.vac.Start()
	e.vac.Stop()
	e.vac.Stop() // no panic, no hang
}

func TestVacuumStopWithoutStart(t *testing.T) {
	e := setup(t)
	done := make(chan struct{})
	go func() {
		e.vac.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop blocked on unstarted vacumer")
	}
}
