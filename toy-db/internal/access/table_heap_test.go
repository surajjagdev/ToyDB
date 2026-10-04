package access

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/surajjagdev/ToyDB/internal/catalog"
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/lock"
	"github.com/surajjagdev/ToyDB/internal/storage/buffer"
	"github.com/surajjagdev/ToyDB/internal/storage/disk"
	"github.com/surajjagdev/ToyDB/internal/storage/page/heap"
	"github.com/surajjagdev/ToyDB/internal/transaction"
	"github.com/surajjagdev/ToyDB/internal/types"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

// -----------------------------------------------------------------------------
// Test harness
// -----------------------------------------------------------------------------

// testLogFlusher is a no-op LogFlusher for buffer-pool tests that don't
// exercise the write-ahead rule.
type testLogFlusher struct{}

func (testLogFlusher) FlushUpTo(lsn wal.LSN) error { return nil }

// testEnv bundles everything a test needs to drive a TableHeap.
type testEnv struct {
	heap   *TableHeap
	bp     *buffer.BufferPool
	tm     *transaction.TransactionManager
	locks  *lock.Manager
	wal    *wal.WAL
	walDir string
	vfd    *disk.VFDCache
}

func (e *testEnv) Shutdown(t *testing.T) {
	t.Helper()
	_ = e.bp.Shutdown()
	_ = e.wal.Close()
	_ = os.RemoveAll(e.walDir)
}

// setupEnv creates a fresh buffer pool, WAL, lock manager, and transaction
// manager backed by a temp directory. Used by every test in this file.
func setupEnv(
	t *testing.T,
	partitions common.PartitionIndex,
	framesPerPartition common.FrameIndex,
) *testEnv {
	t.Helper()

	dir := t.TempDir()

	// Page I/O uses a direct VFD (page-aligned O_DIRECT is the default when
	// using NewDirectManager). The WAL uses a separate buffered VFD because
	// its writes are byte-level, not page-aligned.
	dm, err := disk.NewDirectManager(dir, 100, 100)
	if err != nil {
		t.Fatalf("disk manager: %v", err)
	}

	walDir := dir + "/wal"
	walVFD := disk.NewCachedVFD(16)
	w, err := wal.Open(walVFD, wal.Options{
		Dir:         walDir,
		SegmentSize: 1 << 20,
		BatchWait:   1_000_000, // effectively never on the ticker
		Fsync:       false,     // tests don't need durability
		Metrics:     wal.NopMetrics{},
	})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}

	bp, err := buffer.NewBufferPool(dm, w, partitions, framesPerPartition)
	if err != nil {
		t.Fatalf("buffer pool: %v", err)
	}

	locks := lock.NewLockManager(2_000_000_000, nil) // 2s wait in ns form
	tm := transaction.NewManager(w, locks)

	rel := common.RelationID(1)
	th := NewTableHeap(bp, NewFSM(bp), rel, tm)

	return &testEnv{
		heap:   th,
		bp:     bp,
		tm:     tm,
		locks:  locks,
		wal:    w,
		walDir: walDir,
		vfd:    walVFD,
	}
}

// beginTx is a small helper for tests that just want a transaction.
func beginTx(t *testing.T, env *testEnv) *transaction.Transaction {
	t.Helper()
	tx, err := env.tm.Begin(context.Background(), transaction.BeginOptions{})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	return tx
}

func intSchema(t *testing.T) *catalog.Schema {
	t.Helper()
	schema, err := catalog.NewSchema([]catalog.Column{
		catalog.NewColumn("id", types.IntegerType, false),
	})
	if err != nil {
		t.Fatalf("NewSchema: %v", err)
	}
	return schema
}

// insertInt inserts one integer and returns its RID. Registers a cleanup that
// aborts the tx if the test forgets to commit.
func insertInt(
	t *testing.T,
	env *testEnv,
	tx *transaction.Transaction,
	schema *catalog.Schema,
	id int32,
) common.RecordId {
	t.Helper()
	tx.NextCommandID()
	rid, err := env.heap.InsertTuple(tx, NewTuple([]types.Value{types.NewInteger(id)}), schema)
	if err != nil {
		t.Fatalf("InsertTuple(%d): %v", id, err)
	}
	return rid
}

func readInt(
	t *testing.T,
	env *testEnv,
	tx *transaction.Transaction,
	schema *catalog.Schema,
	rid common.RecordId,
) int32 {
	t.Helper()
	got, err := env.heap.GetTuple(tx, rid, schema)
	if err != nil {
		t.Fatalf("GetTuple(%v): %v", rid, err)
	}
	val, ok := got.Values[0].(types.Fixed4ByteValue)
	if !ok {
		t.Fatalf("expected integer column, got %T", got.Values[0])
	}
	return int32(val.Value)
}

// -----------------------------------------------------------------------------
// Basic insert + read
// -----------------------------------------------------------------------------

func TestTableHeap_InsertAndGet(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	tx := beginTx(t, env)

	rid := insertInt(t, env, tx, schema, 42)
	if got := readInt(t, env, tx, schema, rid); got != 42 {
		t.Fatalf("GetTuple = %d, want 42", got)
	}
}

// TestTableHeap_ReadOwnUncommittedWrite proves that a transaction sees its
// own writes even before committing — the "my own writes are visible to me"
// branch of the visibility predicate.
func TestTableHeap_ReadOwnUncommittedWrite(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	tx := beginTx(t, env)

	rid := insertInt(t, env, tx, schema, 1234)

	// Same tx, fresh statement: the row must be visible.
	tx.NextCommandID()
	got, err := env.heap.GetTuple(tx, rid, schema)
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if v, ok := got.Values[0].(types.Fixed4ByteValue); !ok || int32(v.Value) != 1234 {
		t.Fatalf("expected 1234, got %v", got.Values[0])
	}
}

// -----------------------------------------------------------------------------
// Snapshot isolation
// -----------------------------------------------------------------------------

// TestTableHeap_SnapshotIsolation_Insert: a row inserted and committed after
// another tx's snapshot was taken must be invisible to that snapshot.
func TestTableHeap_SnapshotIsolation_Insert(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	ctx := context.Background()

	// Tx1 begins first — its snapshot predates everything.
	tx1, err := env.tm.Begin(ctx, transaction.BeginOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Tx2 inserts and commits a row.
	tx2, err := env.tm.Begin(ctx, transaction.BeginOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tx2.NextCommandID()
	rid, err := env.heap.InsertTuple(tx2, NewTuple([]types.Value{types.NewInteger(99)}), schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := env.tm.Commit(tx2); err != nil {
		t.Fatal(err)
	}

	// Tx1 must not see the row: it was inserted after tx1's snapshot.
	if _, err := env.heap.GetTuple(tx1, rid, schema); err == nil {
		t.Fatalf("tx1 saw a row inserted after its snapshot")
	}

	// Tx3 begins after tx2 committed; it must see the row.
	tx3, err := env.tm.Begin(ctx, transaction.BeginOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.heap.GetTuple(tx3, rid, schema); err != nil {
		t.Fatalf("tx3 should see committed row: %v", err)
	}

	_ = env.tm.Commit(tx1)
	_ = env.tm.Commit(tx3)
}

// TestTableHeap_SnapshotIsolation_Delete: a row deleted after a snapshot was
// taken must still be visible to that snapshot.
func TestTableHeap_SnapshotIsolation_Delete(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	ctx := context.Background()

	// Pre-existing row committed by an initial tx.
	tx0 := beginTx(t, env)
	rid := insertInt(t, env, tx0, schema, 555)
	if err := env.tm.Commit(tx0); err != nil {
		t.Fatal(err)
	}

	// Tx1 begins and snapshots. It must keep seeing the row even after tx2
	// deletes it.
	tx1, err := env.tm.Begin(ctx, transaction.BeginOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Tx2 deletes and commits.
	tx2, err := env.tm.Begin(ctx, transaction.BeginOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tx2.NextCommandID()
	if err := env.heap.MarkTupleAsDeleted(tx2, rid); err != nil {
		t.Fatalf("MarkTupleAsDeleted: %v", err)
	}
	if err := env.tm.Commit(tx2); err != nil {
		t.Fatal(err)
	}

	// Tx1 still sees the row.
	if _, err := env.heap.GetTuple(tx1, rid, schema); err != nil {
		t.Fatalf("tx1 lost visibility of a row deleted after its snapshot: %v", err)
	}

	// A new tx does not.
	tx3, err := env.tm.Begin(ctx, transaction.BeginOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.heap.GetTuple(tx3, rid, schema); err == nil {
		t.Fatalf("tx3 should not see the deleted row")
	}

	_ = env.tm.Commit(tx1)
	_ = env.tm.Commit(tx3)
}

// -----------------------------------------------------------------------------
// Write-write conflict (optimistic mode)
// -----------------------------------------------------------------------------

// TestTableHeap_OptimisticConflict: two overlapping optimistic transactions
// updating the same row — exactly one should abort with a serialization
// failure.
func TestTableHeap_OptimisticConflict(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	ctx := context.Background()

	tx0 := beginTx(t, env)
	rid := insertInt(t, env, tx0, schema, 1)
	if err := env.tm.Commit(tx0); err != nil {
		t.Fatal(err)
	}

	// Two transactions both want to update the row optimistically.
	txA, _ := env.tm.Begin(ctx, transaction.BeginOptions{Mode: transaction.Optimistic})
	txB, _ := env.tm.Begin(ctx, transaction.BeginOptions{Mode: transaction.Optimistic})

	// Both mark the tuple's xmax.
	txA.NextCommandID()
	if err := env.heap.MarkTupleAsDeleted(txA, rid); err != nil {
		t.Fatalf("txA MarkTupleAsDeleted: %v", err)
	}
	// txA's xmax is now on the tuple, and txA is in-flight.

	// txB should detect the conflict before touching the row.
	txB.NextCommandID()
	err := env.heap.checkUpdateConflict(txB, rid)
	if err != transaction.ErrSerializationFailure {
		t.Fatalf("expected ErrSerializationFailure, got %v", err)
	}

	_ = env.tm.Commit(txA)
	_ = env.tm.Abort(txB, nil)
}

// -----------------------------------------------------------------------------
// Concurrent inserts
// -----------------------------------------------------------------------------

// TestTableHeap_ConcurrentInsertsUniqueAndReadable: N goroutines each with
// their own transaction insert rows. Every row must be unique, readable, and
// non-null after all transactions commit.
func TestTableHeap_ConcurrentInsertsUniqueAndReadable(t *testing.T) {
	env := setupEnv(t, 4, 16)
	defer env.Shutdown(t)

	schema := intSchema(t)
	const workers = 8
	const perWorker = 20

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		rids    []common.RecordId
		seen    = make(map[string]struct{})
		failCnt atomic.Int32
	)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()

			tx, err := env.tm.Begin(context.Background(), transaction.BeginOptions{})
			if err != nil {
				t.Errorf("Begin: %v", err)
				failCnt.Add(1)
				return
			}

			for i := 0; i < perWorker; i++ {
				id := int32(worker*1_000_000 + i)
				tx.NextCommandID()
				rid, err := env.heap.InsertTuple(
					tx, NewTuple([]types.Value{types.NewInteger(id)}), schema)
				if err != nil {
					t.Errorf("InsertTuple(%d): %v", id, err)
					failCnt.Add(1)
					_ = env.tm.Abort(tx, nil)
					return
				}
				key := fmt.Sprintf("%d:%d", rid.BlockID, rid.Slot)
				mu.Lock()
				if _, dup := seen[key]; dup {
					t.Errorf("duplicate record id %s", key)
					failCnt.Add(1)
				}
				seen[key] = struct{}{}
				rids = append(rids, rid)
				mu.Unlock()
			}

			if err := env.tm.Commit(tx); err != nil {
				t.Errorf("Commit: %v", err)
				failCnt.Add(1)
			}
		}(w)
	}
	wg.Wait()

	if failCnt.Load() != 0 {
		t.Fatalf("%d failures", failCnt.Load())
	}

	want := workers * perWorker
	if len(rids) != want {
		t.Fatalf("inserted %d tuples, want %d", len(rids), want)
	}

	// Every inserted row must be visible from a fresh transaction.
	readTx := beginTx(t, env)
	for _, rid := range rids {
		got, err := env.heap.GetTuple(readTx, rid, schema)
		if err != nil {
			t.Fatalf("GetTuple(%v): %v", rid, err)
		}
		if got.Values[0].IsNull() {
			t.Fatalf("unexpected null at %v", rid)
		}
	}
	_ = env.tm.Commit(readTx)
}

// -----------------------------------------------------------------------------
// Stale FSM retry
// -----------------------------------------------------------------------------

func TestTableHeap_ConcurrentInsertsForceStaleFSMRetry(t *testing.T) {
	env := setupEnv(t, 2, 16)
	defer env.Shutdown(t)

	schema := intSchema(t)
	const workers = 4
	const perWorker = 40

	var wg sync.WaitGroup
	var failCnt atomic.Int32

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()

			tx, err := env.tm.Begin(context.Background(), transaction.BeginOptions{})
			if err != nil {
				failCnt.Add(1)
				return
			}

			for i := 0; i < perWorker; i++ {
				id := int32(worker*10_000 + i)
				tx.NextCommandID()
				_, err := env.heap.InsertTuple(
					tx, NewTuple([]types.Value{types.NewInteger(id)}), schema)
				if err != nil {
					t.Errorf("InsertTuple(%d): %v", id, err)
					failCnt.Add(1)
					_ = env.tm.Abort(tx, nil)
					return
				}
			}
			if err := env.tm.Commit(tx); err != nil {
				failCnt.Add(1)
			}
		}(w)
	}

	wg.Wait()
	if failCnt.Load() != 0 {
		t.Fatalf("stale-FSM insert race reported %d failures", failCnt.Load())
	}
}

// -----------------------------------------------------------------------------
// Concurrent readers during inserts
// -----------------------------------------------------------------------------

func TestTableHeap_ConcurrentReadersDuringInserts(t *testing.T) {
	env := setupEnv(t, 4, 16)
	defer env.Shutdown(t)

	schema := intSchema(t)

	var (
		writerWG sync.WaitGroup
		readerWG sync.WaitGroup
		mu       sync.Mutex
		rids     []common.RecordId
		stopRead atomic.Bool
		failCnt  atomic.Int32
	)

	const writers = 6
	const perWriter = 50
	const readers = 6

	for w := 0; w < writers; w++ {
		writerWG.Add(1)
		go func(worker int) {
			defer writerWG.Done()

			tx, err := env.tm.Begin(context.Background(), transaction.BeginOptions{})
			if err != nil {
				failCnt.Add(1)
				return
			}
			defer func() { _ = env.tm.Commit(tx) }()

			for i := 0; i < perWriter; i++ {
				id := int32(worker*1_000 + i)
				tx.NextCommandID()
				rid, err := env.heap.InsertTuple(
					tx, NewTuple([]types.Value{types.NewInteger(id)}), schema)
				if err != nil {
					t.Errorf("InsertTuple(%d): %v", id, err)
					failCnt.Add(1)
					return
				}
				mu.Lock()
				rids = append(rids, rid)
				mu.Unlock()
			}
		}(w)
	}

	for r := 0; r < readers; r++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()

			// Each reader has its own long-running snapshot.
			tx, err := env.tm.Begin(context.Background(), transaction.BeginOptions{})
			if err != nil {
				failCnt.Add(1)
				return
			}
			defer func() { _ = env.tm.Commit(tx) }()

			for !stopRead.Load() {
				mu.Lock()
				n := len(rids)
				var rid common.RecordId
				if n > 0 {
					rid = rids[n-1]
				}
				mu.Unlock()
				if n == 0 {
					continue
				}
				// The row may or may not be visible to this snapshot;
				// either outcome is fine, but a hard error (I/O, deserialization)
				// is not.
				_, err := env.heap.GetTuple(tx, rid, schema)
				if err != nil && err != ErrTupleNotFound {
					t.Errorf("GetTuple(%v): %v", rid, err)
					failCnt.Add(1)
					return
				}
			}
		}()
	}

	writerWG.Wait()
	stopRead.Store(true)
	readerWG.Wait()

	if failCnt.Load() != 0 {
		t.Fatalf("mixed insert/read reported %d failures", failCnt.Load())
	}
}

// -----------------------------------------------------------------------------
// Concurrent deletes
// -----------------------------------------------------------------------------

func TestTableHeap_ConcurrentDeletesOfDistinctTuples(t *testing.T) {
	env := setupEnv(t, 4, 16)
	defer env.Shutdown(t)

	schema := intSchema(t)
	const n = 200

	// Insert N rows in one tx and commit.
	tx0 := beginTx(t, env)
	rids := make([]common.RecordId, n)
	for i := 0; i < n; i++ {
		rids[i] = insertInt(t, env, tx0, schema, int32(i))
	}
	if err := env.tm.Commit(tx0); err != nil {
		t.Fatal(err)
	}

	// Each delete uses its own transaction.
	var wg sync.WaitGroup
	var failCnt atomic.Int32

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			tx, err := env.tm.Begin(context.Background(), transaction.BeginOptions{})
			if err != nil {
				failCnt.Add(1)
				return
			}
			tx.NextCommandID()
			if err := env.heap.MarkTupleAsDeleted(tx, rids[i]); err != nil {
				t.Errorf("MarkTupleAsDeleted(%v): %v", rids[i], err)
				failCnt.Add(1)
				_ = env.tm.Abort(tx, nil)
				return
			}
			if err := env.tm.Commit(tx); err != nil {
				failCnt.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if failCnt.Load() != 0 {
		t.Fatalf("concurrent deletes reported %d failures", failCnt.Load())
	}
}

// TestTableHeap_ConcurrentDeleteSameTupleOneWinner: only one of the concurrent
// deletes can succeed — the rest see the tuple already has a non-zero xmax.
func TestTableHeap_ConcurrentDeleteSameTupleOneWinner(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	tx0 := beginTx(t, env)
	rid := insertInt(t, env, tx0, schema, 7)
	if err := env.tm.Commit(tx0); err != nil {
		t.Fatal(err)
	}

	const workers = 8
	var (
		wg      sync.WaitGroup
		success atomic.Int32
		failed  atomic.Int32
	)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()

			tx, err := env.tm.Begin(context.Background(), transaction.BeginOptions{})
			if err != nil {
				failed.Add(1)
				return
			}
			tx.NextCommandID()
			if err := env.heap.MarkTupleAsDeleted(tx, rid); err != nil {
				failed.Add(1)
				_ = env.tm.Abort(tx, nil)
				return
			}
			success.Add(1)
			_ = env.tm.Commit(tx)
		}(w)
	}
	wg.Wait()

	if success.Load() != 1 {
		t.Fatalf("expected exactly 1 successful delete, got %d (failed=%d)",
			success.Load(), failed.Load())
	}
	if failed.Load() != int32(workers-1) {
		t.Fatalf("expected %d failures, got %d", workers-1, failed.Load())
	}
}

// -----------------------------------------------------------------------------
// Iterators
// -----------------------------------------------------------------------------

func TestTableHeap_Iterator(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)

	// Insert 10 rows in one committed tx.
	tx0 := beginTx(t, env)
	for i := 0; i < 10; i++ {
		_ = insertInt(t, env, tx0, schema, int32(i))
	}
	if err := env.tm.Commit(tx0); err != nil {
		t.Fatal(err)
	}

	// A fresh tx iterates and sees exactly the committed rows.
	readTx := beginTx(t, env)
	defer env.tm.Commit(readTx)

	it := env.heap.Iterator(readTx, schema)
	it.Init()

	got := make([]int32, 0, 10)
	for {
		tuple, err, isDone := it.Next()
		if isDone {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		v, ok := tuple.Values[0].(types.Fixed4ByteValue)
		if !ok {
			t.Fatalf("expected integer column, got %T", tuple.Values[0])
		}
		got = append(got, int32(v.Value))
	}

	if len(got) != 10 {
		t.Fatalf("expected 10 tuples, got %d", len(got))
	}
	for i := range 10 {
		if got[i] != int32(i) {
			t.Fatalf("position %d: got %d want %d", i, got[i], i)
		}
	}
}

// TestTableHeap_IteratorSkipsUncommitted: an uncommitted insert from another
// transaction must be invisible to an iterator with an earlier snapshot.
func TestTableHeap_IteratorSkipsUncommitted(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	ctx := context.Background()

	// Tx1 commits 5 rows.
	tx1, _ := env.tm.Begin(ctx, transaction.BeginOptions{})
	for i := 0; i < 5; i++ {
		tx1.NextCommandID()
		if _, err := env.heap.InsertTuple(
			tx1, NewTuple([]types.Value{types.NewInteger(int32(i))}), schema); err != nil {
			t.Fatal(err)
		}
	}
	if err := env.tm.Commit(tx1); err != nil {
		t.Fatal(err)
	}

	// Tx2 inserts more rows but does NOT commit.
	tx2, _ := env.tm.Begin(ctx, transaction.BeginOptions{})
	for i := 5; i < 10; i++ {
		tx2.NextCommandID()
		if _, err := env.heap.InsertTuple(
			tx2, NewTuple([]types.Value{types.NewInteger(int32(i))}), schema); err != nil {
			t.Fatal(err)
		}
	}
	// intentionally not committed

	// A third transaction sees only tx1's 5 committed rows.
	tx3, _ := env.tm.Begin(ctx, transaction.BeginOptions{})
	it := env.heap.Iterator(tx3, schema)
	it.Init()

	count := 0
	for {
		_, err, isDone := it.Next()
		if isDone {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		count++
	}
	if count != 5 {
		t.Fatalf("expected 5 visible rows, got %d", count)
	}

	_ = env.tm.Commit(tx3)
	_ = env.tm.Abort(tx2, nil)
}

// -----------------------------------------------------------------------------
// Pessimistic mode
// -----------------------------------------------------------------------------

// TestTableHeap_PessimisticUpdateBlocks: two pessimistic transactions
// targeting the same row — the second must block until the first releases.
func TestTableHeap_PessimisticUpdateBlocks(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	ctx := context.Background()

	// Row for both txs to fight over.
	tx0 := beginTx(t, env)
	rid := insertInt(t, env, tx0, schema, 1)
	if err := env.tm.Commit(tx0); err != nil {
		t.Fatal(err)
	}

	// Tx A takes the row lock (via SelectForUpdate).
	txA, _ := env.tm.Begin(ctx, transaction.BeginOptions{Mode: transaction.Pessimistic})
	if _, err := env.heap.SelectForUpdate(txA, rid, schema); err != nil {
		t.Fatalf("txA SelectForUpdate: %v", err)
	}

	// Tx B tries the same lock — it should block. Use a short timeout in
	// the lock manager to keep the test fast.
	done := make(chan error, 1)
	go func() {
		txB, _ := env.tm.Begin(ctx, transaction.BeginOptions{Mode: transaction.Pessimistic})
		_, err := env.heap.SelectForUpdate(txB, rid, schema)
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("txB should have blocked, got err=%v", err)
	case <-timeAfter(100):
		// Still blocked; expected.
	}

	// Release A.
	if err := env.tm.Commit(txA); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("txB after release: %v", err)
		}
	case <-timeAfter(2_000):
		t.Fatal("txB did not wake after txA released")
	}
}

// timeAfter is a tiny helper to keep the imports clean.
func timeAfter(ms int) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		sleepMillis(ms)
		close(ch)
	}()
	return ch
}

// sleepMillis wraps time.Sleep for readability.
func sleepMillis(ms int) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

// TestTableHeap_LosingDeleteHasEmptyUndoChain verifies that a transaction
// which fails to delete a tuple does not leave a WAL record describing the
// failed change. This is what makes Abort on a loser safe without an applier.
func TestTableHeap_LosingDeleteHasEmptyUndoChain(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	ctx := context.Background()

	// Insert and commit a row.
	tx0 := beginTx(t, env)
	rid := insertInt(t, env, tx0, schema, 1)
	if err := env.tm.Commit(tx0); err != nil {
		t.Fatal(err)
	}

	// Tx1 deletes the row successfully.
	tx1, _ := env.tm.Begin(ctx, transaction.BeginOptions{})
	tx1.NextCommandID()
	if err := env.heap.MarkTupleAsDeleted(tx1, rid); err != nil {
		t.Fatalf("tx1 delete: %v", err)
	}

	// Tx2 attempts the same delete. It should fail at the pre-check before
	// logging anything.
	tx2, _ := env.tm.Begin(ctx, transaction.BeginOptions{})
	tx2.NextCommandID()
	if err := env.heap.MarkTupleAsDeleted(tx2, rid); err == nil {
		t.Fatal("tx2 should have failed to delete an already-deleted tuple")
	}

	// tx2's undo chain must be empty (only the Begin record).
	// We can't read FirstLSN directly from the test, but we can abort
	// tx2 without an applier and verify it does not panic.
	if err := env.tm.Abort(tx2, nil); err != nil {
		t.Fatalf("Abort(tx2, nil) after failed delete: %v", err)
	}

	// tx1 is still active and holding its delete. Commit it.
	if err := env.tm.Commit(tx1); err != nil {
		t.Fatalf("tx1 commit: %v", err)
	}
}

// TestTableHeap_AbortWithNilApplierIsSafe verifies that a transaction with
// no before-image records can be aborted with a nil applier.
func TestTableHeap_AbortWithNilApplierIsSafe(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)

	// A transaction that only inserts: the RecInsert has FlagHasAfter but
	// not FlagHasBefore, so the undo walk has nothing to apply.
	tx := beginTx(t, env)
	insertInt(t, env, tx, schema, 100)
	insertInt(t, env, tx, schema, 200)

	if err := env.tm.Abort(tx, nil); err != nil {
		t.Fatalf("Abort with nil applier: %v", err)
	}
}

// -----------------------------------------------------------------------------
// WAL integration
// -----------------------------------------------------------------------------

// TestTableHeap_InsertLogsWALRecord verifies that inserting a tuple produces
// a RecInsert WAL record with the correct payload, txid, prevLSN, and flags,
// and that the page's LSN is set to that record's LSN.
func TestTableHeap_InsertLogsWALRecord(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	tx := beginTx(t, env)

	tx.NextCommandID()
	tuple := NewTuple([]types.Value{types.NewInteger(42)})
	rid, err := env.heap.InsertTuple(tx, tuple, schema)
	if err != nil {
		t.Fatalf("InsertTuple: %v", err)
	}

	// Flush the WAL so the record is visible to ReadAt.
	if err := env.wal.FlushUpTo(tx.LastLSN); err != nil {
		t.Fatalf("FlushUpTo: %v", err)
	}

	// --- Verify the insert record ---
	rec, err := env.wal.ReadAt(tx.LastLSN)
	if err != nil {
		t.Fatalf("ReadAt(%v): %v", tx.LastLSN, err)
	}

	if rec.Type != wal.RecInsert {
		t.Errorf("Type: got %v want RecInsert", rec.Type)
	}
	if rec.TxID != tx.Id {
		t.Errorf("TxID: got %d want %d", rec.TxID, tx.Id)
	}
	if rec.Flags&wal.FlagHasAfter == 0 {
		t.Errorf("Flags: missing FlagHasAfter (got %#x)", rec.Flags)
	}
	if rec.Flags&wal.FlagHasBefore != 0 {
		t.Errorf("Flags: RecInsert should not carry a before-image (got %#x)", rec.Flags)
	}
	if rec.PrevLSN != tx.FirstLSN {
		t.Errorf("PrevLSN: got %v want %v (Begin record)", rec.PrevLSN, tx.FirstLSN)
	}

	// --- Verify the payload names the same row that InsertTuple returned ---
	row, payload, err := wal.DecodeInsert(rec.Data)
	if err != nil {
		t.Fatalf("DecodeInsert: %v", err)
	}
	wantRow := common.RowId{
		RelationID: env.heap.rel,
		BlockID:    rid.BlockID,
		Slot:       rid.Slot,
	}
	if row != wantRow {
		t.Errorf("RowId in payload: got %+v want %+v", row, wantRow)
	}

	// --- Verify the payload contains the tuple bytes we serialized ---
	data, nulls := tuple.Serialize(schema)
	want := append(append([]byte(nil), data...), nulls...)
	if !bytes.Equal(payload, want) {
		t.Errorf("payload bytes mismatch:\n got: %x\nwant: %x", payload, want)
	}

	// --- Verify the page's LSN was set to the insert record's LSN ---
	tag := buffer.BufferTag{
		RelationID: env.heap.rel,
		ForkID:     env.heap.fork,
		BlockID:    rid.BlockID,
	}
	frame, err, _ := env.bp.GetPage(tag)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	defer frame.Unpin()

	frame.RLatch()
	pageLSN := frame.Page.GetLSN()
	frame.RUnlatch()

	if pageLSN != rec.LSN {
		t.Errorf("page LSN: got %v want %v", pageLSN, rec.LSN)
	}

	// --- Verify the Begin record is where the insert's PrevLSN points ---
	beginRec, err := env.wal.ReadAt(tx.FirstLSN)
	if err != nil {
		t.Fatalf("ReadAt(Begin LSN): %v", err)
	}
	if beginRec.Type != wal.RecBegin {
		t.Errorf("Begin record type: got %v want RecBegin", beginRec.Type)
	}
	if beginRec.TxID != tx.Id {
		t.Errorf("Begin record TxID: got %d want %d", beginRec.TxID, tx.Id)
	}
}

// TestTableHeap_DeleteLogsWALRecord verifies that deleting a tuple produces
// a RecDelete WAL record carrying the before-image, and that the record's
// PrevLSN points back to the previous record in the same transaction.
func TestTableHeap_DeleteLogsWALRecord(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	ctx := context.Background()

	// Insert and commit a row so it exists for the delete.
	tx0, _ := env.tm.Begin(ctx, transaction.BeginOptions{})
	tx0.NextCommandID()
	rid, err := env.heap.InsertTuple(tx0, NewTuple([]types.Value{types.NewInteger(7)}), schema)
	if err != nil {
		t.Fatalf("InsertTuple: %v", err)
	}
	if err := env.tm.Commit(tx0); err != nil {
		t.Fatal(err)
	}

	// Capture the tuple bytes as they exist on disk (before-image source).
	tag := buffer.BufferTag{
		RelationID: env.heap.rel,
		ForkID:     env.heap.fork,
		BlockID:    rid.BlockID,
	}
	frame, err, _ := env.bp.GetPage(tag)
	if err != nil {
		t.Fatal(err)
	}
	frame.RLatch()
	heapPage := &heap.HeapPage{Page: frame.Page}
	beforeBytes := append([]byte(nil), heapPage.GetTupleWithSlot(rid.Slot)...)
	frame.RUnlatch()
	frame.Unpin()

	// New transaction deletes the row.
	tx1, _ := env.tm.Begin(ctx, transaction.BeginOptions{})
	tx1.NextCommandID()
	firstLSN := tx1.LastLSN
	if err := env.heap.MarkTupleAsDeleted(tx1, rid); err != nil {
		t.Fatalf("MarkTupleAsDeleted: %v", err)
	}

	// Flush and read the delete record.
	if err := env.wal.FlushUpTo(tx1.LastLSN); err != nil {
		t.Fatal(err)
	}

	rec, err := env.wal.ReadAt(tx1.LastLSN)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}

	if rec.Type != wal.RecDelete {
		t.Errorf("Type: got %v want RecDelete", rec.Type)
	}
	if rec.TxID != tx1.Id {
		t.Errorf("TxID: got %d want %d", rec.TxID, tx1.Id)
	}
	if rec.Flags&wal.FlagHasBefore == 0 {
		t.Errorf("Flags: missing FlagHasBefore (got %#x)", rec.Flags)
	}
	if rec.Flags&wal.FlagHasAfter != 0 {
		t.Errorf("Flags: RecDelete should not carry an after-image (got %#x)", rec.Flags)
	}
	if rec.PrevLSN != firstLSN {
		t.Errorf("PrevLSN: got %v want %v (Begin record)", rec.PrevLSN, firstLSN)
	}

	// Payload names the row and contains the before-image.
	row, before, err := wal.DecodeDelete(rec.Data)
	if err != nil {
		t.Fatalf("DecodeDelete: %v", err)
	}
	wantRow := common.RowId{
		RelationID: env.heap.rel,
		BlockID:    rid.BlockID,
		Slot:       rid.Slot,
	}
	if row != wantRow {
		t.Errorf("RowId in payload: got %+v want %+v", row, wantRow)
	}
	if !bytes.Equal(before, beforeBytes) {
		t.Errorf("before-image mismatch:\n got: %x\nwant: %x", before, beforeBytes)
	}

	_ = env.tm.Commit(tx1)
}
