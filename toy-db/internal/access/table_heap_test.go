package access

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/surajjagdev/ToyDB/internal/catalog"
	"github.com/surajjagdev/ToyDB/internal/clog"
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/buffer"
	"github.com/surajjagdev/ToyDB/internal/storage/disk"
	"github.com/surajjagdev/ToyDB/internal/storage/page"
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
	wal    *wal.WAL
	walDir string
	vfd    *disk.VFDCache
	clog   *clog.Manager
}

func (e *testEnv) Shutdown(t *testing.T) {
	t.Helper()
	_ = e.bp.Shutdown()
	_ = e.clog.Close()
	_ = e.wal.Close()
	_ = os.RemoveAll(e.walDir)
}

func setupEnv(t *testing.T, partitions common.PartitionIndex, framesPerPartition common.FrameIndex) *testEnv {
	t.Helper()

	dir := t.TempDir()

	dm, err := disk.NewDirectManager(dir, 100, 100)
	if err != nil {
		t.Fatalf("disk manager: %v", err)
	}

	walDir := dir + "/wal"
	walVFD := disk.NewCachedVFD(16)
	w, err := wal.Open(walVFD, wal.Options{
		Dir:         walDir,
		SegmentSize: 1 << 20,
		BatchWait:   1_000_000,
		Fsync:       false,
		Metrics:     wal.NopMetrics{},
	})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}

	clogDir := dir + "/clog"
	c, err := clog.Open(walVFD, clogDir)
	if err != nil {
		t.Fatalf("clog.Open: %v", err)
	}

	bp, err := buffer.NewBufferPool(dm, w, partitions, framesPerPartition)
	if err != nil {
		t.Fatalf("buffer pool: %v", err)
	}

	tm := transaction.NewManager(w, c)

	rel := common.RelationID(1)
	// 5th argument: the WAL handle. TableHeap needs it for the
	// page-allocation FPW record written by getPageForInsert.
	th := NewTableHeap(bp, NewFSM(bp), rel, tm, w)

	return &testEnv{
		heap:   th,
		bp:     bp,
		tm:     tm,
		clog:   c,
		wal:    w,
		walDir: walDir,
		vfd:    walVFD,
	}
}

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

func TestTableHeap_ReadOwnUncommittedWrite(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	tx := beginTx(t, env)

	rid := insertInt(t, env, tx, schema, 1234)

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
// Aborted-transaction visibility
// -----------------------------------------------------------------------------

// TestAbortedInsertIsInvisible: a row inserted by an aborted transaction must
// be invisible to all subsequent transactions.
func TestAbortedInsertIsInvisible(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	ctx := context.Background()

	// Tx1 inserts and aborts.
	tx1, _ := env.tm.Begin(ctx, transaction.BeginOptions{})
	tx1.NextCommandID()
	rid, err := env.heap.InsertTuple(tx1,
		NewTuple([]types.Value{types.NewInteger(1)}), schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := env.tm.Abort(tx1, nil); err != nil {
		t.Fatal(err)
	}

	// Tx2 (after tx1 aborted) must NOT see the row.
	tx2, _ := env.tm.Begin(ctx, transaction.BeginOptions{})
	defer env.tm.Commit(tx2)

	if _, err := env.heap.GetTuple(tx2, rid, schema); err == nil {
		t.Fatal("aborted insert is visible — visibility predicate is buggy")
	}

	// Iterator must also skip it.
	it := env.heap.Iterator(tx2, schema)
	it.Init()
	count := 0
	for {
		_, err, done := it.Next()
		if done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		count++
	}
	if count != 0 {
		t.Fatalf("iterator returned %d rows for aborted insert, want 0", count)
	}
}

// TestAbortedDeleteIsVisible: a row deleted by an aborted transaction must
// remain visible.
func TestAbortedDeleteIsVisible(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	ctx := context.Background()

	// Pre-existing row.
	tx0, _ := env.tm.Begin(ctx, transaction.BeginOptions{})
	tx0.NextCommandID()
	rid, _ := env.heap.InsertTuple(tx0,
		NewTuple([]types.Value{types.NewInteger(42)}), schema)
	if err := env.tm.Commit(tx0); err != nil {
		t.Fatal(err)
	}

	// Tx1 deletes and aborts.
	tx1, _ := env.tm.Begin(ctx, transaction.BeginOptions{})
	tx1.NextCommandID()
	if err := env.heap.MarkTupleAsDeleted(tx1, rid); err != nil {
		t.Fatal(err)
	}
	if err := env.tm.Abort(tx1, nil); err != nil {
		t.Fatal(err)
	}

	// Tx2 must still see the row.
	tx2, _ := env.tm.Begin(ctx, transaction.BeginOptions{})
	defer env.tm.Commit(tx2)

	got, err := env.heap.GetTuple(tx2, rid, schema)
	if err != nil {
		t.Fatalf("row deleted by aborted tx is invisible: %v", err)
	}
	if v, ok := got.Values[0].(types.Fixed4ByteValue); !ok || int32(v.Value) != 42 {
		t.Errorf("value = %v, want 42", got.Values[0])
	}
}

// -----------------------------------------------------------------------------
// Snapshot isolation
// -----------------------------------------------------------------------------

func TestTableHeap_SnapshotIsolation_Insert(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	ctx := context.Background()

	tx1, err := env.tm.Begin(ctx, transaction.BeginOptions{})
	if err != nil {
		t.Fatal(err)
	}

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

	if _, err := env.heap.GetTuple(tx1, rid, schema); err == nil {
		t.Fatalf("tx1 saw a row inserted after its snapshot")
	}

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

func TestTableHeap_SnapshotIsolation_Delete(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	ctx := context.Background()

	tx0 := beginTx(t, env)
	rid := insertInt(t, env, tx0, schema, 555)
	if err := env.tm.Commit(tx0); err != nil {
		t.Fatal(err)
	}

	tx1, err := env.tm.Begin(ctx, transaction.BeginOptions{})
	if err != nil {
		t.Fatal(err)
	}

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

	if _, err := env.heap.GetTuple(tx1, rid, schema); err != nil {
		t.Fatalf("tx1 lost visibility of a row deleted after its snapshot: %v", err)
	}

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
// Write-write conflict (optimistic)
// -----------------------------------------------------------------------------

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

	txA, _ := env.tm.Begin(ctx, transaction.BeginOptions{})
	txB, _ := env.tm.Begin(ctx, transaction.BeginOptions{})

	txA.NextCommandID()
	if err := env.heap.MarkTupleAsDeleted(txA, rid); err != nil {
		t.Fatalf("txA MarkTupleAsDeleted: %v", err)
	}

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

	tx0 := beginTx(t, env)
	rids := make([]common.RecordId, n)
	for i := 0; i < n; i++ {
		rids[i] = insertInt(t, env, tx0, schema, int32(i))
	}
	if err := env.tm.Commit(tx0); err != nil {
		t.Fatal(err)
	}

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

	tx0 := beginTx(t, env)
	for i := 0; i < 10; i++ {
		_ = insertInt(t, env, tx0, schema, int32(i))
	}
	if err := env.tm.Commit(tx0); err != nil {
		t.Fatal(err)
	}

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

func TestTableHeap_IteratorSkipsUncommitted(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	ctx := context.Background()

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

	tx2, _ := env.tm.Begin(ctx, transaction.BeginOptions{})
	for i := 5; i < 10; i++ {
		tx2.NextCommandID()
		if _, err := env.heap.InsertTuple(
			tx2, NewTuple([]types.Value{types.NewInteger(int32(i))}), schema); err != nil {
			t.Fatal(err)
		}
	}

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
// Undo chain behaviour
// -----------------------------------------------------------------------------

func TestTableHeap_LosingDeleteHasEmptyUndoChain(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	ctx := context.Background()

	tx0 := beginTx(t, env)
	rid := insertInt(t, env, tx0, schema, 1)
	if err := env.tm.Commit(tx0); err != nil {
		t.Fatal(err)
	}

	// Tx1 deletes the row successfully; it remains in-flight.
	tx1, _ := env.tm.Begin(ctx, transaction.BeginOptions{})
	tx1.NextCommandID()
	if err := env.heap.MarkTupleAsDeleted(tx1, rid); err != nil {
		t.Fatalf("tx1 delete: %v", err)
	}

	// Tx2 attempts the same delete. It must fail at the pre-check before
	// logging a WAL record — either ErrSerializationFailure (in-flight
	// holder) or a generic already-deleted error (committed holder).
	tx2, _ := env.tm.Begin(ctx, transaction.BeginOptions{})
	tx2.NextCommandID()
	err := env.heap.MarkTupleAsDeleted(tx2, rid)
	if err == nil {
		t.Fatal("tx2 should have failed to delete an already-deleted tuple")
	}
	if err != transaction.ErrSerializationFailure {
		t.Fatalf("expected ErrSerializationFailure (tx1 in-flight), got %v", err)
	}

	// tx2's undo chain is empty; aborting without an applier must succeed.
	if err := env.tm.Abort(tx2, nil); err != nil {
		t.Fatalf("Abort(tx2, nil): %v", err)
	}

	if err := env.tm.Commit(tx1); err != nil {
		t.Fatalf("tx1 commit: %v", err)
	}
}

func TestTableHeap_AbortWithNilApplierIsSafe(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)

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
// a RecInsert WAL record carrying the full tuple blob (header + null bitmap +
// user data + padding), with matching txid, prevLSN, flags, and page LSN.
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

	if err := env.wal.FlushUpTo(tx.LastLSN); err != nil {
		t.Fatalf("FlushUpTo: %v", err)
	}

	rec, err := env.wal.ReadAt(tx.LastLSN)
	if err != nil {
		t.Fatalf("ReadAt(%v): %v", tx.LastLSN, err)
	}

	// --- Record metadata ---
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

	// --- Payload identity ---
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

	// --- Payload is a full tuple blob ---
	data, nulls := tuple.Serialize(schema)
	hoff, tupleSize, _ := heap.CalculateBytesNeededToInsertTuple(len(data), len(nulls))
	if len(payload) != tupleSize {
		t.Fatalf("payload size: got %d want %d", len(payload), tupleSize)
	}

	gotXmin := common.TransactionID(common.ByteOrder.Uint32(
		payload[heap.TupleHeaderOffsetXmin:]))
	if gotXmin != tx.Id {
		t.Errorf("payload xmin: got %d want %d", gotXmin, tx.Id)
	}
	gotXmax := common.TransactionID(common.ByteOrder.Uint32(
		payload[heap.TupleHeaderOffsetXmax:]))
	if gotXmax != 0 {
		t.Errorf("payload xmax: got %d want 0", gotXmax)
	}
	gotCtidBlock := common.BlockID(common.ByteOrder.Uint32(
		payload[heap.TupleHeaderOffsetCtid:]))
	gotCtidSlot := common.SlotIndex(common.ByteOrder.Uint16(
		payload[heap.TupleHeaderOffsetCtid+4:]))
	if gotCtidBlock != rid.BlockID || gotCtidSlot != rid.Slot {
		t.Errorf("payload ctid: got (%d,%d) want (%d,%d)",
			gotCtidBlock, gotCtidSlot, rid.BlockID, rid.Slot)
	}
	gotHoff := int(payload[heap.TupleHeaderOffsetHoff])
	if gotHoff != hoff {
		t.Errorf("payload hoff: got %d want %d", gotHoff, hoff)
	}
	if !bytes.Equal(payload[hoff:hoff+len(data)], data) {
		t.Errorf("payload user data mismatch")
	}

	// --- Page LSN matches the record's LSN ---
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

	// --- Begin record sanity ---
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
// a RecDelete WAL record carrying the full before-image (pre-delete tuple
// bytes, header included), with matching txid, prevLSN, and flags.
func TestTableHeap_DeleteLogsWALRecord(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	ctx := context.Background()

	// Insert and commit a row so it exists for the delete.
	tx0, _ := env.tm.Begin(ctx, transaction.BeginOptions{})
	tx0.NextCommandID()
	rid, err := env.heap.InsertTuple(tx0,
		NewTuple([]types.Value{types.NewInteger(7)}), schema)
	if err != nil {
		t.Fatalf("InsertTuple: %v", err)
	}
	if err := env.tm.Commit(tx0); err != nil {
		t.Fatal(err)
	}

	// Capture the on-page tuple bytes as the expected before-image.
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

// TestTableHeap_InsertSetsRecLSN verifies that the WAL LSN assigned to an
// insert propagates to the buffer pool frame's recLSN. This is what makes
// the checkpoint's dirty page table useful: it points recovery at the
// earliest WAL record that dirtied the page.
func TestTableHeap_InsertSetsRecLSN(t *testing.T) {
	env := setupEnv(t, 2, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	tx := beginTx(t, env)

	rid := insertInt(t, env, tx, schema, 42)

	tag := buffer.BufferTag{
		RelationID: env.heap.rel,
		ForkID:     env.heap.fork,
		BlockID:    rid.BlockID,
	}
	frame, err, _ := env.bp.GetPage(tag)
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Unpin()

	// recLSN must equal the insert record's LSN (tx.LastLSN).
	if got := frame.RecLSN(); got != tx.LastLSN {
		t.Errorf("recLSN = %v, want %v (insert's WAL LSN)", got, tx.LastLSN)
	}
}

// TestTableHeap_SecondInsertKeepsFirstRecLSN verifies that a second insert
// on the same page does not lower the recorded recLSN — the DPT must point
// at the earliest record that dirtied the page, not the latest.
func TestTableHeap_SecondInsertKeepsFirstRecLSN(t *testing.T) {
	env := setupEnv(t, 1, 8)
	defer env.Shutdown(t)

	schema := intSchema(t)
	tx := beginTx(t, env)

	first := insertInt(t, env, tx, schema, 1)
	firstLSN := tx.LastLSN

	// Second insert lands on the same page (single partition, one frame,
	// plenty of room).
	tag := buffer.BufferTag{
		RelationID: env.heap.rel,
		ForkID:     env.heap.fork,
		BlockID:    first.BlockID,
	}
	frame, err, _ := env.bp.GetPage(tag)
	if err != nil {
		t.Fatal(err)
	}
	frame.RLatch()
	free := heapFreeSpace(frame.Page)
	frame.RUnlatch()
	frame.Unpin()

	if free < 100 {
		t.Skipf("first page too full to guarantee same-page insert: %d bytes free", free)
	}

	second := insertInt(t, env, tx, schema, 2)
	if second.BlockID != first.BlockID {
		t.Skipf("second insert went to a different page (%d vs %d)",
			second.BlockID, first.BlockID)
	}

	frame, err, _ = env.bp.GetPage(tag)
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Unpin()

	if got := frame.RecLSN(); got != firstLSN {
		t.Errorf("recLSN after 2 inserts = %v, want %v (first insert's LSN)",
			got, firstLSN)
	}
}

// heapFreeSpace is a tiny helper to peek at free space without holding the
// page latch past the return. Defined locally so the test doesn't need a
// package-level export.
func heapFreeSpace(p page.Page) int {
	hp := &heap.HeapPage{Page: p}
	return hp.GetFreeSpace()
}

// -----------------------------------------------------------------------------
// Page allocation FPW
// -----------------------------------------------------------------------------

// TestAllocatePageLogsFPW verifies that extending the relation logs a
// RecFPW record containing the freshly initialized page, so recovery can
// reconstruct the page if it never reaches disk.
func TestAllocatePageLogsFPW(t *testing.T) {
	env := setupEnv(t, 1, 4)
	defer env.Shutdown(t)

	schema := intSchema(t)
	tx := beginTx(t, env)

	// First insert forces a page allocation.
	rid := insertInt(t, env, tx, schema, 42)

	// Flush the WAL so the FPW record is visible.
	if err := env.wal.FlushUpTo(tx.LastLSN); err != nil {
		t.Fatal(err)
	}

	it, err := env.wal.IterateFrom(0)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	var found bool
	for {
		rec, err := it.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("iterate: %v", err)
		}
		if rec.Type != wal.RecFPW {
			continue
		}
		rel, fork, blockID, pageBytes, derr := wal.DecodeFPW(rec.Data)
		if derr != nil {
			t.Fatalf("DecodeFPW: %v", derr)
		}
		if rel != env.heap.rel || fork != env.heap.fork || blockID != rid.BlockID {
			continue
		}
		found = true

		// FPW must be a system record (TxID 0), with an after-image only.
		if rec.TxID != 0 {
			t.Errorf("FPW TxID: got %d want 0 (system record)", rec.TxID)
		}
		if rec.Flags&wal.FlagHasAfter == 0 {
			t.Errorf("FPW missing FlagHasAfter")
		}
		if rec.Flags&wal.FlagHasBefore != 0 {
			t.Errorf("FPW should not carry a before-image")
		}
		if len(pageBytes) != int(common.PageSize) {
			t.Errorf("FPW page size: got %d want %d", len(pageBytes), common.PageSize)
		}

		// The logged page must be a properly initialized heap page.
		hp := &heap.HeapPage{Page: page.Page(pageBytes)}
		if hp.GetLower() != page.PageHeaderSize {
			t.Errorf("logged page lower: got %d want %d",
				hp.GetLower(), page.PageHeaderSize)
		}
		if hp.GetUpper() != uint16(common.PageSize) {
			t.Errorf("logged page upper: got %d want %d",
				hp.GetUpper(), common.PageSize)
		}
		break
	}

	if !found {
		t.Fatal("no RecFPW found for allocated block")
	}
}

// TestAllocatePageFPWSetsRecLSN verifies that the buffer pool frame for a
// freshly allocated page records the FPW's LSN in its recLSN, so the frame
// participates in the dirty page table from the moment of allocation.
func TestAllocatePageFPWSetsRecLSN(t *testing.T) {
	env := setupEnv(t, 1, 4)
	defer env.Shutdown(t)

	schema := intSchema(t)
	tx := beginTx(t, env)
	rid := insertInt(t, env, tx, schema, 1)

	tag := buffer.BufferTag{
		RelationID: env.heap.rel,
		ForkID:     env.heap.fork,
		BlockID:    rid.BlockID,
	}
	frame, err, _ := env.bp.GetPage(tag)
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Unpin()

	// recLSN should be nonzero — the FPW's LSN.
	if got := frame.RecLSN(); got == 0 {
		t.Fatal("recLSN is zero after allocation; FPW was not logged")
	}

	// pageLSN >= recLSN (the insert record advances pageLSN past the FPW).
	frame.RLatch()
	pageLSN := frame.Page.GetLSN()
	frame.RUnlatch()
	if pageLSN < frame.RecLSN() {
		t.Errorf("pageLSN (%v) < recLSN (%v)", pageLSN, frame.RecLSN())
	}
}
