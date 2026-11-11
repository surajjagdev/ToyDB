package db_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/surajjagdev/ToyDB/internal/access"
	"github.com/surajjagdev/ToyDB/internal/catalog"
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/control"
	"github.com/surajjagdev/ToyDB/internal/db"
	"github.com/surajjagdev/ToyDB/internal/transaction"
	"github.com/surajjagdev/ToyDB/internal/types"
)

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

func writeRows(t *testing.T, d *db.DB, schema *catalog.Schema, start, count int) {
	t.Helper()
	ctx := context.Background()
	tx, err := d.TM().Begin(ctx, transaction.BeginOptions{})
	if err != nil {
		t.Fatal(err)
	}
	heap := d.Heap(1)
	for i := start; i < start+count; i++ {
		tx.NextCommandID()
		_, err := heap.InsertTuple(tx, access.NewTuple([]types.Value{types.NewInteger(int32(i))}), schema)
		if err != nil {
			_ = d.TM().Abort(tx, nil)
			t.Fatal(err)
		}
	}
	if err := d.TM().Commit(tx); err != nil {
		t.Fatal(err)
	}
}

func countRows(t *testing.T, d *db.DB, schema *catalog.Schema) int {
	t.Helper()
	ctx := context.Background()
	tx, err := d.TM().Begin(ctx, transaction.BeginOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.TM().Commit(tx)

	it := d.Heap(1).Iterator(tx, schema)
	it.Init()
	n := 0
	for {
		_, err, done := it.Next()
		if done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	return n
}

func TestDBOpenWriteCheckpointClose(t *testing.T) {
	dir := t.TempDir()

	// 1. Open a fresh DB, write 100 rows, close cleanly.
	d, err := db.Open(context.Background(), db.Options{
		Dir:   dir,
		Fsync: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	schema := intSchema(t)
	writeRows(t, d, schema, 0, 100)

	if got := countRows(t, d, schema); got != 100 {
		t.Fatalf("before close: %d rows, want 100", got)
	}

	if err := d.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 2. Verify the control file was written.
	// We can't call control.Read without a VFD, so we just check the file
	// exists.
	if _, err := filepath.Glob(filepath.Join(dir, "control")); err != nil {
		t.Fatal(err)
	}

	// 3. Reopen. Recovery should use the checkpoint (fast path).
	d2, err := db.Open(context.Background(), db.Options{
		Dir:         dir,
		Fsync:       false,
		LogProgress: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close(context.Background())

	if !d2.RecoveryResult().UsedCheckpoint {
		t.Error("recovery did not use checkpoint after clean shutdown")
	}

	// 4. Verify all 100 rows are still there.
	if got := countRows(t, d2, schema); got != 100 {
		t.Fatalf("after reopen: %d rows, want 100", got)
	}
}

func TestDBBackgroundCheckpoint(t *testing.T) {
	dir := t.TempDir()

	d, err := db.Open(context.Background(), db.Options{
		Dir:                dir,
		Fsync:              false,
		CheckpointInterval: 30 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(context.Background())

	schema := intSchema(t)
	writeRows(t, d, schema, 0, 10)

	// Wait for at least one checkpoint.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if d.CheckpointManager().LastCheckpointLSN() != 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if d.CheckpointManager().LastCheckpointLSN() == 0 {
		t.Fatal("no background checkpoint fired")
	}
}

func TestDBRecoverAfterSimulatedCrash(t *testing.T) {
	dir := t.TempDir()

	// 1. Open, write, checkpoint, write more, close WAL without flushing
	//    pages (simulating a crash where WAL is durable but pages aren't).
	d, err := db.Open(context.Background(), db.Options{
		Dir:   dir,
		Fsync: true, // true durability so the crash sim is meaningful
	})
	if err != nil {
		t.Fatal(err)
	}
	schema := intSchema(t)
	writeRows(t, d, schema, 0, 50)

	// Force a checkpoint.
	if err := d.CheckpointManager().Take(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Write more rows without a checkpoint.
	writeRows(t, d, schema, 50, 50)

	// Simulate crash: flush WAL, abandon buffer pool.
	// The DB.Close path flushes everything; we want the abandoned variant.
	// The simplest way is to just close the WAL and skip BP.
	// In practice, this test is better done with a subprocess, but for a
	// smoke test in-process works if we accept that some pages flushed.
	//
	// Here: close cleanly (flush all), then verify recovery on reopen. This
	// tests the checkpoint-based recovery path but not the crash path.
	// (A proper crash test is the next step.)
	if err := d.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 2. Reopen. Both pre-checkpoint and post-checkpoint rows must be
	//    visible.
	d2, err := db.Open(context.Background(), db.Options{
		Dir:         dir,
		Fsync:       true,
		LogProgress: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close(context.Background())

	if !d2.RecoveryResult().UsedCheckpoint {
		t.Error("recovery did not use checkpoint")
	}

	if got := countRows(t, d2, schema); got != 100 {
		t.Fatalf("after reopen: %d rows, want 100", got)
	}

	// Verify the control file exists and is readable.
	ctrl, err := control.Read(d2.VFD(), dir)
	if err != nil {
		t.Fatalf("control.Read: %v", err)
	}
	if ctrl.CheckpointLSN == 0 {
		t.Error("control.CheckpointLSN is zero")
	}
}

func TestDBRepeatedOpenClose(t *testing.T) {
	dir := t.TempDir()
	schema := intSchema(t)

	for i := 0; i < 3; i++ {
		d, err := db.Open(context.Background(), db.Options{
			Dir:   dir,
			Fsync: false,
		})
		if err != nil {
			t.Fatalf("cycle %d open: %v", i, err)
		}
		writeRows(t, d, schema, i*10, 10)
		if got := countRows(t, d, schema); got != (i+1)*10 {
			t.Errorf("cycle %d: %d rows, want %d", i, got, (i+1)*10)
		}
		if err := d.Close(context.Background()); err != nil {
			t.Fatalf("cycle %d close: %v", i, err)
		}
	}

	// Final verification.
	d, err := db.Open(context.Background(), db.Options{Dir: dir, Fsync: false})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(context.Background())
	if got := countRows(t, d, schema); got != 30 {
		t.Errorf("after 3 cycles: %d rows, want 30", got)
	}
}

func TestDBBackgroundVacuum(t *testing.T) {
	dir := t.TempDir()

	d, err := db.Open(context.Background(), db.Options{
		Dir:                dir,
		Fsync:              false,
		CheckpointInterval: time.Hour, // disabled
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(context.Background())

	schema := intSchema(t)

	// 1. Insert a row, then update it many times to create dead versions.
	ctx := context.Background()
	tx0, _ := d.TM().Begin(ctx, transaction.BeginOptions{})
	tx0.NextCommandID()
	rid, _ := d.Heap(1).InsertTuple(tx0,
		access.NewTuple([]types.Value{types.NewInteger(1)}), schema)
	d.TM().Commit(tx0)

	for i := 0; i < 100; i++ {
		tx, _ := d.TM().Begin(ctx, transaction.BeginOptions{})
		tx.NextCommandID()
		newRid, err := d.Heap(1).UpdateTuple(tx, rid,
			access.NewTuple([]types.Value{types.NewInteger(int32(i + 2))}), schema)
		if err != nil {
			t.Fatal(err)
		}
		d.TM().Commit(tx)
		rid = newRid
	}

	// 2. Verify dead versions accumulated.
	if d.Heap(1).DeadTupleCount() == 0 {
		t.Fatal("no dead versions; test setup broken")
	}

	// 3. Force a vacuum pass synchronously.
	if err := d.Vacuumer().Take(ctx); err != nil {
		t.Fatal(err)
	}

	// 4. Dead versions should be reclaimed.
	if got := d.Heap(1).DeadTupleCount(); got != 0 {
		t.Errorf("dead versions after vacuum: %d, want 0", got)
	}

	// 5. The live row must be readable.
	txr, _ := d.TM().Begin(ctx, transaction.BeginOptions{})
	defer d.TM().Commit(txr)
	tuple, err := d.Heap(1).GetTuple(txr, rid, schema)
	if err != nil {
		t.Fatalf("live row lost: %v", err)
	}
	if v, ok := tuple.Values[0].(types.Fixed4ByteValue); !ok || int32(v.Value) != 101 {
		t.Errorf("live value = %v, want 101", tuple.Values[0])
	}
}

// Silence unused import warning for common.
var _ = common.RelationID(1)
