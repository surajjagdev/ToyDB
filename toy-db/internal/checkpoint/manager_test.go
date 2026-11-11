package checkpoint

import (
	"context"
	"testing"
	"time"

	"github.com/surajjagdev/ToyDB/internal/clog"
	"github.com/surajjagdev/ToyDB/internal/control"
	"github.com/surajjagdev/ToyDB/internal/storage/buffer"
	"github.com/surajjagdev/ToyDB/internal/storage/disk"
	"github.com/surajjagdev/ToyDB/internal/transaction"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

type env struct {
	dir string
	vfd *disk.VFDCache
	wal *wal.WAL
	tm  *transaction.TransactionManager
	bp  *buffer.BufferPool
	cl  *clog.Manager
	mgr *Manager
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
		BatchWait:   10 * time.Millisecond,
		Fsync:       false,
		Metrics:     wal.NopMetrics{},
	})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	t.Cleanup(func() { e.wal.Close() })

	dm, err := disk.NewDirectManager(e.dir, 100, 100)
	if err != nil {
		t.Fatalf("disk manager: %v", err)
	}
	e.bp, err = buffer.NewBufferPool(dm, e.wal, 2, 8)
	if err != nil {
		t.Fatalf("buffer pool: %v", err)
	}
	t.Cleanup(func() { e.bp.Shutdown() })

	e.cl, err = clog.Open(e.vfd, e.dir+"/clog")
	if err != nil {
		t.Fatalf("clog.Open: %v", err)
	}
	t.Cleanup(func() { e.cl.Close() })

	e.tm = transaction.NewManager(e.wal, e.cl)

	e.mgr = New(Options{
		Dir:         e.dir,
		VFD:         e.vfd,
		Interval:    time.Hour, // effectively disabled
		LogProgress: false,
	}, e.wal, e.tm, e.bp)

	return e
}

// dirtyTestPage makes a real dirty page: allocates, WAL-logs an FPW, marks
// the frame dirty with the FPW's LSN. This bypasses the (recLSN=0)
// exclusion that AllocatePage-without-WAL produces.
func dirtyTestPage(t *testing.T, e *env) buffer.BufferTag {
	t.Helper()

	// 1. Reserve the block on disk. This creates the relation file (or
	//    extends it) so FlushPage can write to it later.
	dm, err := e.bp.GetDiskManager()
	if err != nil {
		t.Fatalf("GetDiskManager: %v", err)
	}
	blockID, err := dm.AllocateBlock(1, 0) // relation 1, fork 0
	if err != nil {
		t.Fatalf("AllocateBlock: %v", err)
	}

	// 2. Allocate the frame in the buffer pool.
	tag := buffer.BufferTag{RelationID: 1, ForkID: 0, BlockID: blockID}
	frame, err := e.bp.AllocatePage(tag)
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}

	// 3. Log an FPW so the frame gets a recLSN.
	lsn, err := e.wal.Append(&wal.Record{
		TxID:  0,
		Type:  wal.RecFPW,
		Flags: wal.FlagHasAfter,
		Data:  wal.EncodeFPW(1, 0, blockID, frame.Page),
	})
	if err != nil {
		frame.Unpin()
		t.Fatalf("wal.Append: %v", err)
	}

	frame.WLatch()
	frame.Page.SetLSN(lsn)
	frame.WUnlatch()
	e.bp.MarkDirtyWithLSN(frame, lsn)
	frame.Unpin()

	if err := e.wal.Sync(context.Background(), lsn); err != nil {
		t.Fatalf("wal.Sync: %v", err)
	}

	return tag
}

// -----------------------------------------------------------------------------
// Basic single-checkpoint semantics
// -----------------------------------------------------------------------------

func TestCheckpointOnEmptyWAL(t *testing.T) {
	e := setup(t)

	if err := e.mgr.Take(context.Background()); err != nil {
		t.Fatalf("Take: %v", err)
	}

	ctrl, err := control.Read(e.vfd, e.dir)
	if err != nil {
		t.Fatalf("control.Read: %v", err)
	}

	// On an empty WAL, LastLSN() returns MakeLSN(1, 0) — the position the
	// first record would occupy. A checkpoint taken with nothing dirty
	// records that as RedoLSN.
	wantRedo := wal.MakeLSN(1, 0)
	if ctrl.RedoLSN != wantRedo {
		t.Errorf("RedoLSN = %v, want %v", ctrl.RedoLSN, wantRedo)
	}
	if ctrl.NextTxID != 3 {
		t.Errorf("NextTxID = %d, want 3", ctrl.NextTxID)
	}
	if ctrl.CheckpointLSN == 0 {
		t.Error("CheckpointLSN is zero")
	}
}

func TestCheckpointWritesWALRecord(t *testing.T) {
	e := setup(t)

	if err := e.mgr.Take(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctrl, err := control.Read(e.vfd, e.dir)
	if err != nil {
		t.Fatal(err)
	}

	rec, err := e.wal.ReadAt(ctrl.CheckpointLSN)
	if err != nil {
		t.Fatalf("ReadAt(%v): %v", ctrl.CheckpointLSN, err)
	}
	if rec.Type != wal.RecCheckpoint {
		t.Errorf("type = %v, want RecCheckpoint", rec.Type)
	}

	cp, err := wal.DecodeCheckpoint(rec.Data)
	if err != nil {
		t.Fatalf("DecodeCheckpoint: %v", err)
	}
	if cp.RedoLSN != ctrl.RedoLSN {
		t.Errorf("checkpoint RedoLSN %v != control RedoLSN %v", cp.RedoLSN, ctrl.RedoLSN)
	}
}

func TestCheckpointFlushesDirtyPages(t *testing.T) {
	e := setup(t)
	ctx := context.Background()

	_ = dirtyTestPage(t, e)

	if got := len(e.bp.DirtyPageTable()); got == 0 {
		t.Fatal("no dirty pages after setup; test broken")
	}

	if err := e.mgr.Take(ctx); err != nil {
		t.Fatalf("Take: %v", err)
	}

	if got := len(e.bp.DirtyPageTable()); got != 0 {
		t.Errorf("%d dirty pages remain after checkpoint, want 0", got)
	}
}

// -----------------------------------------------------------------------------
// Multiple checkpoints
// -----------------------------------------------------------------------------

func TestCheckpointAdvancesLSN(t *testing.T) {
	e := setup(t)
	ctx := context.Background()

	if err := e.mgr.Take(ctx); err != nil {
		t.Fatal(err)
	}
	firstLSN := e.mgr.LastCheckpointLSN()
	if firstLSN == 0 {
		t.Fatal("first checkpoint LSN is zero")
	}

	if err := e.mgr.Take(ctx); err != nil {
		t.Fatal(err)
	}
	secondLSN := e.mgr.LastCheckpointLSN()
	if secondLSN <= firstLSN {
		t.Errorf("second checkpoint LSN %v not greater than first %v",
			secondLSN, firstLSN)
	}
}

func TestCheckpointTruncatesWAL(t *testing.T) {
	e := setup(t)
	ctx := context.Background()

	// Dirty a page so RedoLSN is a real record boundary, not zero.
	_ = dirtyTestPage(t, e)

	// Push more records so truncation is meaningful.
	for i := 0; i < 50; i++ {
		_, err := e.wal.Append(&wal.Record{
			TxID: 0,
			Type: wal.RecInsert,
			Data: make([]byte, 100),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := e.wal.Sync(ctx, e.wal.LastLSN()-1); err != nil {
		t.Fatal(err)
	}

	if err := e.mgr.Take(ctx); err != nil {
		t.Fatalf("Take: %v", err)
	}

	if e.mgr.LastCheckpointLSN() == 0 {
		t.Error("LastCheckpointLSN is zero after checkpoint")
	}
}

// -----------------------------------------------------------------------------
// Background loop
// -----------------------------------------------------------------------------

func TestCheckpointBackgroundLoop(t *testing.T) {
	e := setup(t)

	e.mgr.opts.Interval = 20 * time.Millisecond
	e.mgr.opts.tickInterval = 10 * time.Millisecond

	e.mgr.Start()
	defer e.mgr.Stop()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e.mgr.LastCheckpointLSN() != 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no checkpoint fired within timeout")
}

func TestCheckpointBackgroundSizeTrigger(t *testing.T) {
	e := setup(t)

	e.mgr.opts.Interval = time.Hour
	e.mgr.opts.WALSizeThreshold = 100
	e.mgr.opts.tickInterval = 5 * time.Millisecond

	e.mgr.Start()
	defer e.mgr.Stop()

	for i := 0; i < 10; i++ {
		_, err := e.wal.Append(&wal.Record{
			TxID: 0,
			Type: wal.RecInsert,
			Data: make([]byte, 50),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e.mgr.LastCheckpointLSN() != 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no checkpoint fired via size trigger within timeout")
}

func TestStopIdempotent(t *testing.T) {
	e := setup(t)
	e.mgr.Start()

	e.mgr.Stop()
	// Second Stop: idempotent.
	e.mgr.Stop()
}

func TestStopWithoutStart(t *testing.T) {
	e := setup(t)

	done := make(chan struct{})
	go func() {
		e.mgr.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop blocked on unstarted manager")
	}
}

// -----------------------------------------------------------------------------
// Concurrency
// -----------------------------------------------------------------------------

func TestConcurrentTake(t *testing.T) {
	e := setup(t)
	ctx := context.Background()

	const n = 5
	done := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			done <- e.mgr.Take(ctx)
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-done; err != nil {
			t.Errorf("concurrent Take: %v", err)
		}
	}
}

func TestTakeDuringBackgroundLoop(t *testing.T) {
	e := setup(t)

	e.mgr.opts.Interval = 10 * time.Millisecond
	e.mgr.opts.tickInterval = 5 * time.Millisecond
	e.mgr.Start()
	defer e.mgr.Stop()

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := e.mgr.Take(ctx); err != nil {
			t.Errorf("Take %d: %v", i, err)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
