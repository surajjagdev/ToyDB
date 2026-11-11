// Package db assembles the storage engine into a runnable database. It owns
// the WAL, buffer pool, CLOG, transaction manager, table heaps, checkpoint
// manager, and recovery driver, and enforces the startup and shutdown order.
package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/surajjagdev/ToyDB/internal/access"
	"github.com/surajjagdev/ToyDB/internal/checkpoint"
	"github.com/surajjagdev/ToyDB/internal/clog"
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/recovery"
	"github.com/surajjagdev/ToyDB/internal/storage/buffer"
	"github.com/surajjagdev/ToyDB/internal/storage/disk"
	"github.com/surajjagdev/ToyDB/internal/transaction"
	"github.com/surajjagdev/ToyDB/internal/vacuum"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

// Options configures a database instance.
type Options struct {
	// Dir is the root directory. WAL, CLOG, control file, and heap files
	// all live under this directory.
	Dir string

	// SegmentSize is the WAL segment size. Zero uses the WAL default.
	SegmentSize uint32

	// Fsync enables real durability. Tests disable it; production must not.
	Fsync bool

	// CheckpointInterval is the maximum time between checkpoints.
	// Zero uses checkpoint.DefaultInterval.
	CheckpointInterval time.Duration

	VacuumInterval time.Duration

	// WALSizeThreshold triggers a checkpoint when WAL grows past this.
	// Zero uses checkpoint.DefaultWALSizeThreshold.
	WALSizeThreshold int64

	// LogProgress enables verbose logging during recovery, checkpointing,
	// and shutdown.
	LogProgress bool
}

// DB is the assembled database.
type DB struct {
	opts Options

	// Subsystems, in the order they were constructed. Shutdown happens in
	// reverse order of startup (with the checkpoint manager stopped before
	// the buffer pool so its final flush sees all dirty pages).
	vfd     *disk.VFDCache
	dm      disk.DiskManager
	wal     *wal.WAL
	bp      *buffer.BufferPool
	clog    *clog.Manager
	tm      *transaction.TransactionManager
	vac     *vacuum.Vacuumer
	heaps   map[common.RelationID]*access.TableHeap
	applier *access.HeapApplier
	ckpt    *checkpoint.Manager

	recoveryResult *recovery.RecoveryResult

	mu     sync.Mutex
	closed bool
}

// Open constructs and starts a database.
//
// Sequence:
//  1. Open VFD, disk manager, WAL, CLOG, buffer pool, TM.
//  2. Run recovery (read control, analyze, redo, undo, populate CLOG).
//  3. Create table heap for relation 1 (the only relation for now).
//  4. Create and start the checkpoint manager.
//
// If any step fails, the resources allocated so far are released in reverse.
func Open(ctx context.Context, opts Options) (*DB, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("db: Dir is required")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("db: mkdir: %w", err)
	}

	d := &DB{
		opts:  opts,
		heaps: make(map[common.RelationID]*access.TableHeap),
	}

	// ---- 1. Subsystems ----
	if err := d.openSubsystems(); err != nil {
		d.releasePartial()
		return nil, err
	}

	// ---- 2. Recovery ----
	heapMap := make(map[common.RelationID]*access.TableHeap)
	// We must construct the TableHeap before recovery so the applier can
	// route records. But we shouldn't register it for client use until
	// recovery is done.
	rel := common.RelationID(1)
	heap := access.NewTableHeap(d.bp, access.NewFSM(d.bp), rel, d.tm, d.wal)
	heapMap[rel] = heap
	applier := access.NewHeapApplier(heapMap)

	if opts.LogProgress {
		fmt.Println("[db] running recovery")
	}
	res, err := recovery.Recover(recovery.Deps{
		WAL:   d.wal,
		TM:    d.tm,
		CLOG:  d.clog,
		Apply: applier,
	}, recovery.Options{
		LogProgress: opts.LogProgress,
		ControlDir:  opts.Dir,
		VFD:         d.vfd,
	})
	if err != nil {
		d.releasePartial()
		return nil, fmt.Errorf("db: recovery: %w", err)
	}
	d.recoveryResult = res
	if opts.LogProgress {
		fmt.Printf("[db] recovery: %v (used checkpoint: %v, next txid %d)\n",
			res.Duration, res.UsedCheckpoint, res.NextTxID)
	}

	// ---- 3. Register heaps for client use ----
	d.heaps = heapMap
	d.applier = applier

	// ---- 4. Checkpoint manager ----
	d.ckpt = checkpoint.New(checkpoint.Options{
		Dir:              opts.Dir,
		VFD:              d.vfd,
		Interval:         opts.CheckpointInterval,
		WALSizeThreshold: opts.WALSizeThreshold,
		LogProgress:      opts.LogProgress,
	}, d.wal, d.tm, d.bp)
	d.ckpt.Start()

	// vaccumer
	d.vac = vacuum.New(vacuum.Options{
		Interval:    opts.VacuumInterval,
		LogProgress: d.opts.LogProgress,
	}, d.tm, d.heaps)
	d.vac.Start()

	return d, nil
}

func (d *DB) openSubsystems() error {
	var err error

	d.vfd = disk.NewCachedVFD(64)

	dm, err := disk.NewDirectManager(d.opts.Dir, 100, 100)
	if err != nil {
		return fmt.Errorf("db: disk manager: %w", err)
	}
	d.dm = dm

	segSize := d.opts.SegmentSize
	if segSize == 0 {
		segSize = 16 * 1024 * 1024
	}
	w, err := wal.Open(d.vfd, wal.Options{
		Dir:         filepath.Join(d.opts.Dir, "wal"),
		SegmentSize: segSize,
		BatchWait:   2 * time.Millisecond,
		Fsync:       d.opts.Fsync,
		Metrics:     wal.NopMetrics{},
	})
	if err != nil {
		return fmt.Errorf("db: wal: %w", err)
	}
	d.wal = w

	c, err := clog.Open(d.vfd, filepath.Join(d.opts.Dir, "clog"))
	if err != nil {
		return fmt.Errorf("db: clog: %w", err)
	}
	d.clog = c

	bp, err := buffer.NewBufferPool(dm, w, 4, 64)
	if err != nil {
		return fmt.Errorf("db: buffer pool: %w", err)
	}
	d.bp = bp

	d.tm = transaction.NewManager(w, c)
	return nil
}

// releasePartial tears down whatever was opened, in reverse order.
func (d *DB) releasePartial() {
	if d.ckpt != nil {
		d.ckpt.Stop()
	}
	if d.tm != nil {
		// TM has no Close currently; it holds no resources itself.
		_ = d.tm
	}
	if d.clog != nil {
		_ = d.clog.Close()
	}
	if d.bp != nil {
		_ = d.bp.Shutdown()
	}
	if d.wal != nil {
		_ = d.wal.Close()
	}
	if d.vfd != nil {
		_ = d.vfd.CloseAll(true)
	}
}

// -----------------------------------------------------------------------------
// Accessors
// -----------------------------------------------------------------------------

// WAL returns the WAL handle. Used by tests that need to inspect records.
func (d *DB) WAL() *wal.WAL { return d.wal }

// BufferPool returns the buffer pool.
func (d *DB) BufferPool() *buffer.BufferPool { return d.bp }

// CLOG returns the commit log.
func (d *DB) CLOG() *clog.Manager { return d.clog }

// TM returns the transaction manager.
func (d *DB) TM() *transaction.TransactionManager { return d.tm }

// VFD returns the file descriptor manager.
func (d *DB) VFD() *disk.VFDCache { return d.vfd }

// Dir returns the root directory.
func (d *DB) Dir() string { return d.opts.Dir }

// Heaps returns the registered table heaps, keyed by relation ID.
func (d *DB) Heaps() map[common.RelationID]*access.TableHeap { return d.heaps }

// Heap returns the table heap for relation rel, or nil.
func (d *DB) Heap(rel common.RelationID) *access.TableHeap {
	return d.heaps[rel]
}

// Applier returns the recovery applier. Used by tests that need to invoke
// recovery manually.
func (d *DB) Applier() *access.HeapApplier { return d.applier }

// RecoveryResult returns the result of the recovery that ran at Open.
func (d *DB) RecoveryResult() *recovery.RecoveryResult { return d.recoveryResult }

// CheckpointManager returns the background checkpoint manager.
func (d *DB) CheckpointManager() *checkpoint.Manager { return d.ckpt }

// returns background vacuumer
func (d *DB) Vacuumer() *vacuum.Vacuumer { return d.vac }

// -----------------------------------------------------------------------------
// Shutdown
// -----------------------------------------------------------------------------

// Close shuts the database down cleanly.
//
// Sequence (order matters):
//  1. Stop the checkpoint loop. Waits for any in-progress Take to finish.
//  2. Take a final checkpoint. This bounds the next startup.
//  3. Shut down the buffer pool. Flushes dirty pages that the final
//     checkpoint didn't cover (e.g., writes after the checkpoint's
//     snapshot).
//  4. Close CLOG. Flushes any pending status updates.
//  5. Close WAL. Flushes any pending records.
//  6. Close VFD.
//
// If any step fails, the remaining steps still run. The first error is
// returned after best-effort cleanup.
func (d *DB) Close(ctx context.Context) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.mu.Unlock()

	var firstErr error
	record := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// 1. close vacuum first
	if d.vac != nil {
		d.vac.Stop()
	}

	// 2. Stop the checkpoint loop.
	if d.ckpt != nil {
		d.ckpt.Stop()
		// 2. Final checkpoint — bounded by the caller's context.
		record(d.ckpt.Take(ctx))
	}

	// 3. Buffer pool: flush all dirty pages.
	if d.bp != nil {
		record(d.bp.Shutdown())
	}

	// 4. CLOG: flush status updates.
	if d.clog != nil {
		record(d.clog.Close())
	}

	// 5. WAL: flush pending records.
	if d.wal != nil {
		record(d.wal.Close())
	}

	// 6. VFD: close OS file handles.
	if d.vfd != nil {
		record(d.vfd.CloseAll(true))
	}

	return firstErr
}
