// Package checkpoint implements periodic fuzzy checkpoints. A checkpoint
// writes a RecCheckpoint record to the WAL, flushes the pages described in
// it, updates the master control file so recovery can start there, and
// truncates older WAL segments.
//
// The whole process is fuzzy — transactions keep running throughout. The
// checkpoint record is a snapshot of a moment in time; pages it lists are
// flushed lazily afterward. If a crash happens between writing the record
// and flushing the pages, recovery redoes the pages from the record's
// RedoLSN. If a crash happens before the record itself is durable, the
// checkpoint is discarded and recovery falls back to the previous one (or
// full scan).
package checkpoint

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/control"
	"github.com/surajjagdev/ToyDB/internal/storage/buffer"
	"github.com/surajjagdev/ToyDB/internal/storage/disk"
	"github.com/surajjagdev/ToyDB/internal/transaction"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

const (
	// DefaultInterval is the default time between automatic checkpoints.
	DefaultInterval = 5 * time.Minute

	// DefaultWALSizeThreshold is the default amount of WAL that triggers a
	// checkpoint.
	DefaultWALSizeThreshold = 1 << 30 // 1 GiB

	// minTickInterval is how often the background loop wakes to check its
	// triggers. Lower values make the loop more responsive at the cost of
	// more wakeups.
	minTickInterval = 1 * time.Second
)

// Options configures the checkpoint manager.
type Options struct {
	// Dir is the directory containing the control file.
	Dir string

	// VFD is the file descriptor manager used to read/write the control
	// file. Must be the same VFD the WAL uses.
	VFD *disk.VFDCache

	// Interval is the minimum time between automatic checkpoints.
	// Zero uses DefaultInterval.
	Interval time.Duration

	// WALSizeThreshold triggers a checkpoint when the WAL has grown by
	// this many bytes since the last checkpoint. Zero uses
	// DefaultWALSizeThreshold.
	WALSizeThreshold int64

	// LogProgress enables stdout diagnostics on each checkpoint.
	LogProgress bool

	// tickInterval controls how often the background loop wakes to check
	// its triggers. Unexported — only settable from inside the checkpoint
	// package. Used by tests to speed up the loop. Zero uses
	// minTickInterval.
	tickInterval time.Duration
}

func (o *Options) withDefaults() {
	if o.Interval == 0 {
		o.Interval = DefaultInterval
	}
	if o.WALSizeThreshold == 0 {
		o.WALSizeThreshold = DefaultWALSizeThreshold
	}
	if o.tickInterval == 0 {
		o.tickInterval = minTickInterval
	}
}

// Manager runs fuzzy checkpoints in a background goroutine. It also exposes
// Take for synchronous checkpoints (from shutdown handlers, from tests, or
// from an explicit "checkpoint now" signal).
type Manager struct {
	opts Options
	wal  *wal.WAL
	tm   *transaction.TransactionManager
	bp   *buffer.BufferPool

	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once
	started  atomic.Bool

	// mu guards lastCheckpointLSN and lastTruncatedLSN.
	mu                sync.Mutex
	lastCheckpointLSN wal.LSN
	lastTruncatedLSN  wal.LSN

	// takeMu serialises concurrent Take calls.
	takeMu sync.Mutex
}

// New constructs a checkpoint manager. Call Start to launch the background
// loop; call Take for one-shot checkpoints; call Stop to shut down.
func New(opts Options, w *wal.WAL, tm *transaction.TransactionManager, bp *buffer.BufferPool) *Manager {
	opts.withDefaults()
	return &Manager{
		opts:   opts,
		wal:    w,
		tm:     tm,
		bp:     bp,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Start launches the background checkpoint loop. Idempotent.
func (m *Manager) Start() {
	if !m.started.CompareAndSwap(false, true) {
		return
	}
	go m.run()
}

// Stop signals the loop to exit and waits for it. Idempotent. Does not
// block if Start was never called.
func (m *Manager) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
		if m.started.Load() {
			<-m.doneCh
		}
	})
}

// LastCheckpointLSN returns the LSN of the most recent successful
// checkpoint. Zero if no checkpoint has completed yet.
func (m *Manager) LastCheckpointLSN() wal.LSN {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastCheckpointLSN
}

// Take runs one checkpoint synchronously. Safe to call concurrently with
// normal traffic. Multiple concurrent Take calls are serialized.
func (m *Manager) Take(ctx context.Context) error {
	m.takeMu.Lock()
	defer m.takeMu.Unlock()
	return m.take(ctx)
}

func (m *Manager) take(ctx context.Context) error {
	start := time.Now()

	// ---- 1. Snapshot state ----
	activeTxIDs := m.tm.ActiveTxIDs()
	dpt := m.bp.DirtyPageTable()

	// ---- 2. Compute RedoLSN ----
	redoLSN := m.wal.LastLSN()
	if len(dpt) > 0 {
		minRec := dpt[0].RecLSN
		for _, e := range dpt[1:] {
			if e.RecLSN < minRec {
				minRec = e.RecLSN
			}
		}
		redoLSN = minRec
	}

	if m.opts.LogProgress {
		fmt.Printf("[checkpoint] snapshot: %d active txns, %d dirty pages, redo from %v\n",
			len(activeTxIDs), len(dpt), redoLSN)
	}

	// ---- 3. Write the RecCheckpoint record ----
	payload := wal.EncodeCheckpoint(wal.CheckpointPayload{
		RedoLSN:     redoLSN,
		ActiveTxIDs: activeTxIDs,
		DirtyPages:  toWALDPT(dpt),
	})
	cpLSN, err := m.wal.Append(&wal.Record{
		Type: wal.RecCheckpoint,
		Data: payload,
	})
	if err != nil {
		return fmt.Errorf("checkpoint: append record: %w", err)
	}

	// ---- 4. Flush the checkpoint record ----
	if err := m.wal.FlushUpTo(cpLSN); err != nil {
		return fmt.Errorf("checkpoint: flush record: %w", err)
	}
	if m.opts.LogProgress {
		fmt.Printf("[checkpoint] record durable at %v\n", cpLSN)
	}

	// ---- 5. Flush the pages listed in the checkpoint ----
	for _, entry := range dpt {
		tag := buffer.BufferTag{
			RelationID: entry.Tag.RelationID,
			ForkID:     entry.Tag.ForkID,
			BlockID:    entry.Tag.BlockID,
		}
		if err := m.bp.FlushPage(tag); err != nil {
			return fmt.Errorf("checkpoint: flush %v: %w", tag, err)
		}
	}

	if m.opts.LogProgress {
		fmt.Printf("[checkpoint] flushed %d pages\n", len(dpt))
	}

	// Checkpoint boundary: all pages flushed. Aborting here leaves the
	// checkpoint record and pages in a consistent (though not fully
	// installed) state — recovery will still work.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("checkpoint: aborted before control update: %w", err)
	}

	// ---- 6. Update the master record ----
	if err := control.Write(m.opts.VFD, m.opts.Dir, &control.Record{
		RedoLSN:       redoLSN,
		CheckpointLSN: cpLSN,
		NextTxID:      m.tm.NextTxID(),
	}); err != nil {
		return fmt.Errorf("checkpoint: write control: %w", err)
	}

	m.mu.Lock()
	m.lastCheckpointLSN = cpLSN
	m.mu.Unlock()

	// ---- 7. Truncate the WAL ----
	m.mu.Lock()
	alreadyTruncated := m.lastTruncatedLSN
	m.mu.Unlock()
	if redoLSN > alreadyTruncated {
		if err := m.wal.TruncateBefore(redoLSN); err != nil {
			if m.opts.LogProgress {
				fmt.Printf("[checkpoint] truncate failed (non-fatal): %v\n", err)
			}
		} else {
			m.mu.Lock()
			m.lastTruncatedLSN = redoLSN
			m.mu.Unlock()
		}
	}

	if m.opts.LogProgress {
		fmt.Printf("[checkpoint] complete in %v\n", time.Since(start))
	}
	return nil
}

// -----------------------------------------------------------------------------
// Background loop
// -----------------------------------------------------------------------------

func (m *Manager) run() {
	defer close(m.doneCh)

	// Use the per-instance tick interval with a fallback in case the field
	// was left zero (e.g., Options constructed without calling
	// withDefaults).
	tick := m.opts.tickInterval
	if tick == 0 {
		tick = minTickInterval
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	var lastCheckpointTime time.Time
	var lastCheckpointWAL wal.LSN

	for {
		select {
		case <-m.stopCh:
			return

		case <-ticker.C:
			shouldFire := false

			// Time-based trigger.
			if time.Since(lastCheckpointTime) >= m.opts.Interval {
				shouldFire = true
			}

			// Size-based trigger.
			if !shouldFire {
				currentWAL := m.wal.LastLSN()
				if currentWAL > lastCheckpointWAL &&
					int64(currentWAL-lastCheckpointWAL) >= m.opts.WALSizeThreshold {
					shouldFire = true
				}
			}

			if !shouldFire {
				continue
			}

			if err := m.Take(context.Background()); err != nil {
				if m.opts.LogProgress {
					fmt.Printf("[checkpoint] error: %v\n", err)
				}
				continue
			}

			lastCheckpointTime = time.Now()
			lastCheckpointWAL = m.wal.LastLSN()
		}
	}
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

// toWALDPT converts a buffer.DirtyPageEntry slice into the WAL's wire form.
func toWALDPT(dpt []buffer.DirtyPageEntry) []wal.CheckpointDirtyPage {
	out := make([]wal.CheckpointDirtyPage, len(dpt))
	for i, e := range dpt {
		out[i] = wal.CheckpointDirtyPage{
			Row: common.RowId{
				RelationID: e.Tag.RelationID,
				BlockID:    e.Tag.BlockID,
				Slot:       0,
			},
			RecLSN: e.RecLSN,
		}
	}
	return out
}
