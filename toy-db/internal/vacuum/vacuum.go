// Package vacuum implements periodic reclamation of dead tuple versions.
package vacuum

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/surajjagdev/ToyDB/internal/access"
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/transaction"
)

const (
	// DefaultInterval is how often the background loop attempts vacuum.
	DefaultInterval = 60 * time.Second

	// DefaultDeadRatio triggers vacuum on a relation whose dead/(dead+live)
	// ratio exceeds this.
	DefaultDeadRatio = 0.20

	// DefaultDeadTupleMin is the minimum absolute dead count below which
	// vacuum skips a relation regardless of ratio. Prevents vacuum on
	// tiny tables where one dead tuple is 33% bloat.
	DefaultDeadTupleMin = 100

	// DefaultPagesPerSecond caps vacuum throughput. Applied as a wall-clock
	// sleep after each relation's pass, not between individual pages.
	DefaultPagesPerSecond = 100

	minTickInterval = 1 * time.Second
)

// Options configures a Vacuumer.
type Options struct {
	Interval       time.Duration
	DeadRatio      float64
	DeadTupleMin   uint64
	PagesPerSecond int
	LogProgress    bool

	// tickInterval is the background loop wake interval. Unexported;
	// tests set it lower.
	tickInterval time.Duration
}

func (o *Options) withDefaults() {
	if o.Interval == 0 {
		o.Interval = DefaultInterval
	}
	if o.DeadRatio == 0 {
		o.DeadRatio = DefaultDeadRatio
	}
	if o.DeadTupleMin == 0 {
		o.DeadTupleMin = DefaultDeadTupleMin
	}
	if o.PagesPerSecond == 0 {
		o.PagesPerSecond = DefaultPagesPerSecond
	}
	if o.tickInterval == 0 {
		o.tickInterval = minTickInterval
	}
}

// Stats summarizes one vacuum pass.
type Stats struct {
	StartTime     time.Time
	Duration      time.Duration
	Relations     int
	RelationsHit  int
	PagesScanned  int
	TuplesRemoved int
}

// Vacuumer runs periodic vacuum passes across all registered relations.
type Vacuumer struct {
	opts  Options
	tm    *transaction.TransactionManager
	heaps map[common.RelationID]*access.TableHeap

	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once
	started  atomic.Bool

	mu        sync.Mutex
	lastStats Stats

	takeMu sync.Mutex
}

func New(opts Options, tm *transaction.TransactionManager, heaps map[common.RelationID]*access.TableHeap) *Vacuumer {
	opts.withDefaults()
	return &Vacuumer{
		opts:   opts,
		tm:     tm,
		heaps:  heaps,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Start launches the background loop.
func (v *Vacuumer) Start() {
	if !v.started.CompareAndSwap(false, true) {
		return
	}
	go v.run()
}

// Stop halts the loop. Idempotent.
func (v *Vacuumer) Stop() {
	v.stopOnce.Do(func() {
		close(v.stopCh)
		if v.started.Load() {
			<-v.doneCh
		}
	})
}

// Take runs one synchronous vacuum pass.
func (v *Vacuumer) Take(ctx context.Context) error {
	v.takeMu.Lock()
	defer v.takeMu.Unlock()
	return v.take(ctx)
}

func (v *Vacuumer) take(ctx context.Context) error {
	start := time.Now()
	oldestXmin := v.tm.OldestActiveSnapshot()
	stats := Stats{StartTime: start}

	for relID, th := range v.heaps {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("vacuum: aborted: %w", err)
		}

		stats.Relations++

		live, dead := th.TupleCounts()
		total := live + dead
		if total == 0 || dead < v.opts.DeadTupleMin {
			continue
		}
		ratio := float64(dead) / float64(total)
		if ratio < v.opts.DeadRatio {
			continue
		}

		stats.RelationsHit++

		relStart := time.Now()
		relStats, err := th.VacuumRelation(oldestXmin)
		if err != nil {
			return fmt.Errorf("vacuum: relation %d: %w", relID, err)
		}

		if v.opts.LogProgress && relStats.TuplesRemoved > 0 {
			fmt.Printf("[vacuum] rel %d: %d removed from %d/%d pages (dead %d/%d = %.1f%%) in %v\n",
				relID, relStats.TuplesRemoved, relStats.PagesModified, relStats.PagesScanned,
				dead, total, ratio*100, time.Since(relStart))
		}

		stats.PagesScanned += relStats.PagesScanned
		stats.TuplesRemoved += relStats.TuplesRemoved

		// Rate limit: wait until this relation's pass took at least
		// (PagesScanned / PagesPerSecond) seconds.
		if v.opts.PagesPerSecond > 0 {
			budget := time.Duration(relStats.PagesScanned) * time.Second /
				time.Duration(v.opts.PagesPerSecond)
			elapsed := time.Since(relStart)
			if elapsed < budget {
				select {
				case <-time.After(budget - elapsed):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}

	stats.Duration = time.Since(start)
	v.mu.Lock()
	v.lastStats = stats
	v.mu.Unlock()
	return nil
}

// LastStats returns the most recent pass's statistics.
func (v *Vacuumer) LastStats() Stats {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.lastStats
}

func (v *Vacuumer) run() {
	defer close(v.doneCh)

	tick := v.opts.tickInterval
	if tick == 0 {
		tick = minTickInterval
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	var lastVacuum time.Time
	for {
		select {
		case <-v.stopCh:
			return
		case <-ticker.C:
			if time.Since(lastVacuum) < v.opts.Interval {
				continue
			}
			if err := v.Take(context.Background()); err != nil {
				if v.opts.LogProgress {
					fmt.Printf("[vacuum] error: %v\n", err)
				}
				continue
			}
			lastVacuum = time.Now()
		}
	}
}
