package transaction

import (
	"testing"
	"time"

	"github.com/surajjagdev/ToyDB/internal/clog"
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/disk"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

// testManager constructs a TM backed by a temp WAL and CLOG. Every resource
// is closed by t.Cleanup.
func testManager(t *testing.T) *TransactionManager {
	t.Helper()

	dir := t.TempDir()
	vfd := disk.NewCachedVFD(16)
	t.Cleanup(func() { vfd.CloseAll(true) })

	w, err := wal.Open(vfd, wal.Options{
		Dir:         dir + "/wal",
		SegmentSize: 1 << 20,
		BatchWait:   5 * time.Millisecond,
		Fsync:       false,
		Metrics:     wal.NopMetrics{},
	})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })

	c, err := clog.Open(vfd, dir+"/clog")
	if err != nil {
		t.Fatalf("clog.Open: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	return NewManager(w, c)
}

// testManagerNoCLOG constructs a TM without CLOG, for testing the nil-CLOG
// fallback paths.
func testManagerNoCLOG(t *testing.T) *TransactionManager {
	t.Helper()

	dir := t.TempDir()
	vfd := disk.NewCachedVFD(16)
	t.Cleanup(func() { vfd.CloseAll(true) })

	w, err := wal.Open(vfd, wal.Options{
		Dir:         dir + "/wal",
		SegmentSize: 1 << 20,
		BatchWait:   5 * time.Millisecond,
		Fsync:       false,
		Metrics:     wal.NopMetrics{},
	})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })

	return NewManager(w, nil)
}

// fakeSnapshot builds a snapshot with the given fields for testing without
// requiring an actual Begin.
func fakeSnapshot(xmin, xmax common.TransactionID, inProgress ...common.TransactionID) *Snapshot {
	return &Snapshot{XMin: xmin, XMax: xmax, InProgress: inProgress}
}
