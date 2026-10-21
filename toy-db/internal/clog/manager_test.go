package clog

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/disk"
)

// openTestManager creates a Manager backed by a temp directory and cleans up
// both the manager and the VFD when the test ends. Returns the manager, the
// directory path, and the VFD so tests can reopen with the same VFD.
func openTestManager(t *testing.T) (*Manager, string, *disk.VFDCache) {
	t.Helper()
	dir := t.TempDir()
	vfd := disk.NewCachedVFD(8)
	m, err := Open(vfd, dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		_ = m.Close()
		_ = vfd.CloseAll(true)
	})
	return m, dir, vfd
}

// -----------------------------------------------------------------------------
// Basic semantics
// -----------------------------------------------------------------------------

func TestGetStatusDefaultsToInProgress(t *testing.T) {
	m, _, _ := openTestManager(t)

	// Never-written slots should read as InProgress across many positions
	// (different pages, different bit offsets).
	xids := []common.TransactionID{
		0, 1, 7, 1000, 1 << 20, 1 << 24,
	}
	for _, xid := range xids {
		s, err := m.GetStatus(xid)
		if err != nil {
			t.Fatalf("GetStatus(%d): %v", xid, err)
		}
		if s != InProgress {
			t.Errorf("xid %d: got %v, want InProgress", xid, s)
		}
	}
}

func TestSetGetRoundTrip(t *testing.T) {
	m, _, _ := openTestManager(t)

	cases := []struct {
		xid  common.TransactionID
		stat Status
	}{
		{0, Committed},
		{1, Aborted},
		{2, Committed},
		{3, Aborted},
		{4, Committed}, // first slot of byte 1
		{100, Aborted},
		{1000, Committed},
		{100_000, Aborted},
		{1 << 20, Committed},
		{1 << 24, Aborted},
	}

	for _, c := range cases {
		if err := m.SetStatus(c.xid, c.stat); err != nil {
			t.Fatalf("SetStatus(%d, %v): %v", c.xid, c.stat, err)
		}
	}

	for _, c := range cases {
		s, err := m.GetStatus(c.xid)
		if err != nil {
			t.Fatalf("GetStatus(%d): %v", c.xid, err)
		}
		if s != c.stat {
			t.Errorf("xid %d: got %v, want %v", c.xid, s, c.stat)
		}
	}
}

// TestSameByteDistinctSlots verifies that 4 xids sharing a byte do not
// interfere — each occupies its own 2 bits.
func TestSameByteDistinctSlots(t *testing.T) {
	m, _, _ := openTestManager(t)

	base := common.TransactionID(400) // 400/4 = byte 100, slots 0..3
	stats := []Status{Committed, Aborted, Committed, Aborted}

	for i := 0; i < 4; i++ {
		if err := m.SetStatus(base+common.TransactionID(i), stats[i]); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 4; i++ {
		got, err := m.GetStatus(base + common.TransactionID(i))
		if err != nil {
			t.Fatal(err)
		}
		if got != stats[i] {
			t.Errorf("slot %d: got %v, want %v", i, got, stats[i])
		}
	}
}

// TestOverwrite verifies that setting the same xid twice overwrites the
// previous status.
func TestOverwrite(t *testing.T) {
	m, _, _ := openTestManager(t)

	xid := common.TransactionID(42)

	if err := m.SetStatus(xid, Committed); err != nil {
		t.Fatal(err)
	}
	if err := m.SetStatus(xid, Aborted); err != nil {
		t.Fatal(err)
	}

	s, _ := m.GetStatus(xid)
	if s != Aborted {
		t.Errorf("got %v, want Aborted", s)
	}
}

// TestAllStatuses verifies that all four 2-bit values round-trip. Note that
// SubCommitted is reserved for subtransactions and unused in v1, but the
// encoder/decoder must still handle it correctly.
func TestAllStatuses(t *testing.T) {
	m, _, _ := openTestManager(t)

	cases := []struct {
		xid  common.TransactionID
		stat Status
	}{
		{10, InProgress},
		{11, Committed},
		{12, Aborted},
		{13, SubCommitted},
	}
	for _, c := range cases {
		if err := m.SetStatus(c.xid, c.stat); err != nil {
			t.Fatalf("SetStatus(%d, %v): %v", c.xid, c.stat, err)
		}
	}
	for _, c := range cases {
		got, err := m.GetStatus(c.xid)
		if err != nil {
			t.Fatal(err)
		}
		if got != c.stat {
			t.Errorf("xid %d: got %v, want %v", c.xid, got, c.stat)
		}
	}
}

// -----------------------------------------------------------------------------
// Durability
// -----------------------------------------------------------------------------

func TestPersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	vfd := disk.NewCachedVFD(8)
	defer vfd.CloseAll(true)

	m, err := Open(vfd, dir)
	if err != nil {
		t.Fatal(err)
	}

	xids := []common.TransactionID{1, 10, 100, 100_000, 1 << 20}
	stats := []Status{Committed, Aborted, Committed, Aborted, Committed}
	for i, xid := range xids {
		if err := m.SetStatus(xid, stats[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen in the same directory with the same VFD.
	m2, err := Open(vfd, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()

	for i, xid := range xids {
		s, err := m2.GetStatus(xid)
		if err != nil {
			t.Fatal(err)
		}
		if s != stats[i] {
			t.Errorf("xid %d after reopen: got %v, want %v", xid, s, stats[i])
		}
	}
}

// TestFlushClearsDirtySet verifies that Flush writes every dirty page and
// then clears the tracking set, and that reads still serve from cache.
func TestFlushClearsDirtySet(t *testing.T) {
	m, _, _ := openTestManager(t)

	if err := m.SetStatus(42, Committed); err != nil {
		t.Fatal(err)
	}
	if err := m.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(m.dirty) != 0 {
		t.Errorf("dirty set not cleared: %d entries", len(m.dirty))
	}
	s, _ := m.GetStatus(42)
	if s != Committed {
		t.Errorf("got %v, want Committed", s)
	}
}

// TestFileGrowsForFarOutXid verifies that writing a status for a far-out xid
// creates a file whose logical size covers that xid's byte offset.
func TestFileGrowsForFarOutXid(t *testing.T) {
	m, dir, _ := openTestManager(t)

	if err := m.SetStatus(10_000_000, Committed); err != nil {
		t.Fatal(err)
	}
	if err := m.Flush(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "clog")
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	// Logical size: 10M / 4 = 2.5M bytes, rounded up to a page boundary.
	wantMin := int64(10_000_000 / TxnIdsPerPage)
	if fi.Size() < wantMin {
		t.Errorf("file size %d < minimum %d", fi.Size(), wantMin)
	}
}

// -----------------------------------------------------------------------------
// Batching
// -----------------------------------------------------------------------------

func TestGetStatusBatch(t *testing.T) {
	m, _, _ := openTestManager(t)

	const n = 100
	xids := make([]common.TransactionID, n)
	want := make([]Status, n)
	for i := 0; i < n; i++ {
		xids[i] = common.TransactionID(i * 7)
		switch i % 3 {
		case 0:
			want[i] = Committed
		case 1:
			want[i] = Aborted
		case 2:
			want[i] = InProgress
		}
		if want[i] != InProgress {
			if err := m.SetStatus(xids[i], want[i]); err != nil {
				t.Fatal(err)
			}
		}
	}

	got, err := m.GetStatusBatch(xids)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if got[i] != want[i] {
			t.Errorf("batch[%d] (xid %d): got %v, want %v", i, xids[i], got[i], want[i])
		}
	}
}

// -----------------------------------------------------------------------------
// Concurrency
// -----------------------------------------------------------------------------

func TestConcurrentSetGet(t *testing.T) {
	m, _, _ := openTestManager(t)

	const workers = 8
	const perWorker = 500

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				xid := common.TransactionID(worker*perWorker + i)
				stat := Committed
				if i%2 == 0 {
					stat = Aborted
				}
				if err := m.SetStatus(xid, stat); err != nil {
					t.Errorf("SetStatus(%d): %v", xid, err)
					return
				}
				got, err := m.GetStatus(xid)
				if err != nil {
					t.Errorf("GetStatus(%d): %v", xid, err)
					return
				}
				if got != stat {
					t.Errorf("xid %d: got %v, want %v", xid, got, stat)
					return
				}
			}
		}(w)
	}
	wg.Wait()
}

func TestCloseIdempotent(t *testing.T) {
	m, _, _ := openTestManager(t)

	if err := m.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestOperationsAfterCloseFail(t *testing.T) {
	m, _, _ := openTestManager(t)

	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := m.GetStatus(1); err != ErrClosed {
		t.Errorf("GetStatus after Close: got %v, want ErrClosed", err)
	}
	if err := m.SetStatus(1, Committed); err != ErrClosed {
		t.Errorf("SetStatus after Close: got %v, want ErrClosed", err)
	}
	if _, err := m.GetStatusBatch([]common.TransactionID{1}); err != ErrClosed {
		t.Errorf("GetStatusBatch after Close: got %v, want ErrClosed", err)
	}
	if err := m.Flush(); err != ErrClosed {
		t.Errorf("Flush after Close: got %v, want ErrClosed", err)
	}
}

// -----------------------------------------------------------------------------
// Bit layout / locate
// -----------------------------------------------------------------------------

func TestLocateFormula(t *testing.T) {
	cases := []struct {
		xid       common.TransactionID
		wantPage  uint64
		wantOff   int
		wantShift uint
	}{
		{0, 0, 0, 0},
		{1, 0, 0, 2},
		{2, 0, 0, 4},
		{3, 0, 0, 6},
		{4, 0, 1, 0},
		{7, 0, 1, 6},
		{8, 0, 2, 0},
		// Last xid that fits in page 0.
		{TxnIdsPerPage - 1, 0, PageSize - 1, 6},
		// First xid of page 1.
		{TxnIdsPerPage, 1, 0, 0},
		// First xid of page 2.
		{2 * TxnIdsPerPage, 2, 0, 0},
	}
	for _, c := range cases {
		p, o, s := locate(c.xid)
		if p != c.wantPage || o != c.wantOff || s != c.wantShift {
			t.Errorf("locate(%d): got (%d, %d, %d), want (%d, %d, %d)",
				c.xid, p, o, s, c.wantPage, c.wantOff, c.wantShift)
		}
	}
}

func TestStatusString(t *testing.T) {
	cases := []struct {
		s    Status
		want string
	}{
		{InProgress, "in-progress"},
		{Committed, "committed"},
		{Aborted, "aborted"},
		{SubCommitted, "sub-committed"},
		{Status(99), "unknown(99)"},
	}
	for _, c := range cases {
		if got := c.s.String(); got != c.want {
			t.Errorf("Status(%d).String() = %q, want %q", c.s, got, c.want)
		}
	}
}

// -----------------------------------------------------------------------------
// IsCommitted convenience wrapper
// -----------------------------------------------------------------------------

func TestIsCommitted(t *testing.T) {
	m, _, _ := openTestManager(t)

	// Never set: InProgress, so IsCommitted is false.
	ok, err := m.IsCommitted(99)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("never-set xid: IsCommitted should be false")
	}

	// Committed.
	if err := m.SetStatus(100, Committed); err != nil {
		t.Fatal(err)
	}
	ok, err = m.IsCommitted(100)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("committed xid: IsCommitted should be true")
	}

	// Aborted.
	if err := m.SetStatus(101, Aborted); err != nil {
		t.Fatal(err)
	}
	ok, err = m.IsCommitted(101)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("aborted xid: IsCommitted should be false")
	}

	// SubCommitted is not Committed.
	if err := m.SetStatus(102, SubCommitted); err != nil {
		t.Fatal(err)
	}
	ok, err = m.IsCommitted(102)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("sub-committed xid: IsCommitted should be false")
	}
}

// -----------------------------------------------------------------------------
// Interleaved commits and aborts across many xids
// -----------------------------------------------------------------------------

// TestMixedCommittedAndAborted exercises a realistic pattern: alternating
// committed and aborted transactions across a wide xid range, verified after
// a flush.
func TestMixedCommittedAndAborted(t *testing.T) {
	m, dir, vfd := openTestManager(t)

	const n = 10_000
	want := make([]Status, n)
	for i := 0; i < n; i++ {
		if i%3 == 0 {
			want[i] = Aborted
		} else {
			want[i] = Committed
		}
		if err := m.SetStatus(common.TransactionID(i), want[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen, verify every slot.
	_ = dir
	m2, err := Open(vfd, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()

	for i := 0; i < n; i++ {
		got, err := m2.GetStatus(common.TransactionID(i))
		if err != nil {
			t.Fatalf("xid %d: %v", i, err)
		}
		if got != want[i] {
			t.Fatalf("xid %d: got %v, want %v", i, got, want[i])
		}
	}
}
