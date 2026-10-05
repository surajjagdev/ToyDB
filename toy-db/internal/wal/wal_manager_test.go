package wal

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/disk"
)

// -----------------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------------

// testOptions returns options tuned for fast tests with small segments so
// rotation and recovery are actually exercised.
func testOptions(t *testing.T) Options {
	t.Helper()
	return Options{
		Dir:         t.TempDir(),
		SegmentSize: 64 * 1024, // 64 KiB
		MaxBatch:    4 * 1024,  // flush every 4 KiB
		BatchWait:   time.Millisecond,
		Fsync:       true,
		Metrics:     NopMetrics{},
	}
}

// readAllRecords walks every segment in dir and returns the records in
// on-disk order. Used to verify recovery.
func readAllRecords(t *testing.T, vfd *disk.VFDCache, dir string) []Record {
	t.Helper()
	segs, err := listAllSegments(dir)
	if err != nil {
		t.Fatalf("listAllSegments: %v", err)
	}

	var records []Record
	for _, segNum := range segs {
		path := filepath.Join(dir, segmentName(segNum))
		entry, err := vfd.GetOrOpen(disk.FileNode{Path: path}, os.O_RDWR)
		if err != nil {
			t.Fatalf("open segment %d: %v", segNum, err)
		}

		var off uint32
		hdr := make([]byte, RecordHeaderSize)
		buf := make([]byte, 4096)
		for {
			n, err := entry.ReadAt(hdr, int64(off))
			if err == io.EOF && n == 0 {
				break
			}
			if err != nil && err != io.EOF {
				t.Fatalf("read header at %d:%d: %v", segNum, off, err)
			}
			if n < RecordHeaderSize {
				break
			}
			total, err := PeekRecordLen(hdr)
			if err != nil {
				break
			}
			if uint32(len(buf)) < total {
				buf = make([]byte, total)
			}
			n, err = entry.ReadAt(buf[:total], int64(off))
			if err != nil && err != io.EOF {
				t.Fatalf("read body at %d:%d: %v", segNum, off, err)
			}
			if uint32(n) < total {
				break
			}
			var rec Record
			m, err := rec.Unmarshal(buf[:total])
			if err != nil {
				t.Fatalf("unmarshal at %d:%d: %v", segNum, off, err)
			}
			records = append(records, rec)
			off += uint32(m)
		}
		vfd.Release(entry)
	}
	return records
}

// -----------------------------------------------------------------------------
// Record encoding
// -----------------------------------------------------------------------------

func TestWALRecordRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		rec  Record
	}{
		{"empty payload", Record{
			LSN: MakeLSN(1, 100), PrevLSN: 0, TxID: 1, Type: RecBegin,
		}},
		{"small payload", Record{
			LSN: MakeLSN(2, 200), PrevLSN: MakeLSN(2, 150),
			TxID: 42, Type: RecInsert, Flags: FlagHasAfter,
			Data: []byte("hello world"),
		}},
		{"both images", Record{
			LSN: MakeLSN(3, 0), PrevLSN: MakeLSN(3, 0),
			TxID: 9999, Type: RecUpdate,
			Flags: FlagHasBefore | FlagHasAfter,
			Data:  []byte("before||after"),
		}},
		{"8k payload", Record{
			LSN: MakeLSN(1, 0), TxID: 7, Type: RecFPW, Flags: FlagHasAfter,
			Data: make([]byte, 8192),
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, tc.rec.EncodedSize())
			tc.rec.MarshalTo(buf)

			var got Record
			n, err := got.Unmarshal(buf)
			if err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if n != len(buf) {
				t.Fatalf("consumed %d, want %d", n, len(buf))
			}
			if got.LSN != tc.rec.LSN {
				t.Errorf("LSN: got %v want %v", got.LSN, tc.rec.LSN)
			}
			if got.PrevLSN != tc.rec.PrevLSN {
				t.Errorf("PrevLSN: got %v want %v", got.PrevLSN, tc.rec.PrevLSN)
			}
			if got.TxID != tc.rec.TxID {
				t.Errorf("TxID: got %v want %v", got.TxID, tc.rec.TxID)
			}
			if got.Type != tc.rec.Type {
				t.Errorf("Type: got %v want %v", got.Type, tc.rec.Type)
			}
			if got.Flags != tc.rec.Flags {
				t.Errorf("Flags: got %v want %v", got.Flags, tc.rec.Flags)
			}
			if string(got.Data) != string(tc.rec.Data) {
				t.Errorf("Data mismatch: got %d bytes want %d", len(got.Data), len(tc.rec.Data))
			}
		})
	}
}

// TestRecordCRCDetectsCorruption flips every bit in the header and payload
// and verifies Unmarshal rejects the result. This is what proves the CRC
// covers the right bytes.
func TestRecordCRCDetectsCorruption(t *testing.T) {
	rec := Record{
		LSN: MakeLSN(1, 0), TxID: 7, Type: RecInsert, Flags: FlagHasAfter,
		Data: []byte("some payload"),
	}
	buf := make([]byte, rec.EncodedSize())
	rec.MarshalTo(buf)

	// Header bytes before the CRC field: [0, 28).
	for i := 0; i < 28; i++ {
		for bit := 0; bit < 8; bit++ {
			corrupted := append([]byte(nil), buf...)
			corrupted[i] ^= 1 << bit
			var got Record
			if _, err := got.Unmarshal(corrupted); err == nil {
				t.Errorf("header bit flip at byte %d bit %d not detected", i, bit)
			}
		}
	}

	// Payload bytes: [32, end).
	for i := 32; i < len(buf); i++ {
		for bit := 0; bit < 8; bit++ {
			corrupted := append([]byte(nil), buf...)
			corrupted[i] ^= 1 << bit
			var got Record
			if _, err := got.Unmarshal(corrupted); err == nil {
				t.Errorf("payload bit flip at byte %d bit %d not detected", i, bit)
			}
		}
	}
}

func TestPeekRecordLen(t *testing.T) {
	rec := Record{Data: []byte("payload")}
	buf := make([]byte, rec.EncodedSize())
	rec.MarshalTo(buf)

	n, err := PeekRecordLen(buf[:4])
	if err != nil {
		t.Fatal(err)
	}
	if n != uint32(rec.EncodedSize()) {
		t.Fatalf("got %d want %d", n, rec.EncodedSize())
	}

	if _, err := PeekRecordLen(buf[:3]); err != ErrShortRead {
		t.Errorf("short buffer: got %v want ErrShortRead", err)
	}

	bad := append([]byte(nil), buf[:4]...)
	common.ByteOrder.PutUint32(bad, 10) // below RecordHeaderSize
	if _, err := PeekRecordLen(bad); err != ErrCorrupt {
		t.Errorf("small length: got %v want ErrCorrupt", err)
	}

	common.ByteOrder.PutUint32(bad, 200<<20) // above MaxPayload
	if _, err := PeekRecordLen(bad); err != ErrCorrupt {
		t.Errorf("large length: got %v want ErrCorrupt", err)
	}
}

// -----------------------------------------------------------------------------
// Basic append + recovery
// -----------------------------------------------------------------------------

func TestWALAppendAndRecover(t *testing.T) {
	opts := testOptions(t)
	vfd := disk.NewCachedVFD(8)

	w, err := Open(vfd, opts)
	if err != nil {
		t.Fatal(err)
	}

	const n = 100
	lsns := make([]LSN, n)
	for i := 0; i < n; i++ {
		rec := Record{Type: RecInsert, Data: []byte(fmt.Sprintf("payload-%d", i))}
		lsn, err := w.Append(&rec)
		if err != nil {
			t.Fatal(err)
		}
		lsns[i] = lsn
		if i%10 == 0 {
			if err := w.Sync(context.Background(), lsn); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Sync(context.Background(), lsns[n-1]); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and verify every record.
	w2, err := Open(vfd, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	recs := readAllRecords(t, vfd, opts.Dir)
	if len(recs) != n {
		t.Fatalf("got %d records, want %d", len(recs), n)
	}
	for i, rec := range recs {
		want := fmt.Sprintf("payload-%d", i)
		if string(rec.Data) != want {
			t.Errorf("record %d: data got %q want %q", i, rec.Data, want)
		}
		if rec.LSN != lsns[i] {
			t.Errorf("record %d: LSN got %v want %v", i, rec.LSN, lsns[i])
		}
	}

	// Recovered WAL's LastLSN must be strictly past the last record's LSN.
	if w2.LastLSN() <= lsns[n-1] {
		t.Errorf("LastLSN %v not past last record %v", w2.LastLSN(), lsns[n-1])
	}
}

// -----------------------------------------------------------------------------
// Segment rotation
// -----------------------------------------------------------------------------

// TestWALSegmentRotation verifies that records spanning multiple segments
// have LSNs whose segment numbers agree with the file they live in.
// This is the test that catches the rotation off-by-one.
func TestWALSegmentRotation(t *testing.T) {
	opts := testOptions(t)
	opts.SegmentSize = 1024 // tiny, forces many rotations
	opts.MaxBatch = 256
	vfd := disk.NewCachedVFD(16)

	w, err := Open(vfd, opts)
	if err != nil {
		t.Fatal(err)
	}

	const n = 500
	lsns := make([]LSN, n)
	for i := 0; i < n; i++ {
		rec := Record{Type: RecInsert, Data: []byte(fmt.Sprintf("p%d", i))}
		lsn, err := w.Append(&rec)
		if err != nil {
			t.Fatal(err)
		}
		lsns[i] = lsn
	}
	if err := w.Sync(context.Background(), lsns[n-1]); err != nil {
		t.Fatal(err)
	}
	w.Close()

	segs, err := listAllSegments(opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) < 2 {
		t.Fatalf("expected multiple segments, got %v", segs)
	}

	// Every LSN's segment file must contain a record at the right offset.
	recs := readAllRecords(t, vfd, opts.Dir)
	if len(recs) != n {
		t.Fatalf("got %d records, want %d", len(recs), n)
	}
	for i, rec := range recs {
		if rec.LSN != lsns[i] {
			t.Errorf("record %d: LSN got %v want %v", i, rec.LSN, lsns[i])
		}
	}
	// Segment numbers must be monotonically non-decreasing across the log.
	for i := 1; i < len(recs); i++ {
		if recs[i].LSN.GetSegmentNumber() < recs[i-1].LSN.GetSegmentNumber() {
			t.Fatalf("segment went backward at %d: %v -> %v",
				i, recs[i-1].LSN, recs[i].LSN)
		}
	}
}

// -----------------------------------------------------------------------------
// Torn tail recovery
// -----------------------------------------------------------------------------

func TestWALTornTailRecovery(t *testing.T) {
	opts := testOptions(t)
	vfd := disk.NewCachedVFD(8)

	w, err := Open(vfd, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		rec := Record{Type: RecInsert, Data: []byte(fmt.Sprintf("r%d", i))}
		lsn, err := w.Append(&rec)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Sync(context.Background(), lsn); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Append garbage bytes to the tail of the last segment.
	segs, _ := listAllSegments(opts.Dir)
	lastSeg := segs[len(segs)-1]
	path := filepath.Join(opts.Dir, segmentName(lastSeg))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Recovery must truncate the garbage.
	w2, err := Open(vfd, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	recs := readAllRecords(t, vfd, opts.Dir)
	if len(recs) != 20 {
		t.Fatalf("got %d records after torn tail, want 20", len(recs))
	}

	// Append a new record; it must land cleanly after the truncated tail.
	rec := Record{Type: RecInsert, Data: []byte("post-recovery")}
	lsn, err := w2.Append(&rec)
	if err != nil {
		t.Fatal(err)
	}
	if err := w2.Sync(context.Background(), lsn); err != nil {
		t.Fatal(err)
	}

	recs = readAllRecords(t, vfd, opts.Dir)
	if len(recs) != 21 {
		t.Fatalf("got %d records after append, want 21", len(recs))
	}
	if string(recs[20].Data) != "post-recovery" {
		t.Errorf("last record data: got %q", recs[20].Data)
	}
}

// -----------------------------------------------------------------------------
// Sync semantics
// -----------------------------------------------------------------------------

func TestWALSyncFastPath(t *testing.T) {
	opts := testOptions(t)
	opts.MaxBatch = 1 << 20 // huge: only Sync will force a flush
	opts.BatchWait = time.Hour
	vfd := disk.NewCachedVFD(8)

	w, err := Open(vfd, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	rec := Record{Type: RecInsert, Data: []byte("x")}
	lsn, _ := w.Append(&rec)

	start := time.Now()
	if err := w.Sync(context.Background(), lsn); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("first Sync took %v", d)
	}

	// Second Sync for the same LSN: fast path, no work.
	start = time.Now()
	if err := w.Sync(context.Background(), lsn); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > time.Millisecond {
		t.Errorf("fast-path Sync took %v", d)
	}
}

func TestWALClosedSemantics(t *testing.T) {
	opts := testOptions(t)
	vfd := disk.NewCachedVFD(8)

	w, err := Open(vfd, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	rec := Record{Type: RecInsert, Data: []byte("x")}
	if _, err := w.Append(&rec); err != ErrClosed {
		t.Errorf("Append on closed: got %v want ErrClosed", err)
	}
	if err := w.Sync(context.Background(), MakeLSN(1, 0)); err != ErrClosed {
		t.Errorf("Sync on closed: got %v want ErrClosed", err)
	}
	// Second Close is a no-op.
	if err := w.Close(); err != nil {
		t.Errorf("double Close: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Full-page writes
// -----------------------------------------------------------------------------

func TestWALNeedsFPW(t *testing.T) {
	opts := testOptions(t)
	vfd := disk.NewCachedVFD(8)

	w, err := Open(vfd, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	page := common.BlockID(42)
	if !w.NeedsFPW(page) {
		t.Error("first NeedsFPW: want true")
	}
	if w.NeedsFPW(page) {
		t.Error("second NeedsFPW: want false")
	}
	if w.NeedsFPW(page) {
		t.Error("third NeedsFPW: want false")
	}

	other := common.BlockID(43)
	if !w.NeedsFPW(other) {
		t.Error("different page: want true")
	}

	w.ResetFPWSet()
	if !w.NeedsFPW(page) {
		t.Error("after Reset: want true")
	}
	if !w.NeedsFPW(other) {
		t.Error("after Reset other page: want true")
	}
}

// -----------------------------------------------------------------------------
// Mixed record sizes
// -----------------------------------------------------------------------------

func TestWALMixedRecordSizes(t *testing.T) {
	opts := testOptions(t)
	opts.SegmentSize = 128 << 10
	opts.MaxBatch = 16 << 10
	vfd := disk.NewCachedVFD(16)

	w, err := Open(vfd, opts)
	if err != nil {
		t.Fatal(err)
	}

	sizes := []int{0, 1, 10, 100, 1000, 5000, 20000}
	expected := make([]string, len(sizes))
	for i, size := range sizes {
		data := make([]byte, size)
		for j := range data {
			data[j] = byte(j % 256)
		}
		expected[i] = string(data)
		rec := Record{Type: RecInsert, Data: data}
		lsn, err := w.Append(&rec)
		if err != nil {
			t.Fatalf("append size %d: %v", size, err)
		}
		if err := w.Sync(context.Background(), lsn); err != nil {
			t.Fatalf("sync size %d: %v", size, err)
		}
	}
	w.Close()

	recs := readAllRecords(t, vfd, opts.Dir)
	if len(recs) != len(sizes) {
		t.Fatalf("got %d records, want %d", len(recs), len(sizes))
	}
	for i, rec := range recs {
		if string(rec.Data) != expected[i] {
			t.Errorf("record %d (size %d): data mismatch", i, sizes[i])
		}
	}
}

// -----------------------------------------------------------------------------
// Concurrent appenders
// -----------------------------------------------------------------------------

func TestWALConcurrentAppenders(t *testing.T) {
	opts := testOptions(t)
	opts.SegmentSize = 256 << 10
	opts.MaxBatch = 8 << 10
	vfd := disk.NewCachedVFD(16)

	w, err := Open(vfd, opts)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 8
	const perGoroutine = 500

	var wg sync.WaitGroup
	var mu sync.Mutex
	var maxLSN LSN
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				rec := Record{
					Type: RecInsert,
					Data: []byte(fmt.Sprintf("g%d-i%d", g, i)),
				}
				lsn, err := w.Append(&rec)
				if err != nil {
					t.Errorf("Append: %v", err)
					return
				}
				if i%50 == 0 {
					if err := w.Sync(context.Background(), lsn); err != nil {
						t.Errorf("Sync: %v", err)
						return
					}
				}
				mu.Lock()
				if lsn > maxLSN {
					maxLSN = lsn
				}
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()

	if err := w.Sync(context.Background(), maxLSN); err != nil {
		t.Fatal(err)
	}
	w.Close()

	recs := readAllRecords(t, vfd, opts.Dir)
	want := goroutines * perGoroutine
	if len(recs) != want {
		t.Fatalf("got %d records, want %d", len(recs), want)
	}

	// Every LSN must be unique.
	seen := make(map[LSN]struct{}, len(recs))
	for _, rec := range recs {
		if _, dup := seen[rec.LSN]; dup {
			t.Fatalf("duplicate LSN %v", rec.LSN)
		}
		seen[rec.LSN] = struct{}{}
	}

	// LSNs must be strictly increasing in on-disk order.
	for i := 1; i < len(recs); i++ {
		if recs[i].LSN <= recs[i-1].LSN {
			t.Fatalf("LSN not increasing at %d: %v then %v",
				i, recs[i-1].LSN, recs[i].LSN)
		}
	}
}

// -----------------------------------------------------------------------------
// ReadAt
// -----------------------------------------------------------------------------

// TestWALReadAtRoundTrip appends a batch of records with distinct payloads,
// then reads each one back by its assigned LSN.
func TestWALReadAtRoundTrip(t *testing.T) {
	opts := testOptions(t)
	vfd := disk.NewCachedVFD(8)

	w, err := Open(vfd, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const n = 50
	type expected struct {
		lsn  LSN
		data []byte
		typ  RecordType
	}
	want := make([]expected, n)

	for i := 0; i < n; i++ {
		data := []byte(fmt.Sprintf("record-%d-payload", i))
		typ := RecInsert
		if i%2 == 0 {
			typ = RecUpdate
		}
		lsn, err := w.Append(&Record{
			Type:  typ,
			Flags: FlagHasAfter,
			Data:  data,
		})
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		want[i] = expected{lsn: lsn, data: data, typ: typ}
	}

	if err := w.Sync(context.Background(), want[n-1].lsn); err != nil {
		t.Fatal(err)
	}

	for i, e := range want {
		got, err := w.ReadAt(e.lsn)
		if err != nil {
			t.Fatalf("ReadAt %d (%v): %v", i, e.lsn, err)
		}
		if got.LSN != e.lsn {
			t.Errorf("record %d: LSN got %v want %v", i, got.LSN, e.lsn)
		}
		if got.Type != e.typ {
			t.Errorf("record %d: Type got %v want %v", i, got.Type, e.typ)
		}
		if string(got.Data) != string(e.data) {
			t.Errorf("record %d: Data got %q want %q", i, got.Data, e.data)
		}
	}
}

// TestWALReadAtAcrossRotation verifies ReadAt can locate records that live
// in different segments after rotation.
func TestWALReadAtAcrossRotation(t *testing.T) {
	opts := testOptions(t)
	opts.SegmentSize = 1024 // force rotation
	opts.MaxBatch = 256
	vfd := disk.NewCachedVFD(16)

	w, err := Open(vfd, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const n = 200
	lsns := make([]LSN, n)
	data := make([][]byte, n)
	for i := 0; i < n; i++ {
		data[i] = []byte(fmt.Sprintf("payload-%03d", i))
		lsn, err := w.Append(&Record{
			Type: RecInsert,
			Data: data[i],
		})
		if err != nil {
			t.Fatal(err)
		}
		lsns[i] = lsn
	}
	if err := w.Sync(context.Background(), lsns[n-1]); err != nil {
		t.Fatal(err)
	}

	// Verify every record is recoverable.
	for i := 0; i < n; i++ {
		rec, err := w.ReadAt(lsns[i])
		if err != nil {
			t.Fatalf("ReadAt %d (%v): %v", i, lsns[i], err)
		}
		if string(rec.Data) != string(data[i]) {
			t.Errorf("record %d: got %q want %q", i, rec.Data, data[i])
		}
	}
}

// TestWALReadAtBadLSN verifies ReadAt returns an error (not a panic) for
// LSNs that don't point at a valid record.
func TestWALReadAtBadLSN(t *testing.T) {
	opts := testOptions(t)
	vfd := disk.NewCachedVFD(8)

	w, err := Open(vfd, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Empty WAL: any LSN is invalid.
	if _, err := w.ReadAt(MakeLSN(1, 0)); err == nil {
		t.Error("ReadAt on empty WAL: expected error")
	}

	// Append one record so the segment exists.
	lsn, _ := w.Append(&Record{Type: RecInsert, Data: []byte("x")})
	if err := w.Sync(context.Background(), lsn); err != nil {
		t.Fatal(err)
	}

	// LSN past the end of valid records.
	if _, err := w.ReadAt(MakeLSN(1, 999999)); err == nil {
		t.Error("ReadAt past end: expected error")
	}

	// LSN in a segment that doesn't exist.
	if _, err := w.ReadAt(MakeLSN(99, 0)); err == nil {
		t.Error("ReadAt on missing segment: expected error")
	}

	// LSN in the middle of a record.
	if _, err := w.ReadAt(MakeLSN(1, 5)); err == nil {
		t.Error("ReadAt mid-record: expected error")
	}
}
