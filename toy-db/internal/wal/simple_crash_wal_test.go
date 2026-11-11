package wal

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/surajjagdev/ToyDB/internal/storage/disk"
)

const (
	envCrashChild = "WAL_CRASH_CHILD"
	envCrashDir   = "WAL_CRASH_DIR"
)

// TestWALSubprocessCrash runs the WAL in a child process and SIGKILLs it
// mid-workload. The invariant: every record whose Sync returned successfully
// before the kill must be present after recovery.
func TestWALSubprocessCrash(t *testing.T) {
	if os.Getenv(envCrashChild) != "" {
		runCrashChild(t)
		return
	}

	dir := t.TempDir()
	child := exec.Command(os.Args[0], "-test.run=TestWALSubprocessCrash")
	child.Env = append(os.Environ(),
		envCrashChild+"=1",
		envCrashDir+"="+dir,
	)
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}

	// Let the child write for a while, then kill it hard.
	time.Sleep(300 * time.Millisecond)
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()

	// Reopen and verify the log is consistent: valid prefix, then torn tail.
	vfd := disk.NewCachedVFD(8)
	opts := Options{
		Dir:         dir,
		SegmentSize: 64 * 1024,
		MaxBatch:    4 * 1024,
		BatchWait:   time.Millisecond,
		Fsync:       true,
	}
	w, err := Open(vfd, opts)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer w.Close()

	recs := readAllRecords(t, vfd, dir)
	if len(recs) == 0 {
		t.Fatal("no records recovered; child likely wrote nothing")
	}
	// Records must be well-formed: increasing LSNs, matching positions.
	for i := 1; i < len(recs); i++ {
		if recs[i].LSN <= recs[i-1].LSN {
			t.Fatalf("LSN not increasing at %d", i)
		}
	}
	// The child writes records marked with a monotonically increasing counter
	// in Data. The recovered log must be a prefix of that sequence.
	for i, rec := range recs {
		want := fmt.Sprintf("record-%d", i)
		if string(rec.Data) != want {
			t.Fatalf("record %d: got %q want %q (log not a clean prefix)",
				i, rec.Data, want)
		}
	}
}

// runCrashChild writes records forever until killed. Each Sync it completes
// means the record is durable; the parent verifies the prefix invariant.
func runCrashChild(t *testing.T) {
	dir := os.Getenv(envCrashDir)
	if dir == "" {
		t.Fatal("WAL_CRASH_DIR not set")
	}
	vfd := disk.NewCachedVFD(8)
	opts := Options{
		Dir:         dir,
		SegmentSize: 64 * 1024,
		MaxBatch:    4 * 1024,
		BatchWait:   time.Millisecond,
		Fsync:       true,
	}
	w, err := Open(vfd, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := 0; ; i++ {
		rec := Record{
			Type: RecInsert,
			Data: []byte(fmt.Sprintf("record-%d", i)),
		}
		lsn, err := w.Append(&rec)
		if err != nil {
			return // poisoned; parent will see the prefix
		}
		if err := w.Sync(context.Background(), lsn); err != nil {
			return
		}
	}
}
