package control

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/surajjagdev/ToyDB/internal/storage/disk"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

func setup(t *testing.T) (string, *disk.VFDCache) {
	t.Helper()
	dir := t.TempDir()
	vfd := disk.NewCachedVFD(8)
	t.Cleanup(func() { vfd.CloseAll(true) })
	return dir, vfd
}

func TestReadNotFound(t *testing.T) {
	dir, vfd := setup(t)
	_, err := Read(vfd, dir)
	if err != ErrNotFound {
		t.Errorf("Read on empty dir: got %v want ErrNotFound", err)
	}
}

func TestWriteReadRoundTrip(t *testing.T) {
	dir, vfd := setup(t)

	rec := &Record{
		RedoLSN:       wal.MakeLSN(5, 1024),
		CheckpointLSN: wal.MakeLSN(5, 2048),
		NextTxID:      42,
	}
	if err := Write(vfd, dir, rec); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := Read(vfd, dir)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if got.RedoLSN != rec.RedoLSN || got.CheckpointLSN != rec.CheckpointLSN || got.NextTxID != rec.NextTxID {
		t.Errorf("got %+v want %+v", got, rec)
	}
}

func TestWriteOverwrites(t *testing.T) {
	dir, vfd := setup(t)

	if err := Write(vfd, dir, &Record{RedoLSN: 100, CheckpointLSN: 200, NextTxID: 10}); err != nil {
		t.Fatal(err)
	}
	if err := Write(vfd, dir, &Record{RedoLSN: 300, CheckpointLSN: 400, NextTxID: 20}); err != nil {
		t.Fatal(err)
	}

	got, err := Read(vfd, dir)
	if err != nil {
		t.Fatal(err)
	}
	if got.RedoLSN != 300 || got.CheckpointLSN != 400 || got.NextTxID != 20 {
		t.Errorf("after overwrite: got %+v", got)
	}
}

func TestNoTempFileLeftBehind(t *testing.T) {
	dir, vfd := setup(t)
	if err := Write(vfd, dir, &Record{RedoLSN: 1, CheckpointLSN: 2, NextTxID: 3}); err != nil {
		t.Fatal(err)
	}

	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.Name() == TempName {
			t.Errorf("temp file %q left behind", e.Name())
		}
	}
}

func TestCorruptFileDetectedByCRC(t *testing.T) {
	dir, vfd := setup(t)
	if err := Write(vfd, dir, &Record{RedoLSN: 100, CheckpointLSN: 200, NextTxID: 10}); err != nil {
		t.Fatal(err)
	}

	// Corrupt directly via os (bypassing VFD) so we don't invalidate the
	// cache. Then reopen the VFD to force a fresh read.
	path := filepath.Join(dir, FileName)
	data, _ := os.ReadFile(path)
	data[10] ^= 0xFF
	os.WriteFile(path, data, 0o644)
	vfd.CloseAll(true)

	_, err := Read(vfd, dir)
	if err == nil {
		t.Fatal("expected error reading corrupt file")
	}
	if !isCorrupt(err) {
		t.Errorf("expected ErrCorrupt, got %v", err)
	}
}

func TestBadMagic(t *testing.T) {
	dir, vfd := setup(t)
	if err := Write(vfd, dir, &Record{RedoLSN: 1, CheckpointLSN: 2, NextTxID: 3}); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, FileName)
	data, _ := os.ReadFile(path)
	data[0], data[1], data[2], data[3] = 0, 0, 0, 0
	os.WriteFile(path, data, 0o644)
	vfd.CloseAll(true)

	_, err := Read(vfd, dir)
	if err == nil {
		t.Fatal("expected error for bad magic")
	}
	if !isCorrupt(err) {
		t.Errorf("expected ErrCorrupt, got %v", err)
	}
}

func TestRemove(t *testing.T) {
	dir, vfd := setup(t)
	if err := Write(vfd, dir, &Record{RedoLSN: 1, CheckpointLSN: 2, NextTxID: 3}); err != nil {
		t.Fatal(err)
	}
	if err := Remove(vfd, dir); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	_, err := Read(vfd, dir)
	if err != ErrNotFound {
		t.Errorf("after Remove: got %v want ErrNotFound", err)
	}

	// Remove on missing file is not an error.
	if err := Remove(vfd, dir); err != nil {
		t.Errorf("Remove on missing file: %v", err)
	}
}

func TestNilRecord(t *testing.T) {
	_, vfd := setup(t)
	if err := Write(vfd, t.TempDir(), nil); err == nil {
		t.Fatal("expected error for nil record")
	}
}

func isCorrupt(err error) bool {
	for err != nil {
		if err == ErrCorrupt {
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
