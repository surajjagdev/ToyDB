package heap

import (
	"bytes"
	"testing"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/page"
)

func newTestPage(t *testing.T) *HeapPage {
	t.Helper()
	p := page.Page(make([]byte, common.PageSize))
	h := InitHeapPage(p)
	return h
}

func TestReplayInsertBasic(t *testing.T) {
	p := newTestPage(t)
	data := []byte("payload")
	blob := p.BuildTupleBytes(data, nil, 1, 0, 100, 0, 1)

	if err := p.ReplayInsert(0, blob); err != nil {
		t.Fatalf("ReplayInsert: %v", err)
	}
	if !p.DoesTupleInSlotExist(0) {
		t.Fatal("tuple not installed")
	}
	got := p.GetTupleWithSlot(0)
	if !bytes.Equal(got, blob) {
		t.Errorf("tuple bytes mismatch")
	}
}

func TestReplayInsertIdempotent(t *testing.T) {
	p := newTestPage(t)
	data := []byte("payload")
	blob := p.BuildTupleBytes(data, nil, 1, 0, 100, 0, 1)

	if err := p.ReplayInsert(0, blob); err != nil {
		t.Fatal(err)
	}
	// Second replay of the same blob: no-op.
	if err := p.ReplayInsert(0, blob); err != nil {
		t.Fatalf("idempotent replay failed: %v", err)
	}
}

func TestReplayInsertDetectsConflict(t *testing.T) {
	p := newTestPage(t)
	blobA := p.BuildTupleBytes([]byte("A"), nil, 1, 0, 100, 0, 1)
	blobB := p.BuildTupleBytes([]byte("B"), nil, 1, 0, 100, 0, 1)

	if err := p.ReplayInsert(0, blobA); err != nil {
		t.Fatal(err)
	}
	err := p.ReplayInsert(0, blobB)
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}
}

func TestReplayDeleteBasic(t *testing.T) {
	p := newTestPage(t)
	blob := p.BuildTupleBytes([]byte("x"), nil, 1, 0, 100, 0, 1)
	if err := p.ReplayInsert(0, blob); err != nil {
		t.Fatal(err)
	}

	if err := p.ReplayDelete(0, 200); err != nil {
		t.Fatalf("ReplayDelete: %v", err)
	}

	hdr, err := p.GetTupleHeader(0)
	if err != nil {
		t.Fatal(err)
	}
	if hdr.XMax != 200 {
		t.Errorf("XMax = %d, want 200", hdr.XMax)
	}
}

func TestReplayDeleteIdempotent(t *testing.T) {
	p := newTestPage(t)
	blob := p.BuildTupleBytes([]byte("x"), nil, 1, 0, 100, 0, 1)
	if err := p.ReplayInsert(0, blob); err != nil {
		t.Fatal(err)
	}
	if err := p.ReplayDelete(0, 200); err != nil {
		t.Fatal(err)
	}
	if err := p.ReplayDelete(0, 200); err != nil {
		t.Fatalf("idempotent delete failed: %v", err)
	}
}

func TestReplayDeleteDetectsConflict(t *testing.T) {
	p := newTestPage(t)
	blob := p.BuildTupleBytes([]byte("x"), nil, 1, 0, 100, 0, 1)
	if err := p.ReplayInsert(0, blob); err != nil {
		t.Fatal(err)
	}
	if err := p.ReplayDelete(0, 200); err != nil {
		t.Fatal(err)
	}
	err := p.ReplayDelete(0, 300)
	if err == nil {
		t.Fatal("expected conflict error for second delete by different tx")
	}
}

func TestReplayRemoveTupleBasic(t *testing.T) {
	p := newTestPage(t)
	blob := p.BuildTupleBytes([]byte("x"), nil, 1, 0, 100, 0, 1)
	if err := p.ReplayInsert(0, blob); err != nil {
		t.Fatal(err)
	}

	if err := p.ReplayRemoveTuple(0); err != nil {
		t.Fatalf("ReplayRemoveTuple: %v", err)
	}
	if p.DoesTupleInSlotExist(0) {
		t.Error("slot still reports tuple existence")
	}
}

func TestReplayRemoveTupleIdempotent(t *testing.T) {
	p := newTestPage(t)
	blob := p.BuildTupleBytes([]byte("x"), nil, 1, 0, 100, 0, 1)
	if err := p.ReplayInsert(0, blob); err != nil {
		t.Fatal(err)
	}
	if err := p.ReplayRemoveTuple(0); err != nil {
		t.Fatal(err)
	}
	if err := p.ReplayRemoveTuple(0); err != nil {
		t.Fatalf("idempotent remove failed: %v", err)
	}
}

func TestReplayRestoreTupleBasic(t *testing.T) {
	p := newTestPage(t)
	blob := p.BuildTupleBytes([]byte("original"), nil, 1, 0, 100, 0, 1)

	// Insert and delete, then restore.
	if err := p.ReplayInsert(0, blob); err != nil {
		t.Fatal(err)
	}
	if err := p.ReplayDelete(0, 200); err != nil {
		t.Fatal(err)
	}

	// Restore should un-set xmax by overwriting bytes.
	if err := p.ReplayRestoreTuple(0, blob); err != nil {
		t.Fatalf("ReplayRestoreTuple: %v", err)
	}

	hdr, err := p.GetTupleHeader(0)
	if err != nil {
		t.Fatal(err)
	}
	if hdr.XMax != 0 {
		t.Errorf("XMax after restore = %d, want 0", hdr.XMax)
	}
}

func TestReplayRestoreTupleIdempotent(t *testing.T) {
	p := newTestPage(t)
	blob := p.BuildTupleBytes([]byte("original"), nil, 1, 0, 100, 0, 1)
	if err := p.ReplayInsert(0, blob); err != nil {
		t.Fatal(err)
	}
	if err := p.ReplayDelete(0, 200); err != nil {
		t.Fatal(err)
	}

	// Restore twice.
	if err := p.ReplayRestoreTuple(0, blob); err != nil {
		t.Fatal(err)
	}
	if err := p.ReplayRestoreTuple(0, blob); err != nil {
		t.Fatalf("idempotent restore failed: %v", err)
	}
}

func TestReplayRestoreTupleSizeMismatch(t *testing.T) {
	p := newTestPage(t)
	blob := p.BuildTupleBytes([]byte("original"), nil, 1, 0, 100, 0, 1)
	if err := p.ReplayInsert(0, blob); err != nil {
		t.Fatal(err)
	}
	// Try to restore with wrong-size bytes.
	if err := p.ReplayRestoreTuple(0, blob[:len(blob)-8]); err == nil {
		t.Fatal("expected size mismatch error")
	}
}

func TestReplayMultipleInserts(t *testing.T) {
	p := newTestPage(t)
	for i := 0; i < 5; i++ {
		blob := p.BuildTupleBytes([]byte{byte(i)}, nil, 1, common.SlotIndex(i), 100, 0, 1)
		if err := p.ReplayInsert(common.SlotIndex(i), blob); err != nil {
			t.Fatalf("ReplayInsert slot %d: %v", i, err)
		}
	}
	for i := 0; i < 5; i++ {
		if !p.DoesTupleInSlotExist(common.SlotIndex(i)) {
			t.Errorf("slot %d missing", i)
		}
	}
}
