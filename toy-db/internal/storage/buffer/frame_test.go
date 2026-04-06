package buffer

import (
	"sync"
	"testing"

	"github.com/surajjagdev/ToyDB/internal/common"
)

func GetNewFrame(t *testing.T) *Frame {
	frame, err := NewFrame()
	if err != nil {
		t.Fatalf("failed to create frame: %v", err)
	}

	blockId, forkId, relId := frame.GetFrameIdentity()
	if blockId != common.InvalidBlockID {
		t.Fatalf("block id = %d, want %d", blockId, common.InvalidBlockID)
	}
	if forkId != common.InvalidForkID {
		t.Fatalf("fork id = %d, want %d", forkId, common.InvalidForkID)
	}
	if relId != common.InvalidRelationID {
		t.Fatalf("relation id = %d, want %d", relId, common.InvalidRelationID)
	}

	return frame
}

func TestSettingFrameIdentity(t *testing.T) {
	frame := GetNewFrame(t)

	defer frame.Free()

	expectedBlock := common.BlockID(1)
	expectedFork := common.ForkID(1)
	expectedRel := common.RelationID(1)

	frame.SetFrameIdentity(expectedBlock, expectedFork, expectedRel)

	blockId, forkId, relId := frame.GetFrameIdentity()

	if blockId != expectedBlock {
		t.Fatalf("block id = %d, want %d", blockId, expectedBlock)
	}
	if forkId != expectedFork {
		t.Fatalf("fork id = %d, want %d", forkId, expectedFork)
	}
	if relId != expectedRel {
		t.Fatalf("relation id = %d, want %d", relId, expectedRel)
	}
}

func TestSetIdentityWhilePinnedPanics(t *testing.T) {
	frame := GetNewFrame(t)
	defer frame.Free()

	frame.Pin()

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()

	frame.SetFrameIdentity(1, 1, 1)
}

func TestGettingNewFramePinCount(t *testing.T) {
	frame := GetNewFrame(t)
	defer frame.Free()

	currentPinCount := frame.GetPinCount()

	if frame.GetPinCount() != 0 {
		t.Fatalf("pin count = %d, want 0", currentPinCount)
	}
}

func TestUnpiningOnZeroPinCount(t *testing.T) {
	frame := GetNewFrame(t)
	defer frame.Free()

	defer func() {
		recovery := recover()
		if recovery == nil {
			t.Fatalf("expected panic on Unpin with zero pin count")
		}
	}()

	frame.Unpin()
}

func TestUnpinOnPinnedFrame(t *testing.T) {
	frame := GetNewFrame(t)

	defer frame.Free()

	frame.Pin()

	defer func() {
		recovery := recover()

		if recovery != nil {
			t.Fatalf("expected no panic on Unpin on pinned frame")
		}
	}()

	frame.Unpin()
}

func TestPinIncrementsCount(t *testing.T) {
	frame := GetNewFrame(t)
	defer frame.Free()

	frame.Pin()
	frame.Pin()

	if frame.GetPinCount() != 2 {
		t.Fatalf("expected pin count 2, got %d", frame.GetPinCount())
	}
}

func TestPinUnpinReturnsToZero(t *testing.T) {
	frame := GetNewFrame(t)
	defer frame.Free()

	frame.Pin()
	frame.Pin()
	frame.Unpin()
	frame.Unpin()

	if frame.GetPinCount() != 0 {
		t.Fatalf("expected 0, got %d", frame.GetPinCount())
	}
}

func TestGettingNewFrameIsDirty(t *testing.T) {
	frame := GetNewFrame(t)
	defer frame.Free()

	isFrameDirty := frame.IsDirty()

	if isFrameDirty {
		t.Fatalf("frame should not be dirty")
	}
}

func TestGettingFrameSetDirty(t *testing.T) {
	frame := GetNewFrame(t)
	defer frame.Free()

	frame.SetDirty()

	isFrameDirty := frame.IsDirty()

	if isFrameDirty == false {
		t.Fatalf("frame should be dirty")
	}
}

func TestDirtyFlagToggle(t *testing.T) {
	frame := GetNewFrame(t)
	defer frame.Free()

	frame.SetDirty()
	if !frame.IsDirty() {
		t.Fatal("expected dirty")
	}

	frame.ClearDirty()
	if frame.IsDirty() {
		t.Fatal("expected not dirty")
	}
}

func TestValidFlag(t *testing.T) {
	frame := GetNewFrame(t)
	defer frame.Free()

	if frame.IsValid() {
		t.Fatal("should not be valid initially")
	}

	frame.SetValid()
	if !frame.IsValid() {
		t.Fatal("expected valid")
	}

	frame.ClearValid()
	if frame.IsValid() {
		t.Fatal("expected not valid")
	}
}

func TestIOFlag(t *testing.T) {
	frame := GetNewFrame(t)
	defer frame.Free()

	frame.SetIOInProgress()
	if !frame.IsIOInProgress() {
		t.Fatal("expected IO in progress")
	}

	frame.ClearIOInProgress()
	if frame.IsIOInProgress() {
		t.Fatal("expected IO cleared")
	}
}

func TestUsageIncrementDecrement(t *testing.T) {
	frame := GetNewFrame(t)
	defer frame.Free()

	frame.IncrementUsage()
	frame.IncrementUsage()

	if frame.GetUsage() != 2 {
		t.Fatalf("expected usage 2, got %d", frame.GetUsage())
	}

	frame.DecrementUsage()
	if frame.GetUsage() != 1 {
		t.Fatalf("expected usage 1, got %d", frame.GetUsage())
	}
}

func TestUsageSaturation(t *testing.T) {
	frame := GetNewFrame(t)
	defer frame.Free()

	for i := 0; i < 20; i++ {
		frame.IncrementUsage()
	}

	if frame.GetUsage() != 15 {
		t.Fatalf("expected max 15, got %d", frame.GetUsage())
	}
}

func TestResetClearsState(t *testing.T) {
	frame := GetNewFrame(t)
	defer frame.Free()

	frame.Pin()
	frame.SetDirty()
	frame.SetValid()
	frame.SetIOInProgress()
	frame.IncrementUsage()

	frame.Reset(0)

	if frame.GetPinCount() != 0 {
		t.Fatal("pin not cleared")
	}
	if frame.IsDirty() {
		t.Fatal("dirty not cleared")
	}
	if frame.IsValid() {
		t.Fatal("valid not cleared")
	}
	if frame.IsIOInProgress() {
		t.Fatal("io flag not cleared")
	}
	if frame.GetUsage() != 0 {
		t.Fatal("usage not cleared")
	}
}

func TestCopyPageDataIndependence(t *testing.T) {
	frame := GetNewFrame(t)
	defer frame.Free()

	frame.Page[0] = 42

	copyPage, _ := frame.CopyPageData()
	copyPage[0] = 100

	if frame.Page[0] == 100 {
		t.Fatal("copy should not affect original")
	}
	if copyPage[0] != 100 {
		t.Fatal("copy should have new value")
	}
	if frame.Page[0] != 42 {
		t.Fatal("original should remain unchanged")
	}
}

func TestCopyDataToExistingFrame(t *testing.T) {
	src := GetNewFrame(t)
	dst := GetNewFrame(t)
	defer src.Free()
	defer dst.Free()

	src.Page[0] = 77

	src.CopyDataToExistingFrame(dst)

	if dst.Page[0] != 77 {
		t.Fatal("copy failed")
	}
}

// concurrent pin and unpin operations
func TestConcurrentPinUnpin(t *testing.T) {
	frame := GetNewFrame(t)
	defer frame.Free()

	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				frame.Pin()
				frame.Unpin()
			}
		}()
	}

	wg.Wait()

	if frame.GetPinCount() != 0 {
		t.Fatalf("expected 0, got %d", frame.GetPinCount())
	}
}
