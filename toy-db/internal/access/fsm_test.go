package access

import (
	"testing"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/buffer"
	"github.com/surajjagdev/ToyDB/internal/storage/disk"
	"github.com/surajjagdev/ToyDB/internal/storage/page/fsm"
)

// Helper to set up a real Buffer Pool for testing the Access Layer
func setupTestFSM(t *testing.T) (*FSM, *buffer.BufferPool) {
	tempDir := t.TempDir()

	// DirectManager with 10 max files
	dm, err := disk.NewDirectManager(tempDir, 10, 10)
	if err != nil {
		t.Fatalf("Failed to create DirectManager: %v", err)
	}

	// BufferPool with 2 partitions, 4 frames each
	bp, err := buffer.NewBufferPool(dm, 2, 4)
	if err != nil {
		t.Fatalf("Failed to create BufferPool: %v", err)
	}

	fsmCoord := NewFSM(bp)
	return fsmCoord, bp
}

func TestGetFSMOffset(t *testing.T) {
	// Test Block 0
	fsmBlock, offset := getFSMOffset(0)
	if fsmBlock != 0 || offset != 0 {
		t.Errorf("Block 0 should be FSM 0, Offset 0. Got FSM %d, Offset %d", fsmBlock, offset)
	}

	// Test the exact boundary (e.g. 2048)
	// This should roll over to the FIRST leaf of the SECOND FSM page
	boundaryBlock := common.BlockID(fsm.LeavesPerPage)
	fsmBlock, offset = getFSMOffset(boundaryBlock)
	if fsmBlock != 1 || offset != 0 {
		t.Errorf("Boundary block %d should be FSM 1, Offset 0. Got FSM %d, Offset %d", boundaryBlock, fsmBlock, offset)
	}

	// Test deep into the second page
	deepBlock := common.BlockID(fsm.LeavesPerPage + 42)
	fsmBlock, offset = getFSMOffset(deepBlock)
	if fsmBlock != 1 || offset != 42 {
		t.Errorf("Deep block %d should be FSM 1, Offset 42. Got FSM %d, Offset %d", deepBlock, fsmBlock, offset)
	}
}

func TestFSM_RecordAndGetFreeSpace(t *testing.T) {
	fsmCoord, bp := setupTestFSM(t)
	defer bp.Shutdown()

	rel := common.RelationID(1)

	// 1. Ask for space on an empty database (Should return InvalidBlockID safely)
	block, err := fsmCoord.GetBlockWithFreeSpace(rel, 500)
	if err != nil {
		t.Fatalf("Expected no error on empty db, got %v", err)
	}
	if block != common.InvalidBlockID {
		t.Errorf("Expected InvalidBlockID, got %d", block)
	}

	// 2. Record that Table Block 42 has 1000 bytes free
	err = fsmCoord.RecordFreeSpace(rel, 42, 1000)
	if err != nil {
		t.Fatalf("Failed to record free space: %v", err)
	}

	// 3. Ask for 500 bytes (Should route us to Block 42)
	block, err = fsmCoord.GetBlockWithFreeSpace(rel, 500)
	if err != nil {
		t.Fatalf("Failed to get free space: %v", err)
	}
	if block != 42 {
		t.Errorf("Expected Block 42, got %d", block)
	}

	// 4. Ask for 2000 bytes (FSM Page exists, but not enough space. Should return InvalidBlockID)
	block, err = fsmCoord.GetBlockWithFreeSpace(rel, 2000)
	if err != nil {
		t.Fatalf("Failed to get free space: %v", err)
	}
	if block != common.InvalidBlockID {
		t.Errorf("Expected InvalidBlockID for request too large, got %d", block)
	}
}

func TestFSM_MultipleFSMPages(t *testing.T) {
	fsmCoord, bp := setupTestFSM(t)
	defer bp.Shutdown()

	rel := common.RelationID(2)

	// 1. Record 500 bytes on Table Block 10 (Lives on FSM Page 0)
	err := fsmCoord.RecordFreeSpace(rel, 10, 500)
	if err != nil {
		t.Fatalf("Failed to record free space: %v", err)
	}

	// 2. Record 3000 bytes on a Table Block that lives on FSM Page 1!
	targetBlock := common.BlockID(fsm.LeavesPerPage + 99)
	err = fsmCoord.RecordFreeSpace(rel, targetBlock, 3000)
	if err != nil {
		t.Fatalf("Failed to record free space on 2nd FSM page: %v", err)
	}

	// 3. Ask for 2000 bytes.
	// FSM Page 0 only has 500 bytes maximum, so the loop in GetBlockWithFreeSpace
	// MUST advance to FSM Page 1 to find our target block!
	block, err := fsmCoord.GetBlockWithFreeSpace(rel, 2000)
	if err != nil {
		t.Fatalf("Failed to get block across multiple pages: %v", err)
	}

	if block != targetBlock {
		t.Errorf("Expected FSM loop to find Block %d, but got %d", targetBlock, block)
	}
}
