//go:build darwin
// +build darwin

package disk

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/surajjagdev/ToyDB/internal/common"
)

func TestDirectManager_WriteReadPage(t *testing.T) {
	tmpDir := t.TempDir()

	// Create manager
	dm, err := NewDirectManager(tmpDir, 10, 10)
	if err != nil {
		t.Fatalf("Failed to create DirectManager: %v", err)
	}

	relID := common.RelationID(1)
	forkID := common.ForkMain
	BlockID := common.BlockID(0)

	// Prepare buffer
	data := make([]byte, common.PageSize)
	for i := 0; i < len(data); i++ {
		data[i] = byte(i % 256)
	}

	// Write page
	if err := dm.WritePage(relID, forkID, BlockID, data, true); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}

	// Read back
	readBuf := make([]byte, common.PageSize)
	if err := dm.ReadPage(relID, forkID, BlockID, readBuf); err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}

	// Compare
	if !bytes.Equal(data, readBuf) {
		t.Fatalf("Read data does not match written data")
	}
}

func TestDirectManagerWriteFailsWithoutAllocation(t *testing.T) {
	tmpDir := t.TempDir()
	dm, _ := NewDirectManager(tmpDir, 10, 10)

	rel := common.RelationID(1)
	fork := common.ForkMain
	pid := common.BlockID(0)

	// write page
	// Prepare buffer
	data := make([]byte, common.PageSize)

	for i := 0; i < len(data); i++ {
		data[i] = byte(i % 256)
	}

	err := dm.WritePage(rel, fork, pid, data, false)

	if err == nil {
		t.Fatalf("Write page should have failed but has passed")
	}
}

func TestDirectManager_FileCreated(t *testing.T) {
	tmpDir := t.TempDir()
	dm, _ := NewDirectManager(tmpDir, 10, 10)

	relID := common.RelationID(1)
	forkID := common.ForkMain
	BlockID := common.BlockID(0)

	data := make([]byte, common.PageSize)
	if err := dm.WritePage(relID, forkID, BlockID, data, true); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}

	// Check that file exists
	path := filepath.Join(tmpDir, "1") // first segment for fork main
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatalf("Expected file %s to exist", path)
	}
}

func TestDirectManager_MultiplePages(t *testing.T) {
	tmpDir := t.TempDir()
	dm, _ := NewDirectManager(tmpDir, 10, 10)

	relID := common.RelationID(2)
	forkID := common.ForkMain

	for BlockID := common.BlockID(0); BlockID < 5; BlockID++ {
		data := make([]byte, common.PageSize)
		data[0] = byte(BlockID)
		if err := dm.WritePage(relID, forkID, BlockID, data, true); err != nil {
			t.Fatalf("WritePage %d failed: %v", BlockID, err)
		}
	}

	for BlockID := common.BlockID(0); BlockID < 5; BlockID++ {
		readBuf := make([]byte, common.PageSize)
		if err := dm.ReadPage(relID, forkID, BlockID, readBuf); err != nil {
			t.Fatalf("ReadPage %d failed: %v", BlockID, err)
		}
		if readBuf[0] != byte(BlockID) {
			t.Fatalf("Read data mismatch for page %d", BlockID)
		}
	}
}

func TestDirectManagerAllocateBlock(t *testing.T) {
	tmpDir := t.TempDir()
	dm, _ := NewDirectManager(tmpDir, 10, 10)

	rel := common.RelationID(1)
	fork := common.ForkMain

	for i := 0; i < 10; i++ {
		pid, err := dm.AllocateBlock(rel, fork)
		if err != nil {
			t.Fatalf("AllocateBlock failed: %v", err)
		}
		if pid != common.BlockID(i) {
			t.Fatalf("expected block id %d, got %d", i, pid)
		}
	}
}

func TestDirectManagerFsync(t *testing.T) {
	tmpDir := t.TempDir()
	dm, _ := NewDirectManager(tmpDir, 10, 10)

	rel := common.RelationID(1)
	fork := common.ForkMain

	pid, err := dm.AllocateBlock(rel, fork)

	if err != nil {
		t.Fatalf("AllocateBlock failed: %v", err)
	}

	// write page
	// Prepare buffer
	data := make([]byte, common.PageSize)
	for i := 0; i < len(data); i++ {
		data[i] = byte(i % 256)
	}

	err = dm.WritePage(rel, fork, pid, data, false)

	if err != nil {
		t.Fatalf("Write page failed: %v", err)
	}

	// fsync contents and try to read it
	err = dm.SyncPage(rel, fork, pid)

	if err != nil {
		t.Fatalf("Fsync failed for single page: %v", err)
	}

	readBuffer := make([]byte, common.PageSize)

	err = dm.ReadPage(rel, fork, pid, readBuffer)

	if err != nil {
		t.Fatalf("Read page failed: %v", err)
	}
}
