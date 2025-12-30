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
	pageID := common.PageID(0)

	// Prepare buffer
	data := make([]byte, common.PageSize)
	for i := 0; i < len(data); i++ {
		data[i] = byte(i % 256)
	}

	// Write page
	if err := dm.WritePage(relID, forkID, pageID, data, true); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}

	// Read back
	readBuf := make([]byte, common.PageSize)
	if err := dm.ReadPage(relID, forkID, pageID, readBuf); err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}

	// Compare
	if !bytes.Equal(data, readBuf) {
		t.Fatalf("Read data does not match written data")
	}
}

func TestDirectManager_FileCreated(t *testing.T) {
	tmpDir := t.TempDir()
	dm, _ := NewDirectManager(tmpDir, 10, 10)

	relID := common.RelationID(1)
	forkID := common.ForkMain
	pageID := common.PageID(0)

	data := make([]byte, common.PageSize)
	if err := dm.WritePage(relID, forkID, pageID, data, true); err != nil {
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

	for pageID := common.PageID(0); pageID < 5; pageID++ {
		data := make([]byte, common.PageSize)
		data[0] = byte(pageID)
		if err := dm.WritePage(relID, forkID, pageID, data, true); err != nil {
			t.Fatalf("WritePage %d failed: %v", pageID, err)
		}
	}

	for pageID := common.PageID(0); pageID < 5; pageID++ {
		readBuf := make([]byte, common.PageSize)
		if err := dm.ReadPage(relID, forkID, pageID, readBuf); err != nil {
			t.Fatalf("ReadPage %d failed: %v", pageID, err)
		}
		if readBuf[0] != byte(pageID) {
			t.Fatalf("Read data mismatch for page %d", pageID)
		}
	}
}

func TestDirectManagerAllocatePage(t *testing.T) {
	tmpDir := t.TempDir()
	dm, _ := NewDirectManager(tmpDir, 10, 10)

	rel := common.RelationID(1)
	fork := common.ForkMain

	for i := 0; i < 10; i++ {
		pid, err := dm.AllocatePage(rel, fork)
		if err != nil {
			t.Fatalf("AllocatePage failed: %v", err)
		}
		if pid != common.PageID(i) {
			t.Fatalf("expected page %d, got %d", i, pid)
		}
	}
}
