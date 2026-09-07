package fsm

import (
	"testing"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/page"
)

// Helper to create a blank FSM page
func createTestPage() *FSMPage {
	rawPage := make(page.Page, common.PageSize)
	return InitFSMPage(rawPage)
}

func TestCategoryChanges(t *testing.T) {
	// Test Category boundaries
	if FreeBytesToCategory(0) != 0 {
		t.Errorf("0 bytes should be category 0")
	}

	// page size is 8192 -> 32 bytes per category
	cat1Bytes := 1 * BytesPerCategory
	if FreeBytesToCategory(cat1Bytes) != 1 {
		t.Errorf("Expected category 1 for %d bytes", cat1Bytes)
	}

	// Test ceiling/max
	if FreeBytesToCategory(999999) != 255 {
		t.Errorf("Values above page size should clamp to category 255")
	}

	// Test back to bytes
	if CategoryToBytes(1) != BytesPerCategory {
		t.Errorf("Category 1 should reverse to %d bytes", BytesPerCategory)
	}
}

func TestFSMPage_EmptySearch(t *testing.T) {
	fsmPage := createTestPage()

	// An empty FSM page should have max category 0
	if fsmPage.GetMaxAvailable() != 0 {
		t.Errorf("Empty page should have max 0, got %d", fsmPage.GetMaxAvailable())
	}

	// Searching for any space should return -1 bc page is empty
	if idx := fsmPage.SearchAvailable(1); idx != -1 {
		t.Errorf("Expected -1 on empty page, got %d", idx)
	}
}

func TestFSMPage_UpdateAndSearch(t *testing.T) {
	fsmPage := createTestPage()

	// Update Leaf 5 to have category 100
	fsmPage.UpdateAvailable(5, 100)

	// The root should be 100
	if fsmPage.GetMaxAvailable() != 100 {
		t.Errorf("Root max should be 100, got %d", fsmPage.GetMaxAvailable())
	}

	// Search for space requiring 50 (Should find leaf 5)
	idx := fsmPage.SearchAvailable(50)

	if idx != 5 {
		t.Errorf("Expected to find space at leaf 5, got %d", idx)
	}

	// Search for space requiring 150 (Should fail)
	idx = fsmPage.SearchAvailable(150)
	if idx != -1 {
		t.Errorf("Expected -1 (not enough space), got %d", idx)
	}
}

func TestFSMPage_MultipleUpdates_BubbleUp(t *testing.T) {
	fsmPage := createTestPage()

	// Set Leaf 10 to 50
	fsmPage.UpdateAvailable(10, 50)
	if fsmPage.GetMaxAvailable() != 50 {
		t.Errorf("Root max should be 50")
	}

	// Set Leaf 20 to 200 (Root should increase!)
	fsmPage.UpdateAvailable(20, 200)
	if fsmPage.GetMaxAvailable() != 200 {
		t.Errorf("Root max should be 200, got %d", fsmPage.GetMaxAvailable())
	}

	// Search for 150 (Should route us to Leaf 20)
	idx := fsmPage.SearchAvailable(150)
	if idx != 20 {
		t.Errorf("Expected to route to leaf 20, got %d", idx)
	}

	// Now reduce Leaf 20 to 10. The root should dynamically bubble DOWN to 50!
	fsmPage.UpdateAvailable(20, 10)
	if fsmPage.GetMaxAvailable() != 50 {
		t.Errorf("Root max should have bubbled down to 50, got %d", fsmPage.GetMaxAvailable())
	}

	// Search for 40 (Should route us to Leaf 10 now)
	idx = fsmPage.SearchAvailable(40)
	if idx != 10 {
		t.Errorf("Expected to route back to leaf 10, got %d", idx)
	}
}

func TestFSMPage_FullCapacity(t *testing.T) {
	fsmPage := createTestPage()
	maxLeaves := fsmPage.getNumLeaves()

	// Update the absolute last leaf in the array
	lastLeaf := maxLeaves - 1
	fsmPage.UpdateAvailable(lastLeaf, 255)

	if fsmPage.GetMaxAvailable() != 255 {
		t.Errorf("Failed to bubble up from the very last leaf")
	}

	if idx := fsmPage.SearchAvailable(200); idx != lastLeaf {
		t.Errorf("Failed to route to the last leaf, got %d", idx)
	}

	// We should find available space even for 1 category = 32 bytes
	if idx := fsmPage.SearchAvailable(1); idx != lastLeaf {
		t.Errorf("Failed to route to the last leaf, got %d", idx)
	}
}

func TestFSMPage_PicksLeftOverRight(t *testing.T) {
	fsmPage := createTestPage()

	// If two leaves have same space, search available should be left bias
	fsmPage.UpdateAvailable(50, 100)
	fsmPage.UpdateAvailable(10, 100)

	idx := fsmPage.SearchAvailable(50)
	if idx != 10 {
		t.Errorf("Expected Left-Bias routing to leaf 10, got %d", idx)
	}
}
