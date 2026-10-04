package common

import "testing"

// TestPageSize tests the PageSize constant
func TestPageSize(t *testing.T) {
	if PageSize != 8192 {
		t.Fatalf("PageSize = %d, want 8192", PageSize)
	}
}

// TestInvalidBlockID tests the InvalidBlockID constant
func TestInvalidBlockID(t *testing.T) {
	var expected BlockID = ^BlockID(0)
	if InvalidBlockID != expected {
		t.Fatalf("InvalidBlockID = %v, want %v", InvalidBlockID, expected)
	}
}

// TestMaxBlockID tests the MaxBlockID constant
func TestMaxBlockID(t *testing.T) {
	var expected BlockID = InvalidBlockID - 1
	if MaxBlockID != expected {
		t.Fatalf("MaxBlockID = %v, want %v", MaxBlockID, expected)
	}
}

// TestInvalidRelationID tests the InvalidRelationID constant
func TestInvalidRelationID(t *testing.T) {
	var expected RelationID = ^RelationID(0)
	if InvalidRelationID != expected {
		t.Fatalf("InvalidRelationID = %v, want %v", InvalidRelationID, expected)
	}
}

// TestInvalidTransactionID tests the InvalidTransactionID constant
func TestInvalidTransactionID(t *testing.T) {
	var expected TransactionID = ^TransactionID(0)
	if InvalidTransactionID != expected {
		t.Fatalf("InvalidTransactionID = %v, want %v", InvalidTransactionID, expected)
	}
}

// TestMaxTransactionID tests the MaxTransactionID constant
func TestMaxTransactionID(t *testing.T) {
	var expected TransactionID = InvalidTransactionID - 1
	if MaxTransactionID != expected {
		t.Fatalf("MaxTransactionID = %v, want %v", MaxTransactionID, expected)
	}
}

// TestInvalidCommandID tests the InvalidTransactionID constant
func TestInvalidCommandID(t *testing.T) {
	var expected CommandID = ^CommandID(0)
	if InvalidCommandID != expected {
		t.Fatalf("InvalidCommandIDs = %v, want %v", InvalidCommandID, expected)
	}
}

// TestMaxCommandID tests the MaxCommandID constant
func TestMaxCommandID(t *testing.T) {
	var expected CommandID = InvalidCommandID - 1
	if MaxCommandID != expected {
		t.Fatalf("MaxCommandID = %v, want %v", MaxCommandID, expected)
	}
}

func TestDataFileExtension(t *testing.T) {
	if DataFileExtension != ".data" {
		t.Fatalf("data file extension not matching expectation of %v", DataFileExtension)
	}
}
