package common

import "testing"

// TestPageSize tests the PageSize constant
func TestPageSize(t *testing.T) {
	if PageSize != 4096 {
		t.Fatalf("PageSize = %d, want 4096", PageSize)
	}
}

// TestInvalidPageID tests the InvalidPageID constant
func TestInvalidPageID(t *testing.T) {
	var expected PageID = ^PageID(0)
	if InvalidPageID != expected {
		t.Fatalf("InvalidPageID = %v, want %v", InvalidPageID, expected)
	}
}

// TestMaxPageID tests the MaxPageID constant
func TestMaxPageID(t *testing.T) {
	var expected PageID = InvalidPageID - 1
	if MaxPageID != expected {
		t.Fatalf("MaxPageID = %v, want %v", MaxPageID, expected)
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

// TestInvalidLogSeqNumber tests the InvalidLogSeqNumber constant
func TestInvalidLogSeqNumber(t *testing.T) {
	var expected LogSeqNumber = ^LogSeqNumber(0)
	if InvalidLogSeqNumber != expected {
		t.Fatalf("InvalidLogSeqNumber = %v, want %v", InvalidLogSeqNumber, expected)
	}
}

// TestMaxLogSeqNumber tests the MaxLogSeqNumber constant
func TestMaxLogSeqNumber(t *testing.T) {
	var expected LogSeqNumber = InvalidLogSeqNumber - 1
	if MaxLogSeqNumber != expected {
		t.Fatalf("MaxLogSeqNumber = %v, want %v", MaxLogSeqNumber, expected)
	}
}

func TestDataFileExtension(t *testing.T) {
	if DataFileExtension != ".data" {
		t.Fatalf("data file extension not matching expectation of %v", DataFileExtension)
	}
}
