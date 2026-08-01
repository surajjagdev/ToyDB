package access

import (
	"testing"
)

func TestReturnsCeilingOfRequiredBytesForColumns(t *testing.T) {
	// make col len 10. So ceil should be 16 bits -> 2 bytes
	cols := 10
	expectedBytes := 2
	actualBytes := GetBitMapSizeInBytes(cols)

	if actualBytes != expectedBytes {
		t.Fatalf("Expected to require %d bytes, got %d bytes", expectedBytes, actualBytes)
	}
}

func TestReturnsByteIndexAndBitIndex(t *testing.T) {
	// make col len 10. So ceil should be 16 bits -> 2 bytes
	index := 9

	byteIndex, bitIndex := getByteInfo(index)

	if byteIndex != 1 {
		t.Fatalf("Expected byte index to be 1, got %d", byteIndex)
	}
	if bitIndex != 1 {
		t.Fatalf("Expected bit index to be 1, got %d", bitIndex)
	}
}

func TestSetBitMap(t *testing.T) {
	bitMap := make([]byte, 2)

	for i := range len(bitMap) {
		if bitMap[i] != 0 {
			t.Fatalf("Expected bit to equal 0, got %v", bitMap[0])
		}
	}

	var expectedByte byte = 0
	expectedByte |= (1 << 2)

	// set the 10th column, so byte 1, bit 2
	SetBitInBitMap(bitMap, 10)

	if expectedByte != bitMap[1] {
		t.Fatalf("Expected byte to equal %v, got %v", expectedByte, bitMap[1])
	}
}

func TestIsBitSet(t *testing.T) {
	bitMap := make([]byte, 2)

	SetBitInBitMap(bitMap, 15)

	if !IsBitSet(bitMap, 15) {
		t.Fatal("expected column 15 to be set")
	}
	if IsBitSet(bitMap, 14) {
		t.Fatal("expected column 14 to be unset")
	}
	if bitMap[1] != (1 << 7) {
		t.Fatalf("expected byte 1 to be %08b, got %08b", 1<<7, bitMap[1])
	}
}

func TestUnsetBitMap(t *testing.T) {
	bitMap := make([]byte, 2)

	for i := range len(bitMap) {
		if bitMap[i] != 0 {
			t.Fatalf("Expected bit to equal 0, got %v", bitMap[0])
		}
	}

	var expectedByte byte = 0
	expectedByte |= (1 << 3)

	// set the 11th column, so byte 1, bit 3
	SetBitInBitMap(bitMap, 11)

	if expectedByte != bitMap[1] {
		t.Fatalf("Expected byte to equal %v, got %v", expectedByte, bitMap[1])
	}
	if !IsBitSet(bitMap, 11) {
		t.Fatal("expected column 11 to be set")
	}

	// unset the 11th column, so byte 1, bit 3 is 0
	SetBitInBitMap(bitMap, 11)

	if !IsBitSet(bitMap, 11) {
		t.Fatal("expected column 11 to be unset")
	}
}
