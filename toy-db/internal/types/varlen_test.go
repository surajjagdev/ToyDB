package types

import (
	"bytes"
	"testing"

	"github.com/surajjagdev/ToyDB/internal/common"
)

func TestNewVarLen(t *testing.T) {
	s := "hello there"
	v := NewVarChar(s)

	if v.GetTypeID() != VarcharType {
		t.Fatalf("expected %v, got %v", VarcharType, v.GetTypeID())
	}

	if v.IsNull() {
		t.Fatal("expected value to be valid")
	}
}

func TestVarLenSerialize(t *testing.T) {
	s := "hello there"
	v := NewVarChar(s)

	serialized := v.Serialize()

	// 1. Check Length Header
	expectedLenBuf := make([]byte, 4)
	common.ByteOrder.PutUint32(expectedLenBuf, uint32(len(s)))

	if !bytes.Equal(expectedLenBuf, serialized[:4]) {
		t.Fatalf("expected length header %v, got %v", expectedLenBuf, serialized[:4])
	}

	// 2. Check String Data
	if !bytes.Equal([]byte(s), serialized[4:]) {
		t.Fatalf("expected data %v, got %v", []byte(s), serialized[4:])
	}
}

func TestVarLenCompare(t *testing.T) {
	v1 := NewVarChar("apple")
	v2 := NewVarChar("banana")
	v3 := NewVarChar("apple")
	v4 := NewVarChar("2apple")

	// a < b
	if v1.Compare(v2) != -1 {
		t.Errorf("Expected apple < banana (-1), got %d", v1.Compare(v2))
	}

	// b > a
	if v2.Compare(v1) != 1 {
		t.Errorf("Expected banana > apple (1), got %d", v2.Compare(v1))
	}

	// 2apple is < than apple
	if v4.Compare(v3) != -1 {
		t.Errorf("Expected 2apple > apple (-1), got %d", v4.Compare(v3))
	}

	// a == a
	if v1.Compare(v3) != 0 {
		t.Errorf("Expected apple == apple (0), got %d", v1.Compare(v3))
	}
}

func TestVarLenNull(t *testing.T) {
	vNull := VarLenValue{Type: VarcharType, Valid: false}
	vValid := NewVarChar("test")

	if !vNull.IsNull() {
		t.Errorf("Expected value to be null")
	}

	if vNull.Serialize() != nil {
		t.Errorf("Expected nil slice when serializing NULL")
	}

	if vNull.Compare(vValid) != 1 {
		t.Errorf("Expected NULL > Valid (1), got %d", vNull.Compare(vValid))
	}
	if vValid.Compare(vNull) != -1 {
		t.Errorf("Expected Valid < NULL (-1), got %d", vValid.Compare(vNull))
	}
}
