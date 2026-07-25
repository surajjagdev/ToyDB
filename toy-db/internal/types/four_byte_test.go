package types

import (
	"bytes"
	"testing"

	"github.com/surajjagdev/ToyDB/internal/common"
)

func TestNewInteger(t *testing.T) {
	v := NewInteger(32)

	if v.GetTypeID() != IntegerType {
		t.Fatalf("expected %v, got %v", IntegerType, v.GetTypeID())
	}

	if v.IsNull() {
		t.Fatal("expected value to be valid")
	}

	if int32(v.Value) != 32 {
		t.Fatalf("expected value 32, got %d", int32(v.Value))
	}
}

func TestNewDate(t *testing.T) {
	v := NewDate(100)

	if v.GetTypeID() != DateType {
		t.Fatalf("expected %v, got %v", DateType, v.GetTypeID())
	}

	if v.IsNull() {
		t.Fatal("expected value to be valid")
	}

	if int32(v.Value) != 100 {
		t.Fatalf("expected 100, got %d", int32(v.Value))
	}
}

func TestNewOid(t *testing.T) {
	v := NewOid(42)

	if v.GetTypeID() != OidType {
		t.Fatalf("expected %v, got %v", OidType, v.GetTypeID())
	}

	if v.IsNull() {
		t.Fatal("expected value to be valid")
	}

	if v.Value != 42 {
		t.Fatalf("expected 42, got %d", v.Value)
	}
}

func TestSerialize(t *testing.T) {
	var val int32 = 1
	v := NewInteger(val)

	buf := make([]byte, 4)
	common.ByteOrder.PutUint32(buf, uint32(val))

	if !bytes.Equal(v.Serialize(), buf) {
		t.Fatalf("expected %v, got %v", buf, v.Serialize())
	}
}

func TestSerializeNull(t *testing.T) {
	v := Fixed4ByteValue{
		Type:  IntegerType,
		Valid: false,
	}

	if v.Serialize() != nil {
		t.Fatal("expected nil serialization for null")
	}
}

func TestCompareIntegers(t *testing.T) {
	tests := []struct {
		name string
		a    int32
		b    int32
		want int
	}{
		{"less", 1, 2, -1},
		{"equal", 2, 2, 0},
		{"greater", 3, 2, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewInteger(tt.a).Compare(NewInteger(tt.b))
			if got != tt.want {
				t.Fatalf("expected %d, got %d", tt.want, got)
			}
		})
	}
}

func TestCompareOids(t *testing.T) {
	tests := []struct {
		a, b uint32
		want int
	}{
		{1, 2, -1},
		{2, 2, 0},
		{3, 2, 1},
	}

	for _, tt := range tests {
		got := NewOid(tt.a).Compare(NewOid(tt.b))
		if got != tt.want {
			t.Fatalf("expected %d, got %d", tt.want, got)
		}
	}
}

func TestCompareNulls(t *testing.T) {
	null := Fixed4ByteValue{
		Type:  IntegerType,
		Valid: false,
	}

	value := NewInteger(1)

	if got := null.Compare(null); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}

	if got := null.Compare(value); got != 1 {
		t.Fatalf("expected 1, got %d", got)
	}

	if got := value.Compare(null); got != -1 {
		t.Fatalf("expected -1, got %d", got)
	}
}

func TestCompareNegativeIntegers(t *testing.T) {
	a := NewInteger(-10)
	b := NewInteger(5)

	if got := a.Compare(b); got != -1 {
		t.Fatalf("expected -1, got %d", got)
	}
}
