package types

import (
	"bytes"
	"math"
	"testing"

	"github.com/surajjagdev/ToyDB/internal/common"
)

func TestNewBigInt(t *testing.T) {
	v := NewBigInt(12345)

	if v.GetTypeID() != BigIntType {
		t.Fatalf("expected %v, got %v", BigIntType, v.GetTypeID())
	}

	if v.IsNull() {
		t.Fatal("expected value to be valid")
	}

	if int64(v.Value) != 12345 {
		t.Fatalf("expected 12345, got %d", int64(v.Value))
	}
}

func TestNewTimeStamp(t *testing.T) {
	v := NewTimeStamp(987654321)

	if v.GetTypeID() != TimeTzType {
		t.Fatalf("expected %v, got %v", TimeTzType, v.GetTypeID())
	}

	if v.IsNull() {
		t.Fatal("expected value to be valid")
	}

	if int64(v.Value) != 987654321 {
		t.Fatalf("expected 987654321, got %d", int64(v.Value))
	}
}

func TestNewFloat8(t *testing.T) {
	v := NewFloat8(3.14159)

	if v.GetTypeID() != Float8Type {
		t.Fatalf("expected %v, got %v", Float8Type, v.GetTypeID())
	}

	if v.IsNull() {
		t.Fatal("expected value to be valid")
	}

	got := math.Float64frombits(v.Value)
	if got != 3.14159 {
		t.Fatalf("expected 3.14159, got %f", got)
	}
}

func TestSerializeForEightByte(t *testing.T) {
	var val int64 = math.MaxInt64 - 1
	v := NewBigInt(val)

	buff := make([]byte, 8)
	common.ByteOrder.PutUint64(buff, uint64(val))

	if !bytes.Equal(v.Serialize(), buff) {
		t.Fatalf("expected %v, got %v", buff, v.Serialize())
	}
}

func TestSerializeNullForEightByte(t *testing.T) {
	v := Fixed8ByteValue{
		Type:  BigIntType,
		Valid: false,
	}

	if v.Serialize() != nil {
		t.Fatal("expected nil serialization")
	}
}

func TestCompareBigInt(t *testing.T) {
	tests := []struct {
		name string
		a    int64
		b    int64
		want int
	}{
		{"less", 1, 2, -1},
		{"equal", 2, 2, 0},
		{"greater", 3, 2, 1},
		{"negative", -10, 5, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewBigInt(tt.a).Compare(NewBigInt(tt.b))
			if got != tt.want {
				t.Fatalf("expected %d, got %d", tt.want, got)
			}
		})
	}
}

func TestCompareTimeStamp(t *testing.T) {
	tests := []struct {
		a, b int64
		want int
	}{
		{100, 200, -1},
		{200, 200, 0},
		{300, 200, 1},
	}

	for _, tt := range tests {
		got := NewTimeStamp(tt.a).Compare(NewTimeStamp(tt.b))
		if got != tt.want {
			t.Fatalf("expected %d, got %d", tt.want, got)
		}
	}
}

func TestCompareFloat8(t *testing.T) {
	tests := []struct {
		a, b float64
		want int
	}{
		{1.5, 2.5, -1},
		{2.5, 2.5, 0},
		{3.5, 2.5, 1},
		{-1.25, 0.0, -1},
	}

	for _, tt := range tests {
		got := NewFloat8(tt.a).Compare(NewFloat8(tt.b))
		if got != tt.want {
			t.Fatalf("expected %d, got %d", tt.want, got)
		}
	}
}

func TestCompareNullsForEightByte(t *testing.T) {
	null := Fixed8ByteValue{
		Type:  BigIntType,
		Valid: false,
	}

	value := NewBigInt(10)

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
