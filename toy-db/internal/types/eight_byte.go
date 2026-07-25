package types

import (
	"math"

	"github.com/surajjagdev/ToyDB/internal/common"
)

/**
A wrapper for BigIntType(int8), timestamps, float (float8)
Anything needing fixed 8 bytes
**/

type Fixed8ByteValue struct {
	Type  TypeID
	Value uint64 // raw int as ptr
	Valid bool
}

func NewBigInt(val int64) Fixed8ByteValue {
	return Fixed8ByteValue{Type: BigIntType, Value: uint64(val), Valid: true}
}

// Microseconds Time since Jan 1, 2000
func NewTimeStamp(microseconds int64) Fixed8ByteValue {
	return Fixed8ByteValue{Type: TimeTzType, Value: uint64(microseconds), Valid: true}
}

func NewFloat8(val float64) Fixed8ByteValue {
	return Fixed8ByteValue{Type: Float8Type, Value: math.Float64bits(val), Valid: true}
}

func (this Fixed8ByteValue) GetTypeID() TypeID {
	return this.Type
}

func (this Fixed8ByteValue) IsNull() bool {
	return !this.Valid
}

func (this Fixed8ByteValue) Serialize() []byte {
	if this.IsNull() {
		return nil
	}
	buf := make([]byte, 8)
	common.ByteOrder.PutUint64(buf, this.Value)
	return buf
}

func (this Fixed8ByteValue) Compare(other Value) int {
	otherVal := other.(Fixed8ByteValue)

	if this.IsNull() && otherVal.IsNull() {
		return 0
	}
	if this.IsNull() {
		return 1
	}
	if otherVal.IsNull() {
		return -1
	}

	switch this.Type {
	case BigIntType, TimeTzType:
		// Signed comparison
		val1 := int64(this.Value)
		val2 := int64(otherVal.Value)
		if val1 < val2 {
			return -1
		}
		if val1 > val2 {
			return 1
		}
		return 0

	case Float8Type:
		// Float comparison
		val1 := math.Float64frombits(this.Value)
		val2 := math.Float64frombits(otherVal.Value)
		if val1 < val2 {
			return -1
		}
		if val1 > val2 {
			return 1
		}
		return 0
	}

	return 0
}
