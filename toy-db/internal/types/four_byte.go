package types

import (
	"github.com/surajjagdev/ToyDB/internal/common"
)

/**
A wrapper for int4, dates and object ids (oid)
Anything needing ixed 4 bytes
**/

type Fixed4ByteValue struct {
	Type  TypeID
	Value uint32 // raw int as ptr
	Valid bool
}

// signed 4 byte
func NewInteger(val int32) Fixed4ByteValue {
	return Fixed4ByteValue{Type: IntegerType, Value: uint32(val), Valid: true}
}

// NewDate creates a date (days since Jan 1, 2000)
func NewDate(daysSinceEpoch int32) Fixed4ByteValue {
	return Fixed4ByteValue{Type: DateType, Value: uint32(daysSinceEpoch), Valid: true}
}

// usigned
func NewOid(val uint32) Fixed4ByteValue {
	return Fixed4ByteValue{Type: OidType, Value: val, Valid: true}
}

func (this Fixed4ByteValue) GetTypeID() TypeID {
	return this.Type
}

func (this Fixed4ByteValue) IsNull() bool {
	return !this.Valid
}

func (this Fixed4ByteValue) Serialize() []byte {
	if this.IsNull() {
		return nil
	}

	buf := make([]byte, 4)
	common.ByteOrder.PutUint32(buf, this.Value)

	return buf
}

/*
*
Compare this obj with another
Returns -1 if this < other,
Returns 0 if equal
Returns 1 if this > other.
*
*/
func (this Fixed4ByteValue) Compare(other Value) int {
	// 1. Cast the other value to a Fixed4Value object
	otherVal := other.(Fixed4ByteValue)

	// Nulls are equal to one-another but treated as greater than equal
	// to all other values
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
	case DateType, IntegerType:
		// cast to int32 for signed comparision
		val1 := int32(this.Value)
		val2 := int32(otherVal.Value)

		if val1 < val2 {
			return -1
		}
		if val1 > val2 {
			return 1
		}
		return 0

	case OidType:
		// unsigned, can directly compare
		val1 := this.Value
		val2 := otherVal.Value

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
