package types

import (
	"bytes"

	"github.com/surajjagdev/ToyDB/internal/common"
)

/**
A wrapper for Varchar
Anything needing var length strings
**/

type VarLenValue struct {
	Type  TypeID
	Value []byte // raw bytes
	Valid bool
}

func NewVarChar(val string) VarLenValue {
	return VarLenValue{Type: VarcharType, Value: []byte(val), Valid: true}
}

func (this VarLenValue) GetTypeID() TypeID {
	return this.Type
}

func (this VarLenValue) IsNull() bool {
	return !this.Valid
}

func (this VarLenValue) Compare(other Value) int {
	otherVal := other.(VarLenValue)

	if this.IsNull() && otherVal.IsNull() {
		return 0
	}

	if this.IsNull() {
		return 1
	}

	if otherVal.IsNull() {
		return -1
	}

	// Compare byte to match lexiograpgically
	return bytes.Compare(this.Value, otherVal.Value)
}

func (this VarLenValue) Serialize() []byte {
	if this.IsNull() {
		return nil
	}

	// 4 bytes len + data
	buf := make([]byte, 4+len(this.Value))
	// store first 4 bytes as length of the varlength buffer
	common.ByteOrder.PutUint32(buf, uint32(len(this.Value)))

	copy(buf[4:], this.Value)

	return buf
}
