package types

type BoolValue struct {
	Type  TypeID
	Value bool
	Valid bool // false if NULL
}

func NewBoolean(val bool) BoolValue {
	return BoolValue{Type: BooleanType, Value: val, Valid: true}
}

func NewNullBoolean() BoolValue {
	return BoolValue{Type: BooleanType, Valid: false}
}

func (this BoolValue) GetTypeID() TypeID {
	return this.Type
}

func (this BoolValue) IsNull() bool {
	return !this.Valid
}

func (this BoolValue) Serialize() []byte {
	if this.IsNull() {
		return nil
	}

	// single byte
	if this.Value {
		return []byte{1}
	}
	return []byte{0}
}

func (this BoolValue) Compare(other Value) int {
	otherVal := other.(BoolValue)

	if this.IsNull() && otherVal.IsNull() {
		return 0
	}
	if this.IsNull() {
		return 1 // NULLs sort last
	}
	if otherVal.IsNull() {
		return -1
	}

	if this.Value == otherVal.Value {
		return 0
	}
	if !this.Value && otherVal.Value {
		return -1 // False < True
	}
	return 1 // True > False
}
