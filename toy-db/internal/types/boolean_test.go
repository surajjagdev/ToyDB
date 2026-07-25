package types

import (
	"testing"
)

func TestBooleanConstructors(t *testing.T) {
	vTrue := NewBoolean(true)
	if vTrue.GetTypeID() != BooleanType || vTrue.IsNull() || vTrue.Value != true {
		t.Errorf("NewBoolean(true) failed")
	}

	vFalse := NewBoolean(false)
	if vFalse.GetTypeID() != BooleanType || vFalse.IsNull() || vFalse.Value != false {
		t.Errorf("NewBoolean(false) failed")
	}

	vNull := NewNullBoolean()
	if !vNull.IsNull() {
		t.Errorf("NewNullBoolean failed")
	}
}

func TestBooleanSerialize(t *testing.T) {
	vTrue := NewBoolean(true)
	bTrue := vTrue.Serialize()

	if len(bTrue) != 1 || bTrue[0] != 1 {
		t.Errorf("Expected []byte{1} for TRUE, got %v", bTrue)
	}

	vFalse := NewBoolean(false)
	bFalse := vFalse.Serialize()

	if len(bFalse) != 1 || bFalse[0] != 0 {
		t.Errorf("Expected []byte{0} for FALSE, got %v", bFalse)
	}

	vNull := NewNullBoolean()
	bNull := vNull.Serialize()
	if bNull != nil {
		t.Errorf("Expected nil slice for NULL serialization")
	}
}

func TestBooleanCompare(t *testing.T) {
	vTrue := NewBoolean(true)
	vFalse := NewBoolean(false)
	vNull := NewNullBoolean()

	// True == True
	if vTrue.Compare(NewBoolean(true)) != 0 {
		t.Errorf("Expected True == True")
	}

	// False == False
	if vFalse.Compare(NewBoolean(false)) != 0 {
		t.Errorf("Expected False == False")
	}

	// False < True (SQL Standard)
	if vFalse.Compare(vTrue) != -1 {
		t.Errorf("Expected False < True (-1), got %d", vFalse.Compare(vTrue))
	}

	// True > False
	if vTrue.Compare(vFalse) != 1 {
		t.Errorf("Expected True > False (1), got %d", vTrue.Compare(vFalse))
	}

	// Valid < NULL
	if vTrue.Compare(vNull) != -1 {
		t.Errorf("Expected True < NULL")
	}
	if vNull.Compare(vFalse) != 1 {
		t.Errorf("Expected NULL > False")
	}
}
