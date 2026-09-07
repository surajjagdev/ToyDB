package access

import (
	"testing"

	"github.com/surajjagdev/ToyDB/internal/catalog"
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/page"
	"github.com/surajjagdev/ToyDB/internal/storage/page/heap"
	"github.com/surajjagdev/ToyDB/internal/types"
)

func TestCreateNullValueBasedOnType(t *testing.T) {
	tests := []struct {
		TypeID types.TypeID
		Want   bool
	}{
		{
			types.BooleanType,
			true,
		},
	}

	for _, val := range tests {
		newVal := createNullValue(val.TypeID)

		if newVal.IsNull() != val.Want {
			t.Fatalf("Expected null value for new Value to be %v, got %v", val.Want, newVal.IsNull())
		}
	}
}

func TestSerializeColumnsNoNulls(t *testing.T) {
	// Create a schema with Id, name, email, age, address
	columns := []catalog.Column{
		catalog.NewColumn("Id", types.OidType, false),
		catalog.NewColumn("Name", types.VarcharType, false),
		catalog.NewColumn("Email", types.VarcharType, false),
		catalog.NewColumn("Age", types.IntegerType, false),
		catalog.NewColumn("Address", types.VarcharType, false),
	}

	schema, err := catalog.NewSchema(columns)

	if err != nil {
		t.Fatalf("Did not expect an error creating schema: %v", err)
	}

	// Lets create some data in a byte buffer and have it serialize into a tuple
	originalValues := []types.Value{
		types.NewOid(0x88),
		types.NewVarChar("My name is"),
		types.NewVarChar("test@testing.com"),
		types.NewInteger(32),
		types.NewVarChar("131 Main Street, Toronto, ON"),
	}

	newTuple := NewTuple(originalValues)

	// 1. serialize the tuple
	data, nullBitmap := newTuple.Serialize(schema)

	// create heap page and insert the tuple
	rawPage := make(page.Page, common.PageSize)
	heapPage := heap.InitHeapPage(rawPage)

	_, err = heapPage.InsertTuple(
		data,
		nullBitmap,
		common.BlockID(0),
		common.TransactionID(1),
		common.CommandID(1),
	)

	if err != nil {
		t.Fatalf("Did not expect an error inserting tuple into heap page: %v", err)
	}

	// It should be the only tuple in the page, read it back
	var slotNumber common.SlotIndex = 0

	if !heapPage.DoesTupleInSlotExist(slotNumber) {
		t.Fatalf("Expected slot at id %v to exist", slotNumber)
	}

	rawTupleBytes := heapPage.GetTupleWithSlot(0)

	// deserialize
	deserializedTuple, err := DeserializeTuple(rawTupleBytes, schema)

	if err != nil {
		t.Fatalf("Did not expect an error deserializing tuple: %v", err)
	}

	// compare bytes
	if len(deserializedTuple.Values) != len(originalValues) {
		t.Fatalf("Expected %v length for tuple, got %v", len(originalValues), len(deserializedTuple.Values))
	}

	for i := range originalValues {
		if originalValues[i].Compare(deserializedTuple.Values[i]) != 0 {
			t.Errorf("Mismatch at column %d. Original: %+v, Restored: %+v", i,
				originalValues[i], deserializedTuple.Values[i])
		}
	}
}

func TestSerializeColumnsWithNulls(t *testing.T) {
	// Create a schema with Id, name, email, age, address
	columns := []catalog.Column{
		catalog.NewColumn("Id", types.OidType, false),
		catalog.NewColumn("Name", types.VarcharType, false),
		catalog.NewColumn("Email", types.VarcharType, true),
		catalog.NewColumn("Age", types.IntegerType, true),
		catalog.NewColumn("Address", types.VarcharType, true),
	}

	schema, err := catalog.NewSchema(columns)

	if err != nil {
		t.Fatalf("Did not expect an error creating schema: %v", err)
	}

	// Lets create some data in a byte buffer and have it serialize into a tuple
	originalValues := []types.Value{
		types.NewOid(0x88),
		types.NewVarChar("My name is"),
		types.VarLenValue{Type: types.VarcharType, Valid: false},
		types.Fixed4ByteValue{Type: types.IntegerType, Valid: false},
		types.VarLenValue{Type: types.VarcharType, Valid: false},
	}

	newTuple := NewTuple(originalValues)

	// 1. serialize the tuple
	data, nullBitmap := newTuple.Serialize(schema)

	// create heap page and insert the tuple
	rawPage := make(page.Page, common.PageSize)
	heapPage := heap.InitHeapPage(rawPage)

	_, err = heapPage.InsertTuple(
		data,
		nullBitmap,
		common.BlockID(0),
		common.TransactionID(1),
		common.CommandID(1),
	)

	if err != nil {
		t.Fatalf("Did not expect an error inserting tuple into heap page: %v", err)
	}

	// It should be the only tuple in the page, read it back
	var slotNumber common.SlotIndex = 0

	if !heapPage.DoesTupleInSlotExist(slotNumber) {
		t.Fatalf("Expected slot at id %v to exist", slotNumber)
	}

	rawTupleBytes := heapPage.GetTupleWithSlot(0)

	// deserialize
	deserializedTuple, err := DeserializeTuple(rawTupleBytes, schema)

	if err != nil {
		t.Fatalf("Did not expect an error deserializing tuple: %v", err)
	}

	// compare bytes
	if len(deserializedTuple.Values) != len(originalValues) {
		t.Fatalf("Expected %v length for tuple, got %v", len(originalValues), len(deserializedTuple.Values))
	}

	// first 3 columns should match, not null
	for i := 0; i < 2; i++ {
		if originalValues[i].Compare(deserializedTuple.Values[i]) != 0 {
			t.Errorf("Mismatch at column %d. Original: %+v, Restored: %+v", i,
				originalValues[i], deserializedTuple.Values[i])
		}
		if originalValues[i].IsNull() || deserializedTuple.Values[i].IsNull() {
			t.Errorf("Mismatch at column %d, expected not null", i)
		}
	}

	for i := 2; i < 5; i++ {
		if !originalValues[i].IsNull() || !deserializedTuple.Values[i].IsNull() {
			t.Errorf("Mismatch at column %d, expected null", i)
		}
	}

}
