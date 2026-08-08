package catalog

import (
	"testing"

	"github.com/surajjagdev/ToyDB/internal/types"
)

func TestNewColumnConstructor(t *testing.T) {

	tests := []struct {
		Name       string
		Type       types.TypeID
		IsNullable bool
	}{
		{
			Name:       "boolean column",
			Type:       types.BooleanType,
			IsNullable: false,
		},
		{
			Name:       "Integer column",
			Type:       types.IntegerType,
			IsNullable: false,
		},
		{
			Name:       "Date column",
			Type:       types.DateType,
			IsNullable: false,
		},
		{
			Name:       "Oid column",
			Type:       types.OidType,
			IsNullable: false,
		},
		{
			Name:       "Big int column",
			Type:       types.BigIntType,
			IsNullable: false,
		},
		{
			Name:       "Time tz column",
			Type:       types.TimeTzType,
			IsNullable: false,
		},
		{
			Name:       "Float 8 byte column",
			Type:       types.Float8Type,
			IsNullable: false,
		},
		{
			Name:       "Var char column",
			Type:       types.VarcharType,
			IsNullable: false,
		},
	}

	for _, val := range tests {
		obj := NewColumn(val.Name, val.Type, val.IsNullable)

		if obj.Name != val.Name {
			t.Fatalf("Expected name %v, got %v", val.Name, obj.Name)
		}
	}
}
