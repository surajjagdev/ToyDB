package catalog

import (
	"fmt"

	"github.com/surajjagdev/ToyDB/internal/types"
)

/**
Representing the table schema, columns, etc
**/

type Column struct {
	Name string       // Name of the column -> check for uniqueness
	Type types.TypeID // Type of the column

	IsNullable bool

	// Auto-calculated
	FixedLength int16 // Length in bytes. -1 if dynamic
	Offset      int16 // -1 if fixed. Otherwise, offset in tuple
}

func NewColumn(name string, typeID types.TypeID, nullable bool) Column {
	col := Column{
		Name:        name,
		Type:        typeID,
		IsNullable:  nullable,
		FixedLength: -1,
		Offset:      -1,
	}

	switch typeID {
	case types.BooleanType:
		col.FixedLength = types.BOOLEAN_TYPE_FIXED_LENGTH
	case types.IntegerType, types.DateType, types.OidType:
		col.FixedLength = types.INTEGER_TYPE_FIXED_LENGTH
	case types.BigIntType, types.TimeTzType, types.Float8Type:
		col.FixedLength = types.FLOAT_64_TYPE_FIXED_LENGTH
	case types.VarcharType:
		col.FixedLength = types.DYNAMIC_TYPE_FIXED_LENGTH //variable length
	default:
		panic(fmt.Sprintf("Unknown type given: %v", typeID))
	}

	return col
}
