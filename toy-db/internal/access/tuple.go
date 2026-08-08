package access

import (
	"fmt"
	"math"

	"github.com/surajjagdev/ToyDB/internal/catalog"
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/page/heap"
	"github.com/surajjagdev/ToyDB/internal/types"
)

/**
Serialize and deserialize tuples
**/

// Tuple is just array of catlog values
type Tuple struct {
	Values []types.Value
}

func NewTuple(vals []types.Value) *Tuple {
	return &Tuple{Values: vals}
}

// Create null values for the value
func createNullValue(typeID types.TypeID) types.Value {
	switch typeID {
	case types.BooleanType:
		return types.NewNullBoolean()
	case types.IntegerType, types.DateType, types.OidType:
		return types.Fixed4ByteValue{Type: typeID, Valid: false}
	case types.BigIntType, types.Float8Type, types.TimeTzType:
		return types.Fixed8ByteValue{Type: typeID, Valid: false}
	case types.VarcharType:
		return types.VarLenValue{Type: typeID, Valid: false}
	}
	return nil
}

// serialize, deserialize and create null values
func (t *Tuple) Serialize(schema *catalog.Schema) (data []byte, nullBitmap []byte) {
	colCount := schema.GetColumnCount()

	// check if we have nulls
	hasNulls := false

	for _, t := range t.Values {
		if t.IsNull() {
			hasNulls = true
			break
		}
	}

	bitMapSz := 0

	if hasNulls {
		bitMapSz = GetBitMapSizeInBytes(colCount)
		nullBitmap = make([]byte, bitMapSz)
	}

	for index, t := range t.Values {
		if t.IsNull() {
			SetBitInBitMap(nullBitmap, index)
		} else {
			data = append(data, t.Serialize()...)
		}
	}

	// Generate payload
	return data, nullBitmap
}

// Read from disk, covert to tuple again
func DeserializeTuple(data []byte, schema *catalog.Schema) (*Tuple, error) {
	// Read the headers of tuple
	infoMask := common.ByteOrder.Uint16(
		data[heap.TupleHeaderOffsetInfomask : heap.TupleHeaderOffsetInfomask+2],
	)

	// byte where tuple data starts
	hoff := data[heap.TupleHeaderOffsetHoff]
	userData := data[hoff:]

	// FIX 1: Correct Go syntax for uninitialized slice
	var nullBitmap []byte

	if infoMask&0x1 != 0 {
		// bitmap exists
		bitMapSz := GetBitMapSizeInBytes(schema.GetColumnCount())

		// Read the bitmap from right after the fixed header
		nullBitmap = data[heap.HeapTupleHeaderMinSize : heap.HeapTupleHeaderMinSize+bitMapSz]
	}

	// translate values using schema
	colCount := schema.GetColumnCount()
	values := make([]types.Value, colCount)
	currentOffset := 0

	for i := 0; i < colCount; i++ {
		col := schema.GetColumn(i)

		if nullBitmap != nil && IsBitSet(nullBitmap, i) {
			values[i] = createNullValue(col.Type)
			continue
		}

		switch col.Type {
		case types.BooleanType:
			val := userData[currentOffset] == 1
			values[i] = types.NewBoolean(val)
			currentOffset += int(types.BOOLEAN_TYPE_FIXED_LENGTH)

		case types.IntegerType:
			raw := common.ByteOrder.Uint32(userData[currentOffset : currentOffset+int(types.INTEGER_TYPE_FIXED_LENGTH)])
			values[i] = types.NewInteger(int32(raw))
			currentOffset += int(types.INTEGER_TYPE_FIXED_LENGTH)

		case types.DateType:
			raw := common.ByteOrder.Uint32(userData[currentOffset : currentOffset+int(types.INTEGER_TYPE_FIXED_LENGTH)])
			values[i] = types.NewDate(int32(raw))
			currentOffset += int(types.INTEGER_TYPE_FIXED_LENGTH)

		case types.OidType:
			raw := common.ByteOrder.Uint32(userData[currentOffset : currentOffset+int(types.INTEGER_TYPE_FIXED_LENGTH)])
			values[i] = types.NewOid(uint32(raw))
			currentOffset += int(types.INTEGER_TYPE_FIXED_LENGTH)

		case types.BigIntType:
			raw := common.ByteOrder.Uint64(userData[currentOffset : currentOffset+int(types.FLOAT_64_TYPE_FIXED_LENGTH)])
			values[i] = types.NewBigInt(int64(raw))
			currentOffset += int(types.FLOAT_64_TYPE_FIXED_LENGTH)

		case types.TimeTzType:
			raw := common.ByteOrder.Uint64(userData[currentOffset : currentOffset+int(types.FLOAT_64_TYPE_FIXED_LENGTH)])
			values[i] = types.NewTimeStamp(int64(raw))
			currentOffset += int(types.FLOAT_64_TYPE_FIXED_LENGTH)

		case types.Float8Type:
			raw := common.ByteOrder.Uint64(userData[currentOffset : currentOffset+int(types.FLOAT_64_TYPE_FIXED_LENGTH)])
			floatVal := math.Float64frombits(raw)
			values[i] = types.NewFloat8(floatVal)
			currentOffset += int(types.FLOAT_64_TYPE_FIXED_LENGTH)
		case types.VarcharType:
			// Read the 4-byte length header first
			strLen := common.ByteOrder.Uint32(userData[currentOffset : currentOffset+types.VARLENGTH_HEADER_BYTES])
			currentOffset += types.VARLENGTH_HEADER_BYTES

			// Then slice the actual string bytes
			strBytes := userData[currentOffset : currentOffset+int(strLen)]
			values[i] = types.NewVarChar(string(strBytes))
			currentOffset += int(strLen)

		default:
			return nil, fmt.Errorf("unsupported type during deserialization: %v", col.Type)
		}
	}

	return NewTuple(values), nil
}
