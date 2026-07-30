package catalog

import (
	"fmt"
	"strings"
)

type Schema struct {
	Columns   []Column
	TupleSize int16 // total size of tuple if fixed size, else -1
}

// take via val
func NewSchema(columns []Column) *Schema {
	colsCp := make([]Column, len(columns))
	copy(colsCp, columns)

	var currentOffset int16 = 0
	isDynamic := false

	for i := range colsCp {
		if colsCp[i].FixedLength == -1 {
			isDynamic = true
		}

		if !isDynamic {
			colsCp[i].Offset = currentOffset
			currentOffset += colsCp[i].FixedLength
		} else {
			colsCp[i].Offset = -1
		}
	}

	tupleSize := currentOffset
	if isDynamic {
		tupleSize = -1 // Total size changes row-by-row
	}

	return &Schema{
		Columns:   colsCp,
		TupleSize: tupleSize,
	}
}

func (s *Schema) GetColumnCount() int {
	return len(s.Columns)
}

// get ptr to column based on index (0-base)
func (s *Schema) GetColumn(i int) *Column {
	colLen := s.GetColumnCount()
	if i < 0 || i >= colLen {
		panic(fmt.Sprintf("column index %d out of bounds (schema  has %d columns)", i, colLen))
	}

	return &s.Columns[i]
}

// GetColumnIndex looks up a column by its string name (case-insensitive).
func (s *Schema) GetColumnIndex(name string) int {
	target := strings.ToLower(name)
	for i, col := range s.Columns {
		if strings.ToLower(col.Name) == target {
			return i
		}
	}
	return -1
}

// Returns true if every single column in schema is fixed length
func (s *Schema) IsFixedLength() bool {
	return s.TupleSize != -1
}
