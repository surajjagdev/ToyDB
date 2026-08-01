package catalog

import (
	"fmt"
	"testing"

	"github.com/surajjagdev/ToyDB/internal/types"
)

func TestSchemaConstructor(t *testing.T) {
	// Create a 5 int column arr
	cols := make([]Column, 0)
	expectedTupleSz := 0

	for i := range 5 {
		cols = append(cols, NewColumn(fmt.Sprintf("Column%d", i),
			types.IntegerType, true))
		expectedTupleSz += int(cols[i].FixedLength)
	}

	schema, err := NewSchema(cols)

	if err != nil {
		t.Fatalf("Expected no error on new Schema creation, got %v", err)
	}

	if schema.TupleSize != int16(expectedTupleSz) {
		t.Fatalf("expected tuple size to be %d, got %d", expectedTupleSz, schema.TupleSize)
	}

	if schema.GetColumnCount() != 5 {
		t.Fatalf("expected column length to be %d, got %d", 5, schema.GetColumnCount())
	}
	if !schema.IsFixedLength() {
		t.Fatalf("expected schema to be fixed length")
	}
}

func TestSchemaConstructorWithNotFixedLength(t *testing.T) {
	// Create a 5 int column arr
	cols := make([]Column, 0)
	expectedTupleSz := 0

	for i := range 5 {
		cols = append(cols, NewColumn(fmt.Sprintf("Column%d", i),
			types.IntegerType, true))
		expectedTupleSz += int(cols[i].FixedLength)
	}

	// add varchar column
	cols = append(cols, NewColumn("VarCharColumn", types.VarcharType, false))

	schema, err := NewSchema(cols)

	if err != nil {
		t.Fatalf("Expected no error on new Schema creation, got %v", err)
	}

	if schema.TupleSize != -1 {
		t.Fatalf("expected tuple size to be %d, got %d", -1, schema.TupleSize)
	}
	if schema.IsFixedLength() {
		t.Fatalf("expected schema not to be fixed length")
	}
}

func TestSchemaFixedLengthColumnOffsets(t *testing.T) {
	cols := []Column{
		NewColumn("flag", types.BooleanType, false),
		NewColumn("count", types.IntegerType, false),
		NewColumn("total", types.BigIntType, false),
	}

	schema, err := NewSchema(cols)

	if err != nil {
		t.Fatalf("Expected no error on new Schema creation, got %v", err)
	}

	expectedOffsets := []int16{0, 1, 5}
	for i, want := range expectedOffsets {
		got := schema.Columns[i].Offset
		if got != want {
			t.Fatalf("column %d offset = %d, want %d", i, got, want)
		}
	}

	if schema.TupleSize != 13 {
		t.Fatalf("expected tuple size 13, got %d", schema.TupleSize)
	}
}

func TestSchemaDynamicColumnClearsOffsetsFromDynamicColumnOnward(t *testing.T) {
	cols := []Column{
		NewColumn("id", types.IntegerType, false),
		NewColumn("name", types.VarcharType, false),
		NewColumn("score", types.IntegerType, false),
	}

	schema, err := NewSchema(cols)

	if err != nil {
		t.Fatalf("Expected no error on new Schema creation, got %v", err)
	}

	// Fixed columns before the first dynamic column keep their computed offsets.
	if schema.Columns[0].Offset != 0 {
		t.Fatalf("column 0 offset = %d, want 0", schema.Columns[0].Offset)
	}

	// Dynamic column and everything after it use offset -1.
	for i := 1; i < len(schema.Columns); i++ {
		if schema.Columns[i].Offset != -1 {
			t.Fatalf("column %d offset = %d, want -1", i, schema.Columns[i].Offset)
		}
	}

	if schema.TupleSize != -1 {
		t.Fatalf("expected tuple size -1, got %d", schema.TupleSize)
	}
}

func TestSchemaDynamicColumnFirstClearsAllOffsets(t *testing.T) {
	cols := []Column{
		NewColumn("name", types.VarcharType, false),
		NewColumn("id", types.IntegerType, false),
	}

	schema, err := NewSchema(cols)

	if err != nil {
		t.Fatalf("Expected no error on new Schema creation, got %v", err)
	}

	for i, col := range schema.Columns {
		if col.Offset != -1 {
			t.Fatalf("column %d offset = %d, want -1 when schema starts with dynamic column", i, col.Offset)
		}
	}
}

func TestSchemaGetColumnIndex(t *testing.T) {
	cols := []Column{
		NewColumn("UserId", types.IntegerType, false),
		NewColumn("Email", types.VarcharType, false),
		NewColumn("CreatedAt", types.DateType, false),
	}
	schema, err := NewSchema(cols)
	if err != nil {
		t.Fatalf("Expected no error on new Schema creation, got %v", err)
	}

	tests := []struct {
		name  string
		query string
		want  int
	}{
		{name: "exact match", query: "UserId", want: 0},
		{name: "case insensitive", query: "email", want: 1},
		{name: "mixed case", query: "CREATEDaT", want: 2},
		{name: "missing column", query: "password", want: -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := schema.GetColumnIndex(tc.query); got != tc.want {
				t.Fatalf("GetColumnIndex(%q) = %d, want %d", tc.query, got, tc.want)
			}
		})
	}
}

func TestSchemaGetColumn(t *testing.T) {
	cols := []Column{
		NewColumn("id", types.IntegerType, false),
		NewColumn("name", types.VarcharType, true),
	}
	schema, err := NewSchema(cols)

	if err != nil {
		t.Fatalf("Expected no error on new Schema creation, got %v", err)
	}

	col := schema.GetColumn(1)
	if col.Name != "name" {
		t.Fatalf("expected column name %q, got %q", "name", col.Name)
	}
	if col.Type != types.VarcharType {
		t.Fatalf("expected column type %v, got %v", types.VarcharType, col.Type)
	}
	if !col.IsNullable {
		t.Fatal("expected column to be nullable")
	}
}

func TestSchemaGetColumnOutOfBoundsPanics(t *testing.T) {
	schema, err := NewSchema([]Column{NewColumn("id", types.IntegerType, false)})

	if err != nil {
		t.Fatalf("Expected no error on new Schema creation, got %v", err)
	}

	tests := []struct {
		name  string
		index int
	}{
		{name: "negative index", index: -1},
		{name: "index equal to count", index: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic for out-of-bounds column index")
				}
			}()
			schema.GetColumn(tc.index)
		})
	}
}

func TestSchemaConstructorGivesErrorOnNoColumns(t *testing.T) {
	_, err := NewSchema([]Column{})

	if err == nil {
		t.Fatalf("Expected an error to be returned giving no columns to schema")
	}
}

func TestSchemaConstructorCopiesInputColumns(t *testing.T) {
	cols := []Column{NewColumn("id", types.IntegerType, false)}
	schema, err := NewSchema(cols)

	if err != nil {
		t.Fatalf("Expected no error on new Schema creation, got %v", err)
	}

	cols[0].Name = "mutated"

	if schema.GetColumn(0).Name != "id" {
		t.Fatalf("expected schema to keep original column name %q, got %q", "id", schema.GetColumn(0).Name)
	}
}
