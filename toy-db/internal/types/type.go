package types

type TypeID uint32

const (
	InvalidType TypeID = 0
	BooleanType TypeID = 16   // 'bool'
	BigIntType  TypeID = 20   // 'int8'
	IntegerType TypeID = 23   // 'int4'
	TextType    TypeID = 25   // 'text'
	OidType     TypeID = 26   // 'oid'
	Float8Type  TypeID = 701  // 'float8'
	VarcharType TypeID = 1043 // 'varchar'
	DateType    TypeID = 1082 // 'date'
	TimeTzType  TypeID = 1184 // 'timestamptz'
)

type Value interface {
	GetTypeID() TypeID
	Serialize() []byte
	IsNull() bool
	Compare(other Value) int
}
