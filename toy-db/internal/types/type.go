package types

type TypeID uint32

const (
	InvalidType TypeID = 0
	BooleanType TypeID = 16   // 'bool'
	BigIntType  TypeID = 20   // 'int8' -> eight_byte
	IntegerType TypeID = 23   // 'int4'  -> four_byte
	OidType     TypeID = 26   // 'oid' -> four_byte
	Float8Type  TypeID = 701  // 'float8' -> eight_byte
	VarcharType TypeID = 1043 // 'varchar' -> varlen
	DateType    TypeID = 1082 // 'date' -> four_byte
	TimeTzType  TypeID = 1184 // 'timestamptz' -> eight_byte
)

type Value interface {
	GetTypeID() TypeID
	Serialize() []byte
	IsNull() bool
	Compare(other Value) int
}
