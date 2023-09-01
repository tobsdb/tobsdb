package types

var VALID_BUILTIN_TYPES = []FieldType{
	Int, String, Date, Float, Bool, Bytes,
}

type FieldType string

const (
	Int    FieldType = "Int"
	String FieldType = "String"
	Date   FieldType = "Date"
	Float  FieldType = "Float"
	Bool   FieldType = "Bool"
	Bytes  FieldType = "Bytes"
)
