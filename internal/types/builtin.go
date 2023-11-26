package types

import "slices"

var VALID_BUILTIN_TYPES = []FieldType{
	FieldTypeInt, FieldTypeString, FieldTypeDate,
	FieldTypeFloat, FieldTypeBool, FieldTypeBytes, FieldTypeVector,
}

type FieldType string

const (
	FieldTypeInt    FieldType = "Int"
	FieldTypeString FieldType = "String"
	FieldTypeDate   FieldType = "Date"
	FieldTypeFloat  FieldType = "Float"
	FieldTypeBool   FieldType = "Bool"
	FieldTypeBytes  FieldType = "Bytes"
	FieldTypeVector FieldType = "Vector"
)

func (s FieldType) IsValid() bool {
	return slices.Contains(VALID_BUILTIN_TYPES, s)
}
