package types

type FieldProp string

const (
	Required FieldProp = "required"
	Default  FieldProp = "default"
	Relation FieldProp = "relation"
	Key      FieldProp = "key"
	Unique   FieldProp = "unique"
)
