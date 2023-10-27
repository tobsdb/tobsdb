package types

type FieldProp string

const (
	FieldPropOptional FieldProp = "optional"
	FieldPropDefault  FieldProp = "default"
	FieldPropRelation FieldProp = "relation"
	FieldPropKey      FieldProp = "key"
	FieldPropUnique   FieldProp = "unique"
	FieldPropVector   FieldProp = "vector"
)
