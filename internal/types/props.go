package types

type FieldProp string

const (
	FieldPropOptional FieldProp = "optional"
	FieldPropDefault  FieldProp = "default"
	FieldPropRelation FieldProp = "relation" // relation(table.field)
	FieldPropKey      FieldProp = "key"
	FieldPropUnique   FieldProp = "unique"
	FieldPropVector   FieldProp = "vector" // vector(type, level)
)
