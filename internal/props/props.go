package props

type FieldProp string

var VALID_BUILTIN_PROPS = []FieldProp{
	FieldPropOptional, FieldPropDefault, FieldPropRelation,
	FieldPropKey, FieldPropUnique, FieldPropVector,
}

const (
	FieldPropOptional FieldProp = "optional" // optional(true/false)
	FieldPropDefault  FieldProp = "default"
	FieldPropRelation FieldProp = "relation" // relation(table.field)
	FieldPropKey      FieldProp = "key"
	FieldPropUnique   FieldProp = "unique" // unique(true/false)
	FieldPropVector   FieldProp = "vector" // vector(type, level)
)
