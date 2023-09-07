package types

type FieldProp string

const (
	Optional    FieldProp = "optional"
	Default     FieldProp = "default"
	Relation    FieldProp = "relation"
	Key         FieldProp = "key"
	Unique      FieldProp = "unique"
	VectorProps FieldProp = "vector"
)
