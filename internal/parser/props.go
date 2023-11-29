package parser

import (
	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
)

func ParseRelationProp(relation string) (string, string) {
	table, field, _ := props.ParseRelationPropSafe(relation)
	return table, field
}

func ParseVectorProp(value string) (types.FieldType, int) {
	v_type, v_level, _ := props.ParseVectorPropSafe(value)
	return v_type, v_level
}
