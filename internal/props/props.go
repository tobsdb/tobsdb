package props

import (
	"fmt"
	"slices"
	"strconv"
)

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

func (p FieldProp) IsValid() bool {
	return slices.Contains(VALID_BUILTIN_PROPS, p)
}

const KeyPropPrimary string = "primary"

func ValidatePropValue(name FieldProp, value string) (any, error) {
	switch name {
	case FieldPropKey:
		if value == KeyPropPrimary {
			return value, nil
		}
	case FieldPropUnique:
		fallthrough
	case FieldPropOptional:
		value, err := strconv.ParseBool(value)
		if err == nil {
			return value, nil
		}
	case FieldPropDefault:
		fallthrough
		// TODO: do parse & check here
	case FieldPropVector:
		fallthrough
	case FieldPropRelation:
		return value, nil
	}

	return nil, invalidPropError(name, value)
}

func invalidPropError(name FieldProp, value string) error {
	return fmt.Errorf("%s(%s) is not a valid prop", name, value)
}
