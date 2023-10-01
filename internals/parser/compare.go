package parser

import (
	"fmt"

	TDBTypes "github.com/tobshub/tobsdb/internals/types"
)

func CompareVector(schema *Table, field *Field, value []any, input any) bool {
	input, err := field.ValidateType(schema, input, false)
	if err != nil {
		return false
	}

	v_type, v_level := ParseVectorProp(field.Properties[TDBTypes.FieldPropVector])

	var v_field Field

	if v_level > 1 {
		v_field = Field{
			Name:        "vector field",
			BuiltinType: TDBTypes.FieldTypeVector,
			Properties:  map[TDBTypes.FieldProp]string{},
		}

		v_field.Properties[TDBTypes.FieldPropVector] = fmt.Sprintf("%s,%d", v_type, v_level-1)
	} else {
		v_field = Field{Name: "vector field", BuiltinType: v_type}
	}

	for i, v_value := range value {
		if !v_field.Compare(schema, v_value, input.([]any)[i]) {
			return false
		}
	}

	return true
}

type IntCompare string

const (
	IntCompareEqual          = "eq"
	IntCompareNotEqual       = "ne"
	IntCompareGreater        = "gt"
	IntCompareLess           = "lt"
	IntCompareGreaterOrEqual = "gte"
	IntCompareLessOrEqual    = "lte"
)

func CompareInt(schema *Table, field *Field, value int, input any) bool {
	switch input.(type) {
	case map[string]any:
		valid := false
		for comp, val := range input.(map[string]any) {
			comp := IntCompare(comp)
			_val, err := field.ValidateType(schema, val, false)
			if err != nil {
				return false
			}
			val := _val.(int)
			switch comp {
			case IntCompareEqual:
				valid = value == val
			case IntCompareNotEqual:
				valid = value != val
			case IntCompareGreater:
				valid = value > val
			case IntCompareLess:
				valid = value < val
			case IntCompareGreaterOrEqual:
				valid = value >= val
			case IntCompareLessOrEqual:
				valid = value <= val
			}

			if !valid {
				break
			}
		}
		return valid
	default:
		input, err := field.ValidateType(schema, input, false)
		if err != nil {
			return false
		}

		return value == input
	}
}
