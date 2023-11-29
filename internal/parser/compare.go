package parser

import (
	"fmt"
	"strings"

	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
)

func (field *Field) compareDefault(value any, input any) bool {
	input, err := field.ValidateType(input, false)
	if err != nil {
		return false
	}
	return value == input
}

func (field *Field) compareVector(value []any, input any) bool {
	input, err := field.ValidateType(input, false)
	if err != nil {
		return false
	}

	v_type, v_level := ParseVectorProp(field.Properties[props.FieldPropVector].(string))

	var v_field Field

	if v_level > 1 {
		v_field = Field{
			Name:        "vector field",
			BuiltinType: types.FieldTypeVector,
			Properties:  map[props.FieldProp]any{},
			Table:       field.Table,
		}

		v_field.Properties[props.FieldPropVector] = fmt.Sprintf("%s,%d", v_type, v_level-1)
	} else {
		v_field = Field{Name: "vector field", BuiltinType: v_type, Table: field.Table}
	}

	for i, v_value := range value {
		if !(&v_field).Compare(v_value, input.([]any)[i]) {
			return false
		}
	}

	return true
}

type IntCompare string

const (
	IntCompareEqual          IntCompare = "eq"
	IntCompareNotEqual       IntCompare = "ne"
	IntCompareGreater        IntCompare = "gt"
	IntCompareLess           IntCompare = "lt"
	IntCompareGreaterOrEqual IntCompare = "gte"
	IntCompareLessOrEqual    IntCompare = "lte"
)

func (field *Field) compareInt(value int, input any) bool {
	switch input := input.(type) {
	case map[string]any:
		valid := false
		for comp, val := range input {
			comp := IntCompare(comp)
			_val, err := field.ValidateType(val, false)
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
		input, err := field.ValidateType(input, false)
		if err != nil {
			return false
		}

		return value == input
	}
}

type StringCompare string

const (
	StringCompareContains   StringCompare = "contains"
	StringCompareStartsWith StringCompare = "startsWith"
	StringCompareEndsWith   StringCompare = "endsWith"
)

func (field *Field) compareString(value string, input any) bool {
	switch input := input.(type) {
	case map[string]any:

		valid := false
		for comp, val := range input {
			comp := StringCompare(comp)
			_val, err := field.ValidateType(val, false)
			if err != nil {
				return false
			}
			val := _val.(string)
			switch comp {
			case StringCompareContains:
				valid = strings.Contains(value, val)
			case StringCompareStartsWith:
				valid = strings.Index(value, val) == 0
			case StringCompareEndsWith:
				valid = strings.LastIndex(value, val) == (len(value) - len(val))
			}

			if !valid {
				break
			}
		}
		return valid
	default:
		input, err := field.ValidateType(input, false)
		if err != nil {
			return false
		}

		return value == input
	}
}
