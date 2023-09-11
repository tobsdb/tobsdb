package parser

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tobshub/tobsdb/internals/types"
)

// TODO: add support for nested vector types
func (field *Field) Compare(schema *Table, value any, input any) bool {
	value, err := field.ValidateType(schema, value, false)
	if err != nil {
		return false
	}

	input, err = field.ValidateType(schema, input, false)
	if err != nil {
		return false
	}

	if field.BuiltinType == types.FieldTypeVector {
		v_type := types.FieldType(field.Properties[types.FieldPropVector])

		v_field := Field{Name: "vector field", BuiltinType: v_type}

		for i, v_value := range value.([]any) {
			if !v_field.Compare(schema, v_value, input.([]any)[i]) {
				return false
			}
		}

		return true
	} else {
		return value == input
	}
}

func (field *Field) ValidateType(table *Table, input any, allow_default bool) (any, error) {
	data_type := fmt.Sprintf("%T", input)
	switch field.BuiltinType {
	case types.FieldTypeInt:
		{
			switch data_type {
			case "int":
				return input.(int), nil
			case "float64":
				return int(input.(float64)), nil
			case "<nil>":
				if default_val, ok := field.Properties[types.FieldPropDefault]; ok && allow_default {
					if default_val == "auto" {
						return table.CreateId(), nil
					} else {
						str_int, err := strconv.ParseInt(default_val, 10, 0)
						if err != nil {
							return nil, err
						}
						return str_int, nil
					}
				} else if field.Name == "id" {
					return table.CreateId(), nil
				} else if field.Properties[types.FieldPropOptional] == "true" {
					return nil, nil
				} else {
					return nil, InvalidFieldTypeError(data_type, field.Name)
				}
			default:
				return nil, InvalidFieldTypeError(data_type, field.Name)
			}
		}
	case types.FieldTypeFloat:
		{
			switch data_type {
			case "float64":
				return input.(float64), nil
			case "int":
				return float64(input.(int)), nil
			case "<nil>":
				if default_val, ok := field.Properties[types.FieldPropDefault]; ok && allow_default {
					str_float, err := strconv.ParseFloat(default_val, 64)
					if err != nil {
						return nil, err
					}
					return str_float, nil
				} else if field.Properties[types.FieldPropOptional] == "true" {
					return nil, nil
				} else {
					return nil, InvalidFieldTypeError(data_type, field.Name)
				}
			default:
				return nil, InvalidFieldTypeError(data_type, field.Name)
			}
		}
	case types.FieldTypeString:
		{
			switch data_type {
			case "string":
				return input.(string), nil
			case "<nil>":
				if default_val, ok := field.Properties[types.FieldPropDefault]; ok && allow_default {
					return default_val, nil
				} else if field.Properties[types.FieldPropOptional] == "true" {
					return nil, nil
				} else {
					return nil, InvalidFieldTypeError(data_type, field.Name)
				}
			default:
				return nil, InvalidFieldTypeError(data_type, field.Name)
			}
		}
	case types.FieldTypeDate:
		{
			switch data_type {
			case "time.Time":
				return input.(time.Time), nil
			case "string":
				val, err := time.Parse(time.RFC3339, input.(string))
				if err != nil {
					return nil, err
				}
				return val, nil
			case "float64":
				val := time.UnixMilli(int64(input.(float64)))
				return val, nil
			case "<nil>":
				if default_val, ok := field.Properties[types.FieldPropDefault]; ok && allow_default {
					if default_val == "now" {
						time_string, _ := time.Now().MarshalText()
						t, _ := time.Parse(time.RFC3339, string(time_string))
						return t, nil
					}
				} else if field.Properties[types.FieldPropOptional] == "true" {
					return nil, nil
				} else {
					return nil, InvalidFieldTypeError(data_type, field.Name)
				}
			default:
				return nil, InvalidFieldTypeError(data_type, field.Name)
			}
		}
	case types.FieldTypeBool:
		{
			switch data_type {
			case "bool":
				return input.(bool), nil
			case "<nil>":
				if default_val, ok := field.Properties[types.FieldPropDefault]; ok && allow_default {
					if default_val == "true" {
						return true, nil
					} else {
						return false, nil
					}
				} else if field.Properties[types.FieldPropOptional] == "true" {
					return nil, nil
				} else {
					return nil, InvalidFieldTypeError(data_type, field.Name)
				}
			default:
				return nil, InvalidFieldTypeError(data_type, field.Name)
			}
		}
	case types.FieldTypeVector:
		{
			v_type := types.FieldType(field.Properties[types.FieldPropVector])
			err := ValidateFieldType(v_type)
			if err != nil {
				return nil, err
			}
			v_field := Field{Name: "vector_value", BuiltinType: v_type}

			switch data_type {
			case "[]interface {}":
				input := input.([]interface{})

				for i := 0; i < len(input); i++ {
					val, err := v_field.ValidateType(table, input[i], false)
					if err != nil {
						return nil, err
					}
					input[i] = val
				}

				return input, nil
			case "<nil>":
				// TODO: better vector default parsing
				if default_val, ok := field.Properties[types.FieldPropDefault]; ok && allow_default {
					default_val := strings.Split(default_val, ",")
					res := make([]any, len(default_val))
					v_type := types.FieldType(field.Properties[types.FieldPropVector])
					v_field := Field{Name: "vector_value", BuiltinType: v_type}

					for i := 0; i < len(default_val); i++ {
						res[i], err = v_field.ValidateType(table, default_val[i], false)
						if err != nil {
							return nil, err
						}
					}

					return res, nil
				} else if field.Properties[types.FieldPropOptional] == "true" {
					return nil, nil
				} else {
					return nil, InvalidFieldTypeError(data_type, field.Name)
				}
			default:
				return nil, InvalidFieldTypeError(data_type, field.Name)
			}
		}
	default:
		return nil, UnsupportedFieldTypeError(string(field.BuiltinType), field.Name)
	}
	return nil, UnsupportedFieldTypeError(string(field.BuiltinType), field.Name)
}

func InvalidFieldTypeError(invalid_type, field_name string) error {
	return fmt.Errorf("Invalid field type for %s: %s", field_name, invalid_type)
}

// if schema validation is working properly this error should never occur
func UnsupportedFieldTypeError(invalid_type, field_name string) error {
	return fmt.Errorf("Unsupported field type for %s: %s", field_name, invalid_type)
}

func (table *Table) CreateId() int {
	table.IdTracker++
	return table.IdTracker
}
