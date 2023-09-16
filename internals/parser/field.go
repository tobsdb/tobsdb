package parser

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	TDBTypes "github.com/tobshub/tobsdb/internals/types"
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

	if field.BuiltinType == TDBTypes.FieldTypeVector {
		v_type := TDBTypes.FieldType(field.Properties[TDBTypes.FieldPropVector])

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

func validateTypeInt(table *Table, field *Field, input any, data_type string, allow_default bool) (any, error) {
	switch data_type {
	case "int":
		return input.(int), nil
	case "float64":
		return int(input.(float64)), nil
	case "<nil>":
		if default_val, ok := field.Properties[TDBTypes.FieldPropDefault]; ok && allow_default {
			if default_val == "auto" {
				return table.createId(), nil
			} else {
				str_int, err := strconv.ParseInt(default_val, 10, 0)
				if err != nil {
					return nil, err
				}
				return str_int, nil
			}
		} else if field.Name == "id" {
			return table.createId(), nil
		} else if field.Properties[TDBTypes.FieldPropOptional] == "true" {
			return nil, nil
		} else {
			return nil, invalidFieldTypeError(data_type, field.Name)
		}
	}
	return nil, invalidFieldTypeError(data_type, field.Name)
}

func validateTypeFloat(field *Field, input any, data_type string, allow_default bool) (any, error) {
	switch data_type {
	case "float64":
		return input.(float64), nil
	case "int":
		return float64(input.(int)), nil
	case "<nil>":
		if default_val, ok := field.Properties[TDBTypes.FieldPropDefault]; ok && allow_default {
			str_float, err := strconv.ParseFloat(default_val, 64)
			if err != nil {
				return nil, err
			}
			return str_float, nil
		} else if field.Properties[TDBTypes.FieldPropOptional] == "true" {
			return nil, nil
		} else {
			return nil, invalidFieldTypeError(data_type, field.Name)
		}
	}
	return nil, invalidFieldTypeError(data_type, field.Name)
}

func validateTypeString(field *Field, input any, data_type string, allow_default bool) (any, error) {
	switch data_type {
	case "string":
		return input.(string), nil
	case "<nil>":
		if default_val, ok := field.Properties[TDBTypes.FieldPropDefault]; ok && allow_default {
			return default_val, nil
		} else if field.Properties[TDBTypes.FieldPropOptional] == "true" {
			return nil, nil
		} else {
			return nil, invalidFieldTypeError(data_type, field.Name)
		}
	}
	return nil, invalidFieldTypeError(data_type, field.Name)
}

func validateTypeDate(field *Field, input any, data_type string, allow_default bool) (any, error) {
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
		if default_val, ok := field.Properties[TDBTypes.FieldPropDefault]; ok && allow_default {
			if default_val == "now" {
				time_string, _ := time.Now().MarshalText()
				t, _ := time.Parse(time.RFC3339, string(time_string))
				return t, nil
			}
		} else if field.Properties[TDBTypes.FieldPropOptional] == "true" {
			return nil, nil
		} else {
			return nil, invalidFieldTypeError(data_type, field.Name)
		}
	}
	return nil, invalidFieldTypeError(data_type, field.Name)
}

func validateTypeBool(field *Field, input any, data_type string, allow_default bool) (any, error) {
	switch data_type {
	case "bool":
		return input.(bool), nil
	case "string":
		val, err := strconv.ParseBool(input.(string))
		if err != nil {
			return nil, invalidFieldTypeError(data_type, field.Name)
		} else {
			return val, nil
		}
	case "<nil>":
		if default_val, ok := field.Properties[TDBTypes.FieldPropDefault]; ok && allow_default {
			if default_val == "true" {
				return true, nil
			} else {
				return false, nil
			}
		} else if field.Properties[TDBTypes.FieldPropOptional] == "true" {
			return nil, nil
		} else {
			return nil, invalidFieldTypeError(data_type, field.Name)
		}
	}
	return nil, invalidFieldTypeError(data_type, field.Name)
}

func validateTypeVector(table *Table, field *Field, input any, data_type string, allow_default bool) (any, error) {
	v_type := TDBTypes.FieldType(field.Properties[TDBTypes.FieldPropVector])
	err := validateFieldType(v_type)
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
		if default_val, ok := field.Properties[TDBTypes.FieldPropDefault]; ok && allow_default {
			default_val := strings.Split(default_val, ",")
			res := make([]any, len(default_val))
			v_type := TDBTypes.FieldType(field.Properties[TDBTypes.FieldPropVector])
			v_field := Field{Name: "vector_value", BuiltinType: v_type}

			for i := 0; i < len(default_val); i++ {
				res[i], err = v_field.ValidateType(table, default_val[i], false)
				if err != nil {
					return nil, err
				}
			}

			return res, nil
		} else if field.Properties[TDBTypes.FieldPropOptional] == "true" {
			return nil, nil
		} else {
			return nil, invalidFieldTypeError(data_type, field.Name)
		}
	}
	return nil, invalidFieldTypeError(data_type, field.Name)
}

func (field *Field) ValidateType(table *Table, input any, allow_default bool) (any, error) {
	data_type := fmt.Sprintf("%T", input)

	switch field.BuiltinType {
	case TDBTypes.FieldTypeInt:
		return validateTypeInt(table, field, input, data_type, allow_default)
	case TDBTypes.FieldTypeFloat:
		return validateTypeFloat(field, input, data_type, allow_default)
	case TDBTypes.FieldTypeString:
		return validateTypeString(field, input, data_type, allow_default)
	case TDBTypes.FieldTypeDate:
		return validateTypeDate(field, input, data_type, allow_default)
	case TDBTypes.FieldTypeBool:
		return validateTypeBool(field, input, data_type, allow_default)
	case TDBTypes.FieldTypeVector:
		return validateTypeVector(table, field, input, data_type, allow_default)
	}

	return nil, unsupportedFieldTypeError(string(field.BuiltinType), field.Name)
}

func invalidFieldTypeError(invalid_type, field_name string) error {
	return fmt.Errorf("Invalid field type for %s: %s", field_name, invalid_type)
}

// if schema validation is working properly this error should never occur
func unsupportedFieldTypeError(invalid_type, field_name string) error {
	return fmt.Errorf("Unsupported field type for %s: %s", field_name, invalid_type)
}

func (table *Table) createId() int {
	table.IdTracker++
	return table.IdTracker
}
