package parser

import (
	"fmt"
	"strconv"
	"time"

	TDBTypes "github.com/tobshub/tobsdb/internals/types"
)

// TODO: work on dynamic compares for vector & string types
func (field *Field) Compare(schema *Table, value any, input any) bool {
	value, err := field.ValidateType(schema, value, false)
	if err != nil {
		return false
	}

	switch field.BuiltinType {
	case TDBTypes.FieldTypeVector:
		return CompareVector(schema, field, value.([]any), input)
	case TDBTypes.FieldTypeInt:
		return CompareInt(schema, field, value.(int), input)
	default:
		input, err := field.ValidateType(schema, input, false)
		if err != nil {
			return false
		}
		return value == input
	}
}

func validateTypeInt(table *Table, field *Field, input any, allow_default bool) (any, error) {
	switch input.(type) {
	case int:
		return input.(int), nil
	case float64:
		return int(input.(float64)), nil
	case nil:
		if default_val, ok := field.Properties[TDBTypes.FieldPropDefault]; ok && allow_default {
			if default_val == "auto" {
				return table.createId(), nil
			}
			str_int, err := strconv.ParseInt(default_val, 10, 0)
			if err != nil {
				return nil, err
			}
			return str_int, nil

		}

		if field.Name == "id" {
			return table.createId(), nil
		}

		if field.Properties[TDBTypes.FieldPropOptional] == "true" {
			return nil, nil
		}
	}
	return nil, invalidFieldTypeError(input, field.Name)
}

func validateTypeFloat(field *Field, input any, allow_default bool) (any, error) {
	switch input.(type) {
	case float64:
		return input.(float64), nil
	case int:
		return float64(input.(int)), nil
	case nil:
		if default_val, ok := field.Properties[TDBTypes.FieldPropDefault]; ok && allow_default {
			str_float, err := strconv.ParseFloat(default_val, 64)
			if err != nil {
				return nil, err
			}
			return str_float, nil
		}

		if field.Properties[TDBTypes.FieldPropOptional] == "true" {
			return nil, nil
		}
	}
	return nil, invalidFieldTypeError(input, field.Name)
}

func validateTypeString(field *Field, input any, allow_default bool) (any, error) {
	switch input.(type) {
	case string:
		return input.(string), nil
	case nil:
		if default_val, ok := field.Properties[TDBTypes.FieldPropDefault]; ok && allow_default {
			return default_val, nil
		}

		if field.Properties[TDBTypes.FieldPropOptional] == "true" {
			return nil, nil
		}
	}
	return nil, invalidFieldTypeError(input, field.Name)
}

func validateTypeDate(field *Field, input any, allow_default bool) (any, error) {
	switch input.(type) {
	case time.Time:
		return input.(time.Time), nil
	case string:
		val, err := time.Parse(time.RFC3339, input.(string))
		if err != nil {
			return nil, err
		}
		return val, nil
	case float64:
		val := time.UnixMilli(int64(input.(float64)))
		return val, nil
	case int:
		val := time.UnixMilli(int64(input.(int)))
		return val, nil
	case nil:
		if default_val, ok := field.Properties[TDBTypes.FieldPropDefault]; ok && allow_default {
			if default_val == "now" {
				time_string, _ := time.Now().MarshalText()
				t, _ := time.Parse(time.RFC3339, string(time_string))
				return t, nil
			}
		} else if field.Properties[TDBTypes.FieldPropOptional] == "true" {
			return nil, nil
		}
	}
	return nil, invalidFieldTypeError(input, field.Name)
}

func validateTypeBool(field *Field, input any, allow_default bool) (any, error) {
	switch input.(type) {
	case bool:
		return input.(bool), nil
	case string:
		val, err := strconv.ParseBool(input.(string))
		if err != nil {
			return nil, invalidFieldTypeError(input, field.Name)
		}
		return val, nil
	case nil:
		if default_val, ok := field.Properties[TDBTypes.FieldPropDefault]; ok && allow_default {
			if default_val == "true" {
				return true, nil
			}
			return false, nil
		} else if field.Properties[TDBTypes.FieldPropOptional] == "true" {
			return nil, nil
		}
	}
	return nil, invalidFieldTypeError(input, field.Name)
}

func validateTypeVector(table *Table, field *Field, input any, allow_default bool) (any, error) {
	v_type, v_level := ParseVectorProp(field.Properties[TDBTypes.FieldPropVector])
	err := validateFieldType(v_type)
	if err != nil {
		return nil, err
	}

	var v_field Field

	if v_level > 1 {
		v_field = Field{
			Name:        fmt.Sprintf("vector_value.%d", v_level-1),
			BuiltinType: TDBTypes.FieldTypeVector,
			Properties:  map[TDBTypes.FieldProp]string{},
		}

		v_field.Properties[TDBTypes.FieldPropVector] = fmt.Sprintf("%s,%d", v_type, v_level-1)
	} else {
		v_field = Field{Name: "vector_value.0", BuiltinType: v_type}
	}

	switch input.(type) {
	case []interface{}:
		input := input.([]interface{})

		for i := 0; i < len(input); i++ {
			val, err := v_field.ValidateType(table, input[i], false)
			if err != nil {
				return nil, err
			}
			input[i] = val
		}

		return input, nil
	case nil:
		if field.Properties[TDBTypes.FieldPropOptional] == "true" {
			return nil, nil
		}
	}
	return nil, invalidFieldTypeError(input, field.Name)
}

func (field *Field) ValidateType(table *Table, input any, allow_default bool) (any, error) {
	switch field.BuiltinType {
	case TDBTypes.FieldTypeInt:
		return validateTypeInt(table, field, input, allow_default)
	case TDBTypes.FieldTypeFloat:
		return validateTypeFloat(field, input, allow_default)
	case TDBTypes.FieldTypeString:
		return validateTypeString(field, input, allow_default)
	case TDBTypes.FieldTypeDate:
		return validateTypeDate(field, input, allow_default)
	case TDBTypes.FieldTypeBool:
		return validateTypeBool(field, input, allow_default)
	case TDBTypes.FieldTypeVector:
		return validateTypeVector(table, field, input, allow_default)
	}

	return nil, unsupportedFieldTypeError(string(field.BuiltinType), field.Name)
}

func invalidFieldTypeError(input any, field_name string) error {
	return fmt.Errorf("Invalid field type for %s: %T", field_name, input)
}

// if schema validation is working properly this error should never occur
func unsupportedFieldTypeError(invalid_type, field_name string) error {
	return fmt.Errorf("Unsupported field type for %s: %s", field_name, invalid_type)
}

func (table *Table) createId() int {
	table.IdTracker++
	return table.IdTracker
}
