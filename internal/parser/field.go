package parser

import (
	"fmt"
	"strconv"
	"time"

	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
)

func (field *Field) Compare(value any, input any) bool {
	if value == nil && input == nil {
		return true
	}

	value, err := field.ValidateType(value, false)
	if err != nil {
		return false
	}

	if value == nil {
		return field.compareDefault(value, input)
	}

	switch field.BuiltinType {
	case types.FieldTypeVector:
		return field.compareVector(value.([]any), input)
	case types.FieldTypeInt:
		return field.compareInt(value.(int), input)
	case types.FieldTypeString:
		return field.compareString(value.(string), input)
	default:
		return field.compareDefault(value, input)
	}
}

func validateTypeInt(field *Field, input any, allow_default bool) (any, error) {
	switch input := input.(type) {
	case int:
		return input, nil
	case float64:
		return int(input), nil
	case nil:
		if default_val, ok := field.Properties[props.FieldPropDefault]; ok && allow_default {
			if default_val == "auto" {
				return int(time.Now().UnixMicro()), nil
			}
			if default_val == "autoincrement" {
				return field.AutoIncrement(), nil
			}
			str_int, err := strconv.ParseInt(default_val.(string), 10, 0)
			if err != nil {
				return nil, err
			}
			return int(str_int), nil

		}

		if is_opt, ok := field.Properties[props.FieldPropOptional]; ok && is_opt.(bool) {
			return nil, nil
		}
	}
	return nil, invalidFieldTypeError(input, field.Name)
}

func validateTypeFloat(field *Field, input any, allow_default bool) (any, error) {
	switch input := input.(type) {
	case float64:
		return input, nil
	case int:
		return float64(input), nil
	case nil:
		if default_val, ok := field.Properties[props.FieldPropDefault]; ok && allow_default {
			str_float, err := strconv.ParseFloat(default_val.(string), 64)
			if err != nil {
				return nil, err
			}
			return str_float, nil
		}

		if is_opt, ok := field.Properties[props.FieldPropOptional]; ok && is_opt.(bool) {
			return nil, nil
		}
	}
	return nil, invalidFieldTypeError(input, field.Name)
}

func validateTypeString(field *Field, input any, allow_default bool) (any, error) {
	switch input := input.(type) {
	case string:
		return input, nil
	case nil:
		if default_val, ok := field.Properties[props.FieldPropDefault]; ok && allow_default {
			default_val := default_val.(string)
			// we assume the user's text starts and ends with " or '
			default_val = default_val[1 : len(default_val)-1]
			return default_val, nil
		}

		if is_opt, ok := field.Properties[props.FieldPropOptional]; ok && is_opt.(bool) {
			return nil, nil
		}
	}
	return nil, invalidFieldTypeError(input, field.Name)
}

func validateTypeDate(field *Field, input any, allow_default bool) (any, error) {
	switch input := input.(type) {
	case time.Time:
		return input, nil
	case string:
		val, err := time.Parse(time.RFC3339, input)
		if err != nil {
			return nil, err
		}
		return val, nil
	case float64:
		val := time.UnixMilli(int64(input))
		return val, nil
	case int:
		val := time.UnixMilli(int64(input))
		return val, nil
	case nil:
		if default_val, ok := field.Properties[props.FieldPropDefault]; ok && allow_default {
			if default_val == "now" {
				time_string, _ := time.Now().MarshalText()
				t, _ := time.Parse(time.RFC3339, string(time_string))
				return t, nil
			}
		} else if is_opt, ok := field.Properties[props.FieldPropOptional]; ok && is_opt.(bool) {
			return nil, nil
		}
	}
	return nil, invalidFieldTypeError(input, field.Name)
}

func validateTypeBool(field *Field, input any, allow_default bool) (any, error) {
	switch input := input.(type) {
	case bool:
		return input, nil
	case string:
		val, err := strconv.ParseBool(input)
		if err != nil {
			return nil, invalidFieldTypeError(input, field.Name)
		}
		return val, nil
	case nil:
		if default_val, ok := field.Properties[props.FieldPropDefault]; ok && allow_default {
			if default_val == "true" {
				return true, nil
			}
			return false, nil
		} else if is_opt, ok := field.Properties[props.FieldPropOptional]; ok && is_opt.(bool) {
			return nil, nil
		}
	}
	return nil, invalidFieldTypeError(input, field.Name)
}

func validateTypeVector(field *Field, input any, allow_default bool) (any, error) {
	v_type, v_level := ParseVectorProp(field.Properties[props.FieldPropVector].(string))
	if !v_type.IsValid() {
		return nil, fmt.Errorf("Invalid field type: %s", v_type)
	}

	var v_field Field

	if v_level > 1 {
		v_field = Field{
			Name:        fmt.Sprintf("vector_value.%d", v_level-1),
			BuiltinType: types.FieldTypeVector,
			Properties:  map[props.FieldProp]any{},
			Table:       field.Table,
		}

		v_field.Properties[props.FieldPropVector] = fmt.Sprintf("%s,%d", v_type, v_level-1)
	} else {
		v_field = Field{Name: "vector_value.0", BuiltinType: v_type, Table: field.Table}
	}

	switch input := input.(type) {
	case []interface{}:
		for i := 0; i < len(input); i++ {
			val, err := (&v_field).ValidateType(input[i], false)
			if err != nil {
				return nil, err
			}
			input[i] = val
		}

		return input, nil
	case nil:
		if is_opt, ok := field.Properties[props.FieldPropOptional]; ok && is_opt.(bool) {
			return nil, nil
		}
	}
	return nil, invalidFieldTypeError(input, field.Name)
}

func validateTypeBytes(field *Field, input any, allow_default bool) (any, error) {
	switch input := input.(type) {
	case []byte:
		return input, nil
	case string:
		return []byte(input), nil
	case nil:
		if is_opt, ok := field.Properties[props.FieldPropOptional]; ok && is_opt.(bool) {
			return nil, nil
		}
	}

	return nil, invalidFieldTypeError(input, field.Name)
}

func (field *Field) ValidateType(input any, allow_default bool) (any, error) {
	switch field.BuiltinType {
	case types.FieldTypeInt:
		return validateTypeInt(field, input, allow_default)
	case types.FieldTypeFloat:
		return validateTypeFloat(field, input, allow_default)
	case types.FieldTypeString:
		return validateTypeString(field, input, allow_default)
	case types.FieldTypeDate:
		return validateTypeDate(field, input, allow_default)
	case types.FieldTypeBool:
		return validateTypeBool(field, input, allow_default)
	case types.FieldTypeVector:
		return validateTypeVector(field, input, allow_default)
	case types.FieldTypeBytes:
		return validateTypeBytes(field, input, allow_default)
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

func (table *Table) CreateId() int {
	table.IdTracker++
	return table.IdTracker
}

func (field *Field) AutoIncrement() int {
	if field.BuiltinType == types.FieldTypeInt {
		field.IncrementTracker++
		return field.IncrementTracker
	}

	return 0
}

type IndexLevel int

const (
	IndexLevelNone IndexLevel = iota
	IndexLevelPrimary
	IndexLevelUnique
)

func (field *Field) IndexLevel() IndexLevel {
	key_prop, has_key_prop := field.Properties[props.FieldPropKey]
	if has_key_prop && key_prop == "primary" {
		return IndexLevelPrimary
	}

	unique_prop, has_unique_prop := field.Properties[props.FieldPropUnique]
	if has_unique_prop && unique_prop.(bool) {
		return IndexLevelUnique
	}

	return IndexLevelNone
}
