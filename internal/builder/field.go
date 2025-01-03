package builder

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
	"maps"

	"github.com/tobsdb/tobsdb/internal/parser"
	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
	"github.com/tobsdb/tobsdb/pkg"
)

type Field struct {
	Name        string
	BuiltinType types.FieldType
	Properties  pkg.Map[props.FieldProp, any]

	IncrementTracker atomic.Int64 `json:"-"`

	Table *Table `json:"-"`

	snapshot *Field
}

func (f *Field) NewSnapshot() {
	f.snapshot = &Field{
		Name:        f.Name,
		BuiltinType: f.BuiltinType,
		Properties:  maps.Clone(f.Properties),
	}
	f.snapshot.IncrementTracker.Store(f.IncrementTracker.Load())
}

func (f *Field) MarshalJSON() ([]byte, error) {
	type T Field
	return json.Marshal(struct {
		*T
		IncrementTracker int64
	}{(*T)(f), f.IncrementTracker.Load()})
}

func (f *Field) UnmarshalJSON(data []byte) error {
	type T Field
	buf := struct {
		*T
		IncrementTracker int64
	}{T: (*T)(f)}
	if err := json.Unmarshal(data, &buf); err != nil {
		return err
	}
	f.IncrementTracker.Store(buf.IncrementTracker)
	return nil
}

// field local rules:
// - primary key field must be type int
// - can't have key primary and optional prop true
// - can't have Vector type and unique prop true
// - can't have Vector/Bytes type and default prop
// - can't have vector prop on non-vector type
// - vector prop can't have Vector type; i.e. vector(Vector)
func CheckFieldRules(field *Field) error {
	if field.Properties.Has(props.FieldPropKey) {
		if key := field.Properties.Get(props.FieldPropKey); key == props.KeyPropPrimary {
			if field.BuiltinType != types.FieldTypeInt {
				return fmt.Errorf("field(%s %s key(primary)) must be type Int", field.Name, field.BuiltinType)
			}

			if opt, ok := field.Properties[props.FieldPropOptional]; ok && opt.(bool) {
				return fmt.Errorf("field(%s %s key(primary)) cannot be optional", field.Name, field.BuiltinType)
			}
		}
	}

	if field.BuiltinType == types.FieldTypeVector || field.BuiltinType == types.FieldTypeBytes {
		if field.Properties.Has(props.FieldPropDefault) {
			return fmt.Errorf("field(%s %s) cannot have default prop", field.Name, field.BuiltinType)
		}
	}

	if field.BuiltinType == types.FieldTypeVector {
		if unique := field.Properties.Get(props.FieldPropUnique); unique != nil && unique.(bool) {
			return fmt.Errorf("field(%s %s) cannot have unique prop", field.Name, field.BuiltinType)
		}

		if !field.Properties.Has(props.FieldPropVector) {
			return fmt.Errorf("field(%s %s) must have vector prop", field.Name, field.BuiltinType)
		}
	}

	if field.Properties.Has(props.FieldPropVector) {
		if field.BuiltinType != types.FieldTypeVector {
			return fmt.Errorf("field(%s %s) cannot have vector prop", field.Name, field.BuiltinType)
		}
		v_type, _ := parser.ParseVectorProp(field.Properties.Get(props.FieldPropVector).(string))
		if v_type == types.FieldTypeVector {
			return fmt.Errorf("vector(%s) is not allowed", v_type)
		}
	}

	return nil
}

func (field *Field) IsLess(a, b any) bool {
	// a nil value is always less than any non-nil value
	if a == nil && b == nil {
		return false
	} else if a == nil {
		return true
	} else if b == nil {
		return false
	}

	switch field.BuiltinType {
	case types.FieldTypeInt:
		a, b := a.(int), b.(int)
		return a < b
	case types.FieldTypeFloat:
		a, b := a.(float64), b.(float64)
		return a < b
	case types.FieldTypeString:
		a, b := a.(string), b.(string)
		return a < b
	case types.FieldTypeBool:
		a, b := a.(bool), b.(bool)
		return !a || b
	case types.FieldTypeBytes:
		return bytes.Compare(a.([]byte), b.([]byte)) < 0
	// Can't order by vector fields
	case types.FieldTypeVector:
		return false
	case types.FieldTypeDate:
		a, b := a.(time.Time), b.(time.Time)
		return a.Before(b)
	}

	return false
}

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
		if default_val := field.Properties.Get(props.FieldPropDefault); allow_default && default_val != nil {
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

		if is_opt := field.Properties.Get(props.FieldPropOptional); is_opt != nil && is_opt.(bool) {
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
		if default_val := field.Properties.Get(props.FieldPropDefault); default_val != nil && allow_default {
			str_float, err := strconv.ParseFloat(default_val.(string), 64)
			if err != nil {
				return nil, err
			}
			return str_float, nil
		}

		if is_opt := field.Properties.Get(props.FieldPropOptional); is_opt != nil && is_opt.(bool) {
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
		if default_val := field.Properties.Get(props.FieldPropDefault); default_val != nil && allow_default {
			default_val := default_val.(string)
			// we assume the user's text starts and ends with " or '
			default_val = default_val[1 : len(default_val)-1]
			return default_val, nil
		}

		if is_opt := field.Properties.Get(props.FieldPropOptional); is_opt != nil && is_opt.(bool) {
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
		if default_val := field.Properties.Get(props.FieldPropDefault); default_val != nil && allow_default {
			if default_val == "now" {
				time_string, _ := time.Now().MarshalText()
				t, _ := time.Parse(time.RFC3339, string(time_string))
				return t, nil
			}
		}

		if is_opt := field.Properties.Get(props.FieldPropOptional); is_opt != nil && is_opt.(bool) {
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
		if default_val := field.Properties.Get(props.FieldPropDefault); default_val != nil && allow_default {
			if default_val == "true" {
				return true, nil
			}
			return false, nil
		}

		if is_opt := field.Properties.Get(props.FieldPropOptional); is_opt != nil && is_opt.(bool) {
			return nil, nil
		}
	}
	return nil, invalidFieldTypeError(input, field.Name)
}

func validateTypeVector(field *Field, input any, allow_default bool) (any, error) {
	v_type, v_level := parser.ParseVectorProp(field.Properties.Get(props.FieldPropVector).(string))
	if !v_type.IsValid() {
		return nil, fmt.Errorf("Invalid field type: %s", v_type)
	}

	var v_field Field

	if v_level > 1 {
		v_field = Field{
			Name:        fmt.Sprintf("vector_value.%d", v_level-1),
			BuiltinType: types.FieldTypeVector,
			Properties:  pkg.Map[props.FieldProp, any]{},
			Table:       field.Table,
		}

		v_field.Properties.Set(props.FieldPropVector, fmt.Sprintf("%s,%d", v_type, v_level-1))
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
		if is_opt := field.Properties.Get(props.FieldPropOptional); is_opt != nil && is_opt.(bool) {
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
		if is_opt := field.Properties.Get(props.FieldPropOptional); is_opt != nil && is_opt.(bool) {
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
	return int(table.IdTracker.Add(1))
}

func (field *Field) AutoIncrement() int {
	if field.BuiltinType == types.FieldTypeInt {
		return int(field.IncrementTracker.Add(1))
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
	key_prop := field.Properties.Get(props.FieldPropKey)
	if key_prop != nil && key_prop == "primary" {
		return IndexLevelPrimary
	}

	unique_prop := field.Properties.Get(props.FieldPropUnique)
	if unique_prop != nil && unique_prop.(bool) {
		return IndexLevelUnique
	}

	return IndexLevelNone
}
