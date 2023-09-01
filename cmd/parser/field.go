package parser

import (
	"fmt"
	"strconv"
	"time"

	"github.com/tobshub/tobsdb/cmd/types"
)

func (field *Field) ValidateType(table *Table, input any, allow_default bool) (any, error) {
	data_type := fmt.Sprintf("%T", input)
	switch field.BuiltinType {
	case types.Int:
		{
			switch data_type {
			case "int":
				return input.(int), nil
			case "float64":
				return int(input.(float64)), nil
			case "<nil>":
				if default_val, ok := field.Properties[types.Default]; ok && allow_default {
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
				} else if field.Properties[types.Optional] == "true" {
					return nil, nil
				} else {
					return nil, InvalidFieldTypeError(data_type, field.Name)
				}
			default:
				return nil, InvalidFieldTypeError(data_type, field.Name)
			}
		}
	case types.Float:
		{
			switch data_type {
			case "float64":
				return input.(float64), nil
			case "int":
				return float64(input.(int)), nil
			case "<nil>":
				if default_val, ok := field.Properties[types.Default]; ok && allow_default {
					str_float, err := strconv.ParseFloat(default_val, 64)
					if err != nil {
						return nil, err
					}
					return str_float, nil
				} else if field.Properties[types.Optional] == "true" {
					return nil, nil
				} else {
					return nil, InvalidFieldTypeError(data_type, field.Name)
				}
			default:
				return nil, InvalidFieldTypeError(data_type, field.Name)
			}
		}
	case types.String:
		{
			switch data_type {
			case "string":
				return input.(string), nil
			case "<nil>":
				if default_val, ok := field.Properties[types.Default]; ok && allow_default {
					return default_val, nil
				} else if field.Properties[types.Optional] == "true" {
					return nil, nil
				} else {
					return nil, InvalidFieldTypeError(data_type, field.Name)
				}
			default:
				return nil, InvalidFieldTypeError(data_type, field.Name)
			}
		}
	case types.Date:
		{
			switch data_type {
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
				if default_val, ok := field.Properties[types.Default]; ok && allow_default {
					if default_val == "now" {
						return time.Now(), nil
					}
				} else if field.Properties[types.Optional] == "true" {
					return nil, nil
				} else {
					return nil, InvalidFieldTypeError(data_type, field.Name)
				}
			default:
				return nil, InvalidFieldTypeError(data_type, field.Name)
			}
		}
	case types.Bool:
		{
			switch data_type {
			case "bool":
				return input.(bool), nil
			case "<nil>":
				if default_val, ok := field.Properties[types.Default]; ok && allow_default {
					if default_val == "true" {
						return true, nil
					} else {
						return false, nil
					}
				} else if field.Properties[types.Optional] == "true" {
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
