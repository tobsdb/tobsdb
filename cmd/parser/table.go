package parser

import (
	"fmt"
	"strconv"
	"time"

	"github.com/tobshub/tobsdb/cmd/types"
)

func (table *Table) Create(d map[string]any) (map[string]any, error) {
	row := make(map[string]any)

	for _, field := range table.fields {
		data := d[field.name]
		data_type := fmt.Sprintf("%T", data)
		switch field.builtin_type {
		case types.Int:
			{
				switch data_type {
				case "float64":
					row[field.name] = int(data.(float64))
				case "<nil>":
					if default_val, ok := field.properties[types.Default]; ok {
						if default_val == "auto" {
							row[field.name] = CreateId()
						} else {
							str_int, err := strconv.ParseInt(default_val, 10, 0)
							if err != nil {
								return row, err
							}
							row[field.name] = str_int
						}
					} else if field.name == "id" {
						row[field.name] = CreateId()
					} else if field.properties[types.Optional] == "true" {
						row[field.name] = nil
					} else {
						return row, InvalidFieldTypeError(data_type, table.Name, field.name, string(field.builtin_type))
					}
				default:
					return row, InvalidFieldTypeError(data_type, table.Name, field.name, string(field.builtin_type))
				}
			}
		case types.Float:
			{
				switch data_type {
				case "float64":
					row[field.name] = data.(float64)
				case "<nil>":
					if default_val, ok := field.properties[types.Default]; ok {
						str_float, err := strconv.ParseFloat(default_val, 64)
						if err != nil {
							return row, err
						}
						row[field.name] = str_float
					} else if field.properties[types.Optional] == "true" {
						row[field.name] = nil
					} else {
						return row, InvalidFieldTypeError(data_type, table.Name, field.name, string(field.builtin_type))
					}
				default:
					return row, InvalidFieldTypeError(data_type, table.Name, field.name, string(field.builtin_type))
				}
			}
		case types.String:
			{
				switch data_type {
				case "string":
					row[field.name] = data.(string)
				case "<nil>":
					if default_val, ok := field.properties[types.Default]; ok {
						row[field.name] = default_val
					} else if field.properties[types.Optional] == "true" {
						row[field.name] = nil
					} else {
						return row, InvalidFieldTypeError(data_type, table.Name, field.name, string(field.builtin_type))
					}
				default:
					return row, InvalidFieldTypeError(data_type, table.Name, field.name, string(field.builtin_type))
				}
			}
		case types.Date:
			{
				switch data_type {
				case "string":
					val, err := time.Parse(time.RFC3339, data.(string))
					if err != nil {
						return row, err
					}
					row[field.name] = val
				case "float64":
					val := time.UnixMilli(int64(data.(float64)))
					row[field.name] = val
				case "<nil>":
					if default_val, ok := field.properties[types.Default]; ok {
						if default_val == "now" {
							row[field.name] = time.Now()
						}
					} else if field.properties[types.Optional] == "true" {
						row[field.name] = nil
					} else {
						return row, InvalidFieldTypeError(data_type, table.Name, field.name, string(field.builtin_type))
					}
				default:
					return row, InvalidFieldTypeError(data_type, table.Name, field.name, string(field.builtin_type))
				}
			}
		case types.Bool:
			{
				switch data_type {
				case "bool":
					row[field.name] = data.(bool)
				case "<nil>":
					if default_val, ok := field.properties[types.Default]; ok {
						if default_val == "true" {
							row[field.name] = true
						} else {
							row[field.name] = false
						}
					} else if field.properties[types.Optional] == "true" {
						row[field.name] = nil
					} else {
						return row, InvalidFieldTypeError(data_type, table.Name, field.name, string(field.builtin_type))
					}
				default:
					return row, InvalidFieldTypeError(data_type, table.Name, field.name, string(field.builtin_type))
				}
			}
		}
	}

	return row, nil
}

var id_tracker int = 0

func CreateId() int {
	id_tracker++
	return id_tracker
}

func InvalidFieldTypeError(invalid_type, table_name, field_name, field_type string) error {
	return fmt.Errorf("Invalid field type %s: %s.%s should be type %s", invalid_type, table_name, field_name, field_type)
}
