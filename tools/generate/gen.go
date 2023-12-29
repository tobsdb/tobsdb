package generate

import (
	"fmt"

	"github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
	"github.com/tobsdb/tobsdb/pkg"
)

type (
	JsonTable struct {
		Name   string      `json:"name"`
		Fields []JsonField `json:"fields"`
	}

	JsonField struct {
		Name        string                        `json:"name"`
		BuiltinType types.FieldType               `json:"type"`
		Properties  pkg.Map[props.FieldProp, any] `json:"properties"`
	}
)

func schemaDestructure(s *builder.Schema) []JsonTable {
	res := []JsonTable{}
	for _, t := range s.Tables {
		fields := []JsonField{}
		for _, f := range t.Fields {
			fields = append(fields,
				JsonField{f.Name, f.BuiltinType, f.Properties})
		}
		res = append(res,
			JsonTable{t.Name, fields})
	}

	return res
}

func SchemaToLang(schema *builder.Schema, lang string) ([]byte, error) {
	s := schemaDestructure(schema)
	switch lang {
	case "json":
		return SchemaToJson(s)
	case "typescript":
		return SchemaToTypescript(s), nil
	default:
		return nil, fmt.Errorf("Unsupported Language: %s", lang)
	}
}
