package generate

import (
	"strings"

	"github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
	"github.com/tobsdb/tobsdb/pkg"
)

func toPascalCase(t string) string {
	res := ""
	for _, v := range strings.Split(t, "_") {
		res += strings.Join([]string{strings.ToUpper(v[0:1]), v[1:]}, "")
	}
	return res
}

type (
	ParsedTable struct {
		Name   string        `json:"name"`
		Fields []ParsedField `json:"fields"`
	}

	ParsedField struct {
		Name        string                        `json:"name"`
		BuiltinType types.FieldType               `json:"type"`
		Properties  pkg.Map[props.FieldProp, any] `json:"properties"`
	}
)

func schemaDestructure(s *builder.Schema) []ParsedTable {
	res := []ParsedTable{}
	for _, k := range s.Tables.Sorted {
		t := s.Tables.Get(k)
		fields := []ParsedField{}
		for _, fk := range t.Fields.Sorted {
			f := t.Fields.Get(fk)
			fields = append(fields,
				ParsedField{f.Name, f.BuiltinType, f.Properties})
		}
		res = append(res,
			ParsedTable{t.Name, fields})
	}

	return res
}
