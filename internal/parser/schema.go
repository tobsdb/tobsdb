package parser

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
	"github.com/tobsdb/tobsdb/pkg"
)

type Table struct {
	Name      string
	Fields    map[string]*Field
	Indexes   []string
	IdTracker int
}

type Field struct {
	Name             string
	BuiltinType      types.FieldType
	Properties       map[props.FieldProp]string
	IncrementTracker int
}

type LineParserState int

const (
	ParserStateTableStart LineParserState = iota
	ParserStateTableEnd
	ParserStateNewField
	ParserStateIdle
)

type ParserData struct {
	Name         string
	Builtin_type types.FieldType
	Properties   map[props.FieldProp]string
}

const (
	table_prefix     = "$TABLE "
	table_prefix_len = len(table_prefix)
)

func LineParser(line string) (LineParserState, *ParserData, error) {
	if strings.HasPrefix(line, table_prefix) {
		line := line[table_prefix_len:]
		name_end := strings.Index(line, " ")

		if name_end > 0 {
			open_bracket := strings.TrimSpace(line[name_end:])
			if open_bracket != "{" {
				return ParserStateIdle, nil, errors.New("Table name cannot include space")
			}
			name := line[:name_end]
			return ParserStateTableStart, &ParserData{Name: name}, nil
		}
	} else if line == "}" {
		return ParserStateTableEnd, nil, nil
	} else {
		splits := strings.Split(line, " ")
		splits = pkg.Filter(splits, func(s string) bool { return len(s) > 0 })
		if len(splits) < 2 {
			return ParserStateIdle, nil, errors.New("Invalid line")
		}
		builtin_type := types.FieldType(splits[1])
		err := validateFieldType(builtin_type)
		if err != nil {
			return ParserStateIdle, nil, err
		}

		raw_field_props := strings.Join(splits[2:], " ")
		field_props, err := parseRawFieldProps(raw_field_props)
		if err != nil {
			return ParserStateIdle, nil, err
		}

		return ParserStateNewField, &ParserData{
			Name:         splits[0],
			Builtin_type: builtin_type,
			Properties:   field_props,
		}, nil
	}
	return ParserStateIdle, nil, errors.New("Invalid line")
}

func parseRawFieldProps(raw string) (map[props.FieldProp]string, error) {
	field_props := make(map[props.FieldProp]string)

	r := regexp.MustCompile(`(?m)(\w+)\(([^)]+)\)`)

	for _, entry := range r.FindAllString(raw, -1) {
		split := strings.Split(entry, "(")
		prop, value := props.FieldProp(split[0]), strings.TrimRight(split[1], ")")
		if !prop.IsValid() {
			return nil, fmt.Errorf("Invalid field prop: %s", prop)
		}
		field_props[prop] = value
	}

	return field_props, nil
}

func validateFieldType(builtin_type types.FieldType) error {
	if !builtin_type.IsValid() {
		return fmt.Errorf("Invalid field type: %s", builtin_type)
	}
	return nil
}
