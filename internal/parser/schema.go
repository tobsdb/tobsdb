package parser

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
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
	Properties       map[props.FieldProp]any
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
	Properties   map[props.FieldProp]any
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
		// regex splits by whitespace execpt inside parentheses: `(` and `)`
		// also allows for escaped parentheses `\(` and `\)` to avoid splitting
		r := regexp.MustCompile(`(?m)(\w+)|(\((?:[^\\)]|\\.)*\))`)
		splits := r.FindAllString(line, -1)
		if len(splits) < 2 {
			return ParserStateIdle, nil, fmt.Errorf("Field %s does not have a type", splits[0])
		}

		builtin_type := types.FieldType(splits[1])
		if !builtin_type.IsValid() {
			return ParserStateIdle, nil, fmt.Errorf("Invalid field type: %s", builtin_type)
		}

		raw_field_props := splits[2:]

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

func parseRawFieldProps(raw []string) (map[props.FieldProp]any, error) {
	field_props := make(map[props.FieldProp]any)

	for i := 0; i < len(raw); i += 2 {
		prop_name := props.FieldProp(raw[i])
		if !prop_name.IsValid() {
			return nil, fmt.Errorf("Invalid field prop: %s", prop_name)
		}
		j := i + 1
		if j >= len(raw) {
			return nil, fmt.Errorf("No value for prop: %s", prop_name)
		}

		value := raw[j]
		// remove surrounding parentheses
		value = strings.TrimLeft(value, "(")
		value = strings.TrimRight(value, ")")
		// replace escaped parentheses with real parentheses
		value = strings.ReplaceAll(value, "\\)", ")")
		value = strings.ReplaceAll(value, "\\(", "(")
		prop_value, err := props.ValidatePropValue(prop_name, value)
		if err != nil {
			return nil, err
		}
		field_props[prop_name] = prop_value
	}

	return field_props, nil
}
