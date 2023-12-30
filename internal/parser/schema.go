package parser

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
)

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
			if !checkAlphanumericUnderScore(name) {
				return ParserStateIdle, nil,
					fmt.Errorf("Table name contains invalid characters: %s", name)
			}
			return ParserStateTableStart, &ParserData{Name: name}, nil
		}
		return ParserStateIdle, nil, errors.New("Invalid line")
	}

	if line == "}" {
		return ParserStateTableEnd, nil, nil
	}

	// regex splits by whitespace execpt inside parentheses: `(` and `)`
	// also allows for escaped parentheses `\(` and `\)` to avoid splitting
	r := regexp.MustCompile(`(?m)(\w+)|(\((?:[^\\)]|\\.)*\))`)
	splits := r.FindAllString(line, -1)
	if len(splits) == 0 {
		return ParserStateIdle, nil, fmt.Errorf("Invalid line: %s", line)
	}

	if raw_name := strings.SplitN(line, " ", 2)[0]; !checkAlphanumericUnderScore(raw_name) {
		return ParserStateIdle, nil,
			fmt.Errorf("Field name contains invalid characters: %s", raw_name)
	}

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

	return ParserStateNewField, &ParserData{splits[0], builtin_type, field_props}, nil
}

func checkAlphanumericUnderScore(name string) bool {
	if len(name) == 0 {
		return false
	}
	// alphanumeric or underscore, first character can't be number
	r := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	return r.MatchString(name)
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

		if len(value) == 0 {
			return nil, fmt.Errorf("No value for prop: %s", prop_name)
		}
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
