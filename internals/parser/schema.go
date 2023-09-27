package parser

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	TDBTypes "github.com/tobshub/tobsdb/internals/types"
	"golang.org/x/exp/slices"
)

type Table struct {
	Name      string
	Fields    map[string]Field
	Indexes   []string
	IdTracker int
}

type Field struct {
	Name        string
	BuiltinType TDBTypes.FieldType
	Properties  map[TDBTypes.FieldProp]string
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
	Builtin_type TDBTypes.FieldType
	Properties   map[TDBTypes.FieldProp]string
}

func LineParser(line string) (LineParserState, ParserData, error) {
	if strings.HasPrefix(line, "$TABLE") {
		name := line[7:]
		name_end := strings.Index(name, " ")

		if name_end > 0 {
			open_bracket := strings.TrimSpace(name[name_end:])
			if open_bracket != "{" {
				return ParserStateIdle, ParserData{}, fmt.Errorf("Table name cannot include space")
			}
			name = name[:name_end]
			return ParserStateTableStart, ParserData{Name: name}, nil
		}
	} else if line == "}" {
		return ParserStateTableEnd, ParserData{}, nil
	} else {
		splits := cleanLineSplit(strings.Split(line, " "))
		if len(splits) < 2 {
			return ParserStateIdle, ParserData{}, errors.New("Invalid line")
		}
		builtin_type := TDBTypes.FieldType(splits[1])

		field_props, err := parseRawFieldProps(strings.Join(splits[2:], " "))
		err = validateFieldType(builtin_type)

		if err != nil {
			return ParserStateIdle, ParserData{}, err
		}

		return ParserStateNewField, ParserData{Name: splits[0], Builtin_type: builtin_type, Properties: field_props}, nil
	}
	return ParserStateIdle, ParserData{}, errors.New("Invalid line")
}

func parseRawFieldProps(raw string) (map[TDBTypes.FieldProp]string, error) {
	props := make(map[TDBTypes.FieldProp]string)

	r := regexp.MustCompile(`(?m)(\w+)\(([^)]+)\)`)

	for _, entry := range r.FindAllString(raw, -1) {
		split := strings.Split(entry, "(")
		prop, value := split[0], strings.TrimRight(split[1], ")")
		props[TDBTypes.FieldProp(prop)] = value
	}

	return props, nil
}

func validateFieldType(builtin_type TDBTypes.FieldType) error {
	if slices.Contains(TDBTypes.VALID_BUILTIN_TYPES, builtin_type) {
		return nil
	}
	return fmt.Errorf("Invalid field type %s", builtin_type)
}

func cleanLineSplit(splits []string) []string {
	for i := 0; i < len(splits); i++ {
		if len(splits[i]) == 0 {
			splits = append(splits[:i], splits[i+1:]...)
			i--
		}
	}
	return splits
}
