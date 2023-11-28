package props

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/tobsdb/tobsdb/internal/types"
)

func ParseRelationPropSafe(relation string) (string, string, error) {
	parsed_rel := strings.Split(relation, ".")
	if len(parsed_rel) <= 1 || len(parsed_rel) > 2 {
		return "", "", fmt.Errorf("Invalid syntax: relation(%s)", relation)
	}
	table, field := strings.TrimSpace(parsed_rel[0]), strings.TrimSpace(parsed_rel[1])
	if len(table) == 0 || len(field) == 0 {
		return "", "", fmt.Errorf("Invalid syntax: relation(%s)", relation)
	}
	return table, field, nil
}

func ParseVectorPropSafe(value string) (types.FieldType, int, error) {
	parsed_val := strings.Split(value, ",")

	if len(parsed_val) == 0 || len(parsed_val) > 2 {
		return "", 0, fmt.Errorf("Invalid syntax: vector(%s)", value)
	}

	v_type := types.FieldType(strings.TrimSpace(parsed_val[0]))
	if !v_type.IsValid() {
		return "", 0, fmt.Errorf("vector(%s) is not a valid prop; %s is not a valid type", value, v_type)
	}

	if len(parsed_val) < 2 {
		return v_type, 1, nil
	}

	v_level, err := strconv.ParseInt(strings.TrimSpace(parsed_val[1]), 10, 0)
	if err != nil {
		return "", 0, fmt.Errorf("vector(%s) is not a valid prop; %s", value, err.Error())
	} else if v_level < 1 {
		return "", 0, fmt.Errorf("vector(%s) is not a valid prop; level must be >= 1", value)
	}

	return v_type, int(v_level), nil
}
