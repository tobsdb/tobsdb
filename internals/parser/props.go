package parser

import (
	"strconv"
	"strings"

	TDBTypes "github.com/tobshub/tobsdb/internals/types"
)

func ParseRelationProp(relation string) (string, string) {
	parsed_rel := strings.Split(relation, ".")
	return parsed_rel[0], parsed_rel[1]
}

