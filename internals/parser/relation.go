package parser

import "strings"

func ParseRelation(relation string) (string, string) {
	parsed_rel := strings.Split(relation, ".")
	return parsed_rel[0], parsed_rel[1]
}
