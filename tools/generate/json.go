package generate

import "encoding/json"

func SchemaToJson(s []ParsedTable) ([]byte, error) {
	return json.Marshal(s)
}
