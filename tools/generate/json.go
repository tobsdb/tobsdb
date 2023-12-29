package generate

import "encoding/json"

func SchemaToJson(s []JsonTable) ([]byte, error) {
	return json.Marshal(s)
}
