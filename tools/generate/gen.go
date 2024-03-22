package generate

import (
	"fmt"

	"github.com/tobsdb/tobsdb/internal/builder"
)

func SchemaToLang(schema *builder.Schema, lang string) ([]byte, error) {
	s := schemaDestructure(schema)
	switch lang {
	case "json":
		return SchemaToJson(s)
	case "typescript", "ts":
		return SchemaToTypescript(s), nil
	case "rust", "rs":
		return SchemaToRust(s), nil
	case "golang", "go":
		return SchemaToGo(s), nil
	default:
		return nil, fmt.Errorf("Unsupported Language: %s", lang)
	}
}
