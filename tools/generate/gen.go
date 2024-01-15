package generate

import (
	"fmt"

	"github.com/tobsdb/tobsdb/internal/builder"
)

// TODO: write some tests for supported langs
func SchemaToLang(schema *builder.Schema, lang string) ([]byte, error) {
	s := schemaDestructure(schema)
	switch lang {
	case "json":
		return SchemaToJson(s)
	case "typescript":
		fallthrough
	case "ts":
		return SchemaToTypescript(s), nil
	case "rust":
		fallthrough
	case "rs":
		return SchemaToRust(s), nil
	case "golang":
		fallthrough
	case "go":
		return SchemaToGo(s), nil
	default:
		return nil, fmt.Errorf("Unsupported Language: %s", lang)
	}
}
