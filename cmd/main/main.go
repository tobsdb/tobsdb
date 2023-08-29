package main

import (
	"fmt"
	"os"

	TobsdbParser "github.com/tobshub/tobsdb/cmd/parser"
)

func main() {
	cwd, _ := os.Getwd()
	schema_path := cwd + "/schema.tobs"

	schema, _ := TobsdbParser.SchemaParser(schema_path)
	fmt.Println(schema)
}
