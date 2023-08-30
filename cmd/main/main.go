package main

import (
	"os"

	TobsdbBuilder "github.com/tobshub/tobsdb/cmd/builder"
	TobsdbParser "github.com/tobshub/tobsdb/cmd/parser"
)

func main() {
	cwd, _ := os.Getwd()
	schema_path := cwd + "/schema.tobs"
	db_write_path := cwd + "/db.tobs"

	schema, _ := TobsdbParser.SchemaParser(schema_path)
	db := TobsdbBuilder.NewTobsDB(&schema, db_write_path)

	// default port 7085
	db.Listen(7085)
}
