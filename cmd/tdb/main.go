package main

import (
	"flag"
	"log"
	"os"

	TobsdbBuilder "github.com/tobshub/tobsdb/internals/builder"
	TobsdbParser "github.com/tobshub/tobsdb/internals/parser"
)

func main() {
	cwd, _ := os.Getwd()

	schema_path := flag.String("schema", cwd+"/schema.tdb", "path to schema file")
	db_write_path := flag.String("db", cwd+"/db.tdb", "path to save db data")
	in_mem := flag.Bool("m", false, "don't persist db")

	flag.Parse()

	schema, _ := TobsdbParser.SchemaParser(*schema_path)
	db := TobsdbBuilder.NewTobsDB(&schema, *db_write_path, *in_mem)

	// default port 7085
	log.Println("TobsDB listening on port 7085")
	db.Listen(7085)
}
