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
	port := flag.Int("port", 7085, "listening port")
	parse_schema_only := flag.Bool("check", false, "check the schema file for errors and exit")

	flag.Parse()

	schema := TobsdbParser.SchemaParser(*schema_path)
	if !(*parse_schema_only) {
		db := TobsdbBuilder.NewTobsDB(&schema, *db_write_path, *in_mem)

		log.Println("TobsDB listening on port", *port)
		db.Listen(*port)
	} else {
		log.Println("Schema checks completed")
	}
}
