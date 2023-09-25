package main

import (
	"flag"
	"log"
	"os"

	TDBBuilder "github.com/tobshub/tobsdb/internals/builder"
	TDBParser "github.com/tobshub/tobsdb/internals/parser"
)

func main() {
	cwd, _ := os.Getwd()

	schema_path := flag.String("schema", cwd+"/schema.tdb", "path to schema file")
	db_write_path := flag.String("db", cwd+"/db.tdb", "path to save db data")
	in_mem := flag.Bool("m", false, "don't persist db")
	port := flag.Int("port", 7085, "listening port")
	parse_schema_only := flag.Bool("check", false, "check the schema file for errors and exit")

	flag.Parse()

	schema := TDBParser.NewSchema(*schema_path)
	if *parse_schema_only {
		log.Println("Schema checks completed")
	} else {
		db := TDBBuilder.NewTobsDB(&schema, *db_write_path, *in_mem)

		db.Listen(*port)
	}
}
