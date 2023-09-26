package main

import (
	"flag"
	"os"

	TDBBuilder "github.com/tobshub/tobsdb/internals/builder"
)

func main() {
	cwd, _ := os.Getwd()

	db_write_path := flag.String("db", cwd+"/db.tdb", "path to save db data")
	in_mem := flag.Bool("m", false, "don't persist db")
	port := flag.Int("port", 7085, "listening port")

	flag.Parse()

	db := TDBBuilder.NewTobsDB(*db_write_path, *in_mem)
	db.Listen(*port)
}
