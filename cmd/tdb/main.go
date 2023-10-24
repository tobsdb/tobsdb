package main

import (
	"flag"
	"os"

	"github.com/tobshub/tobsdb/internals/builder"
)

func main() {
	cwd, _ := os.Getwd()

	db_write_path := flag.String("db", cwd+"/db.tdb", "path to save db data")
	in_mem := flag.Bool("m", false, "don't persist db")
	port := flag.Int("port", 7085, "listening port")
	should_log := flag.Bool("log", false, "print error logs")
	show_debug_logs := flag.Bool("dbg", false, "show extra logs")
	username := flag.String("u", os.Getenv("TDB_USER"), "username")
	password := flag.String("p", os.Getenv("TDB_PASS"), "password")
	idle_interval := flag.Int("w", 1000, "time to wait before writing data when idle")

	flag.Parse()

	os.Setenv("TDB_USER", *username)
	os.Setenv("TDB_PASS", *password)

	log_options := builder.LogOptions{
		Should_log:      *should_log,
		Show_debug_logs: *show_debug_logs,
	}
	write_settings := builder.NewWriteSettings(*db_write_path, *in_mem, *idle_interval)

	db := builder.NewTobsDB(write_settings, log_options)
	db.Listen(*port)
}
