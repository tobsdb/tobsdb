package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/tools/generate"
)

func main() {
	var path, schema, out, lang string

	flag.StringVar(&path, "path", "", "Path to schema file")
	flag.StringVar(&schema, "schema", "", "Schema string. Preferred over -path")
	flag.StringVar(&out, "out", "", "Output file")
	flag.StringVar(&lang, "lang", "json", "Output language. Options: json, typescript, rust, golang")

	flag.Parse()

	if path == "" && schema == "" {
		fmt.Println("Must specify either -path or -schema")
		os.Exit(1)
	}

	if schema == "" {
		data, err := os.ReadFile(path)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		schema = string(data)
	}

	s, err := builder.ParseSchema(schema)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	data, err := generate.SchemaToLang(s, lang)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if out == "" {
		fmt.Println(string(data))
		return
	}

	err = os.WriteFile(out, data, 0644)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
