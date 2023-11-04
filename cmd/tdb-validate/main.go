package main

import (
	"fmt"
	"os"
	"path"

	"github.com/tobsdb/tobsdb/internal/builder"
)

func main() {
	args := os.Args
	var schema_path string

	if len(args) > 1 {
		schema_path = args[1]
	} else {
		schema_path = "./schema.tdb"
	}

	if !path.IsAbs(schema_path) {
		cwd, _ := os.Getwd()
		schema_path = path.Join(cwd, schema_path)
	}

	fmt.Printf("Checking %s for errors\n", schema_path)

	schema_data, err := os.ReadFile(schema_path)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		return
	}

	_, err = builder.ParseSchema(string(schema_data))
	if err != nil {
		fmt.Printf("Invalid schema; %s\n", err.Error())
		return
	}

	fmt.Println("Schema checks successful: Schema is valid")
}
