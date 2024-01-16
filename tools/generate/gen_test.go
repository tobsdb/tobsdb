package generate_test

import (
	"fmt"
	"testing"

	"github.com/tobsdb/tobsdb/internal/builder"
	gen "github.com/tobsdb/tobsdb/tools/generate"
	"gotest.tools/assert"
)

func createSimpleSchema() *builder.Schema {
	schema, err := builder.ParseSchema(`
$TABLE a {
    id Int key(primary)
    b  String default("hello")
    c  Bytes optional(true)
    d  String unique(true)
    e  Vector vector(Int, 2)
}`)
	if err != nil {
		panic(err)
	}
	return schema
}

func TestSimpleSchemaToTypescript(t *testing.T) {
	schema := createSimpleSchema()
	res, err := gen.SchemaToLang(schema, "ts")
	assert.NilError(t, err)

	assert.Equal(t, string(res), fmt.Sprint(`import { PrimaryKey, Unique, Default } from "tobsdb";

export type Schema = {`,
		"\n\ta: {\n",
		"\t\tid: PrimaryKey<number>;\n",
		"\t\tb: Default<string>;\n",
		"\t\tc?: Buffer;\n",
		"\t\td: Unique<string>;\n",
		"\t\te: number[][];\n",
		"\t};\n",
		"}"))
}

func TestSimpleSchemaToRust(t *testing.T) {
	schema := createSimpleSchema()
	res, err := gen.SchemaToLang(schema, "rs")
	assert.NilError(t, err)

	assert.Equal(t, string(res), fmt.Sprint(`use tobsdb::types::*;
use serde::{Deserialize, Serialize};
`, "\n#[derive(Serialize, Deserialize)]\npub struct A {\n",
		"\tpub id: TdbInt;\n",
		"\tpub b: Option<TdbString>;\n",
		"\tpub c: Option<TdbBytes>;\n",
		"\tpub d: TdbString;\n",
		"\tpub e: TdbVector<TdbVector<TdbInt>>;\n",
		"}\n"))
}

func TestSimpleSchemaToGo(t *testing.T) {
	schema := createSimpleSchema()
	res, err := gen.SchemaToLang(schema, "go")
	assert.NilError(t, err)

	assert.Equal(t, string(res), fmt.Sprint(`package schema

import . "github.com/tobsdb/tobsdb/tools/client/go"
`, "\ntype A struct {\n",
		"\tId TdbInt\n",
		"\tB TdbString\n",
		"\tC TdbBytes\n",
		"\tD TdbString\n",
		"\tE TdbVector[TdbVector[TdbInt]]\n",
		"}\n"))
}
