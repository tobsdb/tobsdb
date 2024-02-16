# TobsDB NodeJS client

This is the official TobsDB NodeJS client.

## Usage

```js
import TobsDB from "tobsdb";

const db = new TobsDB(
{
  host: "localhost",
  port: 7085,
  db: "db_name",
  schema_path: "path/to/schema.tdb",
  username: "user",
  password: "password",
},
{ log: true, });
await db.connect();

const res = db.create("table_name", { hello: "world" });
```

## Typescript Support

This is an example TobsDB schema:

```
// schema.tdb

$TABLE table_name {
    id      Int     key(primary)
    hello   String  default("world")
}

$TABLE example {
    id      Int     key(primary)
    world   String  unique(true)
    nested  Vector  vector(String, 2) optional(true)
}
```

The Typescript schema declaration for the above can be written as:

```ts
// index.ts
import TobsDB, { PrimaryKey, Unique, Default } from "tobsdb";

// schema declaration that translates to schema passed to the server
type Schema = {
  table_name: {
    id: PrimaryKey<number>;
    hello: Default<string>;
  };

  example: {
    id: PrimaryKey<number>;
    world: Unique<string>;
    nested?: string[][];
  };
};

const db = new TobsDB<Schema>(
                   // ^ schema type argument gives strict type inference
{
  host: "localhost",
  port: 7085,
  db: "db_name",
  schema_path: "path/to/schema.tdb",
  username: "user", 
  password: "password",
},
{ log: true }
);
await db.connect();

db.create(
  "table_name",
  // ^? "table_name" | "example"
  { hello: 2 }, // (???) typescript: Type 'number' is not assignable to type 'string'. [2322]
  // ^? { hello?: string, id?: number }
); 
```

In the above, the `hello` field is optional in `create` because we tell typescript it has a fallback default value.
And the `id` is optional because TobsDB manages primary keys for you.

For more information see [TobsDB docs](https://tobsdb.github.io/tobsdb/clients/js).
