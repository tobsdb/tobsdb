# NodeJS Client

The official tobsdb nodejs client is available [here](https://npmjs.com/package/tobsdb).


## Installation

```bash
pnpm add tobsdb
```

Or if you're feeling funky and using npm or yarn

```bash
npm|yarn install tobsdb
```

## API documentation

### class TobsDB

#### `static async connect<Schema>(url, db_name, conn_options, tdb_options): Promise<TobsDB<Schema>>` 

Connects to a tobsdb server.

##### Parameters:

- `url`: the url of the tobsdb server
- `db_name`: the name of the database to use in the tobsdb server(databse will
be created if it did not previously exist)
- `conn_options`: connection options
    - `schema_path`: the to the schema.tdb file. Defaults to `$(cwd)/schema.tdb`
    - `auth`: authentication credentials
        - `username`: username corresponding to the user of the tobsdb server
        - `password`: password corresponding to the password of the tobsdb server
- `tdb_options`: change default client behaviour

##### Type Parameters:

- `Schema`: gives type inference for all database query function parameters and return types.
The type should correspond to the types in your schema.tdb file.

##### Return: 

Returns a new instance of `class TobsDB`.

#### `static async validateSchema(url, schema_path): Promise<TDBSchemaValidationResponse>`

Run validation checks on a schema.tdb file

##### Parameters:

- `url`: the url of the tobsdb server
- `schema_path`: the path to the schema.tdb file. Defaults to `$(cwd)/schema.tdb`

#### `async disconnect()`

Gracefully disconnect from tobsdb server.

#### `async create(table, data)`

Send a create request to the tobsdb server.

##### Parameters:

- `table`: the name of the table to create a row in.
Must correspond to the name of a table in the schema.tdb file.
- `data`: data to use in the create request.

#### `async createMany(table, data)`

Send a create-many request to the tobsdb server.

##### Parameters:

- `table`: the name of the table to create rows in.
Must correspond to the name of a table in the schema.tdb file.
- `data`: an array data to use in the create-many request.
