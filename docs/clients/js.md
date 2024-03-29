# NodeJS Client

The official TobsDB nodejs client is available [here](https://npmjs.com/package/tobsdb).


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

#### `constructor<Schema>(connectionInfo: TobsDBConnectionInfo, options: Partial<TobsDBOptions>): TobsDB<Schema>` 

Create a new TobsDB client instance.

##### Parameters:

- `connectionInfo`: connection info
    - `host`: host of the TDB server
    - `port`: port of the TDB server
    - `db`: name of the database to connect to
    - `schema_path`: path to the schema.tdb file
    - `username`: username to use in connection
    - `password`: password to use in connection
- `options`: client options
    - `log`: enable logging. Defaults to `false`
    - `debug`: enable debug-logging. Defaults to `false`

##### Type Parameters:

- `Schema`: gives type inference for all database query function parameters and return types.
The type should correspond to the types in your schema.tdb file. See [tdb-generate](../tdb-generate.md)


#### `async connect(): Promise<void>`

Connect to the TobsDB server at the specified host and port.

#### `async disconnect(): Promise<void>`

Gracefully disconnect from TobsDB server.

#### `async create(table: string, data: object): TDBResponse`

Send a create request to the TobsDB server.

##### Parameters:

- `table`: the name of the table to create a row in.
Must correspond to the name of a table in the schema.tdb file.
- `data`: data to use in the create request.

#### `async createMany(table: string, data: object): TDBResponse`

Send a create-many request to the TobsDB server.

##### Parameters:

- `table`: the name of the table to create rows in.
Must correspond to the name of a table in the schema.tdb file.
- `data`: an array data to use in the create-many request.

#### `async findUnique(table: string, where: object): TDBResponse`

Send a findUnique request to the TobsDB server.

##### Parameters:

- `table`: the name of the table to search in.
Must correspond to the name of a table in the schema.tdb file.
- `where`: an object containing key-value pairs to look for. 
In `findUnique` requests, the only used keys are keys that correspond to unique or primary-key fields in the schema.

#### `async findMany(table: string, where: object): TDBResponse`

Send a findMany request to the TobsDB server.

##### Parameters:

- `table`: the name of the table to search in.
Must correspond to the name of a table in the schema.tdb file.
- `where`: an object containing key-value pairs to look for.

#### `async updateUnique(table: string, where: object, data: object): TDBResponse`

Send an updateUnique request to the TobsDB server.

##### Parameters:

- `table`: the name of the table to search in.
Must correspond to the name of a table in the schema.tdb file.
- `where`: an object containing key-value pairs to look for.
In `updateUnique` requests, the only used keys are keys that correspond to unique or primary-key fields in the schema.
- `data`: data to use in the update request.

#### `async updateMany(table: string, where: object, data: object): TDBResponse`

Send an updateMany request to the TobsDB server.

##### Parameters:

- `table`: the name of the table to search in.
Must correspond to the name of a table in the schema.tdb file.
- `where`: an object containing key-value pairs to look for.
- `data`: data to use in the update request.

#### `async deleteUnique(table: string, where: object): TDBResponse`

Send a deleteUnique request to the TobsDB server.

##### Parameters:

- `table`: the name of the table to search in.
Must correspond to the name of a table in the schema.tdb file.
- `where`: an object containing key-value pairs to look for.
In `deleteUnique` requests, the only used keys are keys that correspond to unique or primary-key fields in the schema.

#### `async deleteMany(table: string, where: object): TDBResponse`

Send a deleteMany request to the TobsDB server.

##### Parameters:

- `table`: the name of the table to search in.
Must correspond to the name of a table in the schema.tdb file.
- `where`: an object containing key-value pairs to look for.
