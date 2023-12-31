# Client Overview

TobsDB has official clients for the following languages:

- JavaScript/Typescript

Support for other languages will be added.

## Making a TobsDB client.

Anything can be a TobsDB client if it:

1. Connects to a TobsDB server via a websocket connect request.
2. Sends requests to the TobsDB servers through the connected websocket.

That's it.

Well there's a little bit more to it:

### Connecting:

When connecting, the client needs to send some data to the server, all of which can be included in the url as query params.

- `db`: the name of the database to use on the TobsDB server (the database will be created if it did not previously exist) 
- `schema`: The content of the `schema.tdb` file.
- `username`: The username to use when connecting to the server.
- `password`: The password to use when connecting to the server.
- `auth`: The username and password, in the format `<username>:<password>`. Takes precedence over `username` and `password`.
- `check_schema`: Validate the schema and close connection. Optional.
- `migration`: Prefer the client schema to the server schema if any. Optional.

For clients that support it, the `username` and `password` can be sent in the `Authorization` header, in the same format as the `auth` url param.

### Queries:

All queries require these two arguments:

- `action`: The query action to execute. 
One of: `create`, `createMany`, `findUnique`, `findMany`, `updateUnique`, 
`updateMany`, `deleteUnique`, `deleteMany`
- `table`: The name of the table in the TobsDB server to run the query on.

For the purpose of this explanation let's divide all the possible queries in 3 groups:

- Change-only queries: create, createMany
- Search-only queries: findUnique, findMany, deleteUnique, deleteMany
- and Search-Change queries: updateUnique, updateMany

The reason this grouping was made because of the arguments they take.

Change-only queries require the following additional argument:

- `data`: The data to use in the query. 
In the case of `createMany` queries, this is expected to be an array.

Search-only queries require the following additional argument:

- `where`: The conditions to check against in the query.

Search-Change queries require the following additional arguments:

- `where`: The conditions to check against in the query.
- `data`: The data to use in the query. 

In the `where` argument for Search-only and Search-Change `*Unique` queries, 
only unique or primary-key fields can used in the operation. Other fields are ignored, even if provided.

In the `where` argument for Search-only and Search-Change `*Many` queries,
there are dynamic search options available for `Int` and `String` fields:

- `Int`: eq, ne, gt, gte, lt, lte
- `String`: contains, startsWith, endsWith

In the `data` argument for Search-Change queries,
there are dynamic change options available for `Int` and `Vector` fields:

- `Int`: increment, decrement
- `Vector`: push

*In all queries, only fields that are defined in the schema are used,
if others are provided they will be ignored.

## Example Request 

With of a schema.tdb:

```sql
$TABLE user {
    id      Int key(primary)
    name    String
    age     Int
    height  String default("short")
}
```

### create

```json
{
    "action": "create",
    "table": "user",
    "data": { "name": "Maya", "age": 28 }, 
}
```

### createMany

```json
{
    "action": "createMany",
    "table": "user",
    "data": [
        { "name": "Karolis", "age": 21, "height": "tall" }
        { "name": "Wolfred", "age": 24 }, 
    ]
}
```

### findUnique

```json
{
    "action": "findUnique",
    "table": "user",
    "where": { "id": 1 }
}
```

### findMany

```json
{
    "action": "findMany",
    "table": "user",
    "where": { "age": { "gte": 18 }}
}
```

### updateUnique

```json
{
    "action": "updateUnique",
    "table": "user",
    "where": { "id": 1 },
    "data": { "age": { "increment": 1 }, "height": "tall" } 
}
```

### updateMany

```json
{
    "action": "updateMany",
    "table": "user",
    "where": { "age": { "gte": 69 }},
    "data": { "age": { "decrement": 1 }} 
}
```

### deleteUnique

```json
{
    "action": "deleteUnique",
    "table": "user",
    "where": { "id": 1 },
}
```

### deleteMany

```json
{
    "action": "deleteMany",
    "table": "user",
    "where": { "name": { "contains": "o" }},
}
```
