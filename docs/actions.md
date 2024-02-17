# Actions

Actions are instructions a client can send to a server to perform specific operations or get specific data.

All TDB actions are written in camelCase.

## Sending Requests

An action request is sent with the `action` field set to the desired action name:

```json
{
    "action": "action_name"
    ...
}
```

Each action request has a set of fields that are specific to the action.

## Responses

All action responses have the following fields:

- `status`: (int) the status code of the response.
- `message`: (string) a description of the response. This will contain the error message if the request failed.
- `data`: (any) the data returned by the action. Is `null` in some cases. (e.g. errors, data-less actions etc)
- `__tdb_client_req_id__`: (int) the id of the client request.

## Row Actions

### create

Make a new row in a table.

Required fields:

- `table`: the name of the table in the db.
- `data`: the data to insert.

The `data` field cannot be contain the primary key field, and must contain all non-optional fields.

Example Request:
```json
{
    "action": "create",
    "table": "table_name",
    "data": {...}
}
```
Example Response:
```json
{
    "status": 201,
    "message": "Created new row in table table_name",
    "data": {...}
}
```

### createMany

Make new rows in a table.

Required fields:

- `table`: the name of the table in the db.
- `data`: an array of data to insert.

The individual objects in the `data` field must follow the same rules as in the [`create`](#create) action.

Example Request:
```json
{
    "action": "create",
    "table": "table_name",
    "data": [{...}, {...}, ...]
}
```
Example Response:
```json
{
    "status": 201,
    "message": "Created 10 new rows in table table_name",
    "data": [{...}, {...}, ...]
}
```

### findUnique

Find a row in a table.

Required fields:

- `table`: the name of the table in the db.
- `where`: the where clause for the query.

The `where` field in a `findUnique` request must contain at least one unique field. If no unique fields are found (or the table doesn't have any unique fields), an error will be returned.

Example Request:
```json
{
    "action": "findUnique",
    "table": "table_name",
    "where": {...}
}
```
Example Response:
```json
{
    "status": 200,
    "message": "Found row in table table_name",
    "data": {...}
}
```

### findMany

Find rows in a table.

Required fields:

- `table`: the name of the table in the db.
- `where`: the where clause for the query.
- `orderBy`: ("asc" or "desc") manipulate the order of the results.
- `take`: (int) the maximum number of rows to return.
- `skip`: (int) the number of rows to skip from the results.
- `cursor`: a cursor to use for pagination. Has a similar shape to the `where` field.


The `where` field in a `findMany` request can contain any, all, or none of the fields in the table.
In the case where no fields are used in the `where` clause, all rows in the table will be returned.

The `where` and `cursor` field in a `findMany` request also support [dynamic queries](dynamic-queries.md).

Example Request:
```json
{
    "action": "findMany",
    "table": "table_name",
    "where": {...}
}
```
Example Response:
```json
{
    "status": 200,
    "message": "Found 10 rows in table table_name",
    "data": [{...}, {...}, ...]
}
```

### deleteUnique

Delete a row in a table.

Required fields:

- `table`: the name of the table in the db.
- `where`: the where clause for the query.

The `where` field in a `deleteUnique` request must contain at least one unique field.

Example Request:
```json
{
    "action": "deleteUnique",
    "table": "table_name",
    "where": {...}
}
```
Example Response:
```json
{
    "status": 200,
    "message": "Deleted row in table table_name",
    "data": {...}
}
```

### deleteMany

Delete rows in a table.

Required fields:

- `table`: the name of the table in the db.
- `where`: the where clause for the query.

The `where` field in a `deleteMany` request can contain any, all, or none of the fields in the table.
In the case where no fields are used in the `where` clause, all rows in the table are deleted.
It also supports [dynamic queries](dynamic-queries.md#where).

Example Request:
```json
{
    "action": "deleteMany",
    "table": "table_name",
    "where": {...}
}
```
Example Response:
```json
{
    "status": 200,
    "message": "Deleted 10 rows in table table_name",
    "data": [{...}, {...}, ...]
}
```

### updateUnique

Update a row in a table.

Required fields:

- `table`: the name of the table in the db.
- `where`: the where clause for the query.
- `data`: the data to use to update the row.

The `data` field in `updateUnique` requests supports [dynamic queries](dynamic-queries.md#data)

Example Request:
```json
{
    "action": "updateUnique",
    "table": "table_name",
    "where": {...},
    "data": {...},
}
```
Example Response:
```json
{
    "status": 200,
    "message": "Updated row in table table_name",
    "data": {...}
}
```

### updateMany

Update rows in a table.

Required fields:

- `table`: the name of the table in the db.
- `where`: the where clause for the query.
- `data`: the data to use to update the row.

The `where` and `data` fields in `updateMany` requests support [dynamic queries](dynamic-queries.md)

Example Request:
```json
{
    "action": "updateMany",
    "table": "table_name",
    "where": {...},
    "data": {...},
}
```
Example Response:
```json
{
    "status": 200,
    "message": "Updated row in table table_name",
    "data": [{...}, {...}, ...]
}
```

<!--
// database actions
RequestActionCreateDB RequestAction = "createDatabase"
RequestActionUseDB    RequestAction = "useDatabase"
RequestActionDropDB   RequestAction = "dropDatabase"
RequestActionListDB   RequestAction = "listDatabases"
RequestActionDBStat   RequestAction = "databaseStats"

// table actions
RequestActionDropTable RequestAction = "dropTable"
ReuqestActionMigration RequestAction = "migration"

// user actions
RequestActionCreateUser RequestAction = "createUser"
RequestActionDeleteUser RequestAction = "deleteUser"

// TODO: transaction actions
ReuqestActionTransaction RequestAction = "transaction"
ReuqestActionCommit      RequestAction = "commit"
ReuqestActionRollback    RequestAction = "rollback"
-->
