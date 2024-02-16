
The schema is the backbone of TobsDB. It defines the structure of a database and is required when creating a new database.

# Schema.tdb

`schema.tdb` (the schema file) is where the type declarations for tables and fields in a TobsDB database go.
The schema.tdb file lives client side and is sent to the TobsDB server on the initial database creation request.

## Types

TobsDB has a few built-in types which are accepted in the schema.tdb file:

<!-- Write detailed information about these types -->
- `Int`
- `String`
- `Vector`
- `Float`
- `Date`
- `Bool`
- `Bytes`

## Declaration Syntax

### Tables

In the schema.tdb file, only one type of top level declaration can be found. Which is the `$TABLE` declaration.

`$TABLE` is used to start the declaration for a new table, and is used in the following way:

```
$TABLE <table_name> {
    ...
}
```

where `<table_name>` is the name you want to give to the table being declared.

There's a few rules for declaring a new table aside from starting with `$TABLE`:

- the opening brace, `{`, must always be on the same line as the `$TABLE <table_name>` declaration.
- the closing braces, `}`, must always be on a line after the `$TABLE <table_name>` declaration.
- all fields belonging to a table must be declared between the opening and closing braces - on a line of their own.

### Fields

Fields are properties that exist on a `$TABLE`.

The syntax for declaring a field is simple: `<field_name> <data_type> <...properties?>`

That is, start with the field's name, then its type (which must be a [TobsDB type](#types) ), then lastly and optionally any extra field properties.

<!-- TODO: document field props -->

It is important to exhaustively declare all fields on a table because fields not declared will **never** be used, even if they are sent in a query.

### Comments

Comments are allowed in the schema.tdb file but must always be on a line of their own and start with double forward slash (`//`).


### Example

```
$TABLE user {
    id              Int     key(primary)
    name            String  unique(true)
    DOB             Date    optional(true)
    best_friend     Int     relation(user.id)
    // store the user's favorite games
    favorite_games  Vector  vector(String)
}
```
