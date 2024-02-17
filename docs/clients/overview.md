# Client Overview

TobsDB has official clients for the following languages:

- JavaScript/Typescript
- Rust
- Golang

## Making a TobsDB client.

A TDB client is a program that:

1. connects to a TobsDB server via a TCP socket.
2. sends requests to the TobsDB servers through the connected socket as json encoded data.
3. processes data sent from the server.

### Connecting:

After the initial TCP connection request, the **next** request sent to the server **must** be the TDB connect request.

- `tryConnect`: (bool) notify the server that it should use this request to authenticate the connection.
- `db`: (string?) the name of the database to use on the TobsDB server (the database will be created if it did not previously exist)
- `schema`: (string?) The content of the `schema.tdb` file.
- `username`: (string) The username to use when connecting to the server.
- `password`: (string) The password to use when connecting to the server.
- `checkOnly`: (bool?) Validate the schema and close connection.

### Actions:

The client interacts with the TDB server using actions.
An action is an instruction to the server to perform an operation.
You can read more about TobsDB actions [here](../actions.md)
