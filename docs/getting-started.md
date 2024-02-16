# Getting Started

## Installation

There's a few ways to install TobsDB.

### 1. go install

```bash
$ go install github.com/tobsdb/tobsdb/cmd/tdb@latest
```

### 2. Github Releases (Recommended)

Navigate to the [releases page](https://github.com/tobsdb/tobsdb/releases) and download the latest release.

### 3. Docker

```bash
$ docker pull tobani/tobsdb
```


## Configuration

### Command line flags

- `-v`: print the version and exit.
- `-db=<path>`: the path to the db.tdb file (if the file does not exits the first write will create it).
If the value is relative then it is resolved from the current working directory.
- `-m`: when used or set to true, the database data remains in memory and is not written a file when the program exits. Defaults to false.
- `-port=<port>`: the listening port. Defaults to 7085 (tobs in leet-speak :D)
- `-log`: optionally print logs. Defaults to false
- `-dbg`: optionally print extra logs. Defaults to false
- `-u`: set the root username. Defaults to ENV.TDB_USER
- `-p`: set the root password. Defaults to ENV.TDB_PASS
- `-w`: set the time to wait(in ms) before writing db data to file. Defaults to 1000ms

### Environment variables

- `TDB_USER`: set the root username.
- `TDB_PASS`: set the root password.
