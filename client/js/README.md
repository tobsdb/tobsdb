# TobsDB NodeJS client

This is the official TobsDB NodeJS client.

## Usage

```js
import TobsDB from "tobsdb";

const db = await TobsDB.connect("ws://localhost:7085");

const res = db.create("example", {hello: "world"});
```
