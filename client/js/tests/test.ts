import test, { after, describe } from "node:test";
import assert from "node:assert";
import TobsDB, { PrimaryKey, Default, Unique } from "../dist/index";
import path from "path";

type DB = {
  example: {
    id: PrimaryKey<number>;
    name: Default<string>;
    vector: number[];
    createdAt: Default<Date>;
  };

  first: {
    id: PrimaryKey<number>;
    createdAt: Default<Date>;
    updatedAt?: Date;
    user: number;
  };

  second: {
    id: PrimaryKey<number>;
    createdAt: Default<Date>;
    updatedAt?: Date;
    rel_str: string;
  };

  third: {
    id: PrimaryKey<number>;
    str: Unique<string>;
  };

  nested_vec: {
    id: PrimaryKey<number>;
    vec2: number[][];
    vec3?: string[][][];
  };
};

const tdb_url = "ws://localhost:7085";
const schema_path = path.join(__dirname, "../schema.tdb");

let db = new TobsDB<DB>(tdb_url, "test_nodejs_client", {
  schema_path: schema_path,
  username: "user",
  password: "pass",
  log: true,
  debug: true,
});

describe("TEEEEEEEESSSTTTTTSSSSS", async () => {
  after(() => {
    if (db.__allDone()) {
      db.disconnect();
    }
  });

  test("Schema Validation", async (t) => {
    await t.test("Valid schema", async () => {
      const valid = await TobsDB.validateSchema(tdb_url, schema_path);
      assert.ok(valid.toLowerCase(), "schema is valid");
    });
  });

  test("Connection", async () => {
    await db.connect();
  });

  test("NESTED vectors", async (t) => {
    await t.test("Nested vectors: Create a new table", async () => {
      const vec3 = [
        [
          ["hello", "world"],
          ["world", "hello"],
        ],
        [["hi there"], ["how are you?"]],
      ];

      const res = await db.create("nested_vec", {
        vec2: [[1], [2], [3]],
        vec3,
      });

      assert.strictEqual(res.status, 201);
      assert.ok(res.data.id);
      assert.deepStrictEqual(res.data.vec2, [[1], [2], [3]]);
      assert.deepStrictEqual(res.data.vec3, vec3);
    });

    await t.test("Nested vectors: Find tables with nested vector", async () => {
      const count = 20;
      const vec2 = [[101], [6969], [420]];
      const r_create = await db.createMany(
        "nested_vec",
        Array(count).fill({ vec2 }),
      );

      assert.strictEqual(r_create.status, 201);
      assert.strictEqual(r_create.data.length, count);

      const res = await db.findMany("nested_vec", { vec2 });

      assert.strictEqual(res.status, 200);
      assert.strictEqual(res.data.length % count, 0);
      assert.deepStrictEqual(res.data[0].vec2, vec2);
    });
  });

  test("FIND", async (t) => {
    await t.test("Find a table", async () => {
      // create row
      const r_create = await db.create("example", {
        name: "find example",
        vector: [1, 2, 3],
      });

      assert.strictEqual(r_create.status, 201);

      const res = await db.findUnique("example", { id: r_create.data.id });

      assert.strictEqual(res.status, 200);
      assert.strictEqual(res.data.id, r_create.data.id);
      assert.strictEqual(res.data.name, "find example");
      assert.ok(res.__tdb_client_req_id__);
    });
  });
});
