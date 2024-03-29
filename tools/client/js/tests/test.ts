import test, { after, describe } from "node:test";
import assert from "node:assert";
import TobsDB from "../src/index";
import path from "path";
import { Schema } from "./schema";

const schemaPath = path.join(__dirname, "../schema.tdb");

let db = new TobsDB<Schema>(
  {
    host: "localhost",
    port: 7085,
    db: "test_nodejs_client",
    schemaPath,
    username: "user",
    password: "pass",
  },
  { log: true, debug: true },
);

describe("TEEEEEEEESSSTTTTTSSSSS", async () => {
  after(() => {
    if (db.__allDone()) {
      db.disconnect();
    }
  });

  await test("Connection", async () => {
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

  test("UPDATE", async (t) => {
    await t.test("Update a row", async () => {
      // create row
      const r_create = await db.create("nested_vec", {
        vec2: [[1], [2, 3]],
        vec3: [[["hello"]], [["world"]]],
      });

      assert.strictEqual(r_create.status, 201);

      const res = await db.updateUnique(
        "nested_vec",
        { id: r_create.data.id },
        { vec3: { push: [[["goodbye"]]] } },
      );

      assert.strictEqual(res.status, 200);
      assert.strictEqual(res.data.id, r_create.data.id);
      assert.strictEqual(res.data.vec3?.length, 3);
      assert.ok(res.__tdb_client_req_id__);
    });
  });
});
