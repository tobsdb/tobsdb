import test, { describe, it, before } from "node:test";
import fs from "fs/promises";
import assert from "assert";

const API = async (path, body) => {
  return await fetch(`http://localhost:7085/${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  }).then((res) => res.json());
};

test("CREATE", async (t) => {
  // remove saved db data before starting test
  await fs.rm(process.cwd() + "/db.tdb").catch(() => null);

  await t.test("Create a new table", async () => {
    const res = await API("createUnique", {
      table: "example",
      data: { name: "first example" },
    });

    assert.equal(res.data.name, "first example");
    assert.ok(res.data.id, "Returned row should have an id");
    assert.ok(res.data.createdAt, "Returned row should have a createdAt");
  });

  await t.test("CreateUnique: 1_000 new tables", async (t) => {
    const data = Array(1_000).fill({ name: "1 of 1_000" });

    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      const res = await API("createUnique", {
        table: "example",
        data: row,
      });
      assert.equal(res.status, 201, "Status code should be 201");
    }
  });

  await t.test("CreateMany: 10_000 new tables", async () => {
    const res = await API("createMany", {
      table: "example",
      data: Array(10_000).fill({
        name: "group of 10_000",
        createdAt: Date.now(),
      }),
    });

    assert.equal(res.data.length, 10_000);
    assert.equal(res.message, "Created 10000 new rows in table example");
  });

  await t.test("Error because of missing required field", async () => {
    const res = await API("createUnique", {
      table: "example",
      data: {},
    });

    assert.equal(res.status, 400);
  });

  await t.test("Error because of passing unknown table", async () => {
    const res = await API("createUnique", {
      table: "bad_example",
      data: { deez: "nuts" },
    });

    assert.equal(res.status, 404);
  });
});

test("FIND", async (t) => {
  await t.test("Find a table", async () => {});
  await t.test("Find 3 tables", async () => {});

  await t.test(
    "Error because of passing empty where statement to findUnique",
    async () => {}
  );
  await t.test("Error because of passing unknown table", async () => {});
});

test("UPDATE", async (t) => {
  await t.test("Update a table", async () => {});
  await t.test("Update 3 tables", async () => {});

  await t.test(
    "Error because of passing empty where statement to updateUnique",
    async () => {}
  );
  await t.test("Error because of passing unknown table", async () => {});
});

test("DELETE", async (t) => {
  await t.test("Delete a table", async () => {});
  await t.test("Delete 3 tables", async () => {});

  await t.test(
    "Error because of passing empty where statement to deleteUnique",
    async () => {}
  );
  await t.test("Error because of passing unknown table", async () => {});
});
