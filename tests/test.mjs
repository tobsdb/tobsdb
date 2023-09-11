import test from "node:test";
import assert from "assert";
// import { spawn } from "child_process";
// import path from "path";
// import { fileURLToPath } from "url";

// await new Promise((res, rej) => {
//   const cwd = path.join(fileURLToPath(new URL(".", import.meta.url)), "..");
//   const proc = spawn("make run", { cwd });
//   proc.on("error", (err) => {
//     console.log(err);
//     rej(err);
//   });

//   proc.stdin.on("", (data) => {
//     console.log(data);
//     res();
//   });
//   proc.stderr.on("data", (data) => {
//     console.error(data);
//   });
// });

const API = async (path, body) => {
  return await fetch(`http://localhost:7085/${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  }).then(async (res) => {
    return res.json();
  });
};

test("CREATE", async (t) => {
  await t.test("Create a new table", async () => {
    const res = await API("create", {
      table: "example",
      data: { name: "first example", vector: [1, 2, 3] },
    });

    assert.equal(res.data.name, "first example");
    assert.ok(res.data.id, "Returned row should have an id");
    assert.ok(res.data.createdAt, "Returned row should have a createdAt");
  });

  await t.test("CreateUnique: 500 new tables", async (t) => {
    const count = 500;
    const data = Array(count).fill({ name: `1 of ${count}`, vector: [count] });

    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      const res = await API("create", {
        table: "example",
        data: row,
      });
      assert.equal(res.status, 201, "Status code should be 201");
    }
  });

  await t.test("CreateMany: 10_000 new tables", async () => {
    const table = "example";
    const count = 10000;
    const res = await API("createMany", {
      table: table,
      data: Array(count).fill({
        name: `group of ${count}`,
        createdAt: Date.now(),
        vector: [count],
      }),
    });

    assert.equal(res.data.length, count);
    assert.equal(res.message, `Created ${count} new rows in table ${table}`);
  });

  await t.test("Error because of missing required field", async () => {
    const res = await API("create", {
      table: "example",
      data: {},
    });

    assert.equal(res.status, 400);
  });

  await t.test("Error because of passing unknown table", async () => {
    const res = await API("create", {
      table: "bad_example",
      data: { deez: "nuts" },
    });

    assert.equal(res.status, 404);
  });
});

test("FIND", async (t) => {
  await t.test("Find a table", async () => {
    // create row
    const r = await API("create", {
      table: "example",
      data: { name: "find example", vector: [1, 2, 3] },
    });

    assert.equal(r.status, 201);

    const res = await API("findUnique", {
      table: "example",
      where: { id: r.data.id },
    });

    assert.equal(res.status, 200);
    assert.equal(res.data.id, r.data.id);
    assert.equal(res.data.name, "find example");
  });

  await t.test(
    "Error because of passing empty where statement to findUnique",
    async () => {}
  );
  await t.test("Error because of passing unknown table", async () => {});
});

test("UPDATE", async (t) => {
  await t.test("Update a table", async () => {});
  await t.test("Update 1_000 tables", async () => {});

  await t.test(
    "Error because of passing empty where statement to updateUnique",
    async () => {}
  );
  await t.test("Error because of passing unknown table", async () => {});
});

test("DELETE", async (t) => {
  await t.test("Delete a table", async () => {});
  await t.test("Delete 1_000 tables", async () => {});

  await t.test(
    "Error because of passing empty where statement to deleteUnique",
    async () => {}
  );
  await t.test("Error because of passing unknown table", async () => {});
});
