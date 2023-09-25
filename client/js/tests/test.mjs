import test from "node:test";
import TobsDB from "../dist/index.mjs";

const db = await TobsDB.connect("ws://localhost:7085");
await db.create("warm-up", {});

// TODO: add tests
test("Connection", async (t) => {
  await t.test("Create one table", async () => {
    const res = await db.create("example", { vector: [] });
  });

  t.after(() => {
    db.disconnect();
  });
});
