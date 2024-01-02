#!/usr/bin/env node

const path = require("path");
const fs = require("fs");
const cp = require("child_process");

function main() {
  const args = process.argv.slice(2);
  let schemaPath;
  let outPath;

  for (let i = 0; i < args.length; i++) {
    const flag = args[i];
    switch (flag) {
      case "--path":
      case "-p":
        schemaPath = args[i + 1];
        i++;
        break;
      case "--out":
      case "-o":
        outPath = args[i + 1];
        i++;
        break;
      case "--help":
      case "-h":
        helpMessage();
        return;
    }
  }

  const tdbGeneratePath = path.join(
    process.cwd(),
    "node_modules/.bin/tdb-generate",
  );

  if (process.platform === "win32") {
    tdbGeneratePath += ".exe";
  }

  if (!fs.existsSync(tdbGeneratePath)) {
    console.error(`tdb-generate not found: ${tdbGeneratePath}`);
    process.exit(1);
  }

  if (!schemaPath) {
    schemaPath = path.join(process.cwd(), "schema.tdb");
  }

  if (!fs.existsSync(schemaPath)) {
    console.error(`Schema file not found: ${schemaPath}`);
    process.exit(1);
  }

  if (!outPath) {
    outPath = path.join(process.cwd(), "schema.ts");
  }

  if (fs.existsSync(outPath) && fs.statSync(outPath).isDirectory()) {
    outPath = path.join(outPath, "schema.ts");
  }

  cp.execSync(
    `${tdbGeneratePath} -path ${schemaPath} -out ${outPath} -lang typescript`,
  );
}

main();

function helpMessage() {
  console.log(`\
Usage: tobsdb -p <schema.tdb> -o <outfile>

\t-p, --path <schema.tdb>   Path to schema.tdb file
\t-o, --out <outfile>       Path to output file
\t-h, --help                Show this help message
`);
}
