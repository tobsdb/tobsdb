#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

function getArch() {
  switch (process.arch) {
    case "ia32":
      return "i386";
    case "x64":
      return "x86_64";
    case "arm64":
      return process.arch;
    default:
      throw new Error("Unsupported architecture: " + process.arch);
  }
}

function getPlatform() {
  switch (process.platform) {
    case "darwin":
      return "Darwin";
    case "linux":
      return "Linux";
    case "win32":
      return "Windows";
    default:
      throw new Error("Unsupported platform: " + process.platform);
  }
}

function formatUrl(version, platform, arch) {
  let url =
    "https://github.com/tobsdb/tobsdb/releases/download/" +
    `v${version}/tdb-generate_${platform}_${arch}`;
  if (platform === "Windows") {
    url += ".exe";
  }
  return url;
}

async function fetchBuffer(url) {
  const res = await fetch(url);
  const blob = await res.blob();
  return Buffer.from(await blob.arrayBuffer());
}

function writeBuffer(buffer) {
  const location = path.join(process.env.INIT_CWD, "node_modules/.bin");
  if (!fs.existsSync(location)) {
    fs.mkdirSync(location);
  }
  fs.writeFileSync(path.join(location, "/tdb-generate"), buffer, {
    mode: 0o555,
  });
}

async function main() {
  const platform = getPlatform();
  const arch = getArch();

  const url = formatUrl("0.1.2-alpha", platform, arch);
  const buffer = await fetchBuffer(url);
  writeBuffer(buffer);
}

main();
