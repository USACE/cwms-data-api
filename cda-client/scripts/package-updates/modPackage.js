const fs = require("node:fs");
const path = require("node:path");

const rootDir = path.resolve(__dirname, "..", "..");

function readJson(relativePath) {
  return JSON.parse(fs.readFileSync(path.join(rootDir, relativePath), "utf8"));
}

function writeJson(relativePath, value) {
  fs.writeFileSync(
    path.join(rootDir, relativePath),
    `${JSON.stringify(value, null, 2)}\n`,
    "utf8",
  );
}

function getVersionSuffix() {
  const rawSpec = readJson("cwms-swagger-raw.json");
  return rawSpec?.info?.version || new Date().toISOString().slice(0, 10).replace(/-/g, ".");
}

function main() {
  const rootPackage = readJson("package.json");
  const generatedPackage = readJson("cwmsjs/package.json");
  const updates = readJson("scripts/package-updates/updates.json");
  const versionSuffix = getVersionSuffix();

  const nextPackage = {
    ...generatedPackage,
    ...updates,
    author: rootPackage.author,
    generatorVersion: rootPackage.version,
    keywords: rootPackage.keywords,
    repository: rootPackage.repository,
    version: `${rootPackage.version}-${versionSuffix}`,
  };

  delete nextPackage.publishConfig;

  writeJson("cwmsjs/package.json", nextPackage);
  fs.copyFileSync(path.join(rootDir, "README.md"), path.join(rootDir, "cwmsjs", "README.md"));
}

main();
