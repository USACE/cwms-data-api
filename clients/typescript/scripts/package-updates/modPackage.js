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

function getVersionSuffixArg() {
  const prefix = "--version-suffix=";
  const value = process.argv.find((arg) => arg.startsWith(prefix));
  return value ? value.slice(prefix.length) : process.argv[2];
}

function getVersionSuffix() {
  const explicitVersion =
    getVersionSuffixArg() ||
    process.env.CDA_CLIENT_VERSION_SUFFIX ||
    process.env.CWMSJS_VERSION_SUFFIX;
  if (explicitVersion) {
    return explicitVersion;
  }

  throw new Error(
    "Missing version suffix. Set CDA_CLIENT_VERSION_SUFFIX or pass --version-suffix=<version>.",
  );
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
  fs.copyFileSync(
    path.join(rootDir, "README.md"),
    path.join(rootDir, "cwmsjs", "README.md"),
  );
}

main();
