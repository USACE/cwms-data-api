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

function getVersionSuffixArg(argv = process.argv.slice(2)) {
  const prefix = "--version-suffix=";
  const value = argv.find((arg) => arg.startsWith(prefix));
  return value ? value.slice(prefix.length) : argv[0];
}

function normalizeVersionSuffix(value) {
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error("Version suffix must be a non-empty string.");
  }

  return value
    .trim()
    .split(".")
    .map((identifier) => {
      if (!/^[0-9A-Za-z-]+$/.test(identifier)) {
        throw new Error(`Invalid version suffix identifier: ${identifier}`);
      }

      return /^\d+$/.test(identifier)
        ? BigInt(identifier).toString()
        : identifier;
    })
    .join(".");
}

function getVersionSuffix(argv = process.argv.slice(2), env = process.env) {
  const explicitVersion =
    getVersionSuffixArg(argv) ||
    env.CDA_CLIENT_VERSION_SUFFIX ||
    env.CWMSJS_VERSION_SUFFIX;
  if (explicitVersion) {
    return normalizeVersionSuffix(explicitVersion);
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

if (require.main === module) {
  main();
}

module.exports = {
  getVersionSuffix,
  normalizeVersionSuffix,
};
