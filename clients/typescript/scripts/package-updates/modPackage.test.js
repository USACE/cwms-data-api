const assert = require("node:assert/strict");
const test = require("node:test");

const { getVersionSuffix, normalizeVersionSuffix } = require("./modPackage.js");

test("normalizes zero-padded production CalVer identifiers", () => {
  assert.equal(normalizeVersionSuffix("2026.08.25"), "2026.8.25");
});

test("preserves a suffixed production release", () => {
  assert.equal(normalizeVersionSuffix("2026.03.31-b"), "2026.3.31-b");
});

test("normalizes prerelease and nightly version suffixes", () => {
  assert.equal(normalizeVersionSuffix("2026.08.25-dev"), "2026.8.25-dev");
  assert.equal(normalizeVersionSuffix("develop-nightly"), "develop-nightly");
});

test("rejects empty and invalid version suffixes", () => {
  assert.throws(() => normalizeVersionSuffix(""), /non-empty string/);
  assert.throws(() => normalizeVersionSuffix("2026..25"), /Invalid version/);
  assert.throws(
    () => normalizeVersionSuffix("release/2026.08.25"),
    /Invalid version/,
  );
});

test("reads the CLI argument before environment variables", () => {
  assert.equal(
    getVersionSuffix(["--version-suffix=2026.08.25"], {
      CDA_CLIENT_VERSION_SUFFIX: "2026.03.31",
    }),
    "2026.8.25",
  );
});

test("reads supported environment variables", () => {
  assert.equal(
    getVersionSuffix([], { CDA_CLIENT_VERSION_SUFFIX: "2026.08.25-test" }),
    "2026.8.25-test",
  );
  assert.equal(
    getVersionSuffix([], { CWMSJS_VERSION_SUFFIX: "2026.08.25-dev" }),
    "2026.8.25-dev",
  );
});

test("requires an explicit version suffix", () => {
  assert.throws(() => getVersionSuffix([], {}), /Missing version suffix/);
});
