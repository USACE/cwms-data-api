import assert from "node:assert/strict";
import test from "node:test";
import { toUsernameRegex } from "./username-search.js";

test("converts the user-facing wildcard to a CDA regex wildcard", () => {
  assert.equal(toUsernameRegex("CHAR*"), "CHAR.*");
  assert.equal(toUsernameRegex("*CHAR*"), ".*CHAR.*");
});

test("escapes regex punctuation while preserving username search behavior", () => {
  assert.equal(toUsernameRegex("new.staff"), "new\\.staff");
  assert.equal(toUsernameRegex("user+test"), "user\\+test");
});
