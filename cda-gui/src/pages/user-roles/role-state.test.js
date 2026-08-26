import test from "node:test";
import assert from "node:assert/strict";

import { filterUsers, rolesForOffice, sameRoles } from "./role-state.js";

const users = [
  {
    "user-name": "ALPHA",
    principal: "alpha.one",
    email: "alpha@example.com",
    roles: { SWT: ["TS ID Creator", "CWMS Users"] },
  },
  {
    "user-name": "BRAVO",
    principal: "bravo.two",
    email: "bravo@example.com",
    roles: { SWT: ["CCP Mgr"] },
  },
];

test("rolesForOffice returns a sorted office-scoped copy", () => {
  assert.deepEqual(rolesForOffice(users[0], "SWT"), ["CWMS Users", "TS ID Creator"]);
  assert.deepEqual(rolesForOffice(users[0], "SPK"), []);
});

test("sameRoles ignores selection order", () => {
  assert.equal(sameRoles(["CWMS Users", "CCP Mgr"], ["CCP Mgr", "CWMS Users"]), true);
  assert.equal(sameRoles(["CWMS Users"], ["CCP Mgr"]), false);
});

test("filterUsers searches identity fields and office roles", () => {
  assert.deepEqual(filterUsers(users, "creator", "SWT"), [users[0]]);
  assert.deepEqual(filterUsers(users, "bravo@", "SWT"), [users[1]]);
});
