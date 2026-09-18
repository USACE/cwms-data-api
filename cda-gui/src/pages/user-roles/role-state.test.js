import test from "node:test";
import assert from "node:assert/strict";
import { matchCwmsUserRolePreset, resolveCwmsUserRolePreset } from "./role-presets.js";

test("batchadmin resolves catalog casing and matches saved roles", () => {
  const roles = ["All Users", "CWMS Users", "TS ID Creator", "Data Acquisition Mgr"];
  const catalog = [...roles.map((role) => role.toUpperCase()), "CWMS User Admins"];
  const resolved = resolveCwmsUserRolePreset("batchadmin", catalog);
  assert.deepEqual(resolved, {
    roles: roles.map((role) => role.toUpperCase()),
    unavailableRoles: [],
  });
  assert.equal(matchCwmsUserRolePreset([...resolved.roles].reverse()), "batchadmin");
  assert.equal(matchCwmsUserRolePreset([...roles, "CWMS User Admins"]), null);
});

test("batchadmin reports unavailable acquisition access", () => {
  const roles = ["All Users", "CWMS Users", "TS ID Creator"];
  assert.deepEqual(resolveCwmsUserRolePreset("batchadmin", roles), {
    roles,
    unavailableRoles: ["Data Acquisition Mgr"],
  });
  assert.equal(matchCwmsUserRolePreset(roles), "readwrite");
});

import {
  filterUsers,
  paginateUsers,
  rolesForOffice,
  sameRoles,
  usersForOffice,
} from "./role-state.js";

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

test("office users include baseline-only members and exclude other offices", () => {
  const newUser = { "user-name": "NEW", roles: { HQ: ["All Users"] } };
  const assignedUser = {
    "user-name": "ASSIGNED",
    roles: { HQ: ["All Users"], SWT: ["All Users"] },
  };
  assert.deepEqual(usersForOffice([...users, newUser, assignedUser], "SWT"), [
    ...users,
    assignedUser,
  ]);
  assert.deepEqual(usersForOffice([newUser, assignedUser], "SPK"), []);
});

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

test("paginateUsers clamps pages and reports the visible range", () => {
  const page = paginateUsers([1, 2, 3, 4, 5], 2, 2);
  assert.deepEqual(page, {
    currentPage: 2,
    pageCount: 3,
    users: [3, 4],
    start: 3,
    end: 4,
  });
  assert.equal(paginateUsers([1, 2, 3], 99, 2).currentPage, 2);
  assert.deepEqual(paginateUsers([], 1, 10), {
    currentPage: 1,
    pageCount: 1,
    users: [],
    start: 0,
    end: 0,
  });
});
