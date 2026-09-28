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

test("admin includes paired-data and acquisition access and reports unavailable roles", () => {
  const previousRoles = [
    "All Users",
    "CWMS Users",
    "TS ID Creator",
    "CWMS User Admins",
  ];
  const roles = [...previousRoles, "CWMS PD Users", "Data Acquisition Mgr"];
  assert.deepEqual(resolveCwmsUserRolePreset("admin", roles), {
    roles,
    unavailableRoles: [],
  });
  assert.equal(matchCwmsUserRolePreset(roles), "admin");
  assert.equal(matchCwmsUserRolePreset(previousRoles), null);
  assert.deepEqual(resolveCwmsUserRolePreset("admin", previousRoles), {
    roles: previousRoles,
    unavailableRoles: ["CWMS PD Users", "Data Acquisition Mgr"],
  });
});

import {
  filterUsers,
  onboardingUsers,
  officeAssignmentOffices,
  paginateUsers,
  rolesForOffice,
  sameRoles,
  usersForOffice,
} from "./role-state.js";

test("onboarding includes unassigned and basic HQ-only users across the full office map", () => {
  const candidates = [
    { "user-name": "no-map" },
    { "user-name": "no-office", roles: {} },
    { "user-name": "empty-office", roles: { SWT: [] } },
    { "user-name": "hq-baseline", roles: { HQ: ["All Users"] } },
    { "user-name": "hq-user", roles: { HQ: ["All Users", "CWMS Users"], SWT: [] } },
    { "user-name": "lowercase", roles: { hq: ["cwms users"] } },
  ];
  const assigned = [
    { roles: { SWT: ["All Users"] } },
    { roles: { HQ: ["CWMS Users"], SWT: ["All Users"] } },
    { roles: { HQ: ["All Users", "CWMS Users", "CWMS User Admins"] } },
    { roles: { HQ: ["CWMS PD Users"] } },
  ];
  assert.deepEqual(onboardingUsers([...candidates, ...assigned]), candidates);
});

test("office assignment requires both Admin and PD in the same office", () => {
  assert.deepEqual(
    officeAssignmentOffices({
      HQ: ["CWMS User Admins"],
      SPK: ["CWMS PD Users"],
      SWT: ["CWMS User Admins", "CWMS PD Users"],
      NWD: ["cwms user admins", "cwms pd users"],
    }),
    ["NWD", "SWT"],
  );
  assert.deepEqual(officeAssignmentOffices(), []);
});

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
