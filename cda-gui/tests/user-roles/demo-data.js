export const demoRoles = [
  "All Users",
  "CWMS Users",
  "TS ID Creator",
  "CWMS User Admins",
  "CWMS PD Users",
  "Data Acquisition Mgr",
];

export const demoOffices = [
  { name: "HQ", "long-name": "Headquarters" },
  { name: "SWT", "long-name": "Tulsa District" },
  { name: "SPK", "long-name": "Sacramento District" },
];

export const demoProfile = {
  "user-name": "demo.admin",
  roles: { SWT: [...demoRoles], SPK: [...demoRoles] },
};

export function createDemoUsers() {
  return [
    { "user-name": "alex.hq", roles: { HQ: ["All Users", "CWMS Users"] } },
    { "user-name": "blair.unassigned", roles: {} },
    { "user-name": "casey.baseline", roles: { HQ: ["All Users"] } },
    {
      "user-name": "devon.tulsa",
      roles: { SWT: ["All Users", "CWMS Users", "TS ID Creator"] },
    },
    {
      "user-name": "ellis.multi",
      roles: {
        HQ: ["All Users", "CWMS Users"],
        SWT: ["All Users", "CWMS Users"],
        SPK: ["All Users"],
      },
    },
    { "user-name": "frankie.hq.admin", roles: { HQ: [...demoRoles] } },
    { "user-name": "gray.sacramento", roles: { SPK: ["All Users", "CWMS Users"] } },
  ].map((user) => ({
    ...user,
    email: `${user["user-name"]}@example.test`,
    principal: user["user-name"],
  }));
}
