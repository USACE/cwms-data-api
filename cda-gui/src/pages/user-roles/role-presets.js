export const CWMS_USER_ROLE_PRESETS = [
  {
    id: "readonly",
    label: "Read only",
    description: "View CWMS data without creating time-series identifiers.",
    roles: ["All Users", "CWMS Users"],
  },
  {
    id: "readwrite",
    label: "Read/write",
    description: "View CWMS data and create time-series identifiers.",
    roles: ["All Users", "CWMS Users", "TS ID Creator"],
  },
  {
    id: "batchadmin",
    label: "Batch administrator",
    description: "Manage data acquisition in addition to read/write access.",
    roles: ["All Users", "CWMS Users", "TS ID Creator", "Data Acquisition Mgr"],
  },
  {
    id: "admin",
    label: "User administrator",
    description: "Manage CWMS users in addition to read/write access.",
    roles: ["All Users", "CWMS Users", "TS ID Creator", "CWMS User Admins"],
  },
];

export function resolveCwmsUserRolePreset(presetId, availableRoles) {
  const preset = CWMS_USER_ROLE_PRESETS.find(({ id }) => id === presetId);
  const roleLookup = new Map(availableRoles.map((role) => [role.toLowerCase(), role]));

  if (!preset) return { roles: [], unavailableRoles: [] };

  const roles = [];
  const unavailableRoles = [];
  preset.roles.forEach((role) => {
    const availableRole = roleLookup.get(role.toLowerCase());
    if (availableRole) roles.push(availableRole);
    else unavailableRoles.push(role);
  });

  return { roles, unavailableRoles };
}

export function matchCwmsUserRolePreset(roles) {
  const normalized = new Set(roles.map((role) => role.toLowerCase()));
  const preset = CWMS_USER_ROLE_PRESETS.find(
    ({ roles: presetRoles }) =>
      presetRoles.length === normalized.size &&
      presetRoles.every((role) => normalized.has(role.toLowerCase())),
  );
  return preset?.id ?? null;
}
