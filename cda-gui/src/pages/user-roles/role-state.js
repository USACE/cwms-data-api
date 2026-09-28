export function rolesForOffice(user, office) {
  return [...(user?.roles?.[office] ?? [])].sort((left, right) =>
    left.localeCompare(right),
  );
}

export function usersForOffice(users, office) {
  return users.filter((user) => rolesForOffice(user, office).length > 0);
}

export function onboardingUsers(users) {
  return users.filter((user) => {
    const offices = Object.entries(user.roles ?? {}).filter(
      ([, roles]) => roles.length,
    );
    return (
      offices.length === 0 ||
      (offices.length === 1 &&
        offices[0][0].toUpperCase() === "HQ" &&
        offices[0][1].every((role) =>
          ["all users", "cwms users"].includes(role.toLowerCase()),
        ))
    );
  });
}

export function officeAssignmentOffices(rolesByOffice = {}) {
  return Object.entries(rolesByOffice)
    .filter(([, roles]) => {
      const normalized = roles.map((role) => role.toLowerCase());
      return ["cwms user admins", "cwms pd users"].every((role) =>
        normalized.includes(role),
      );
    })
    .map(([office]) => office)
    .sort();
}

export function sameRoles(left, right) {
  if (left.length !== right.length) return false;
  const selected = new Set(right);
  return left.every((role) => selected.has(role));
}

export function filterUsers(users, search, office) {
  const term = search.trim().toLowerCase();
  if (!term) return users;
  return users.filter((user) =>
    [user["user-name"], user.principal, user.email, ...rolesForOffice(user, office)]
      .filter(Boolean)
      .some((value) => value.toLowerCase().includes(term)),
  );
}

export function paginateUsers(users, page, pageSize) {
  const pageCount = Math.max(1, Math.ceil(users.length / pageSize));
  const currentPage = Math.min(Math.max(page, 1), pageCount);
  const start = (currentPage - 1) * pageSize;
  return {
    currentPage,
    pageCount,
    users: users.slice(start, start + pageSize),
    start: users.length ? start + 1 : 0,
    end: Math.min(start + pageSize, users.length),
  };
}
