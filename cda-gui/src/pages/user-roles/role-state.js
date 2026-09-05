export function rolesForOffice(user, office) {
  return [...(user?.roles?.[office] ?? [])].sort((left, right) =>
    left.localeCompare(right),
  );
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
