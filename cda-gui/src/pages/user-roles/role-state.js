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
