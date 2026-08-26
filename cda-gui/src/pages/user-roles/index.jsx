import { useEffect, useMemo, useState } from "react";
import {
  OfficeDropdown,
  useAuth,
  useCdaRoles,
  useCdaUsers,
  useUpdateCdaUserRoles,
} from "@usace-watermanagement/groundwork-water";
import {
  Badge,
  Button,
  Card,
  H1,
  H2,
  Input,
  Skeleton,
  Strong,
  Text,
} from "@usace/groundwork";
import { FaSearch, FaShieldAlt, FaUserShield, FaUsers } from "react-icons/fa";

import { HelpTip } from "../../components/HelpTip";
import { EmptyState, Notice } from "../user-lists/components/StatusMessages";
import { filterUsers, rolesForOffice, sameRoles } from "./role-state";

const cdaUrl = import.meta.env.VITE_CDA_API_ROOT;

function updateSummary(userName, additions, removals) {
  const changes = [];
  if (additions.length) changes.push(`added ${additions.length}`);
  if (removals.length) changes.push(`removed ${removals.length}`);
  return changes.length
    ? `Updated ${userName}: ${changes.join(", ")} role${additions.length + removals.length === 1 ? "" : "s"}.`
    : `No role changes were needed for ${userName}.`;
}

export default function UserRoles() {
  const auth = useAuth();
  const [office, setOffice] = useState("");
  const [selectedUserName, setSelectedUserName] = useState("");
  const [search, setSearch] = useState("");
  const [draftRoles, setDraftRoles] = useState([]);
  const [message, setMessage] = useState("");
  const [mutationError, setMutationError] = useState("");

  const adminOffices = useMemo(
    () =>
      Object.entries(auth.profile?.roles ?? {})
        .filter(([, roles]) => roles.includes("CWMS User Admins"))
        .map(([officeId]) => officeId)
        .sort(),
    [auth.profile],
  );

  useEffect(() => {
    if (!adminOffices.includes(office)) setOffice(adminOffices[0] ?? "");
  }, [adminOffices, office]);

  const usersQuery = useCdaUsers({
    cdaUrl,
    token: auth.token,
    office,
    queryOptions: { enabled: auth.isAuth && Boolean(office) },
  });
  const rolesQuery = useCdaRoles({
    cdaUrl,
    token: auth.token,
    queryOptions: { enabled: auth.isAuth && adminOffices.length > 0 },
  });
  const updateRoles = useUpdateCdaUserRoles({ cdaUrl, token: auth.token });

  const users = useMemo(() => usersQuery.data?.users ?? [], [usersQuery.data]);
  const selectedUser = users.find((user) => user["user-name"] === selectedUserName);
  const currentRoles = useMemo(
    () => rolesForOffice(selectedUser, office),
    [selectedUser, office],
  );
  const visibleUsers = useMemo(
    () => filterUsers(users, search, office),
    [users, search, office],
  );
  const roleCatalog = useMemo(
    () => [...(rolesQuery.data ?? [])].sort((left, right) => left.localeCompare(right)),
    [rolesQuery.data],
  );

  useEffect(() => {
    if (!users.some((user) => user["user-name"] === selectedUserName)) {
      setSelectedUserName(users[0]?.["user-name"] ?? "");
    }
  }, [selectedUserName, users]);

  useEffect(() => {
    setDraftRoles(currentRoles);
  }, [currentRoles]);

  function changeOffice(nextOffice) {
    setOffice(nextOffice);
    setSelectedUserName("");
    setSearch("");
    setMessage("");
    setMutationError("");
  }

  function toggleRole(role) {
    setDraftRoles((current) =>
      current.includes(role)
        ? current.filter((item) => item !== role)
        : [...current, role].sort((left, right) => left.localeCompare(right)),
    );
    setMessage("");
    setMutationError("");
  }

  async function saveRoles(event) {
    event.preventDefault();
    if (!selectedUser) return;
    setMessage("");
    setMutationError("");
    try {
      const result = await updateRoles.mutateAsync({
        userName: selectedUser["user-name"],
        office,
        previousRoles: currentRoles,
        roles: draftRoles,
      });
      setMessage(
        updateSummary(selectedUser["user-name"], result.additions, result.removals),
      );
    } catch (error) {
      setMutationError(error?.message ?? "Unable to update this user's roles.");
    }
  }

  if (!auth.isAuth) {
    return (
      <Card className="mx-auto my-12 max-w-2xl p-8 text-center">
        <div className="mx-auto mb-4 w-fit rounded-full bg-blue-50 p-4 text-blue-700">
          <FaUserShield aria-hidden="true" className="h-8 w-8" />
        </div>
        <H1>User Roles</H1>
        <Text className="mx-auto mt-3 max-w-lg">
          Sign in with a CWMS User Administrator account to review staff and manage
          office-scoped role assignments.
        </Text>
        <Button className="mt-6" type="button" onClick={auth.login}>
          Log in
        </Button>
      </Card>
    );
  }

  return (
    <section className="pb-12">
      <div className="mb-6 border-b border-zinc-200 pb-6">
        <div className="mb-2 flex flex-wrap items-center gap-2">
          <Badge color="blue">CDA</Badge>
          <Badge color="green">User administrator</Badge>
        </div>
        <div className="flex items-center gap-1">
          <H1>User Roles</H1>
          <HelpTip title="Office role authority">
            The signed-in profile determines which offices appear. CDA and the CWMS
            database enforce whether an administrator may grant or remove each role.
          </HelpTip>
        </div>
        <Text className="mt-2 max-w-3xl">
          Review every active user in an authorized office and maintain the roles that
          control their CWMS access.
        </Text>
      </div>

      {mutationError && <Notice kind="error">{mutationError}</Notice>}
      {usersQuery.error && <Notice kind="error">{usersQuery.error.message}</Notice>}
      {rolesQuery.error && <Notice kind="error">{rolesQuery.error.message}</Notice>}
      {message && <Notice kind="success">{message}</Notice>}

      <Card className="mb-6 p-5">
        <div className="grid gap-5 md:grid-cols-[minmax(18rem,24rem)_1fr] md:items-center">
          <div>
            <Strong>Office</Strong>
            <Text className="mt-1">
              Choose an office where you are a User Administrator.
            </Text>
          </div>
          {auth.isLoading ? (
            <Skeleton className="h-10 w-full" />
          ) : adminOffices.length ? (
            <OfficeDropdown
              cdaUrl={cdaUrl}
              includeOffices={adminOffices}
              value={office}
              initOverrides={{
                headers: auth.token
                  ? { Authorization: `Bearer ${auth.token}` }
                  : undefined,
              }}
              onChange={changeOffice}
            />
          ) : (
            <Text role="status">
              Your profile does not include CWMS User Administrator access for an
              office.
            </Text>
          )}
        </div>
      </Card>

      {adminOffices.length > 0 && (
        <div className="grid gap-6 lg:grid-cols-[minmax(19rem,0.8fr)_minmax(0,1.4fr)]">
          <Card className="p-0">
            <div className="flex items-center justify-between border-b border-zinc-200 px-5 py-4">
              <div>
                <H2 className="text-xl">Office users</H2>
                <Text>{office || "Select an office"}</Text>
              </div>
              <Badge color="blue">{usersQuery.data?.total ?? users.length}</Badge>
            </div>
            <div className="p-3">
              <div className="relative mb-3">
                <FaSearch
                  aria-hidden="true"
                  className="pointer-events-none absolute left-3 top-3 text-zinc-500"
                />
                <Input
                  aria-label="Filter office users"
                  className="pl-9"
                  placeholder="Name, email, user ID, or role"
                  value={search}
                  onChange={(event) => setSearch(event.target.value)}
                />
              </div>

              {usersQuery.isLoading ? (
                <div className="space-y-3 p-2">
                  <Skeleton className="h-20 w-full" />
                  <Skeleton className="h-20 w-full" />
                  <Skeleton className="h-20 w-full" />
                </div>
              ) : users.length === 0 ? (
                <EmptyState icon={FaUsers} title="No office users">
                  CDA did not return any users with active privileges in this office.
                </EmptyState>
              ) : visibleUsers.length === 0 ? (
                <EmptyState icon={FaSearch} title="No matching users">
                  Try a different name, email, user ID, or role.
                </EmptyState>
              ) : (
                <div
                  className="max-h-[42rem] space-y-2 overflow-y-auto"
                  role="list"
                  aria-label={`${office} users`}
                >
                  {visibleUsers.map((user) => {
                    const userName = user["user-name"];
                    const active = userName === selectedUserName;
                    const officeRoles = rolesForOffice(user, office);
                    return (
                      <button
                        key={userName}
                        type="button"
                        role="listitem"
                        aria-current={active ? "true" : undefined}
                        className={`w-full rounded-lg border p-4 text-left transition ${
                          active
                            ? "border-blue-600 bg-blue-50 shadow-sm"
                            : "border-zinc-200 bg-white hover:border-blue-300 hover:bg-zinc-50"
                        }`}
                        onClick={() => {
                          setSelectedUserName(userName);
                          setMessage("");
                          setMutationError("");
                        }}
                      >
                        <div className="flex items-start justify-between gap-3">
                          <Strong>{userName}</Strong>
                          <Badge color={officeRoles.length ? "green" : "zinc"}>
                            {officeRoles.length} role
                            {officeRoles.length === 1 ? "" : "s"}
                          </Badge>
                        </div>
                        <Text className="mt-1 truncate">
                          {user.email || user.principal}
                        </Text>
                        {officeRoles.length > 0 && (
                          <Text className="mt-2 line-clamp-2 text-xs">
                            {officeRoles.join(" · ")}
                          </Text>
                        )}
                      </button>
                    );
                  })}
                </div>
              )}
            </div>
          </Card>

          <Card className="p-0">
            <div className="border-b border-zinc-200 px-5 py-4">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div>
                  <H2 className="text-xl">
                    {selectedUser?.["user-name"] ?? "Role assignment"}
                  </H2>
                  <Text>
                    {selectedUser
                      ? selectedUser.email || selectedUser.principal
                      : "Choose an office user to manage."}
                  </Text>
                </div>
                {selectedUser && <Badge color="blue">{office}</Badge>}
              </div>
            </div>

            {!selectedUser ? (
              <div className="p-5">
                <EmptyState icon={FaShieldAlt} title="Choose a user">
                  Select an office user to review and edit their roles.
                </EmptyState>
              </div>
            ) : (
              <form className="p-5" onSubmit={saveRoles}>
                <div className="mb-5 rounded-lg border border-blue-100 bg-blue-50 p-4">
                  <Strong>Assign roles for {office}</Strong>
                  <Text className="mt-1">
                    Changes take effect only after you save. CDA records additions and
                    removals through its existing user-management endpoints.
                  </Text>
                </div>

                {rolesQuery.isLoading ? (
                  <div className="grid gap-3 sm:grid-cols-2">
                    <Skeleton className="h-14 w-full" />
                    <Skeleton className="h-14 w-full" />
                    <Skeleton className="h-14 w-full" />
                    <Skeleton className="h-14 w-full" />
                  </div>
                ) : roleCatalog.length === 0 ? (
                  <EmptyState icon={FaShieldAlt} title="No roles available">
                    CDA did not return a role catalog for this administrator.
                  </EmptyState>
                ) : (
                  <fieldset>
                    <legend className="sr-only">
                      Roles for {selectedUser["user-name"]}
                    </legend>
                    <div className="grid gap-3 sm:grid-cols-2">
                      {roleCatalog.map((role) => {
                        const checked = draftRoles.includes(role);
                        return (
                          <label
                            key={role}
                            className={`flex cursor-pointer items-start gap-3 rounded-lg border p-4 transition ${
                              checked
                                ? "border-blue-500 bg-blue-50"
                                : "border-zinc-200 hover:border-blue-300"
                            }`}
                          >
                            <input
                              type="checkbox"
                              className="mt-1 h-4 w-4 rounded border-zinc-400 text-blue-700 focus:ring-blue-600"
                              checked={checked}
                              onChange={() => toggleRole(role)}
                            />
                            <span>
                              <Strong>{role}</Strong>
                              <Text className="mt-1 text-xs">
                                {currentRoles.includes(role)
                                  ? "Currently assigned"
                                  : "Not currently assigned"}
                              </Text>
                            </span>
                          </label>
                        );
                      })}
                    </div>
                  </fieldset>
                )}

                <div className="mt-6 flex flex-wrap items-center justify-between gap-3 border-t border-zinc-200 pt-5">
                  <Text>
                    {draftRoles.length} role{draftRoles.length === 1 ? "" : "s"}{" "}
                    selected
                  </Text>
                  <div className="flex gap-2">
                    <Button
                      type="button"
                      color="light"
                      disabled={sameRoles(currentRoles, draftRoles)}
                      onClick={() => setDraftRoles(currentRoles)}
                    >
                      Reset
                    </Button>
                    <Button
                      type="submit"
                      disabled={
                        updateRoles.isPending || sameRoles(currentRoles, draftRoles)
                      }
                    >
                      <FaUserShield aria-hidden="true" />
                      {updateRoles.isPending ? "Saving…" : "Save roles"}
                    </Button>
                  </div>
                </div>
              </form>
            )}
          </Card>
        </div>
      )}
    </section>
  );
}
