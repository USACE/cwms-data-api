import { useEffect, useMemo, useState } from "react";
import {
  CWMS_USER_ROLE_PRESETS,
  OfficeDropdown,
  matchCwmsUserRolePreset,
  resolveCwmsUserRolePreset,
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
  Search,
  Skeleton,
  Strong,
  Text,
} from "@usace/groundwork";
import {
  FaExternalLinkAlt,
  FaSearch,
  FaShieldAlt,
  FaUserShield,
  FaUsers,
} from "react-icons/fa";

import { HelpTip } from "../../components/HelpTip";
import { EmptyState, Notice } from "../user-lists/components/StatusMessages";
import { filterUsers, paginateUsers, rolesForOffice, sameRoles } from "./role-state";

const cdaUrl = import.meta.env.VITE_CDA_API_ROOT;
const cliRoleDocsUrl =
  "https://cwms-cli.readthedocs.io/en/latest/cli/users.html#add-a-role-to-a-user";
const usersPerPage = 10;
const roleDescriptions = {
  "All Users": "Baseline membership required for every CWMS user.",
  "CCP Mgr": "Allows management of CWMS Control Point configuration.",
  "CWMS User Admins": "Allows management of users and office-scoped role assignments.",
  "CWMS Users": "Provides standard authenticated CWMS access.",
  "Data Acquisition Mgr": "Allows management of data-acquisition configuration.",
  "TS ID Creator": "Allows creation of time-series identifiers.",
  "VT Mgr": "Allows management of validation and transformation configuration.",
};

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
  const [userPage, setUserPage] = useState(1);
  const [draftRoles, setDraftRoles] = useState([]);
  const [roleMode, setRoleMode] = useState("custom");
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
  const userTotal = usersQuery.data?.total ?? users.length;
  const selectedUser = users.find((user) => user["user-name"] === selectedUserName);
  const currentRoles = useMemo(
    () => rolesForOffice(selectedUser, office),
    [selectedUser, office],
  );
  const visibleUsers = useMemo(
    () => filterUsers(users, search, office),
    [users, search, office],
  );
  const pagination = useMemo(
    () => paginateUsers(visibleUsers, userPage, usersPerPage),
    [userPage, visibleUsers],
  );
  const roleCatalog = useMemo(
    () => [...(rolesQuery.data ?? [])].sort((left, right) => left.localeCompare(right)),
    [rolesQuery.data],
  );
  const presetResolution = useMemo(
    () =>
      roleMode === "custom" ? null : resolveCwmsUserRolePreset(roleMode, roleCatalog),
    [roleCatalog, roleMode],
  );

  useEffect(() => {
    if (!users.some((user) => user["user-name"] === selectedUserName)) {
      setSelectedUserName(users[0]?.["user-name"] ?? "");
    }
  }, [selectedUserName, users]);

  useEffect(() => {
    if (pagination.currentPage !== userPage) setUserPage(pagination.currentPage);
  }, [pagination.currentPage, userPage]);

  useEffect(() => {
    setDraftRoles(currentRoles);
    setRoleMode(matchCwmsUserRolePreset(currentRoles) ?? "custom");
  }, [currentRoles]);

  function changeOffice(nextOffice) {
    setOffice(nextOffice);
    setSelectedUserName("");
    setSearch("");
    setUserPage(1);
    setMessage("");
    setMutationError("");
  }

  function changeSearch(event) {
    const nextSearch = event.target.value;
    const matches = filterUsers(users, nextSearch, office);
    setSearch(nextSearch);
    setUserPage(1);
    setSelectedUserName(matches[0]?.["user-name"] ?? "");
  }

  function changeUserPage(nextPage) {
    const next = paginateUsers(visibleUsers, nextPage, usersPerPage);
    setUserPage(next.currentPage);
    setMessage("");
    setMutationError("");
  }

  function toggleRole(role) {
    if (role === "All Users") return;
    setRoleMode("custom");
    setDraftRoles((current) =>
      current.includes(role)
        ? current.filter((item) => item !== role)
        : [...current, role].sort((left, right) => left.localeCompare(right)),
    );
    setMessage("");
    setMutationError("");
  }

  function selectRoleMode(mode) {
    setRoleMode(mode);
    if (mode === "custom") return;
    const { roles } = resolveCwmsUserRolePreset(mode, roleCatalog);
    const protectedRoles = currentRoles.filter((role) => role === "All Users");
    setDraftRoles(
      [...new Set([...protectedRoles, ...roles])].sort((left, right) =>
        left.localeCompare(right),
      ),
    );
    setMessage("");
    setMutationError("");
  }

  function resetRoles() {
    setDraftRoles(currentRoles);
    setRoleMode(matchCwmsUserRolePreset(currentRoles) ?? "custom");
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
        <div className="mt-3 flex flex-wrap items-center gap-x-2 gap-y-1 text-sm">
          <Text>Prefer the command line?</Text>
          <a
            className="inline-flex items-center gap-2 font-semibold text-blue-700 underline decoration-blue-300 underline-offset-4 hover:text-blue-900"
            href={cliRoleDocsUrl}
            target="_blank"
            rel="noreferrer"
          >
            CWMS CLI user-role guide
            <FaExternalLinkAlt aria-hidden="true" className="h-3 w-3" />
          </a>
        </div>
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
        <div className="grid items-start gap-6 lg:grid-cols-[minmax(19rem,0.8fr)_minmax(0,1.4fr)]">
          <Card className="p-0">
            <div className="flex items-center justify-between border-b border-zinc-200 px-5 py-4">
              <div>
                <H2 className="text-xl">Office users</H2>
                <Text>{office || "Select an office"}</Text>
              </div>
              <Badge color="blue">
                {userTotal} {userTotal === 1 ? "user" : "users"}
              </Badge>
            </div>
            <div className="p-3">
              <div className="mb-3 pr-4">
                <Search
                  aria-label="Filter office users"
                  placeholder="Name, email, user ID, or role"
                  value={search}
                  onChange={changeSearch}
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
                <>
                  <div
                    className="h-[42rem] space-y-2 overflow-y-auto pr-1"
                    role="list"
                    aria-label={`${office} users`}
                  >
                    {pagination.users.map((user) => {
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
                  <nav
                    className="mt-3 border-t border-zinc-200 pt-3"
                    aria-label="Office user pages"
                  >
                    <Text className="mb-2 text-center text-xs">
                      Showing {pagination.start}–{pagination.end} of{" "}
                      {visibleUsers.length}
                    </Text>
                    <div className="flex items-center justify-between gap-2">
                      <Button
                        type="button"
                        color="light"
                        disabled={pagination.currentPage === 1}
                        onClick={() => changeUserPage(pagination.currentPage - 1)}
                      >
                        Previous
                      </Button>
                      <Strong className="text-sm">
                        Page {pagination.currentPage} of {pagination.pageCount}
                      </Strong>
                      <Button
                        type="button"
                        color="light"
                        disabled={pagination.currentPage === pagination.pageCount}
                        onClick={() => changeUserPage(pagination.currentPage + 1)}
                      >
                        Next
                      </Button>
                    </div>
                  </nav>
                </>
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

                <fieldset className="mb-6">
                  <legend>
                    <Strong>Role configuration</Strong>
                  </legend>
                  <Text className="mt-1">
                    Choose a cwms-cli-compatible configuration, or select Custom to
                    assign specific roles. Applying a preset replaces other optional
                    roles when you save.
                  </Text>
                  <div className="mt-3 overflow-hidden rounded-lg border border-zinc-200 bg-white divide-y divide-zinc-200">
                    {CWMS_USER_ROLE_PRESETS.map((preset) => (
                      <div
                        key={preset.id}
                        className={`flex items-center gap-3 p-4 transition ${
                          roleMode === preset.id ? "bg-blue-50" : "hover:bg-zinc-50"
                        }`}
                      >
                        <input
                          id={`role-mode-${preset.id}`}
                          type="radio"
                          name="role-configuration"
                          className="h-4 w-4 border-zinc-400 text-blue-700 focus:ring-blue-600"
                          checked={roleMode === preset.id}
                          onChange={() => selectRoleMode(preset.id)}
                        />
                        <label
                          htmlFor={`role-mode-${preset.id}`}
                          className="min-w-0 flex-1 cursor-pointer"
                        >
                          <Strong>{preset.label}</Strong>
                          <Text className="mt-1 text-xs">
                            {preset.roles.join(" · ")}
                          </Text>
                        </label>
                        <HelpTip title={`${preset.label} configuration`}>
                          {preset.description} Selecting this configuration sets the
                          user&apos;s office roles to {preset.roles.join(", ")}.
                        </HelpTip>
                      </div>
                    ))}
                    <div
                      className={`flex items-center gap-3 p-4 transition ${
                        roleMode === "custom" ? "bg-blue-50" : "hover:bg-zinc-50"
                      }`}
                    >
                      <input
                        id="role-mode-custom"
                        type="radio"
                        name="role-configuration"
                        className="h-4 w-4 border-zinc-400 text-blue-700 focus:ring-blue-600"
                        checked={roleMode === "custom"}
                        onChange={() => selectRoleMode("custom")}
                      />
                      <label
                        htmlFor="role-mode-custom"
                        className="min-w-0 flex-1 cursor-pointer"
                      >
                        <Strong>Custom</Strong>
                        <Text className="mt-1 text-xs">
                          Select specific roles from the expandable list.
                        </Text>
                      </label>
                      <HelpTip title="Custom role configuration">
                        Use Custom when a staff member needs a combination that does not
                        exactly match the read-only, read/write, or administrator
                        presets.
                      </HelpTip>
                    </div>
                  </div>
                </fieldset>

                {presetResolution?.unavailableRoles.length > 0 && (
                  <Notice kind="error">
                    This CDA role catalog does not include:{" "}
                    {presetResolution.unavailableRoles.join(", ")}. The available
                    portions of the preset are selected; review them before saving.
                  </Notice>
                )}

                {roleMode === "custom" &&
                  (rolesQuery.isLoading ? (
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
                    <fieldset className="overflow-hidden rounded-lg border border-zinc-200 bg-zinc-50">
                      <div className="flex items-center justify-between border-b border-zinc-200 bg-white px-4 py-3">
                        <div>
                          <Strong>Specific roles</Strong>
                          <Text className="mt-1 text-xs">
                            Choose the exact roles to assign.
                          </Text>
                        </div>
                        <HelpTip title="Specific role selection">
                          These selections apply only to the chosen office. All Users is
                          required by CWMS and cannot be removed directly.
                        </HelpTip>
                      </div>
                      <legend className="sr-only">
                        Roles for {selectedUser["user-name"]}
                      </legend>
                      <div className="max-h-80 overflow-y-auto divide-y divide-zinc-200">
                        {roleCatalog.map((role) => {
                          const checked = draftRoles.includes(role);
                          const protectedRole = role === "All Users";
                          return (
                            <div
                              key={role}
                              className={`flex items-center gap-3 px-4 py-3 transition ${
                                checked ? "bg-blue-50" : "bg-white hover:bg-zinc-50"
                              } ${protectedRole ? "cursor-not-allowed opacity-75" : "cursor-pointer"}`}
                            >
                              <input
                                id={`role-${role.replaceAll(" ", "-")}`}
                                type="checkbox"
                                className="mt-1 h-4 w-4 rounded border-zinc-400 text-blue-700 focus:ring-blue-600"
                                checked={checked}
                                disabled={protectedRole}
                                onChange={() => toggleRole(role)}
                              />
                              <label
                                htmlFor={`role-${role.replaceAll(" ", "-")}`}
                                className={`min-w-0 flex-1 ${protectedRole ? "cursor-not-allowed" : "cursor-pointer"}`}
                              >
                                <Strong>{role}</Strong>
                                <Text className="mt-1 text-xs">
                                  {protectedRole
                                    ? "Required by CWMS and cannot be removed directly"
                                    : currentRoles.includes(role)
                                      ? "Currently assigned"
                                      : "Not currently assigned"}
                                </Text>
                              </label>
                              <HelpTip title={role}>
                                {roleDescriptions[role] ??
                                  "An office-scoped CWMS role returned by the CDA role catalog."}
                              </HelpTip>
                            </div>
                          );
                        })}
                      </div>
                    </fieldset>
                  ))}

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
                      onClick={resetRoles}
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

      <div className="mt-8 flex flex-wrap items-center gap-3 border-t border-zinc-200 pt-5">
        <Badge color="amber">Planned transition</Badge>
        <Text>
          This page may be replaced at a later date pending completion of the new CWMS
          authorization development.
        </Text>
      </div>
    </section>
  );
}
