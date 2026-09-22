import { useMemo, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Badge, Button, Input, Modal, Strong, Text } from "@usace/groundwork";
import {
  OfficeDropdown,
  useCdaRoles,
  useCdaUsers,
  useUpdateCdaUserRoles,
} from "@usace-watermanagement/groundwork-water";
import PropTypes from "prop-types";
import { Notice } from "../user-lists/components/StatusMessages";
import { filterUsers, onboardingUsers, rolesForOffice } from "./role-state";
import { CWMS_USER_ROLE_PRESETS, resolveCwmsUserRolePreset } from "./role-presets";

export function OnboardingDialog({ cdaUrl, token, offices, initialOffice, onClose }) {
  const [search, setSearch] = useState("");
  const [selectedName, setSelectedName] = useState("");
  const [office, setOffice] = useState(
    offices.includes(initialOffice) ? initialOffice : (offices[0] ?? ""),
  );
  const [preset, setPreset] = useState("readonly");
  const [customRoles, setCustomRoles] = useState([]);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const queryClient = useQueryClient();
  // Fetch across all offices before filtering: an HQ response alone cannot tell
  // whether a user also belongs to another office. The hook follows all pages.
  const usersQuery = useCdaUsers({ cdaUrl, token });
  const rolesQuery = useCdaRoles({ cdaUrl, token });
  const updateRoles = useUpdateCdaUserRoles({ cdaUrl, token });
  const candidates = useMemo(
    () => onboardingUsers(usersQuery.data?.users ?? []),
    [usersQuery.data],
  );
  const users = filterUsers(candidates, search, "HQ");
  const selectedUser = candidates.find((user) => user["user-name"] === selectedName);
  const catalog = rolesQuery.data ?? [];
  const baseline = catalog.find((role) => role.toLowerCase() === "all users");
  const resolution = resolveCwmsUserRolePreset(preset, catalog);
  const selectedRoles = preset === "custom" ? customRoles : resolution.roles;
  const working = updateRoles.isPending;
  const unavailable = resolution.unavailableRoles.length > 0 || !baseline;
  const canSave =
    selectedUser &&
    offices.includes(office) &&
    !working &&
    !usersQuery.isFetching &&
    !usersQuery.isError &&
    !rolesQuery.isFetching &&
    !rolesQuery.isError &&
    !unavailable;

  async function save() {
    if (!canSave) return;
    setError("");
    setMessage("");
    try {
      const previousRoles = rolesForOffice(selectedUser, office);
      await updateRoles.mutateAsync({
        userName: selectedName,
        office,
        previousRoles,
        roles: [...new Set([...previousRoles, baseline, ...selectedRoles])],
      });
      setMessage(
        `Assigned ${selectedName} to ${office}. Existing office roles were kept.`,
      );
      setSelectedName("");
      await queryClient.invalidateQueries({ queryKey: ["cda", "users", cdaUrl] });
    } catch (cause) {
      setError(
        `${cause?.message ?? "Unable to assign roles."} Refresh users to review any roles already saved before trying again.`,
      );
    }
  }

  return (
    <Modal
      opened
      onClose={() => !working && onClose()}
      dialogTitle="Onboard users"
      dialogDescription="Assign offices and roles to users with no office roles or only basic HQ access."
      size="4xl"
    >
      <Text className="mb-4 text-sm">
        To assign an office, you must have both CWMS User Admins and CWMS PD Users in
        that office. Existing HQ roles are kept.
      </Text>
      {error && <Notice kind="error">{error}</Notice>}
      {message && <Notice kind="success">{message}</Notice>}
      {usersQuery.error && <Notice kind="error">{usersQuery.error.message}</Notice>}
      {rolesQuery.error && <Notice kind="error">{rolesQuery.error.message}</Notice>}
      <div className="grid gap-5 md:grid-cols-2">
        <div className="min-w-0">
          <label htmlFor="onboarding-search">
            <Strong>Users to onboard</Strong>
          </label>
          <Input
            id="onboarding-search"
            className="mt-2"
            value={search}
            disabled={working}
            placeholder="Search name, email, or user ID"
            onChange={(event) => setSearch(event.target.value)}
          />
          <Text className="my-2 text-sm" role="status">
            {usersQuery.isFetching
              ? "Loading users…"
              : `${users.length} users to onboard`}
          </Text>
          {!usersQuery.isFetching && !usersQuery.error && users.length === 0 && (
            <Text>
              {search
                ? "No matching users."
                : "No users need onboarding. New users must sign in to CDA first."}
            </Text>
          )}
          <div
            className="max-h-80 space-y-2 overflow-y-auto"
            aria-label="Users to onboard"
          >
            {users.map((user) => (
              <button
                key={user["user-name"]}
                type="button"
                disabled={working}
                aria-pressed={selectedName === user["user-name"]}
                className={`w-full rounded-lg border p-3 text-left ${selectedName === user["user-name"] ? "border-blue-600 bg-blue-50" : "border-zinc-200"}`}
                onClick={() => {
                  setSelectedName(user["user-name"]);
                  setPreset("readonly");
                  setCustomRoles([]);
                  setError("");
                }}
              >
                <Strong className="break-all">{user["user-name"]}</Strong>
                <Text className="break-all text-sm">
                  {user.email || user.principal}
                </Text>
                <Badge color="blue">
                  {Object.values(user.roles ?? {}).some((roles) => roles.length)
                    ? "HQ only"
                    : "No office"}
                </Badge>
              </button>
            ))}
          </div>
        </div>
        <div className="min-w-0 space-y-4">
          {selectedUser ? (
            <>
              <Strong className="break-all">Assign {selectedName}</Strong>
              <div>
                <Strong>Current offices and roles</Strong>
                {Object.entries(selectedUser.roles ?? {})
                  .filter(([, roles]) => roles.length)
                  .map(([id, roles]) => (
                    <Text key={id} className="text-sm">
                      {id}: {roles.join(", ")}
                    </Text>
                  ))}
                {!Object.values(selectedUser.roles ?? {}).some(
                  (roles) => roles.length,
                ) && <Text>No office roles assigned.</Text>}
              </div>
              <div>
                <label htmlFor="onboarding-office">
                  <Strong>Office to assign</Strong>
                </label>
                <OfficeDropdown
                  id="onboarding-office"
                  cdaUrl={cdaUrl}
                  includeOffices={offices}
                  value={office}
                  disabled={working}
                  onChange={setOffice}
                  initOverrides={{
                    headers: token ? { Authorization: `Bearer ${token}` } : undefined,
                  }}
                />
              </div>
              <div>
                <label htmlFor="onboarding-preset">
                  <Strong>Roles to assign</Strong>
                </label>
                <select
                  id="onboarding-preset"
                  className="mt-2 w-full rounded border border-zinc-300 p-2"
                  value={preset}
                  disabled={working}
                  onChange={(event) => {
                    setCustomRoles(selectedRoles);
                    setPreset(event.target.value);
                  }}
                >
                  {CWMS_USER_ROLE_PRESETS.map(({ id, label }) => (
                    <option key={id} value={id}>
                      {label}
                    </option>
                  ))}
                  <option value="custom">Custom</option>
                </select>
              </div>
              {preset === "custom" ? (
                <fieldset
                  className="max-h-52 space-y-2 overflow-y-auto rounded border p-3"
                  disabled={working}
                >
                  <legend>Specific roles</legend>
                  {[...catalog].sort().map((role) => (
                    <label key={role} className="flex items-center gap-2 text-sm">
                      <input
                        type="checkbox"
                        checked={role === baseline || customRoles.includes(role)}
                        disabled={role === baseline}
                        onChange={(event) =>
                          setCustomRoles((current) =>
                            event.target.checked
                              ? [...current, role]
                              : current.filter((item) => item !== role),
                          )
                        }
                      />
                      {role}
                    </label>
                  ))}
                </fieldset>
              ) : (
                <Text className="text-sm">{selectedRoles.join(", ")}</Text>
              )}
              {unavailable && !rolesQuery.isFetching && (
                <Notice kind="error">
                  Required roles are missing from the role catalog:{" "}
                  {[
                    ...resolution.unavailableRoles,
                    ...(!baseline ? ["All Users"] : []),
                  ].join(", ")}
                  . Choose available custom roles or contact an administrator.
                </Notice>
              )}
            </>
          ) : (
            <Text className="rounded-lg bg-zinc-50 p-4">
              Choose a user to assign their office and roles.
            </Text>
          )}
        </div>
      </div>
      <div className="mt-6 flex flex-wrap justify-end gap-3 border-t border-zinc-200 pt-4">
        <Button
          type="button"
          color="light"
          disabled={working || usersQuery.isFetching}
          onClick={() => usersQuery.refetch()}
        >
          Refresh users
        </Button>
        <Button type="button" color="light" disabled={working} onClick={onClose}>
          Close
        </Button>
        <Button type="button" disabled={!canSave} onClick={save}>
          {working ? "Saving…" : "Assign office and roles"}
        </Button>
      </div>
    </Modal>
  );
}

OnboardingDialog.propTypes = {
  cdaUrl: PropTypes.string.isRequired,
  token: PropTypes.string,
  offices: PropTypes.arrayOf(PropTypes.string).isRequired,
  initialOffice: PropTypes.string.isRequired,
  onClose: PropTypes.func.isRequired,
};
