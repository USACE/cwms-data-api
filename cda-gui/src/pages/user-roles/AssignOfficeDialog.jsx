import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Badge, Button, Input, Modal, Strong, Text } from "@usace/groundwork";
import {
  OfficeDropdown,
  useCdaUsers,
  useUpdateCdaUserRoles,
} from "@usace-watermanagement/groundwork-water";
import PropTypes from "prop-types";
import { Notice } from "../user-lists/components/StatusMessages";

export function AssignOfficeDialog({
  cdaUrl,
  token,
  adminOffices,
  initialOffice,
  onClose,
  onAssigned,
}) {
  const [search, setSearch] = useState("");
  const [submittedSearch, setSubmittedSearch] = useState("");
  const [selectedName, setSelectedName] = useState("");
  const [office, setOffice] = useState(initialOffice);
  const [error, setError] = useState("");
  const queryClient = useQueryClient();
  const usersQuery = useCdaUsers({
    cdaUrl,
    token,
    usernameLike: submittedSearch.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"),
    queryOptions: { enabled: Boolean(submittedSearch) },
  });
  const updateRoles = useUpdateCdaUserRoles({ cdaUrl, token });
  const users = usersQuery.data?.users ?? [];
  const selectedUser = users.find((user) => user["user-name"] === selectedName);
  const assignedOffices = Object.entries(selectedUser?.roles ?? {})
    .filter(([, roles]) => roles.length > 0)
    .sort(([left], [right]) => left.localeCompare(right));
  const alreadyAssigned = assignedOffices.some(([id]) => id === office);
  const working = updateRoles.isPending;

  async function assignOffice() {
    if (!selectedUser || !adminOffices.includes(office) || alreadyAssigned || working)
      return;
    setError("");
    try {
      await updateRoles.mutateAsync({
        userName: selectedName,
        office,
        previousRoles: [],
        roles: ["All Users"],
      });
      await queryClient.invalidateQueries({ queryKey: ["cda", "users", cdaUrl] });
      await onAssigned(selectedName, office);
    } catch (cause) {
      setError(cause?.message ?? "Unable to assign this office.");
    }
  }

  return (
    <Modal
      opened
      onClose={() => !working && onClose()}
      dialogTitle="Assign office"
      dialogDescription="Find a registered user across all offices, then add an office assignment."
      size="2xl"
    >
      <form
        className="flex items-end gap-3"
        onSubmit={(event) => {
          event.preventDefault();
          setSubmittedSearch(search.trim());
          setSelectedName("");
          setError("");
        }}
      >
        <div className="min-w-0 flex-1">
          <label htmlFor="office-user-search">
            <Strong>Username</Strong>
          </label>
          <Input
            id="office-user-search"
            autoFocus
            placeholder="Enter all or part of a username"
            value={search}
            disabled={working}
            onChange={(event) => setSearch(event.target.value)}
            className="mt-2"
          />
        </div>
        <Button
          type="submit"
          disabled={!search.trim() || working || usersQuery.isFetching}
        >
          Search
        </Button>
      </form>
      {usersQuery.error && <Notice kind="error">{usersQuery.error.message}</Notice>}
      {error && <Notice kind="error">{error}</Notice>}
      {usersQuery.isFetching ? (
        <Text className="mt-4" role="status">
          Searching users…
        </Text>
      ) : (
        submittedSearch && (
          <div className="mt-4">
            <Text className="mb-2">
              {users.length} matching {users.length === 1 ? "user" : "users"}
            </Text>
            {users.length === 0 ? (
              <Text>
                No matching user. New staff must sign in to CDA first so their account
                is registered.
              </Text>
            ) : (
              <div
                className="max-h-44 space-y-2 overflow-y-auto"
                aria-label="Matching users"
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
                      setError("");
                    }}
                  >
                    <Strong>{user["user-name"]}</Strong>
                    <Text className="text-sm">{user.email || user.principal}</Text>
                  </button>
                ))}
              </div>
            )}
          </div>
        )
      )}
      {selectedUser && (
        <div className="mt-5 space-y-4">
          <div>
            <Strong>Assigned offices</Strong>
            {assignedOffices.length ? (
              <div className="mt-2 space-y-2">
                {assignedOffices.map(([id, roles]) => (
                  <div
                    key={id}
                    className="flex items-start gap-3 rounded-lg bg-zinc-50 p-3"
                  >
                    <Badge color="blue">{id}</Badge>
                    <Text className="text-sm">{roles.join(" · ")}</Text>
                  </div>
                ))}
              </div>
            ) : (
              <Text className="mt-2">No office roles assigned.</Text>
            )}
          </div>
          <div>
            <Strong>Add office</Strong>
            <div className="mt-2">
              <OfficeDropdown
                cdaUrl={cdaUrl}
                includeOffices={adminOffices}
                value={office}
                disabled={working}
                onChange={setOffice}
                initOverrides={{
                  headers: token ? { Authorization: `Bearer ${token}` } : undefined,
                }}
              />
            </div>
          </div>
          <Text className="rounded-lg border border-blue-100 bg-blue-50 p-3 text-sm">
            {alreadyAssigned
              ? `This user is already assigned to ${office}.`
              : "Adds the required All Users role in this office. Then choose read-only, read/write, or custom roles on the role page. Existing office roles are kept."}
          </Text>
        </div>
      )}
      <div className="mt-6 flex justify-end gap-3 border-t border-zinc-200 pt-4">
        <Button type="button" color="light" disabled={working} onClick={onClose}>
          Cancel
        </Button>
        <Button
          type="button"
          disabled={
            !selectedUser ||
            !office ||
            alreadyAssigned ||
            working ||
            usersQuery.isFetching
          }
          onClick={assignOffice}
        >
          {working ? "Assigning…" : "Assign office"}
        </Button>
      </div>
    </Modal>
  );
}

AssignOfficeDialog.propTypes = {
  cdaUrl: PropTypes.string.isRequired,
  token: PropTypes.string,
  adminOffices: PropTypes.arrayOf(PropTypes.string).isRequired,
  initialOffice: PropTypes.string.isRequired,
  onClose: PropTypes.func.isRequired,
  onAssigned: PropTypes.func.isRequired,
};
