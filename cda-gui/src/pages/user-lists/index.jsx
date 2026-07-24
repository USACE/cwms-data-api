import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Badge,
  Button,
  Card,
  DeleteConfirm,
  Description,
  Dropdown,
  Field,
  H1,
  H2,
  H3,
  Input,
  Label,
  Modal,
  Skeleton,
  Strong,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  Text,
  Textarea,
} from "@usace/groundwork";
import { useAuth } from "@usace-watermanagement/groundwork-water";
import PropTypes from "prop-types";
import { FaListUl, FaPlus, FaUserPlus, FaUsers } from "react-icons/fa";

const apiRoot = import.meta.env.VITE_CDA_API_ROOT.replace(/\/$/, "");

async function request(path, token, options = {}) {
  const response = await fetch(`${apiRoot}${path}`, {
    ...options,
    headers: {
      Accept: "application/json",
      Authorization: `Bearer ${token}`,
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...options.headers,
    },
  });
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`.trim();
    try {
      const payload = await response.json();
      detail = payload.message ?? payload.detail ?? detail;
    } catch {
      // Keep the HTTP status when CDA does not return a JSON error body.
    }
    throw new Error(detail);
  }
  return response.status === 204 ? null : response.json();
}

function values(payload) {
  return payload?.["user-lists"] ?? payload?.entries ?? payload ?? [];
}

function Notice({ kind, children }) {
  const isError = kind === "error";
  return (
    <div
      role={isError ? "alert" : "status"}
      className={`mb-5 rounded-lg border px-4 py-3 ${
        isError
          ? "border-red-200 bg-red-50 text-red-800"
          : "border-emerald-200 bg-emerald-50 text-emerald-800"
      }`}
    >
      {isError ? (
        <Text className="text-red-800">{children}</Text>
      ) : (
        <Strong>{children}</Strong>
      )}
    </div>
  );
}

function EmptyState({ icon: Icon, title, children }) {
  return (
    <div className="flex min-h-52 flex-col items-center justify-center rounded-lg border border-dashed border-zinc-300 bg-zinc-50 px-6 py-10 text-center">
      <div className="mb-3 rounded-full bg-white p-3 text-zinc-500 shadow-sm">
        <Icon aria-hidden="true" className="h-6 w-6" />
      </div>
      <H3 className="text-base">{title}</H3>
      <Text className="mt-1 max-w-sm">{children}</Text>
    </div>
  );
}

Notice.propTypes = {
  kind: PropTypes.oneOf(["error", "success"]).isRequired,
  children: PropTypes.node.isRequired,
};

EmptyState.propTypes = {
  icon: PropTypes.elementType.isRequired,
  title: PropTypes.string.isRequired,
  children: PropTypes.node.isRequired,
};

export default function UserLists() {
  const auth = useAuth();
  const [profile, setProfile] = useState(null);
  const [office, setOffice] = useState("");
  const [lists, setLists] = useState([]);
  const [selected, setSelected] = useState("");
  const [members, setMembers] = useState([]);
  const [newList, setNewList] = useState("");
  const [description, setDescription] = useState("");
  const [newMember, setNewMember] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [listsLoading, setListsLoading] = useState(false);
  const [membersLoading, setMembersLoading] = useState(false);
  const [working, setWorking] = useState(false);
  const [createOpened, setCreateOpened] = useState(false);

  const offices = useMemo(
    () => Object.keys(profile?.roles ?? profile?.["office-roles"] ?? {}).sort(),
    [profile],
  );
  const roles = profile?.roles?.[office] ?? profile?.["office-roles"]?.[office] ?? [];
  const canWrite = roles.includes("CWMS User Admins");
  const selectedList = lists.find((item) => item["user-list-id"] === selected);

  const loadLists = useCallback(
    async (targetOffice = office) => {
      if (!targetOffice) return [];
      setListsLoading(true);
      try {
        const payload = await request(
          `/user/list?office=${encodeURIComponent(targetOffice)}`,
          auth.token,
        );
        const nextLists = values(payload);
        setLists(nextLists);
        setSelected((current) =>
          nextLists.some((item) => item["user-list-id"] === current)
            ? current
            : (nextLists[0]?.["user-list-id"] ?? ""),
        );
        return nextLists;
      } finally {
        setListsLoading(false);
      }
    },
    [auth.token, office],
  );

  const loadMembers = useCallback(
    async (userListId = selected) => {
      if (!userListId || !office || !auth.token) {
        setMembers([]);
        return [];
      }
      setMembersLoading(true);
      try {
        const payload = await request(
          `/user/list/${encodeURIComponent(userListId)}/members?office=${encodeURIComponent(office)}`,
          auth.token,
        );
        const nextMembers = payload?.members ?? payload ?? [];
        setMembers(nextMembers);
        return nextMembers;
      } finally {
        setMembersLoading(false);
      }
    },
    [auth.token, office, selected],
  );

  useEffect(() => {
    if (!auth.isAuth || !auth.token) return;
    request("/user/profile", auth.token)
      .then((nextProfile) => {
        setProfile(nextProfile);
        const nextOffice = Object.keys(
          nextProfile?.roles ?? nextProfile?.["office-roles"] ?? {},
        ).sort()[0];
        setOffice((current) => current || nextOffice || "");
      })
      .catch((cause) => setError(cause.message));
  }, [auth.isAuth, auth.token]);

  useEffect(() => {
    setError("");
    loadLists().catch((cause) => setError(cause.message));
  }, [loadLists]);

  useEffect(() => {
    setError("");
    loadMembers().catch((cause) => setError(cause.message));
  }, [loadMembers]);

  async function mutate(action, success, refresh = {}) {
    setError("");
    setMessage("");
    setWorking(true);
    try {
      const result = await action();
      if (refresh.lists !== false) await loadLists();
      if (refresh.members) await loadMembers(refresh.members);
      setMessage(success);
      return result;
    } catch (cause) {
      setError(cause.message);
      return null;
    } finally {
      setWorking(false);
    }
  }

  async function createList(event) {
    event.preventDefault();
    const listId = newList.trim().toUpperCase();
    const created = await mutate(
      () =>
        request("/user/list", auth.token, {
          method: "POST",
          body: JSON.stringify({
            "office-id": office,
            "user-list-id": listId,
            description: description.trim() || null,
          }),
        }),
      `Created ${listId}.`,
    );
    if (created) {
      setSelected(listId);
      setNewList("");
      setDescription("");
      setCreateOpened(false);
    }
  }

  if (!auth.isAuth) {
    return (
      <Card className="mx-auto my-12 max-w-2xl p-8 text-center">
        <div className="mx-auto mb-4 w-fit rounded-full bg-blue-50 p-4 text-blue-700">
          <FaUsers aria-hidden="true" className="h-8 w-8" />
        </div>
        <H1>User Lists</H1>
        <Text className="mx-auto mt-3 max-w-lg">
          Sign in to view reusable recipient lists and manage membership for your
          authorized CWMS offices.
        </Text>
        <Button className="mt-6" type="button" onClick={auth.login}>
          Log in
        </Button>
      </Card>
    );
  }

  return (
    <section className="pb-12">
      <div className="mb-6 flex flex-col gap-4 border-b border-zinc-200 pb-6 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <div className="mb-2 flex items-center gap-2">
            <Badge color="blue">CDA</Badge>
            <Badge color={canWrite ? "green" : "zinc"}>
              {canWrite ? "User list administrator" : "Read only"}
            </Badge>
          </div>
          <H1>User Lists</H1>
          <Text className="mt-2 max-w-3xl">
            Build reusable, office-owned groups for notifications and other CDA-aware
            applications. Membership stays in CDA so every consumer uses the same list.
          </Text>
        </div>
        {canWrite && (
          <Button type="button" onClick={() => setCreateOpened(true)}>
            <FaPlus aria-hidden="true" />
            Create user list
          </Button>
        )}
      </div>

      {error && <Notice kind="error">{error}</Notice>}
      {message && <Notice kind="success">{message}</Notice>}

      <Card className="mb-6 p-5">
        <div className="grid gap-5 md:grid-cols-[minmax(0,20rem)_1fr] md:items-end">
          <Field>
            <Label>Office</Label>
            <Description>
              User lists are isolated by their owning CWMS office.
            </Description>
            {offices.length > 0 ? (
              <Dropdown
                aria-label="Office"
                label="Office"
                labelClassName="sr-only"
                className="mt-0"
                value={office}
                onChange={(event) => {
                  setOffice(event.target.value);
                  setSelected("");
                  setMembers([]);
                  setMessage("");
                  setError("");
                }}
                options={offices.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              />
            ) : (
              <Skeleton className="h-10 w-full" />
            )}
          </Field>
          <div className="rounded-lg bg-blue-50 px-4 py-3">
            <Text className="text-blue-900">
              <Strong>{office || "No office selected"}</Strong>
              {canWrite
                ? " administrators can create lists and update membership."
                : " lists are available for viewing with your current role."}
            </Text>
          </div>
        </div>
      </Card>

      <div className="grid gap-6 lg:grid-cols-[minmax(18rem,0.8fr)_minmax(0,1.5fr)]">
        <Card className="p-0">
          <div className="flex items-center justify-between border-b border-zinc-200 px-5 py-4">
            <div>
              <H2 className="text-xl">Lists</H2>
              <Text>Select a list to inspect its members.</Text>
            </div>
            <Badge color="blue">{lists.length}</Badge>
          </div>

          <div className="p-3">
            {listsLoading ? (
              <div className="space-y-3 p-2">
                <Skeleton className="h-20 w-full" />
                <Skeleton className="h-20 w-full" />
              </div>
            ) : lists.length === 0 ? (
              <EmptyState icon={FaListUl} title="No user lists yet">
                {canWrite
                  ? "Create the first reusable list for this office."
                  : "Ask a CWMS User Administrator to create a list for this office."}
              </EmptyState>
            ) : (
              <div
                className="space-y-2"
                role="list"
                aria-label={`${office} user lists`}
              >
                {lists.map((item) => {
                  const listId = item["user-list-id"];
                  const active = listId === selected;
                  return (
                    <button
                      key={listId}
                      type="button"
                      role="listitem"
                      aria-current={active ? "true" : undefined}
                      className={`w-full rounded-lg border p-4 text-left transition ${
                        active
                          ? "border-blue-600 bg-blue-50 shadow-sm"
                          : "border-zinc-200 bg-white hover:border-blue-300 hover:bg-zinc-50"
                      }`}
                      onClick={() => {
                        setSelected(listId);
                        setMessage("");
                        setError("");
                      }}
                    >
                      <div className="flex items-start justify-between gap-3">
                        <Strong>{listId}</Strong>
                        {active && <Badge color="blue">Selected</Badge>}
                      </div>
                      <Text className="mt-1 line-clamp-2">
                        {item.description || "No description provided."}
                      </Text>
                      {item["owned-by-userid"] && (
                        <Text className="mt-2 text-xs">
                          Owner: {item["owned-by-userid"]}
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
          <div className="flex flex-col gap-3 border-b border-zinc-200 px-5 py-4 sm:flex-row sm:items-start sm:justify-between">
            <div>
              <div className="flex flex-wrap items-center gap-2">
                <H2 className="text-xl">{selected || "Members"}</H2>
                {selected && <Badge color="green">{members.length} members</Badge>}
              </div>
              <Text className="mt-1">
                {selectedList?.description ||
                  "Select a user list to view its current membership."}
              </Text>
            </div>
            {canWrite && selected && (
              <DeleteConfirm
                alignConfirm="left"
                onDelete={() =>
                  mutate(
                    () =>
                      request(
                        `/user/list/${encodeURIComponent(selected)}?office=${encodeURIComponent(office)}`,
                        auth.token,
                        { method: "DELETE" },
                      ),
                    `Deleted ${selected}.`,
                  )
                }
              />
            )}
          </div>

          {!selected ? (
            <div className="p-5">
              <EmptyState icon={FaUsers} title="Choose a list">
                Select a user list from the left to see names, user IDs, and email
                addresses.
              </EmptyState>
            </div>
          ) : (
            <>
              {canWrite && (
                <form
                  className="border-b border-zinc-200 bg-zinc-50 px-5 py-4"
                  onSubmit={async (event) => {
                    event.preventDefault();
                    const userId = newMember.trim().toUpperCase();
                    const added = await mutate(
                      () =>
                        request(
                          `/user/list/${encodeURIComponent(selected)}/members?office=${encodeURIComponent(office)}`,
                          auth.token,
                          {
                            method: "POST",
                            body: JSON.stringify({ "user-id": userId }),
                          },
                        ),
                      `Added ${userId} to ${selected}.`,
                      { lists: false, members: selected },
                    );
                    if (added) setNewMember("");
                  }}
                >
                  <Field>
                    <Label>Add a member</Label>
                    <Description>
                      Enter an existing CWMS user ID. CDA supplies the display name and
                      email address.
                    </Description>
                    <div className="mt-3 flex flex-col gap-2 sm:flex-row">
                      <Input
                        required
                        aria-label="CWMS user ID"
                        placeholder="CWMS user ID"
                        value={newMember}
                        onChange={(event) => setNewMember(event.target.value)}
                      />
                      <Button type="submit" disabled={working}>
                        <FaUserPlus aria-hidden="true" />
                        Add member
                      </Button>
                    </div>
                  </Field>
                </form>
              )}

              <div className="p-5">
                {membersLoading ? (
                  <div className="space-y-3">
                    <Skeleton className="h-12 w-full" />
                    <Skeleton className="h-12 w-full" />
                  </div>
                ) : members.length === 0 ? (
                  <EmptyState icon={FaUsers} title="This list is empty">
                    {canWrite
                      ? "Add a CWMS user to make this list available to notification consumers."
                      : "A list administrator has not added any members yet."}
                  </EmptyState>
                ) : (
                  <Table striped dense>
                    <TableHead>
                      <TableRow>
                        <TableHeader>User</TableHeader>
                        <TableHeader>Email</TableHeader>
                        {canWrite && (
                          <TableHeader className="text-right">Action</TableHeader>
                        )}
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {members.map((member) => {
                        const userId = member["user-id"] ?? member;
                        return (
                          <TableRow key={userId}>
                            <TableCell>
                              <Strong>{member["full-name"] || userId}</Strong>
                              {member["full-name"] && (
                                <Text className="text-xs">{userId}</Text>
                              )}
                            </TableCell>
                            <TableCell>
                              {member.email ? (
                                <a
                                  className="text-blue-700 underline hover:text-blue-900"
                                  href={`mailto:${member.email}`}
                                >
                                  {member.email}
                                </a>
                              ) : (
                                <Text>No email in CDA profile</Text>
                              )}
                            </TableCell>
                            {canWrite && (
                              <TableCell className="text-right">
                                <Button
                                  type="button"
                                  color="danger"
                                  style="outline"
                                  size="sm"
                                  disabled={working}
                                  onClick={() =>
                                    mutate(
                                      () =>
                                        request(
                                          `/user/list/${encodeURIComponent(selected)}/members/${encodeURIComponent(userId)}?office=${encodeURIComponent(office)}`,
                                          auth.token,
                                          { method: "DELETE" },
                                        ),
                                      `Removed ${userId} from ${selected}.`,
                                      { lists: false, members: selected },
                                    )
                                  }
                                >
                                  Remove
                                </Button>
                              </TableCell>
                            )}
                          </TableRow>
                        );
                      })}
                    </TableBody>
                  </Table>
                )}
              </div>
            </>
          )}
        </Card>
      </div>

      <Modal
        opened={createOpened}
        onClose={() => setCreateOpened(false)}
        dialogTitle="Create a user list"
        dialogDescription={`Create an office-owned list for ${office}.`}
        size="lg"
      >
        <form className="space-y-5" onSubmit={createList}>
          <Field>
            <Label>List ID</Label>
            <Description>
              Use a short, recognizable name such as ON-CALL-HYDROLOGISTS.
            </Description>
            <Input
              required
              autoFocus
              maxLength={128}
              aria-label="List ID"
              placeholder="ON-CALL-HYDROLOGISTS"
              value={newList}
              onChange={(event) => setNewList(event.target.value.toUpperCase())}
            />
          </Field>
          <Field>
            <Label>Description</Label>
            <Description>
              Explain who belongs in the list and how it is used.
            </Description>
            <Textarea
              rows={4}
              aria-label="Description"
              placeholder="Hydrologists who receive operational alerts."
              value={description}
              onChange={(event) => setDescription(event.target.value)}
            />
          </Field>
          <div className="flex justify-end gap-3">
            <Button type="button" color="light" onClick={() => setCreateOpened(false)}>
              Cancel
            </Button>
            <Button type="submit" disabled={working || !newList.trim()}>
              <FaPlus aria-hidden="true" />
              Create list
            </Button>
          </div>
        </form>
      </Modal>
    </section>
  );
}
