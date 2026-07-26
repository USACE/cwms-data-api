import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Badge,
  Button,
  Card,
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
import { FaListUl, FaPen, FaPlus, FaSearch, FaUserPlus, FaUsers } from "react-icons/fa";
import { HelpTip } from "../../components/HelpTip";

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
  const [profileLoading, setProfileLoading] = useState(true);
  const [office, setOffice] = useState("");
  const [lists, setLists] = useState([]);
  const [selected, setSelected] = useState("");
  const [members, setMembers] = useState([]);
  const [newList, setNewList] = useState("");
  const [description, setDescription] = useState("");
  const [newMember, setNewMember] = useState("");
  const [candidateSearch, setCandidateSearch] = useState("");
  const [candidates, setCandidates] = useState([]);
  const [candidateLoading, setCandidateLoading] = useState(false);
  const [listSearch, setListSearch] = useState("");
  const [memberSearch, setMemberSearch] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [listsLoading, setListsLoading] = useState(false);
  const [membersLoading, setMembersLoading] = useState(false);
  const [working, setWorking] = useState(false);
  const [createOpened, setCreateOpened] = useState(false);
  const [editOpened, setEditOpened] = useState(false);
  const [editDescription, setEditDescription] = useState("");
  const [deleteOpened, setDeleteOpened] = useState(false);

  const offices = useMemo(
    () => Object.keys(profile?.roles ?? profile?.["office-roles"] ?? {}).sort(),
    [profile],
  );
  const roles = profile?.roles?.[office] ?? profile?.["office-roles"]?.[office] ?? [];
  const canWrite = roles.includes("CWMS User Admins");
  const selectedList = lists.find((item) => item["user-list-id"] === selected);
  const filteredLists = useMemo(() => {
    const term = listSearch.trim().toLowerCase();
    if (!term) return lists;
    return lists.filter(
      (item) =>
        item["user-list-id"]?.toLowerCase().includes(term) ||
        item.description?.toLowerCase().includes(term),
    );
  }, [listSearch, lists]);
  const filteredMembers = useMemo(() => {
    const term = memberSearch.trim().toLowerCase();
    if (!term) return members;
    return members.filter((member) =>
      [member["user-id"], member["full-name"], member.email]
        .filter(Boolean)
        .some((value) => value.toLowerCase().includes(term)),
    );
  }, [memberSearch, members]);

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
      .catch((cause) => setError(cause.message))
      .finally(() => setProfileLoading(false));
  }, [auth.isAuth, auth.token]);

  useEffect(() => {
    setError("");
    loadLists().catch((cause) => setError(cause.message));
  }, [loadLists]);

  useEffect(() => {
    setError("");
    loadMembers().catch((cause) => setError(cause.message));
  }, [loadMembers]);

  useEffect(() => {
    const search = candidateSearch.trim();
    if (!auth.token || !office || search.length < 2) {
      setCandidates([]);
      setCandidateLoading(false);
      return undefined;
    }
    const controller = new AbortController();
    setError("");
    setCandidates([]);
    const timeout = window.setTimeout(async () => {
      setCandidateLoading(true);
      try {
        const parameters = new URLSearchParams({
          office,
          search,
          "page-size": "20",
        });
        const payload = await request(
          `/user/list-member-candidates?${parameters}`,
          auth.token,
          { signal: controller.signal },
        );
        setCandidates(payload?.candidates ?? payload ?? []);
      } catch (cause) {
        if (cause.name !== "AbortError") setError(cause.message);
      } finally {
        if (!controller.signal.aborted) setCandidateLoading(false);
      }
    }, 250);
    return () => {
      window.clearTimeout(timeout);
      controller.abort();
    };
  }, [auth.token, candidateSearch, office]);

  async function mutate(action, success, refresh = {}) {
    setError("");
    setMessage("");
    setWorking(true);
    try {
      const result = await action();
      if (refresh.lists !== false) await loadLists();
      if (refresh.members) await loadMembers(refresh.members);
      setMessage(success);
      return result ?? true;
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
          <div className="flex items-center gap-1">
            <H1>User Lists</H1>
            <HelpTip title="List ownership">
              CDA records the authenticated creator as the list owner for audit history.
              The owner does not change when the description or membership changes;
              office User Administrators control edits.
            </HelpTip>
          </div>
          <Text className="mt-2 max-w-3xl">
            Build reusable, office-owned lists for notifications and other CDA-aware
            applications. Membership stays in CDA so every consumer uses the same list.
          </Text>
        </div>
        {canWrite && (
          <Button type="button" onClick={() => setCreateOpened(true)}>
            <FaPlus aria-hidden="true" />
            New list
          </Button>
        )}
      </div>

      {error && <Notice kind="error">{error}</Notice>}
      {message && <Notice kind="success">{message}</Notice>}

      <Card className="mb-6 p-5">
        <div className="grid gap-5 md:grid-cols-[minmax(0,20rem)_1fr] md:items-end">
          <Field>
            <div className="flex items-center gap-1">
              <Label>Office</Label>
              <HelpTip title="Office-scoped user lists">
                Each office owns a separate collection of lists. The same list ID may
                exist in two offices without sharing members. Your office role
                determines whether you can view or edit a list.
              </HelpTip>
            </div>
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
            ) : profileLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <Text role="status">No authorized CWMS offices are available.</Text>
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
            {lists.length > 0 && (
              <div className="relative mb-3">
                <FaSearch
                  aria-hidden="true"
                  className="pointer-events-none absolute left-3 top-3 text-zinc-500"
                />
                <Input
                  aria-label="Filter user lists"
                  className="pl-9"
                  placeholder="Filter lists"
                  value={listSearch}
                  onChange={(event) => setListSearch(event.target.value)}
                />
              </div>
            )}
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
            ) : filteredLists.length === 0 ? (
              <EmptyState icon={FaSearch} title="No matching lists">
                Try a different list ID or description.
              </EmptyState>
            ) : (
              <div
                className="space-y-2"
                role="list"
                aria-label={`${office} user lists`}
              >
                {filteredLists.map((item) => {
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
                      {item["owned-by-user-id"] && (
                        <Text className="mt-2 text-xs">
                          Created by {item["owned-by-user-id"]}
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
              <div className="flex flex-wrap gap-2">
                <Button
                  type="button"
                  color="light"
                  onClick={() => {
                    setEditDescription(selectedList?.description ?? "");
                    setEditOpened(true);
                  }}
                >
                  <FaPen aria-hidden="true" />
                  Edit details
                </Button>
                <Button
                  type="button"
                  color="danger"
                  style="outline"
                  onClick={() => setDeleteOpened(true)}
                >
                  Delete list
                </Button>
              </div>
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
                    if (added) {
                      setNewMember("");
                      setCandidateSearch("");
                      setCandidates([]);
                    }
                  }}
                >
                  <Field>
                    <div className="flex items-center gap-1">
                      <Label>Add a member</Label>
                      <HelpTip title="List members">
                        Members must already have a CDA user profile. CDA stores the
                        user ID in the list and resolves the member&apos;s current name
                        and email for consumers such as notifications.
                      </HelpTip>
                    </div>
                    <Description>
                      Search by user ID, name, or email, then choose an existing CWMS
                      user.
                    </Description>
                    <div className="relative mt-3">
                      <FaSearch
                        aria-hidden="true"
                        className="pointer-events-none absolute left-3 top-3 text-zinc-500"
                      />
                      <Input
                        aria-label="Search CWMS users"
                        className="pl-9"
                        placeholder="Search users"
                        value={candidateSearch}
                        onChange={(event) => {
                          setCandidateSearch(event.target.value);
                          setNewMember("");
                        }}
                      />
                    </div>
                    {candidateLoading && (
                      <Text className="mt-2" role="status">
                        Searching…
                      </Text>
                    )}
                    {!candidateLoading && candidateSearch.trim().length >= 2 && (
                      <div
                        className="mt-2 max-h-56 overflow-y-auto rounded-lg border border-zinc-200 bg-white p-1"
                        role="listbox"
                        aria-label="Matching CWMS users"
                      >
                        {candidates.length === 0 ? (
                          <Text className="p-3">No matching users found.</Text>
                        ) : (
                          candidates.map((candidate) => {
                            const candidateId = candidate["user-id"];
                            const chosen = candidateId === newMember;
                            return (
                              <button
                                key={candidateId}
                                type="button"
                                role="option"
                                aria-selected={chosen}
                                className={`block w-full rounded-md px-3 py-2 text-left ${
                                  chosen
                                    ? "bg-blue-50 ring-1 ring-blue-600"
                                    : "hover:bg-zinc-50"
                                }`}
                                onClick={() => setNewMember(candidateId)}
                              >
                                <Strong>{candidate["full-name"] || candidateId}</Strong>
                                <Text className="text-sm">
                                  {[candidateId, candidate.email]
                                    .filter(Boolean)
                                    .join(" · ")}
                                </Text>
                              </button>
                            );
                          })
                        )}
                      </div>
                    )}
                    <div className="mt-3 flex flex-wrap items-center justify-between gap-3">
                      <Text>
                        {newMember
                          ? `Selected: ${newMember}`
                          : "Select a user from the search results."}
                      </Text>
                      <Button
                        type="submit"
                        className="whitespace-nowrap"
                        disabled={working || !newMember}
                      >
                        <FaUserPlus aria-hidden="true" />
                        Add
                      </Button>
                    </div>
                  </Field>
                </form>
              )}

              <div className="min-w-0 p-5">
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
                  <>
                    <div className="relative mb-4">
                      <FaSearch
                        aria-hidden="true"
                        className="pointer-events-none absolute left-3 top-3 text-zinc-500"
                      />
                      <Input
                        aria-label="Filter list members"
                        className="pl-9"
                        placeholder="Filter members"
                        value={memberSearch}
                        onChange={(event) => setMemberSearch(event.target.value)}
                      />
                    </div>
                    {filteredMembers.length === 0 ? (
                      <EmptyState icon={FaSearch} title="No matching members">
                        Try a different name, user ID, or email.
                      </EmptyState>
                    ) : (
                      <div className="overflow-x-auto">
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
                            {filteredMembers.map((member) => {
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
                      </div>
                    )}
                  </>
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
            <div className="flex items-center gap-1">
              <Label>List ID</Label>
              <HelpTip title="List ID rules">
                The ID is unique within the selected office and cannot be renamed after
                creation. Use up to 128 uppercase letters, numbers, periods, hyphens, or
                underscores.
              </HelpTip>
            </div>
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

      <Modal
        opened={editOpened}
        onClose={() => setEditOpened(false)}
        dialogTitle="Edit list details"
        dialogDescription={`Update the description for ${selected}. The list ID and creator do not change.`}
        size="lg"
      >
        <form
          className="space-y-5"
          onSubmit={async (event) => {
            event.preventDefault();
            const updated = await mutate(
              () =>
                request(
                  `/user/list/${encodeURIComponent(selected)}?office=${encodeURIComponent(office)}`,
                  auth.token,
                  {
                    method: "PATCH",
                    body: JSON.stringify({
                      description: editDescription.trim() || null,
                    }),
                  },
                ),
              `Updated ${selected}.`,
            );
            if (updated) setEditOpened(false);
          }}
        >
          <Field>
            <Label>Description</Label>
            <Description>
              Explain who belongs in the list and how it is used.
            </Description>
            <Textarea
              autoFocus
              rows={4}
              maxLength={1024}
              aria-label="Description"
              value={editDescription}
              onChange={(event) => setEditDescription(event.target.value)}
            />
          </Field>
          <div className="flex flex-wrap justify-end gap-3">
            <Button type="button" color="light" onClick={() => setEditOpened(false)}>
              Cancel
            </Button>
            <Button type="submit" disabled={working}>
              Save changes
            </Button>
          </div>
        </form>
      </Modal>

      <Modal
        opened={deleteOpened}
        onClose={() => setDeleteOpened(false)}
        dialogTitle="Delete user list?"
        dialogDescription={`Delete ${selected} and remove all of its memberships. This cannot be undone.`}
        size="md"
      >
        <div className="flex flex-wrap justify-end gap-3">
          <Button type="button" color="light" onClick={() => setDeleteOpened(false)}>
            Cancel
          </Button>
          <Button
            type="button"
            color="danger"
            disabled={working}
            onClick={async () => {
              const deleted = await mutate(
                () =>
                  request(
                    `/user/list/${encodeURIComponent(selected)}?office=${encodeURIComponent(office)}`,
                    auth.token,
                    { method: "DELETE" },
                  ),
                `Deleted ${selected}.`,
              );
              if (deleted) setDeleteOpened(false);
            }}
          >
            Delete list
          </Button>
        </div>
      </Modal>
    </section>
  );
}
