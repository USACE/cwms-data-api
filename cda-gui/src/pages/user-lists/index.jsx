import { useCallback, useEffect, useMemo, useState } from "react";
import { Button, Card, H1, Text } from "@usace/groundwork";
import { useAuth } from "@usace-watermanagement/groundwork-water";
import { useAuthConfiguration } from "../../components/auth-configuration-context";
import { FaUsers } from "react-icons/fa";
import { request, userListsFrom } from "./api";
import { OfficeSelector } from "./components/OfficeSelector";
import { Notice } from "./components/StatusMessages";
import { UserListBrowser } from "./components/UserListBrowser";
import { UserListDialogs } from "./components/UserListDialogs";
import { UserListMembers } from "./components/UserListMembers";
import { UserListsHeader } from "./components/UserListsHeader";

export default function UserLists() {
  const auth = useAuth();
  const { error: authConfigurationError } = useAuthConfiguration();
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
        const nextLists = userListsFrom(payload);
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
        const parameters = new URLSearchParams({ office, search, "page-size": "20" });
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

  async function addMember(event) {
    event.preventDefault();
    const userId = newMember.trim().toUpperCase();
    const added = await mutate(
      () =>
        request(
          `/user/list/${encodeURIComponent(selected)}/members?office=${encodeURIComponent(office)}`,
          auth.token,
          { method: "POST", body: JSON.stringify({ "user-id": userId }) },
        ),
      `Added ${userId} to ${selected}.`,
      { lists: false, members: selected },
    );
    if (added) {
      setNewMember("");
      setCandidateSearch("");
      setCandidates([]);
    }
  }

  function removeMember(userId) {
    return mutate(
      () =>
        request(
          `/user/list/${encodeURIComponent(selected)}/members/${encodeURIComponent(userId)}?office=${encodeURIComponent(office)}`,
          auth.token,
          { method: "DELETE" },
        ),
      `Removed ${userId} from ${selected}.`,
      { lists: false, members: selected },
    );
  }

  async function editList(event) {
    event.preventDefault();
    const updated = await mutate(
      () =>
        request(
          `/user/list/${encodeURIComponent(selected)}?office=${encodeURIComponent(office)}`,
          auth.token,
          {
            method: "PATCH",
            body: JSON.stringify({ description: editDescription.trim() || null }),
          },
        ),
      `Updated ${selected}.`,
    );
    if (updated) setEditOpened(false);
  }

  async function deleteList() {
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
  }

  function changeOffice(nextOffice) {
    setOffice(nextOffice);
    setSelected("");
    setMembers([]);
    setMessage("");
    setError("");
  }

  function selectList(listId) {
    setSelected(listId);
    setMessage("");
    setError("");
  }

  if (!auth.isAuth) {
    return (
      <Card className="mx-auto my-12 max-w-2xl p-8 text-center">
        <div className="mx-auto mb-4 w-fit rounded-full bg-blue-50 p-4 text-blue-700">
          <FaUsers aria-hidden="true" className="h-8 w-8" />
        </div>
        <H1>User Lists</H1>
        <Text className="mx-auto mt-3 max-w-lg">
          {authConfigurationError ??
            "Sign in to view reusable recipient lists and manage membership for your authorized CWMS offices."}
        </Text>
        {!authConfigurationError && (
          <Button className="mt-6" type="button" onClick={auth.login}>
            Log in
          </Button>
        )}
      </Card>
    );
  }

  return (
    <section className="pb-12">
      <UserListsHeader canWrite={canWrite} onCreate={() => setCreateOpened(true)} />
      {error && <Notice kind="error">{error}</Notice>}
      {message && <Notice kind="success">{message}</Notice>}
      <OfficeSelector
        offices={offices}
        office={office}
        canWrite={canWrite}
        loading={profileLoading}
        onChange={changeOffice}
      />
      <div className="grid gap-6 lg:grid-cols-[minmax(18rem,0.8fr)_minmax(0,1.5fr)]">
        <UserListBrowser
          lists={lists}
          filteredLists={filteredLists}
          office={office}
          selected={selected}
          canWrite={canWrite}
          loading={listsLoading}
          search={listSearch}
          onSearch={setListSearch}
          onSelect={selectList}
        />
        <UserListMembers
          selected={selected}
          selectedList={selectedList}
          members={members}
          filteredMembers={filteredMembers}
          canWrite={canWrite}
          working={working}
          loading={membersLoading}
          candidateSearch={candidateSearch}
          candidates={candidates}
          candidateLoading={candidateLoading}
          newMember={newMember}
          memberSearch={memberSearch}
          onEdit={() => {
            setEditDescription(selectedList?.description ?? "");
            setEditOpened(true);
          }}
          onDelete={() => setDeleteOpened(true)}
          onCandidateSearch={(value) => {
            setCandidateSearch(value);
            setNewMember("");
          }}
          onCandidateSelect={setNewMember}
          onAddMember={addMember}
          onMemberSearch={setMemberSearch}
          onRemoveMember={removeMember}
        />
      </div>
      <UserListDialogs
        office={office}
        selected={selected}
        working={working}
        createOpened={createOpened}
        newList={newList}
        description={description}
        editOpened={editOpened}
        editDescription={editDescription}
        deleteOpened={deleteOpened}
        onCreateClose={() => setCreateOpened(false)}
        onCreate={createList}
        onNewListChange={setNewList}
        onDescriptionChange={setDescription}
        onEditClose={() => setEditOpened(false)}
        onEdit={editList}
        onEditDescriptionChange={setEditDescription}
        onDeleteClose={() => setDeleteOpened(false)}
        onDelete={deleteList}
      />
    </section>
  );
}
