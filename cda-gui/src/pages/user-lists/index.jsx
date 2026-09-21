import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSessionAuth } from "../../components/use-session-auth";
import { request, userListsFrom } from "./api";
import { OfficeSelector } from "./components/OfficeSelector";
import { Notice } from "./components/StatusMessages";
import { UserListBrowser } from "./components/UserListBrowser";
import { UserListDialogs } from "./components/UserListDialogs";
import { UserListMembers } from "./components/UserListMembers";
import { UserListsHeader } from "./components/UserListsHeader";

export default function UserLists() {
  const auth = useSessionAuth();
  const tokenRef = useRef(auth.token);
  useEffect(() => {
    tokenRef.current = auth.token;
  }, [auth.token]);
  const profile = auth.profile;
  const profileLoading = auth.isLoading && !profile;
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
          tokenRef.current,
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
    [office],
  );

  const loadMembers = useCallback(
    async (userListId = selected) => {
      if (!userListId || !office || !tokenRef.current) {
        setMembers([]);
        return [];
      }
      setMembersLoading(true);
      try {
        const payload = await request(
          `/user/list/${encodeURIComponent(userListId)}/members?office=${encodeURIComponent(office)}`,
          tokenRef.current,
        );
        const nextMembers = payload?.members ?? payload ?? [];
        setMembers(nextMembers);
        return nextMembers;
      } finally {
        setMembersLoading(false);
      }
    },
    [office, selected],
  );

  useEffect(() => {
    setOffice((current) => (offices.includes(current) ? current : (offices[0] ?? "")));
  }, [offices]);

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
    if (!tokenRef.current || !office || search.length < 2) {
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
          tokenRef.current,
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
  }, [candidateSearch, office]);

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
        request("/user/list", tokenRef.current, {
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
          tokenRef.current,
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
          tokenRef.current,
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
          tokenRef.current,
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
          tokenRef.current,
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
