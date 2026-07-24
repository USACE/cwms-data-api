import { useCallback, useEffect, useMemo, useState } from "react";
import { useAuth } from "@usace-watermanagement/groundwork-water";

const apiRoot = import.meta.env.VITE_CDA_API_ROOT.replace(/\/$/, "");

async function request(path, token, options = {}) {
  const response = await fetch(`${apiRoot}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
      ...options.headers,
    },
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || `${response.status} ${response.statusText}`);
  }
  return response.status === 204 ? null : response.json();
}

function values(payload) {
  return payload?.["user-lists"] ?? payload?.entries ?? payload ?? [];
}

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

  const offices = useMemo(
    () => Object.keys(profile?.roles ?? profile?.["office-roles"] ?? {}).sort(),
    [profile],
  );
  const roles = profile?.roles?.[office] ?? profile?.["office-roles"]?.[office] ?? [];
  const canWrite = roles.includes("CWMS User Admins");

  const loadLists = useCallback(
    async (targetOffice = office) => {
      if (!targetOffice) return;
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
    },
    [auth.token, office],
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
    if (!selected || !office || !auth.token) {
      setMembers([]);
      return;
    }
    request(
      `/user/list/${encodeURIComponent(selected)}/members?office=${encodeURIComponent(office)}`,
      auth.token,
    )
      .then((payload) => setMembers(payload?.members ?? payload ?? []))
      .catch((cause) => setError(cause.message));
  }, [selected, office, auth.token]);

  async function mutate(action, success) {
    setError("");
    setMessage("");
    try {
      await action();
      setMessage(success);
      await loadLists();
    } catch (cause) {
      setError(cause.message);
    }
  }

  if (!auth.isAuth) {
    return (
      <section>
        <h1>User Lists</h1>
        <p>Log in to view and manage reusable CDA user lists.</p>
        <button type="button" onClick={auth.login}>
          Log in
        </button>
      </section>
    );
  }

  return (
    <section className="user-lists-page">
      <h1>User Lists</h1>
      <p>
        Reusable lists are available to authenticated CDA users. Creating and changing a
        list requires the CWMS User Admins role for its office.
      </p>
      {error && <p role="alert">{error}</p>}
      {message && <p role="status">{message}</p>}

      <label>
        Office
        <input
          list="user-list-offices"
          value={office}
          onChange={(event) => setOffice(event.target.value.toUpperCase())}
        />
        <datalist id="user-list-offices">
          {offices.map((item) => (
            <option key={item} value={item} />
          ))}
        </datalist>
      </label>

      <div className="user-list-grid">
        <div>
          <h2>Lists</h2>
          {lists.length === 0 ? (
            <p>No user lists have been created for this office.</p>
          ) : (
            <select
              size={Math.min(10, Math.max(2, lists.length))}
              value={selected}
              onChange={(event) => setSelected(event.target.value)}
            >
              {lists.map((item) => (
                <option key={item["user-list-id"]} value={item["user-list-id"]}>
                  {item["user-list-id"]}
                </option>
              ))}
            </select>
          )}
          {canWrite && (
            <form
              onSubmit={(event) => {
                event.preventDefault();
                mutate(
                  () =>
                    request("/user/list", auth.token, {
                      method: "POST",
                      body: JSON.stringify({
                        "office-id": office,
                        "user-list-id": newList,
                        description,
                      }),
                    }),
                  `Created ${newList}.`,
                ).then(() => {
                  setSelected(newList);
                  setNewList("");
                  setDescription("");
                });
              }}
            >
              <h3>Create a list</h3>
              <input
                required
                aria-label="List name"
                placeholder="List name"
                value={newList}
                onChange={(event) => setNewList(event.target.value)}
              />
              <input
                aria-label="Description"
                placeholder="Description"
                value={description}
                onChange={(event) => setDescription(event.target.value)}
              />
              <button type="submit">Create</button>
            </form>
          )}
        </div>

        <div>
          <h2>{selected ? `${selected} members` : "Members"}</h2>
          {members.length === 0 ? (
            <p>This list has no members.</p>
          ) : (
            <ul>
              {members.map((member) => {
                const userId = member["user-id"] ?? member;
                return (
                  <li key={userId}>
                    {userId}{" "}
                    {canWrite && (
                      <button
                        type="button"
                        onClick={() =>
                          mutate(
                            () =>
                              request(
                                `/user/list/${encodeURIComponent(selected)}/members/${encodeURIComponent(userId)}?office=${encodeURIComponent(office)}`,
                                auth.token,
                                { method: "DELETE" },
                              ),
                            `Removed ${userId}.`,
                          )
                        }
                      >
                        Remove
                      </button>
                    )}
                  </li>
                );
              })}
            </ul>
          )}
          {canWrite && selected && (
            <form
              onSubmit={(event) => {
                event.preventDefault();
                mutate(
                  () =>
                    request(
                      `/user/list/${encodeURIComponent(selected)}/members?office=${encodeURIComponent(office)}`,
                      auth.token,
                      {
                        method: "POST",
                        body: JSON.stringify({ "user-id": newMember }),
                      },
                    ),
                  `Added ${newMember}.`,
                ).then(() => setNewMember(""));
              }}
            >
              <input
                required
                aria-label="CWMS user ID"
                placeholder="CWMS user ID"
                value={newMember}
                onChange={(event) => setNewMember(event.target.value)}
              />
              <button type="submit">Add member</button>
            </form>
          )}
        </div>
      </div>
    </section>
  );
}
