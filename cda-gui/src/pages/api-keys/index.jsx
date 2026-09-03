import { useEffect, useMemo, useRef, useState } from "react";
import PropTypes from "prop-types";
import { Navigate } from "react-router-dom";
import { useAuth } from "@usace-watermanagement/groundwork-water";
import { useQueryClient } from "@tanstack/react-query";
import {
  Badge,
  Button,
  Card,
  Description,
  Field,
  H1,
  H2,
  Input,
  Label,
  Modal,
  Dropdown,
  Skeleton,
  Strong,
  Text,
} from "@usace/groundwork";
import { FaKey } from "react-icons/fa";
import { EmptyState, Notice } from "../user-lists/components/StatusMessages";
import { createApiKeyClient, keyDate, keyError, keyStatus } from "./api";
import "./api-keys.css";

const cdaUrl = import.meta.env.VITE_CDA_API_ROOT;
const formatDate = (value) =>
  keyDate(value)?.toLocaleString() ?? (value ? "Unknown" : "None");

export default function ApiKeys() {
  const auth = useAuth();
  if (auth.isLoading) return <Skeleton className="my-8 h-40 w-full" />;
  if (!auth.isAuth) return <Navigate to="/" replace />;
  // Remount on a session change so key material and outstanding work cannot leak
  // into a different login. Secrets stay only in this component's memory.
  return <KeyManager key={auth.token} token={auth.token} />;
}

function KeyManager({ token }) {
  const { profile } = useAuth();
  const queryClient = useQueryClient();
  const api = useMemo(() => createApiKeyClient(cdaUrl, token), [token]);
  const controller = useRef(null);
  const [keys, setKeys] = useState([]);
  const [loading, setLoading] = useState(true);
  const [working, setWorking] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [officeChoice, setOfficeChoice] = useState("");
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState(null);
  const [createOpen, setCreateOpen] = useState(false);
  const [revokeOpen, setRevokeOpen] = useState(false);
  const [helpOpen, setHelpOpen] = useState(false);
  const [created, setCreated] = useState(null);
  const [name, setName] = useState("");
  const [expires, setExpires] = useState(() =>
    new Date(Date.now() + 90 * 86400000).toISOString().slice(0, 10),
  );
  const offices = Object.keys(profile?.roles ?? {}).sort();
  const office = offices.includes(officeChoice) ? officeChoice : (offices[0] ?? "");
  const visibleKeys = keys.filter((key) =>
    key["key-name"].toLowerCase().includes(search.toLowerCase()),
  );

  useEffect(() => {
    const current = new AbortController();
    controller.current = current;
    api
      .list(current.signal)
      .then(setKeys)
      .catch(async (cause) => {
        if (!current.signal.aborted) setError(await keyError(cause));
      })
      .finally(() => {
        if (!current.signal.aborted) setLoading(false);
      });
    return () => current.abort();
  }, [api]);

  async function refresh() {
    setLoading(true);
    setError("");
    try {
      setKeys(await api.list(controller.current.signal));
    } catch (cause) {
      setError(await keyError(cause));
    } finally {
      setLoading(false);
    }
  }

  async function view(keyName) {
    setWorking(true);
    setError("");
    try {
      setSelected(await api.get(keyName, controller.current.signal));
    } catch (cause) {
      setError(await keyError(cause));
    } finally {
      setWorking(false);
    }
  }

  async function create(event) {
    event.preventDefault();
    if (!name.trim() || !profile?.userName || working) return;
    if (expires && new Date(`${expires}T00:00:00Z`).getTime() <= Date.now()) {
      setError("Choose an expiration date after today.");
      return;
    }
    setWorking(true);
    setError("");
    setMessage("");
    try {
      const result = await api.create(
        profile.userName,
        name.trim(),
        expires ? `${expires}T00:00:00Z` : null,
        controller.current.signal,
      );
      if (controller.current.signal.aborted) return;
      setCreated(result);
      // Keep only metadata in the list and detail view, never the secret.
      const metadata = { ...result, "api-key": undefined };
      setKeys((current) => [metadata, ...current]);
      setSelected(metadata);
      setCreateOpen(false);
      setName("");
    } catch (cause) {
      setError(await keyError(cause));
    } finally {
      setWorking(false);
    }
  }

  async function revoke() {
    if (!selected || working) return;
    setWorking(true);
    setError("");
    setMessage("");
    try {
      await api.revoke(selected["key-name"], controller.current.signal);
      setKeys((current) =>
        current.filter((key) => key["key-name"] !== selected["key-name"]),
      );
      setMessage(
        `Revoked ${selected["key-name"]}. Applications using it must switch to another key.`,
      );
      setSelected(null);
      setRevokeOpen(false);
    } catch (cause) {
      setError(await keyError(cause));
    } finally {
      setWorking(false);
    }
  }

  async function copySecret() {
    try {
      await navigator.clipboard.writeText(created["api-key"]);
      setMessage(
        "Key copied. Save it in a secure secret store, then clear your clipboard.",
      );
    } catch {
      setMessage("Clipboard access is unavailable. Select and copy the key manually.");
    }
  }

  return (
    <section className="pb-12">
      <div className="mb-6 border-b border-zinc-200 pb-6">
        <div className="mb-2 flex flex-wrap gap-2">
          <Badge color="blue">CDA</Badge>
          <Badge color="green">Personal keys</Badge>
        </div>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <H1>API Keys</H1>
          <div className="flex flex-wrap gap-2">
            <Button type="button" color="light" onClick={() => setHelpOpen(true)}>
              How to use keys
            </Button>
            <Button
              type="button"
              disabled={!profile?.userName || working || loading}
              onClick={() => {
                setError("");
                setCreateOpen(true);
              }}
            >
              <FaKey aria-hidden="true" />
              Create key
            </Button>
          </div>
        </div>
        <Text className="mt-2 max-w-3xl">
          Create, view, and revoke your API keys for scripts and applications that
          access CWMS data.
        </Text>
        <Text className="mt-2 max-w-3xl">
          Keys belong to your user account and use your existing office permissions.
          They are not shared office credentials or restricted to the office selected
          below. You can manage only your own keys.
        </Text>
      </div>
      {error && !createOpen && !revokeOpen && <Notice kind="error">{error}</Notice>}
      {!profile && (
        <Notice kind="error">
          Waiting for your CWMS profile. If it does not load, retry or sign in again.{" "}
          <Button
            type="button"
            color="light"
            onClick={() =>
              queryClient.invalidateQueries({ queryKey: ["auth", "profile"] })
            }
          >
            Retry profile
          </Button>
        </Notice>
      )}
      {message && !created && <Notice kind="success">{message}</Notice>}
      <Card className="mb-6 p-5">
        <div className="grid gap-5 md:grid-cols-2">
          <div>
            <Strong>Signed in as</Strong>
            <Text>{profile?.userName ?? "Loading profile…"}</Text>
          </div>
          <Field>
            <Label>Office context</Label>
            <Dropdown
              label="Office context"
              labelClassName="sr-only"
              aria-label="Office context"
              value={office}
              onChange={(event) => setOfficeChoice(event.target.value)}
              disabled={!offices.length}
              options={
                offices.length
                  ? offices.map((id) => (
                      <option key={id} value={id}>
                        {id}
                      </option>
                    ))
                  : [
                      <option key="none" value="">
                        No office roles available
                      </option>,
                    ]
              }
            />
            <Description>
              Sets the example office; your key list stays the same.
            </Description>
          </Field>
        </div>
        {office && (
          <Text className="mt-3">
            Your roles in {office}: {(profile.roles[office] ?? []).join(", ") || "None"}
            .
          </Text>
        )}
      </Card>
      <div className="grid items-start gap-6 lg:grid-cols-[minmax(19rem,0.8fr)_minmax(0,1.4fr)]">
        <Card className="p-5">
          <div className="mb-4 flex flex-wrap items-center justify-between gap-2">
            <H2 className="text-xl">
              Your keys <Badge color="blue">{keys.length}</Badge>
            </H2>
            <Button
              type="button"
              color="light"
              disabled={loading || working}
              onClick={refresh}
            >
              Refresh
            </Button>
          </div>
          <Input
            aria-label="Filter keys"
            placeholder="Filter by key name"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
          />
          {loading ? (
            <Skeleton className="mt-4 h-32 w-full" />
          ) : visibleKeys.length === 0 ? (
            <EmptyState
              icon={FaKey}
              title={keys.length ? "No matching keys" : "No keys to display"}
            >
              Create a key to connect a script or application.
            </EmptyState>
          ) : (
            <ul className="mt-4 max-h-[32rem] space-y-2 overflow-y-auto">
              {visibleKeys.map((key) => (
                <li key={key["key-name"]}>
                  <button
                    type="button"
                    disabled={working}
                    aria-pressed={selected?.["key-name"] === key["key-name"]}
                    onClick={() => view(key["key-name"])}
                    className={`w-full rounded-lg border p-4 text-left focus-visible:ring-2 focus-visible:ring-blue-600 ${selected?.["key-name"] === key["key-name"] ? "border-blue-500 bg-blue-50" : "border-zinc-200 hover:bg-zinc-50"}`}
                  >
                    <span className="block break-all font-semibold">
                      {key["key-name"]}
                    </span>
                    <span className="text-sm text-zinc-600">{keyStatus(key)}</span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </Card>
        <Card className="min-w-0 p-5">
          <H2 className="mb-4 break-all text-xl">
            {selected?.["key-name"] ?? "Key details"}
          </H2>
          {selected ? (
            <>
              <dl className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-5 gap-y-3 text-sm">
                <dt>Owner</dt>
                <dd className="break-all">{selected["user-id"]}</dd>
                <dt>Status</dt>
                <dd>{keyStatus(selected)}</dd>
                <dt>Created (local)</dt>
                <dd>{formatDate(selected.created)}</dd>
                <dt>Expires (local)</dt>
                <dd>{formatDate(selected.expires)}</dd>
              </dl>
              <Text className="mt-5">
                The secret is shown only when the key is created. If you lose it, create
                a replacement and revoke the old key.
              </Text>
              <Button
                className="mt-5"
                type="button"
                color="danger"
                disabled={working}
                onClick={() => {
                  setError("");
                  setRevokeOpen(true);
                }}
              >
                Revoke key
              </Button>
            </>
          ) : (
            <EmptyState icon={FaKey} title="Choose a key">
              Select a key to view its owner, creation date, and expiration or to revoke
              it.
            </EmptyState>
          )}
        </Card>
      </div>
      <Modal
        opened={helpOpen}
        className="api-key-dialog"
        onClose={() => setHelpOpen(false)}
        dialogTitle="How to use API keys"
        autoFocus
        size="lg"
      >
        <div className="space-y-4 outline-none" tabIndex={-1} data-autofocus>
          <Text>
            Send the key in the Authorization header with the prefix{" "}
            <code>apikey </code> (including the space). Set CWMS_API_KEY in your
            application&apos;s secure environment; never put it in a URL, source code,
            or a public browser application.
          </Text>
          <pre className="overflow-x-auto rounded bg-zinc-100 p-4 text-sm">
            <code>
              {[
                'curl --header "Authorization: apikey $CWMS_API_KEY"',
                '  --header "Accept: application/json;version=2"',
                `  "${new URL(`${cdaUrl.replace(/\/$/, "")}/timeseries`, window.location.origin).href}?office=${encodeURIComponent(office || "YOUR_OFFICE")}&name=YOUR_TIMESERIES"`,
              ].join(" \\\n")}
            </code>
          </pre>
          <Text>
            This shell example reads a time series. Replace YOUR_TIMESERIES with a valid
            ID. Choose the office required by each endpoint. Selecting an office here
            does not limit a key&apos;s permissions.
          </Text>
          <Text>
            Keys act as you and do not grant additional access. Do not share them with
            coworkers. Use a separate named key for each application so you can revoke
            one without disrupting others.
          </Text>
          <Text>
            Save the secret when it is generated; CDA cannot show it again. Keys expire
            at the chosen date or remain valid until revoked if no expiration is set.
            Rotate keys before expiration. Revocation cannot be undone and applications
            using the old key will lose access.
          </Text>
          <Text>
            You must sign in interactively to manage keys. An API key cannot create,
            list, or revoke keys. A 401 response can mean an invalid or expired
            credential; a 403 response means access was denied. Check the key, office,
            and your user permissions.
          </Text>
          <Button type="button" onClick={() => setHelpOpen(false)}>
            Close help
          </Button>
        </div>
      </Modal>
      <Modal
        opened={createOpen}
        className="api-key-dialog"
        onClose={() => {
          if (!working) setCreateOpen(false);
        }}
        dialogTitle="Create API key"
        size="lg"
      >
        <form className="space-y-5" onSubmit={create}>
          {error && <Notice kind="error">{error}</Notice>}
          <Text>
            This key belongs to {profile?.userName} and uses your permissions across
            offices. The generated secret will be shown once.
          </Text>
          <Field>
            <Label>Key name</Label>
            <Input
              autoFocus
              required
              aria-label="Key name"
              value={name}
              onChange={(event) => setName(event.target.value)}
            />
            <Description>
              Use a unique, case-sensitive name for this application, such as
              daily-report.
            </Description>
          </Field>
          <Field>
            <Label>Expiration date (UTC)</Label>
            <Input
              type="date"
              aria-label="Expiration date (UTC)"
              value={expires}
              onChange={(event) => setExpires(event.target.value)}
              min={new Date(Date.now() + 86400000).toISOString().slice(0, 10)}
            />
            <Description>
              Expires at the start of this date in UTC. Clear the date for no
              expiration.
            </Description>
          </Field>
          <div className="flex justify-end gap-3">
            <Button
              type="button"
              color="light"
              disabled={working}
              onClick={() => setCreateOpen(false)}
            >
              Cancel
            </Button>
            <Button
              type="submit"
              disabled={
                working ||
                !name.trim() ||
                keys.some((key) => key["key-name"] === name.trim())
              }
            >
              {working ? "Creating…" : "Generate key"}
            </Button>
          </div>
          {keys.some((key) => key["key-name"] === name.trim()) && (
            <Text>A key with this name already exists. Choose another name.</Text>
          )}
        </form>
      </Modal>
      <Modal
        opened={Boolean(created)}
        className="api-key-dialog"
        onClose={() => {}}
        dialogTitle="Save your new API key"
        size="lg"
      >
        <div className="space-y-4">
          <Text>
            Copy this secret now and save it securely. You cannot retrieve it after
            closing this dialog.
          </Text>
          {created?.["api-key"] ? (
            <>
              <Input
                aria-label="Generated API key"
                readOnly
                value={created["api-key"]}
                autoComplete="off"
                spellCheck={false}
              />
              <Button type="button" color="light" onClick={copySecret}>
                Copy key
              </Button>
            </>
          ) : (
            <Notice kind="error">
              CDA did not return a secret. Revoke this key and create a replacement.
            </Notice>
          )}
          {message && <Text role="status">{message}</Text>}
          <Button
            type="button"
            onClick={() => {
              setCreated(null);
              setMessage("");
            }}
          >
            I have saved the key — close
          </Button>
        </div>
      </Modal>
      <Modal
        opened={revokeOpen}
        className="api-key-dialog"
        onClose={() => {
          if (!working) setRevokeOpen(false);
        }}
        dialogTitle="Revoke API key?"
        size="md"
      >
        {error && <Notice kind="error">{error}</Notice>}
        <Text>
          Revoke {selected?.["key-name"]}? Applications using this key will lose access.
          This cannot be undone.
        </Text>
        <div className="mt-5 flex justify-end gap-3">
          <Button
            type="button"
            color="light"
            disabled={working}
            onClick={() => setRevokeOpen(false)}
          >
            Cancel
          </Button>
          <Button type="button" color="danger" disabled={working} onClick={revoke}>
            {working ? "Revoking…" : "Confirm revoke"}
          </Button>
        </div>
      </Modal>
    </section>
  );
}

KeyManager.propTypes = { token: PropTypes.string.isRequired };
