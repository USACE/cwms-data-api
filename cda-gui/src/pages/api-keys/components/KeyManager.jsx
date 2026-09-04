import { useEffect, useMemo, useRef, useState } from "react";
import PropTypes from "prop-types";
import { useSearchParams } from "react-router-dom";
import { useAuth } from "@usace-watermanagement/groundwork-water";
import { useQueryClient } from "@tanstack/react-query";
import { Button } from "@usace/groundwork";
import { Notice } from "../../user-lists/components/StatusMessages";
import { createApiKeyClient, keyError } from "../api";
import "../api-keys.css";
import KeyHeader from "./KeyHeader";
import OfficeContext from "./OfficeContext";
import KeyList from "./KeyList";
import KeyDetails from "./KeyDetails";
import CreateKeyDialog from "./CreateKeyDialog";
import SaveKeyDialog from "./SaveKeyDialog";
import RevokeKeyDialog from "./RevokeKeyDialog";
const cdaUrl = import.meta.env.VITE_CDA_API_ROOT;
export default function KeyManager({ token }) {
  const [params] = useSearchParams();
  const { profile } = useAuth();
  const queryClient = useQueryClient();
  const api = useMemo(() => createApiKeyClient(cdaUrl, token), [token]);
  const controller = useRef(null);
  const [keys, setKeys] = useState([]);
  const [loading, setLoading] = useState(true);
  const [working, setWorking] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [officeChoice, setOfficeChoice] = useState(() => params.get("office") ?? "");
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState(null);
  const [createOpen, setCreateOpen] = useState(false);
  const [revokeOpen, setRevokeOpen] = useState(false);
  const [created, setCreated] = useState(null);
  const [rotationSource, setRotationSource] = useState(null);
  const [now, setNow] = useState(Date.now);
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
    const timer = window.setInterval(() => setNow(Date.now()), 30000);
    return () => window.clearInterval(timer);
  }, []);

  function changeCreateOpen(open) {
    setRotationSource(null);
    setCreateOpen(open);
  }

  function rotate() {
    if (!selected || working) return;
    const base = `${selected["key-name"]}-replacement`;
    let candidate = base;
    let suffix = 2;
    while (keys.some((key) => key["key-name"] === candidate))
      candidate = `${base}-${suffix++}`;
    setName(candidate);
    setExpires(new Date(Date.now() + 90 * 86400000).toISOString().slice(0, 10));
    setRotationSource(selected);
    setError("");
    setMessage("");
    setCreateOpen(true);
  }

  function saved() {
    const canFinish = rotationSource && created?.["api-key"];
    setCreated(null);
    setMessage("");
    if (canFinish) setRevokeOpen(true);
    else setRotationSource(null);
  }

  function changeRevokeOpen(open) {
    setRevokeOpen(open);
    if (!open && rotationSource) {
      setMessage(
        `Replacement created. The old key ${rotationSource["key-name"]} has not been revoked. Revoke it after updating your application.`,
      );
      setRotationSource(null);
    }
  }

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
    const target = rotationSource ?? selected;
    if (!target || working) return;
    setWorking(true);
    setError("");
    setMessage("");
    try {
      await api.revoke(target["key-name"], controller.current.signal);
      setKeys((current) =>
        current.filter((key) => key["key-name"] !== target["key-name"]),
      );
      setMessage(
        rotationSource
          ? `Rotation complete. Revoked ${target["key-name"]}; use the replacement key in your application.`
          : `Revoked ${target["key-name"]}. Applications using it must switch to another key.`,
      );
      if (!rotationSource) setSelected(null);
      setRotationSource(null);
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
      <KeyHeader
        {...{ profile, working, loading, office, setError }}
        setCreateOpen={changeCreateOpen}
      />
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

      <OfficeContext {...{ profile, office, offices, setOfficeChoice }} />
      <div className="grid items-start gap-6 lg:grid-cols-[minmax(19rem,0.8fr)_minmax(0,1.4fr)]">
        <KeyList
          {...{
            keys,
            loading,
            working,
            refresh,
            search,
            setSearch,
            visibleKeys,
            selected,
            view,
            now,
          }}
        />
        <KeyDetails
          {...{ selected, working, setError, rotate, now }}
          setRevokeOpen={changeRevokeOpen}
        />
      </div>
      <CreateKeyDialog
        {...{
          createOpen,
          working,
          create,
          error,
          profile,
          name,
          setName,
          expires,
          setExpires,
          keys,
          rotationSource,
        }}
        setCreateOpen={changeCreateOpen}
      />
      <SaveKeyDialog
        {...{ created, copySecret, message, rotationSource }}
        onSaved={saved}
      />
      <RevokeKeyDialog
        {...{ revokeOpen, working, error, revoke }}
        selected={rotationSource ?? selected}
        rotation={Boolean(rotationSource)}
        setRevokeOpen={changeRevokeOpen}
      />
    </section>
  );
}
KeyManager.propTypes = { token: PropTypes.string.isRequired };
