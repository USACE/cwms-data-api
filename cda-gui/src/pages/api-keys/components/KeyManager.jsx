import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import PropTypes from "prop-types";
import { useSearchParams } from "react-router-dom";
import { useAuth } from "@usace-watermanagement/groundwork-water";
import { useQueryClient } from "@tanstack/react-query";
import { Button } from "@usace/groundwork";
import { Notice } from "../../user-lists/components/StatusMessages";
import { createApiKeyClient, keyDate, keyError, keyStatus } from "../api";
import "../api-keys.css";
import KeyHeader from "./KeyHeader";
import OfficeContext from "./OfficeContext";
import KeyList from "./KeyList";
import KeyDetails from "./KeyDetails";
import CreateKeyDialog from "./CreateKeyDialog";
import SaveKeyDialog from "./SaveKeyDialog";
import RevokeKeyDialog from "./RevokeKeyDialog";
import KeyFeedback from "./KeyFeedback";
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
  const [notification, setNotification] = useState(null);
  const notificationId = useRef(0);
  const notify = useCallback((kind, message) => {
    setNotification({ id: ++notificationId.current, kind, message });
  }, []);
  const dismiss = useCallback(() => setNotification(null), []);
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
    setError("");
    dismiss();
    setRotationSource(null);
    setCreateOpen(open);
  }

  function rotate() {
    if (!selected || working) return;
    let candidate;
    let suffix = 1;
    do {
      const ending = `-replacement${suffix > 1 ? `-${suffix}` : ""}`;
      candidate = `${selected["key-name"].slice(0, 64 - ending.length)}${ending}`;
      suffix++;
    } while (keys.some((key) => key["key-name"] === candidate));
    setName(candidate);
    setExpires(new Date(Date.now() + 90 * 86400000).toISOString().slice(0, 10));
    setRotationSource(selected);
    setError("");
    dismiss();
    setCreateOpen(true);
  }

  function saved() {
    const canFinish = rotationSource && created?.["api-key"];
    setCreated(null);
    dismiss();
    if (canFinish) setRevokeOpen(true);
    else setRotationSource(null);
  }

  function changeRevokeOpen(open) {
    setRevokeOpen(open);
    if (!open && rotationSource) {
      notify(
        "warning",
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
        const message = await keyError(cause);
        if (!current.signal.aborted) {
          setError(message);
          notify("error", message);
        }
      })
      .finally(() => {
        if (!current.signal.aborted) setLoading(false);
      });
    return () => current.abort();
  }, [api, notify]);

  async function showError(cause, signal) {
    const message = await keyError(cause);
    if (!signal.aborted) {
      setError(message);
      notify("error", message);
    }
  }

  async function refresh() {
    const signal = controller.current.signal;
    setLoading(true);
    setError("");
    try {
      const current = await api.list(signal);
      if (signal.aborted) return;
      setKeys(current);
      setSelected(
        (previous) =>
          current.find((key) => key["key-name"] === previous?.["key-name"]) ?? null,
      );
      notify("success", "Your keys are up to date.");
    } catch (cause) {
      await showError(cause, signal);
    } finally {
      setLoading(false);
    }
  }

  async function view(keyName) {
    const signal = controller.current.signal;
    setWorking(true);
    setError("");
    try {
      const key = await api.get(keyName, signal);
      if (signal.aborted) return;
      setSelected(key);
      dismiss();
      if (keyStatus(key) === "Expired")
        notify(
          "warning",
          "This key has expired. Rotate it to get a replacement, or revoke it if it is no longer needed.",
        );
      if (keyStatus(key) === "Unknown expiration")
        notify(
          "warning",
          "This key's expiration could not be read. Refresh your keys before using it.",
        );
    } catch (cause) {
      if (!signal.aborted && cause?.response?.status === 404) {
        setKeys((current) => current.filter((key) => key["key-name"] !== keyName));
        setSelected(null);
      }
      await showError(cause, signal);
    } finally {
      setWorking(false);
    }
  }

  async function create(event) {
    event.preventDefault();
    if (!name.trim() || !profile?.userName || working) return;
    const expiration = expires ? keyDate(`${expires}T00:00:00Z`) : null;
    if (expires && (!expiration || expiration.getTime() <= Date.now())) {
      const message =
        "Choose a valid expiration date after today, or clear it for no expiration.";
      setError(message);
      notify("error", message);
      return;
    }
    const signal = controller.current.signal;
    setWorking(true);
    setError("");
    dismiss();
    try {
      const result = await api.create(
        profile.userName,
        name.trim(),
        expires ? `${expires}T00:00:00Z` : null,
        signal,
      );
      if (signal.aborted) return;
      setCreated(result);
      // Keep only metadata in the list and detail view, never the secret.
      const metadata = { ...result, "api-key": undefined };
      setKeys((current) => [metadata, ...current]);
      setSelected(metadata);
      setSearch("");
      setCreateOpen(false);
      setName("");
      notify(
        result["api-key"] ? "success" : "warning",
        result["api-key"]
          ? rotationSource
            ? "Replacement created. Save its secret before revoking the old key."
            : "API key created. Save its secret now; it will only be shown once."
          : "The key was created without a returned secret. Revoke it and create a replacement.",
      );
    } catch (cause) {
      await showError(cause, signal);
    } finally {
      setWorking(false);
    }
  }

  async function revoke() {
    const target = rotationSource ?? selected;
    if (!target || working) return;
    const signal = controller.current.signal;
    setWorking(true);
    setError("");
    dismiss();
    try {
      await api.revoke(target["key-name"], signal);
      if (signal.aborted) return;
      setKeys((current) =>
        current.filter((key) => key["key-name"] !== target["key-name"]),
      );
      notify(
        "success",
        rotationSource
          ? `Rotation complete. Revoked ${target["key-name"]}; use the replacement key in your application.`
          : `Revoked ${target["key-name"]}. Applications using it must switch to another key.`,
      );
      if (!rotationSource) setSelected(null);
      setRotationSource(null);
      setRevokeOpen(false);
    } catch (cause) {
      await showError(cause, signal);
    } finally {
      setWorking(false);
    }
  }

  async function copySecret() {
    try {
      await navigator.clipboard.writeText(created["api-key"]);
      notify(
        "success",
        "Key copied. Save it in a secure secret store, then clear your clipboard.",
      );
    } catch {
      notify(
        "error",
        "Clipboard access is unavailable. Select and copy the key manually.",
      );
    }
  }

  return (
    <section className="pb-12">
      <KeyHeader
        {...{ profile, working, loading, office, setError }}
        setCreateOpen={changeCreateOpen}
      />
      {!createOpen && !revokeOpen && !created && (
        <KeyFeedback {...{ notification }} onDismiss={dismiss} />
      )}
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
        feedback={
          createOpen && <KeyFeedback {...{ notification }} onDismiss={dismiss} />
        }
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
        feedback={created && <KeyFeedback {...{ notification }} onDismiss={dismiss} />}
        {...{ created, copySecret, rotationSource }}
        onSaved={saved}
      />
      <RevokeKeyDialog
        feedback={
          revokeOpen && <KeyFeedback {...{ notification }} onDismiss={dismiss} />
        }
        {...{ revokeOpen, working, error, revoke }}
        selected={rotationSource ?? selected}
        rotation={Boolean(rotationSource)}
        setRevokeOpen={changeRevokeOpen}
      />
    </section>
  );
}
KeyManager.propTypes = { token: PropTypes.string.isRequired };
