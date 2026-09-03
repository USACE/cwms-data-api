import PropTypes from "prop-types";
import { Card, H2, Text, Button } from "@usace/groundwork";
import { FaKey } from "react-icons/fa";
import { EmptyState } from "../../user-lists/components/StatusMessages";
import { keyStatus, keyDate } from "../api";
const formatDate = (value) =>
  keyDate(value)?.toLocaleString() ?? (value ? "Unknown" : "None");
export default function KeyDetails({ selected, working, setError, setRevokeOpen }) {
  return (
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
            The secret is shown only when the key is created. If you lose it, create a
            replacement and revoke the old key.
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
          Select a key to view its owner, creation date, and expiration or to revoke it.
        </EmptyState>
      )}
    </Card>
  );
}
KeyDetails.propTypes = {
  selected: PropTypes.object,
  working: PropTypes.bool.isRequired,
  setError: PropTypes.func.isRequired,
  setRevokeOpen: PropTypes.func.isRequired,
};
