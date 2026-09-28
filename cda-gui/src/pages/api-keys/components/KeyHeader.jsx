import PropTypes from "prop-types";
import { Button, Text, UsaceBox } from "@usace/groundwork";
import { FaKey } from "react-icons/fa";
export default function KeyHeader({
  profile,
  working,
  loading,
  office,
  setError,
  setCreateOpen,
}) {
  return (
    <UsaceBox title="API Keys" className="mb-6">
      <div className="flex flex-wrap items-center justify-end gap-3">
        <div className="flex flex-wrap gap-2">
          <Button
            color="light"
            href={`/api-keys/help${office ? `?office=${encodeURIComponent(office)}` : ""}`}
          >
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
        Create, view, and revoke your API keys for scripts and applications that access
        CWMS data.
      </Text>
      <Text className="mt-2 max-w-3xl">
        Keys belong to your user account and use your existing office permissions. They
        are not shared office credentials or restricted to the office selected below.
        You can manage only your own keys.
      </Text>
    </UsaceBox>
  );
}
KeyHeader.propTypes = {
  profile: PropTypes.object,
  working: PropTypes.bool.isRequired,
  loading: PropTypes.bool.isRequired,
  office: PropTypes.string.isRequired,
  setError: PropTypes.func.isRequired,
  setCreateOpen: PropTypes.func.isRequired,
};
