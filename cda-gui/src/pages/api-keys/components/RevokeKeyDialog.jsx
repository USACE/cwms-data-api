import PropTypes from "prop-types";
import { Modal, Text, Button } from "@usace/groundwork";
import { Notice } from "../../user-lists/components/StatusMessages";
export default function RevokeKeyDialog({
  revokeOpen,
  working,
  setRevokeOpen,
  error,
  selected,
  revoke,
  rotation = false,
}) {
  return (
    <Modal
      opened={revokeOpen}
      className="api-key-dialog"
      onClose={() => {
        if (!working) setRevokeOpen(false);
      }}
      dialogTitle={
        rotation ? "Finish rotation: revoke the old key?" : "Revoke API key?"
      }
      size="md"
    >
      {error && <Notice kind="error">{error}</Notice>}
      {rotation && (
        <Text>
          Your replacement has been created. Confirm only after you have saved its
          secret and updated your application. Cancel to keep both keys for now.
        </Text>
      )}
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
  );
}
RevokeKeyDialog.propTypes = {
  rotation: PropTypes.bool,
  revokeOpen: PropTypes.bool.isRequired,
  working: PropTypes.bool.isRequired,
  setRevokeOpen: PropTypes.func.isRequired,
  error: PropTypes.string.isRequired,
  selected: PropTypes.object,
  revoke: PropTypes.func.isRequired,
};
