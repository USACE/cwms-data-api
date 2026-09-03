import PropTypes from "prop-types";
import { Modal, Text, Input, Button } from "@usace/groundwork";
import { Notice } from "../../user-lists/components/StatusMessages";
export default function SaveKeyDialog({
  created,
  copySecret,
  message,
  setCreated,
  setMessage,
}) {
  return (
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
  );
}
SaveKeyDialog.propTypes = {
  created: PropTypes.object,
  copySecret: PropTypes.func.isRequired,
  message: PropTypes.string.isRequired,
  setCreated: PropTypes.func.isRequired,
  setMessage: PropTypes.func.isRequired,
};
