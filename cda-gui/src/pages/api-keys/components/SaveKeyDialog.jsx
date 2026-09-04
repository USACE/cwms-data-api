import PropTypes from "prop-types";
import { Modal, Text, Input, Button } from "@usace/groundwork";
import { FaExclamationTriangle } from "react-icons/fa";
import { Notice } from "../../user-lists/components/StatusMessages";
export default function SaveKeyDialog({
  created,
  copySecret,
  message,
  onSaved,
  rotationSource,
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
        <div className="flex gap-3 rounded-lg border-l-4 border-amber-500 bg-amber-50 p-4 text-amber-950">
          <FaExclamationTriangle
            aria-hidden="true"
            className="mt-1 h-6 w-6 shrink-0 text-amber-600"
          />
          <div>
            <p className="text-lg font-bold">Save this key now</p>
            <p className="mt-1">
              Copy the key and store it securely. This is the only time you can see it.
              After you close this dialog, you cannot retrieve it.
            </p>
          </div>
        </div>
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
        {rotationSource && (
          <Text>
            The old key <strong>{rotationSource["key-name"]}</strong> has not been
            revoked. Save this replacement and update your application before revoking
            the old key.
          </Text>
        )}
        <Button type="button" onClick={onSaved}>
          Close
        </Button>
      </div>
    </Modal>
  );
}
SaveKeyDialog.propTypes = {
  created: PropTypes.object,
  copySecret: PropTypes.func.isRequired,
  message: PropTypes.string.isRequired,
  onSaved: PropTypes.func.isRequired,
  rotationSource: PropTypes.object,
};
