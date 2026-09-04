import PropTypes from "prop-types";
import {
  Modal,
  Text,
  Field,
  Label,
  Input,
  Description,
  Button,
} from "@usace/groundwork";
import { Notice } from "../../user-lists/components/StatusMessages";
import ExpirationShortcuts from "./ExpirationShortcuts";
export default function CreateKeyDialog({
  createOpen,
  working,
  setCreateOpen,
  create,
  error,
  profile,
  name,
  setName,
  expires,
  setExpires,
  keys,
  rotationSource,
}) {
  return (
    <Modal
      opened={createOpen}
      className="api-key-dialog"
      onClose={() => {
        if (!working) setCreateOpen(false);
      }}
      dialogTitle={rotationSource ? "Rotate API key" : "Create API key"}
      size="lg"
    >
      <form className="space-y-5" onSubmit={create}>
        {error && <Notice kind="error">{error}</Notice>}
        {rotationSource && (
          <Text>
            Create a replacement for <strong>{rotationSource["key-name"]}</strong> with
            a new name. The old key will not be revoked until you save the new secret
            and confirm revocation. If creation fails, the old key is unchanged.
          </Text>
        )}
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
            Expires at the start of this date in UTC. Clear the date for no expiration.
          </Description>
          <ExpirationShortcuts {...{ setExpires, working }} />
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
            {working
              ? "Creating…"
              : rotationSource
                ? "Generate replacement"
                : "Generate key"}
          </Button>
        </div>
        {keys.some((key) => key["key-name"] === name.trim()) && (
          <Text>A key with this name already exists. Choose another name.</Text>
        )}
      </form>
    </Modal>
  );
}
CreateKeyDialog.propTypes = {
  rotationSource: PropTypes.object,
  createOpen: PropTypes.bool.isRequired,
  working: PropTypes.bool.isRequired,
  setCreateOpen: PropTypes.func.isRequired,
  create: PropTypes.func.isRequired,
  error: PropTypes.string.isRequired,
  profile: PropTypes.object,
  name: PropTypes.string.isRequired,
  setName: PropTypes.func.isRequired,
  expires: PropTypes.string.isRequired,
  setExpires: PropTypes.func.isRequired,
  keys: PropTypes.array.isRequired,
};
