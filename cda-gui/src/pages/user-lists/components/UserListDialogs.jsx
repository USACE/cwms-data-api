import {
  Button,
  Description,
  Field,
  Input,
  Label,
  Modal,
  Textarea,
} from "@usace/groundwork";
import PropTypes from "prop-types";
import { FaPlus } from "react-icons/fa";
import { HelpTip } from "../../../components/HelpTip";

export function UserListDialogs({
  office,
  selected,
  working,
  createOpened,
  newList,
  description,
  editOpened,
  editDescription,
  deleteOpened,
  onCreateClose,
  onCreate,
  onNewListChange,
  onDescriptionChange,
  onEditClose,
  onEdit,
  onEditDescriptionChange,
  onDeleteClose,
  onDelete,
}) {
  return (
    <>
      <Modal
        opened={createOpened}
        onClose={onCreateClose}
        dialogTitle="Create a user list"
        dialogDescription={`Create an office-owned list for ${office}.`}
        size="lg"
      >
        <form className="space-y-5" onSubmit={onCreate}>
          <Field>
            <div className="flex items-center gap-1">
              <Label>List ID</Label>
              <HelpTip title="List ID rules">
                The ID is unique within the selected office and cannot be renamed after
                creation. Use up to 128 uppercase letters, numbers, periods, hyphens, or
                underscores.
              </HelpTip>
            </div>
            <Description>
              Use a short, recognizable name such as ON-CALL-HYDROLOGISTS.
            </Description>
            <Input
              required
              autoFocus
              maxLength={128}
              aria-label="List ID"
              placeholder="ON-CALL-HYDROLOGISTS"
              value={newList}
              onChange={(event) => onNewListChange(event.target.value.toUpperCase())}
            />
          </Field>
          <Field>
            <Label>Description</Label>
            <Description>
              Explain who belongs in the list and how it is used.
            </Description>
            <Textarea
              rows={4}
              aria-label="Description"
              placeholder="Hydrologists who receive operational alerts."
              value={description}
              onChange={(event) => onDescriptionChange(event.target.value)}
            />
          </Field>
          <div className="flex justify-end gap-3">
            <Button type="button" color="light" onClick={onCreateClose}>
              Cancel
            </Button>
            <Button type="submit" disabled={working || !newList.trim()}>
              <FaPlus aria-hidden="true" />
              Create list
            </Button>
          </div>
        </form>
      </Modal>

      <Modal
        opened={editOpened}
        onClose={onEditClose}
        dialogTitle="Edit list details"
        dialogDescription={`Update the description for ${selected}. The list ID and creator do not change.`}
        size="lg"
      >
        <form className="space-y-5" onSubmit={onEdit}>
          <Field>
            <Label>Description</Label>
            <Description>
              Explain who belongs in the list and how it is used.
            </Description>
            <Textarea
              autoFocus
              rows={4}
              maxLength={1024}
              aria-label="Description"
              value={editDescription}
              onChange={(event) => onEditDescriptionChange(event.target.value)}
            />
          </Field>
          <div className="flex flex-wrap justify-end gap-3">
            <Button type="button" color="light" onClick={onEditClose}>
              Cancel
            </Button>
            <Button type="submit" disabled={working}>
              Save changes
            </Button>
          </div>
        </form>
      </Modal>

      <Modal
        opened={deleteOpened}
        onClose={onDeleteClose}
        dialogTitle="Delete user list?"
        dialogDescription={`Delete ${selected} and remove all of its memberships. This cannot be undone.`}
        size="md"
      >
        <div className="flex flex-wrap justify-end gap-3">
          <Button type="button" color="light" onClick={onDeleteClose}>
            Cancel
          </Button>
          <Button type="button" color="danger" disabled={working} onClick={onDelete}>
            Delete list
          </Button>
        </div>
      </Modal>
    </>
  );
}

UserListDialogs.propTypes = {
  office: PropTypes.string.isRequired,
  selected: PropTypes.string.isRequired,
  working: PropTypes.bool.isRequired,
  createOpened: PropTypes.bool.isRequired,
  newList: PropTypes.string.isRequired,
  description: PropTypes.string.isRequired,
  editOpened: PropTypes.bool.isRequired,
  editDescription: PropTypes.string.isRequired,
  deleteOpened: PropTypes.bool.isRequired,
  onCreateClose: PropTypes.func.isRequired,
  onCreate: PropTypes.func.isRequired,
  onNewListChange: PropTypes.func.isRequired,
  onDescriptionChange: PropTypes.func.isRequired,
  onEditClose: PropTypes.func.isRequired,
  onEdit: PropTypes.func.isRequired,
  onEditDescriptionChange: PropTypes.func.isRequired,
  onDeleteClose: PropTypes.func.isRequired,
  onDelete: PropTypes.func.isRequired,
};
