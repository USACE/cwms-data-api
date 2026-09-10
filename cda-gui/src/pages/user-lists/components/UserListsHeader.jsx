import { Badge, Button, H1, Text } from "@usace/groundwork";
import PropTypes from "prop-types";
import { FaPlus } from "react-icons/fa";
import { HelpTip } from "../../../components/HelpTip";

export function UserListsHeader({ canWrite, onCreate }) {
  return (
    <div className="mb-6 flex flex-col gap-4 border-b border-zinc-200 pb-6 sm:flex-row sm:items-end sm:justify-between">
      <div>
        <div className="mb-2 flex items-center gap-2">
          <Badge color="blue">CDA</Badge>
          <Badge color={canWrite ? "green" : "zinc"}>
            {canWrite ? "User list administrator" : "Read only"}
          </Badge>
        </div>
        <div className="flex items-center gap-1">
          <H1>User Lists</H1>
          <HelpTip title="List ownership">
            CDA records the authenticated creator as the list owner for audit history.
            The owner does not change when the description or membership changes; office
            User Administrators control edits.
          </HelpTip>
        </div>
        <Text className="mt-2 max-w-3xl">
          Build reusable, office-owned lists for notifications and other CDA-aware
          applications. Membership stays in CDA so every consumer uses the same list.
        </Text>
      </div>
      {canWrite && (
        <Button type="button" onClick={onCreate}>
          <FaPlus aria-hidden="true" />
          New list
        </Button>
      )}
    </div>
  );
}

UserListsHeader.propTypes = {
  canWrite: PropTypes.bool.isRequired,
  onCreate: PropTypes.func.isRequired,
};
