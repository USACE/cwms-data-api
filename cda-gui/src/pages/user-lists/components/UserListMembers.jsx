import {
  Badge,
  Button,
  Card,
  Description,
  Field,
  H2,
  Input,
  Label,
  Skeleton,
  Strong,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  Text,
} from "@usace/groundwork";
import PropTypes from "prop-types";
import { FaPen, FaSearch, FaUserPlus, FaUsers } from "react-icons/fa";
import { HelpTip } from "../../../components/HelpTip";
import { EmptyState } from "./StatusMessages";

export function UserListMembers({
  selected,
  selectedList,
  members,
  filteredMembers,
  canWrite,
  working,
  loading,
  candidateSearch,
  candidates,
  candidateLoading,
  newMember,
  memberSearch,
  onEdit,
  onDelete,
  onCandidateSearch,
  onCandidateSelect,
  onAddMember,
  onMemberSearch,
  onRemoveMember,
}) {
  return (
    <Card className="p-0">
      <div className="flex flex-col gap-3 border-b border-zinc-200 px-5 py-4 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <div className="flex flex-wrap items-center gap-2">
            <H2 className="text-xl">{selected || "Members"}</H2>
            {selected && <Badge color="green">{members.length} members</Badge>}
          </div>
          <Text className="mt-1">
            {selectedList?.description ||
              "Select a user list to view its current membership."}
          </Text>
        </div>
        {canWrite && selected && (
          <div className="flex flex-wrap gap-2">
            <Button type="button" color="light" onClick={onEdit}>
              <FaPen aria-hidden="true" />
              Edit details
            </Button>
            <Button type="button" color="danger" style="outline" onClick={onDelete}>
              Delete list
            </Button>
          </div>
        )}
      </div>

      {!selected ? (
        <div className="p-5">
          <EmptyState icon={FaUsers} title="Choose a list">
            Select a user list from the left to see names, user IDs, and email
            addresses.
          </EmptyState>
        </div>
      ) : (
        <>
          {canWrite && (
            <form
              className="border-b border-zinc-200 bg-zinc-50 px-5 py-4"
              onSubmit={onAddMember}
            >
              <Field>
                <div className="flex items-center gap-1">
                  <Label>Add a member</Label>
                  <HelpTip title="List members">
                    Members must already have a CDA user profile. CDA stores the user ID
                    in the list and resolves the member&apos;s current name and email
                    for consumers such as notifications.
                  </HelpTip>
                </div>
                <Description>
                  Search by user ID, name, or email, then choose an existing CWMS user.
                </Description>
                <div className="relative mt-3">
                  <FaSearch
                    aria-hidden="true"
                    className="pointer-events-none absolute left-3 top-3 text-zinc-500"
                  />
                  <Input
                    aria-label="Search CWMS users"
                    className="pl-9"
                    placeholder="Search users"
                    value={candidateSearch}
                    onChange={(event) => onCandidateSearch(event.target.value)}
                  />
                </div>
                {candidateLoading && (
                  <Text className="mt-2" role="status">
                    Searching…
                  </Text>
                )}
                {!candidateLoading && candidateSearch.trim().length >= 2 && (
                  <div
                    className="mt-2 max-h-56 overflow-y-auto rounded-lg border border-zinc-200 bg-white p-1"
                    role="listbox"
                    aria-label="Matching CWMS users"
                  >
                    {candidates.length === 0 ? (
                      <Text className="p-3">No matching users found.</Text>
                    ) : (
                      candidates.map((candidate) => {
                        const candidateId = candidate["user-id"];
                        const chosen = candidateId === newMember;
                        return (
                          <button
                            key={candidateId}
                            type="button"
                            role="option"
                            aria-selected={chosen}
                            className={`block w-full rounded-md px-3 py-2 text-left ${
                              chosen
                                ? "bg-blue-50 ring-1 ring-blue-600"
                                : "hover:bg-zinc-50"
                            }`}
                            onClick={() => onCandidateSelect(candidateId)}
                          >
                            <Strong>{candidate["full-name"] || candidateId}</Strong>
                            <Text className="text-sm">
                              {[candidateId, candidate.email]
                                .filter(Boolean)
                                .join(" · ")}
                            </Text>
                          </button>
                        );
                      })
                    )}
                  </div>
                )}
                <div className="mt-3 flex flex-wrap items-center justify-between gap-3">
                  <Text>
                    {newMember
                      ? `Selected: ${newMember}`
                      : "Select a user from the search results."}
                  </Text>
                  <Button
                    type="submit"
                    className="whitespace-nowrap"
                    disabled={working || !newMember}
                  >
                    <FaUserPlus aria-hidden="true" />
                    Add
                  </Button>
                </div>
              </Field>
            </form>
          )}

          <div className="min-w-0 p-5">
            {loading ? (
              <div className="space-y-3">
                <Skeleton className="h-12 w-full" />
                <Skeleton className="h-12 w-full" />
              </div>
            ) : members.length === 0 ? (
              <EmptyState icon={FaUsers} title="This list is empty">
                {canWrite
                  ? "Add a CWMS user to make this list available to notification consumers."
                  : "A list administrator has not added any members yet."}
              </EmptyState>
            ) : (
              <>
                <div className="relative mb-4">
                  <FaSearch
                    aria-hidden="true"
                    className="pointer-events-none absolute left-3 top-3 text-zinc-500"
                  />
                  <Input
                    aria-label="Filter list members"
                    className="pl-9"
                    placeholder="Filter members"
                    value={memberSearch}
                    onChange={(event) => onMemberSearch(event.target.value)}
                  />
                </div>
                {filteredMembers.length === 0 ? (
                  <EmptyState icon={FaSearch} title="No matching members">
                    Try a different name, user ID, or email.
                  </EmptyState>
                ) : (
                  <div className="overflow-x-auto">
                    <Table striped dense>
                      <TableHead>
                        <TableRow>
                          <TableHeader>User</TableHeader>
                          <TableHeader>Email</TableHeader>
                          {canWrite && (
                            <TableHeader className="text-right">Action</TableHeader>
                          )}
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {filteredMembers.map((member) => {
                          const userId = member["user-id"] ?? member;
                          return (
                            <TableRow key={userId}>
                              <TableCell>
                                <Strong>{member["full-name"] || userId}</Strong>
                                {member["full-name"] && (
                                  <Text className="text-xs">{userId}</Text>
                                )}
                              </TableCell>
                              <TableCell>
                                {member.email ? (
                                  <a
                                    className="text-blue-700 underline hover:text-blue-900"
                                    href={`mailto:${member.email}`}
                                  >
                                    {member.email}
                                  </a>
                                ) : (
                                  <Text>No email in CDA profile</Text>
                                )}
                              </TableCell>
                              {canWrite && (
                                <TableCell className="text-right">
                                  <Button
                                    type="button"
                                    color="danger"
                                    style="outline"
                                    size="sm"
                                    disabled={working}
                                    onClick={() => onRemoveMember(userId)}
                                  >
                                    Remove
                                  </Button>
                                </TableCell>
                              )}
                            </TableRow>
                          );
                        })}
                      </TableBody>
                    </Table>
                  </div>
                )}
              </>
            )}
          </div>
        </>
      )}
    </Card>
  );
}

const userShape = PropTypes.shape({
  "user-id": PropTypes.string,
  "full-name": PropTypes.string,
  email: PropTypes.string,
});

UserListMembers.propTypes = {
  selected: PropTypes.string.isRequired,
  selectedList: PropTypes.shape({ description: PropTypes.string }),
  members: PropTypes.arrayOf(PropTypes.oneOfType([userShape, PropTypes.string]))
    .isRequired,
  filteredMembers: PropTypes.arrayOf(PropTypes.oneOfType([userShape, PropTypes.string]))
    .isRequired,
  canWrite: PropTypes.bool.isRequired,
  working: PropTypes.bool.isRequired,
  loading: PropTypes.bool.isRequired,
  candidateSearch: PropTypes.string.isRequired,
  candidates: PropTypes.arrayOf(userShape).isRequired,
  candidateLoading: PropTypes.bool.isRequired,
  newMember: PropTypes.string.isRequired,
  memberSearch: PropTypes.string.isRequired,
  onEdit: PropTypes.func.isRequired,
  onDelete: PropTypes.func.isRequired,
  onCandidateSearch: PropTypes.func.isRequired,
  onCandidateSelect: PropTypes.func.isRequired,
  onAddMember: PropTypes.func.isRequired,
  onMemberSearch: PropTypes.func.isRequired,
  onRemoveMember: PropTypes.func.isRequired,
};
