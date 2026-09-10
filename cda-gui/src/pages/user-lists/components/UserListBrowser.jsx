import { Badge, Card, H2, Input, Skeleton, Strong, Text } from "@usace/groundwork";
import PropTypes from "prop-types";
import { FaListUl, FaSearch } from "react-icons/fa";
import { EmptyState } from "./StatusMessages";

const userListShape = PropTypes.shape({
  "user-list-id": PropTypes.string.isRequired,
  description: PropTypes.string,
  "owned-by-user-id": PropTypes.string,
});

export function UserListBrowser({
  lists,
  filteredLists,
  office,
  selected,
  canWrite,
  loading,
  search,
  onSearch,
  onSelect,
}) {
  return (
    <Card className="p-0">
      <div className="flex items-center justify-between border-b border-zinc-200 px-5 py-4">
        <div>
          <H2 className="text-xl">Lists</H2>
          <Text>Select a list to inspect its members.</Text>
        </div>
        <Badge color="blue">{lists.length}</Badge>
      </div>

      <div className="p-3">
        {lists.length > 0 && (
          <div className="relative mb-3">
            <FaSearch
              aria-hidden="true"
              className="pointer-events-none absolute left-3 top-3 text-zinc-500"
            />
            <Input
              aria-label="Filter user lists"
              className="pl-9"
              placeholder="Filter lists"
              value={search}
              onChange={(event) => onSearch(event.target.value)}
            />
          </div>
        )}
        {loading ? (
          <div className="space-y-3 p-2">
            <Skeleton className="h-20 w-full" />
            <Skeleton className="h-20 w-full" />
          </div>
        ) : lists.length === 0 ? (
          <EmptyState icon={FaListUl} title="No user lists yet">
            {canWrite
              ? "Create the first reusable list for this office."
              : "Ask a CWMS User Administrator to create a list for this office."}
          </EmptyState>
        ) : filteredLists.length === 0 ? (
          <EmptyState icon={FaSearch} title="No matching lists">
            Try a different list ID or description.
          </EmptyState>
        ) : (
          <div className="space-y-2" role="list" aria-label={`${office} user lists`}>
            {filteredLists.map((item) => {
              const listId = item["user-list-id"];
              const active = listId === selected;
              return (
                <button
                  key={listId}
                  type="button"
                  role="listitem"
                  aria-current={active ? "true" : undefined}
                  className={`w-full rounded-lg border p-4 text-left transition ${
                    active
                      ? "border-blue-600 bg-blue-50 shadow-sm"
                      : "border-zinc-200 bg-white hover:border-blue-300 hover:bg-zinc-50"
                  }`}
                  onClick={() => onSelect(listId)}
                >
                  <div className="flex items-start justify-between gap-3">
                    <Strong>{listId}</Strong>
                    {active && <Badge color="blue">Selected</Badge>}
                  </div>
                  <Text className="mt-1 line-clamp-2">
                    {item.description || "No description provided."}
                  </Text>
                  {item["owned-by-user-id"] && (
                    <Text className="mt-2 text-xs">
                      Created by {item["owned-by-user-id"]}
                    </Text>
                  )}
                </button>
              );
            })}
          </div>
        )}
      </div>
    </Card>
  );
}

UserListBrowser.propTypes = {
  lists: PropTypes.arrayOf(userListShape).isRequired,
  filteredLists: PropTypes.arrayOf(userListShape).isRequired,
  office: PropTypes.string.isRequired,
  selected: PropTypes.string.isRequired,
  canWrite: PropTypes.bool.isRequired,
  loading: PropTypes.bool.isRequired,
  search: PropTypes.string.isRequired,
  onSearch: PropTypes.func.isRequired,
  onSelect: PropTypes.func.isRequired,
};
