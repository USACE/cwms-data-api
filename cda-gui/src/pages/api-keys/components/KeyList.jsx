import PropTypes from "prop-types";
import { Card, H2, Badge, Button, Input, Skeleton } from "@usace/groundwork";
import { FaKey } from "react-icons/fa";
import { EmptyState } from "../../user-lists/components/StatusMessages";
import { keyStatus } from "../api";
export default function KeyList({
  keys,
  loading,
  working,
  refresh,
  search,
  setSearch,
  visibleKeys,
  selected,
  view,
}) {
  return (
    <Card className="p-5">
      <div className="mb-4 flex flex-wrap items-center justify-between gap-2">
        <H2 className="text-xl">
          Your keys <Badge color="blue">{keys.length}</Badge>
        </H2>
        <Button
          type="button"
          color="light"
          disabled={loading || working}
          onClick={refresh}
        >
          Refresh
        </Button>
      </div>
      <Input
        aria-label="Filter keys"
        placeholder="Filter by key name"
        value={search}
        onChange={(event) => setSearch(event.target.value)}
      />
      {loading ? (
        <Skeleton className="mt-4 h-32 w-full" />
      ) : visibleKeys.length === 0 ? (
        <EmptyState
          icon={FaKey}
          title={keys.length ? "No matching keys" : "No keys to display"}
        >
          Create a key to connect a script or application.
        </EmptyState>
      ) : (
        <ul className="mt-4 max-h-[32rem] space-y-2 overflow-y-auto">
          {visibleKeys.map((key) => (
            <li key={key["key-name"]}>
              <button
                type="button"
                disabled={working}
                aria-pressed={selected?.["key-name"] === key["key-name"]}
                onClick={() => view(key["key-name"])}
                className={`w-full rounded-lg border p-4 text-left focus-visible:ring-2 focus-visible:ring-blue-600 ${selected?.["key-name"] === key["key-name"] ? "border-blue-500 bg-blue-50" : "border-zinc-200 hover:bg-zinc-50"}`}
              >
                <span className="block break-all font-semibold">{key["key-name"]}</span>
                <span className="text-sm text-zinc-600">{keyStatus(key)}</span>
              </button>
            </li>
          ))}
        </ul>
      )}
    </Card>
  );
}
KeyList.propTypes = {
  keys: PropTypes.array.isRequired,
  loading: PropTypes.bool.isRequired,
  working: PropTypes.bool.isRequired,
  refresh: PropTypes.func.isRequired,
  search: PropTypes.string.isRequired,
  setSearch: PropTypes.func.isRequired,
  visibleKeys: PropTypes.array.isRequired,
  selected: PropTypes.object,
  view: PropTypes.func.isRequired,
};
