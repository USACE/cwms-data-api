import { FaTrash, FaEye, FaEyeSlash } from "react-icons/fa";
import PropTypes from "prop-types";

export default function TimeSeriesManager({
  statusByTsid = {},
  tsidOffices = {},
  tsids,
  visibleTSIDs,
  setTsids,
  setTsidOffices,
  toggleTSID,
}) {
  const getStatusClasses = (tsid) => {
    const status = statusByTsid[tsid]?.status || "pending";

    if (status === "success") {
      return "border-green-200 bg-green-50 text-green-950";
    }
    if (status === "error") {
      return "border-red-200 bg-red-50 text-red-950";
    }
    return "border-yellow-200 bg-yellow-50 text-yellow-950";
  };
  const getStatusLabel = (tsid) => {
    const status = statusByTsid[tsid]?.status || "pending";

    if (status === "success") return "Loaded successfully";
    if (status === "error") return "Failed or no values found";
    return "Waiting for data";
  };

  return (
    <div
      className={`bg-gray-50 border p-2 w-full min-w-0 rounded shadow-sm overflow-auto max-h-56 ${
        tsids.length == 0 ? "hidden" : ""
      }`}
    >
      {tsids.length === 0 && (
        <div className="text-center text-gray-500 mt-2 text-lg">
          <p>Select a TimeSeries to Begin</p>
        </div>
      )}
      <h4 className="text-md font-bold mb-2">TimeSeries</h4>

      {tsids.map((tsid) => (
        <div
          key={tsid}
          data-status={statusByTsid[tsid]?.status || "pending"}
          title={statusByTsid[tsid]?.message || getStatusLabel(tsid)}
          className={`mb-1 flex min-w-0 items-center justify-between gap-2 rounded border px-2 py-1 ${getStatusClasses(tsid)}`}
        >
          <div className="min-w-0 flex-1">
            <div className="truncate text-sm font-medium">
              {tsidOffices[tsid] ? `${tsidOffices[tsid]} / ` : ""}
              {tsid}
            </div>
            <div className="truncate text-xs opacity-80">
              {statusByTsid[tsid]?.message || getStatusLabel(tsid)}
            </div>
          </div>
          <div className="flex shrink-0 items-center gap-2">
            <button
              onClick={() => toggleTSID(tsid)}
              title="Toggle visibility"
              className="text-gray-700 hover:text-black"
            >
              {visibleTSIDs.includes(tsid) ? (
                <FaEye size={16} />
              ) : (
                <FaEyeSlash size={16} />
              )}
            </button>
            <button
              onClick={() => {
                setTsids((prev) => prev.filter((t) => t !== tsid));
                setTsidOffices?.((current) => {
                  const next = { ...current };
                  delete next[tsid];
                  return next;
                });
              }}
              title="Remove"
              className="text-red-600 hover:text-red-800"
            >
              <FaTrash size={16} />
            </button>
          </div>
        </div>
      ))}
    </div>
  );
}

TimeSeriesManager.propTypes = {
  statusByTsid: PropTypes.objectOf(
    PropTypes.shape({
      message: PropTypes.string.isRequired,
      status: PropTypes.oneOf(["error", "pending", "success"]).isRequired,
    }),
  ),
  tsidOffices: PropTypes.objectOf(PropTypes.string),
  tsids: PropTypes.arrayOf(PropTypes.string).isRequired,
  visibleTSIDs: PropTypes.arrayOf(PropTypes.string).isRequired,
  setTsids: PropTypes.func.isRequired,
  setTsidOffices: PropTypes.func,
  toggleTSID: PropTypes.func.isRequired,
};
