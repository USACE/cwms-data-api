import { FaTrash, FaEye, FaEyeSlash } from "react-icons/fa";
export default function TimeSeriesManager({
  tsids,
  visibleTSIDs,
  setTsids,
  toggleTSID,
}) {
  return (
    <div
      className={`bg-gray-50 border p-2 w-3/4 rounded shadow-sm md:mt-4 md:w-2/5 overflow-auto h-[20vh] max-h-[40vh] ${
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
          className="flex items-center justify-between gap-2 mb-1"
        >
          <span className="truncate text-sm">{tsid}</span>
          <div className="flex items-center gap-2">
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
              onClick={() => setTsids((prev) => prev.filter((t) => t !== tsid))}
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
