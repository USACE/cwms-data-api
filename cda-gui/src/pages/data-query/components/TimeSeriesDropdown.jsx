import { useState, useEffect, Fragment } from "react";
import {
  Combobox,
  ComboboxInput,
  ComboboxOptions,
  ComboboxOption,
} from "@headlessui/react";
import { CatalogApi, Configuration } from "cwmsjs";
import dayjs from "dayjs";
import PropTypes from "prop-types";

const catalogApi = new CatalogApi(
  new Configuration({
    basePath: import.meta.env.CDA_URL,
    headers: { accept: "application/json;version=2" },
  })
);

function getFreshnessColor(lastUpdateIso) {
  if (!lastUpdateIso) return "gray";

  const now = dayjs();
  const updated = dayjs(lastUpdateIso);

  const diffHours = now.diff(updated, "hour");
  const diffDays = now.diff(updated, "day");

  // Data is current if updated within the last hour
  if (diffHours <= 24) return "green";
  // Data is semi-current if updated within the last 7 days
  if (diffDays <= 7) return "yellow";
  // Data is stale if older than 7 days
  return "red";
}

export default function TimeSeriesDropdown({ office, tsids, setTsids }) {
  const [searchTerm, setSearchTerm] = useState("");
  const [suggestions, setSuggestions] = useState([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (searchTerm.length < 3) {
      setSuggestions([]);
      return;
    }

    const timeout = setTimeout(async () => {
      try {
        setLoading(true);
        const { entries } = await catalogApi.getCatalogWithDataset({
          dataset: "TIMESERIES",
          excludeEmpty: false,
          like: `*${searchTerm}*`,
          office,
          pageSize: 20,
        });
        setSuggestions(entries);
      } catch (e) {
        console.error("Catalog fetch failed", e);
        setSuggestions([]);
      } finally {
        setLoading(false);
      }
    }, 400);

    return () => clearTimeout(timeout);
  }, [searchTerm, office]);

  return (
    <div className="flex w-full">
      <label className="mb-4 mt-6">Select a timeseries:</label>
      <div className="flex flex-col my-4 w-3/4 ms-2 me-auto">
        <Combobox
          value={tsids[0] || ""}
          onChange={(value) => {
            if (!value) {
              return;
            }
            if ((value.match(/\./g) || []).length === 5) {
              setTsids((prev) =>
                prev.includes(value) ? prev : [...prev, value]
              );
            } else {
              alert(
                "TSID must have 6 parts: Location.Parameter.Type.Interval.Duration.Version"
              );
            }
          }}
        >
          <ComboboxInput
            onChange={(event) => setSearchTerm(event.target.value)}
            className="px-3 py-2 border rounded w-full"
            placeholder="Search TSID (e.g. Location.Elev.Inst.1Hour.0.Version)"
          />
          <ComboboxOptions className="bg-white border mt-1 max-h-60 overflow-auto">
            {loading ? (
              <li className="p-2 text-gray-500 italic">Searching...</li>
            ) : (
              suggestions.map((entry, idx) => {
                const suggestion_color = getFreshnessColor(
                  entry.extents?.[0]?.lastUpdate
                );
                return (
                  <ComboboxOption key={idx} value={entry.name} as={Fragment}>
                    {({ active }) => (
                      <li
                        className={`flex items-center gap-2 ${
                          active ? "bg-blue-100" : ""
                        } p-2 cursor-pointer`}
                      >
                        <span
                          className={`inline-block w-2 h-2 rounded-full ${
                            suggestion_color === "green"
                              ? "bg-green-500"
                              : suggestion_color === "yellow"
                              ? "bg-yellow-400"
                              : suggestion_color === "gray"
                              ? "bg-gray-500"
                              : "bg-red-500"
                          }`}
                        />
                        {entry.name}
                      </li>
                    )}
                  </ComboboxOption>
                );
              })
            )}
          </ComboboxOptions>
        </Combobox>
      </div>
    </div>
  );
}

TimeSeriesDropdown.propTypes = {
  office: PropTypes.string.isRequired,
  tsids: PropTypes.array.isRequired,
  setTsids: PropTypes.func.isRequired,
};
