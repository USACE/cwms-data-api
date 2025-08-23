import { useState, Fragment } from "react";
import {
  Combobox,
  ComboboxInput,
  ComboboxOptions,
  ComboboxOption,
} from "@headlessui/react";
import { CatalogApi, Configuration } from "cwmsjs";
import dayjs from "dayjs";
import PropTypes from "prop-types";
import { useQuery } from "@tanstack/react-query";
import { useDebounce } from "use-debounce";

// Catalog client
const catalogApi = new CatalogApi(
  new Configuration({
    basePath: import.meta.env.VITE_CDA_API_ROOT,
    headers: { accept: "application/json;version=2" },
  })
);

function getFreshnessColor(lastUpdateIso) {
  if (!lastUpdateIso) return "gray";
  const now = dayjs();
  const updated = dayjs(lastUpdateIso);
  const diffHours = now.diff(updated, "hour");
  const diffDays = now.diff(updated, "day");
  if (diffHours <= 24) return "green";
  if (diffDays <= 7) return "yellow";
  return "red";
}

export default function TimeSeriesDropdown({ office, tsids, setTsids }) {
  const [searchTerm, setSearchTerm] = useState("");
  const [debouncedSearchTerm] = useDebounce(searchTerm, 400);

  const {
    data: suggestions = [],
    isFetching: loading,
    isError,
    error,
  } = useQuery({
    queryKey: ["tsid-catalog", office, debouncedSearchTerm],
    queryFn: async () => {
      if (!debouncedSearchTerm || debouncedSearchTerm.length < 3 || !office)
        return [];
      const { entries } = await catalogApi.getCatalogWithDataset({
        dataset: "TIMESERIES",
        excludeEmpty: false,
        like: `*${debouncedSearchTerm}*`,
        office,
        pageSize: 500,
      });
      return entries;
    },
    enabled: !!office && debouncedSearchTerm.length >= 3,
  });

  return (
    <div className="flex w-full">
      <label className="mb-4 mt-6">Select a timeseries:</label>
      <div className="flex flex-col my-4 w-3/4 ms-2 me-auto">
        <Combobox
          value={tsids[0] || ""}
          onChange={(value) => {
            if (!value) return;
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
            ) : isError ? (
              <li className="p-2 text-red-600">Error: {error.message}</li>
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
