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
  }),
);

function getExtentLastUpdate(extent) {
  return extent?.lastUpdate || extent?.["last-update"];
}

function getFreshnessColor(extents) {
  const lastUpdates = (extents || [])
    .map(getExtentLastUpdate)
    .filter(Boolean)
    .map((lastUpdate) => dayjs(lastUpdate))
    .filter((lastUpdate) => lastUpdate.isValid());

  if (!lastUpdates.length) return "red";

  const now = dayjs();
  const newestLastUpdate = lastUpdates.reduce((newest, lastUpdate) =>
    lastUpdate.isAfter(newest) ? lastUpdate : newest,
  );

  return now.diff(newestLastUpdate, "hour", true) <= 24 ? "green" : "yellow";
}

export default function TimeSeriesDropdown({
  office,
  setOffice,
  tsids,
  setTsids,
  setTsidOffices,
  includeMissingTimeseries,
}) {
  const [searchTerm, setSearchTerm] = useState("");
  const [debouncedSearchTerm] = useDebounce(searchTerm, 400);

  const {
    data: suggestions = [],
    isFetching: loading,
    isError,
    error,
  } = useQuery({
    queryKey: ["tsid-catalog", office, debouncedSearchTerm, includeMissingTimeseries],
    queryFn: async () => {
      if (!debouncedSearchTerm || debouncedSearchTerm.length < 3) return [];
      const request = {
        dataset: "TIMESERIES",
        excludeEmpty: !includeMissingTimeseries,
        includeAliases: true,
        like: `*${debouncedSearchTerm}*`,
        pageSize: 500,
      };
      if (office) request.office = office;
      const { entries } = await catalogApi.getCatalogWithDataset(request);
      return entries;
    },
    enabled: debouncedSearchTerm.length >= 3,
  });

  return (
    <div className="flex flex-col sm:flex-row sm:items-start w-full">
      <label className="mb-2 sm:mb-4 sm:mt-6">Select a timeseries:</label>
      <div className="flex flex-col my-2 sm:my-4 w-full sm:w-3/4 sm:ms-2 me-auto">
        <Combobox
          value={tsids[0] || ""}
          onChange={(value) => {
            if (!value) return;
            const selected = suggestions.find(
              (entry) => `${entry.office}/${entry.name}` === value,
            );
            const tsid = selected?.name || value;
            if ((tsid.match(/\./g) || []).length === 5) {
              if (!office && selected?.office) setOffice(selected.office);
              setTsids((prev) => (prev.includes(tsid) ? prev : [...prev, tsid]));
              if (selected?.office || office) {
                setTsidOffices?.((current) => ({
                  ...current,
                  [tsid]: selected?.office || office,
                }));
              }
            } else {
              alert(
                "TSID must have 6 parts: Location.Parameter.Type.Interval.Duration.Version",
              );
            }
          }}
        >
          <ComboboxInput
            onChange={(event) => setSearchTerm(event.target.value)}
            className="px-3 py-2 border rounded w-full"
            placeholder="Search TSID (e.g. Location.Elev.Inst.1Hour.0.Version)"
            autoComplete="off"
            autoCorrect="off"
            autoCapitalize="none"
            spellCheck={false}
            name="tsid-search"
          />
          <ComboboxOptions className="bg-white border mt-1 max-h-60 overflow-auto">
            {loading ? (
              <li className="p-2 text-gray-500 italic">Searching...</li>
            ) : isError ? (
              <li className="p-2 text-red-600">Error: {error.message}</li>
            ) : (
              suggestions.map((entry, idx) => {
                const suggestion_color = getFreshnessColor(entry.extents);
                return (
                  <ComboboxOption
                    key={`${entry.office}/${entry.name}/${idx}`}
                    value={`${entry.office}/${entry.name}`}
                    as={Fragment}
                  >
                    {({ active }) => (
                      <li
                        className={`flex items-center gap-2 ${active ? "bg-blue-100" : ""} p-2 cursor-pointer`}
                      >
                        <span
                          className={`inline-block w-2 h-2 rounded-full ${
                            suggestion_color === "green"
                              ? "bg-green-500"
                              : suggestion_color === "yellow"
                                ? "bg-yellow-400"
                                : "bg-red-500"
                          }`}
                        />
                        <span className="font-semibold">{entry.office}</span>
                        <span>{entry.name}</span>
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
  office: PropTypes.string,
  setOffice: PropTypes.func.isRequired,
  tsids: PropTypes.array.isRequired,
  setTsids: PropTypes.func.isRequired,
  setTsidOffices: PropTypes.func,
  includeMissingTimeseries: PropTypes.bool.isRequired,
};
