import { useState, useEffect, Fragment } from "react";
import {
  Combobox,
  ComboboxInput,
  ComboboxOptions,
  ComboboxOption,
} from "@headlessui/react";
import { CatalogApi, Configuration } from "cwmsjs";

const catalogApi = new CatalogApi(
  new Configuration({
    basePath: import.meta.env.CDA_URL,
    headers: { accept: "application/json;version=2" },
  })
);

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
          excludeEmpty: true,
          like: `*${searchTerm}*`,
          office,
          pageSize: 20,
        });
        setSuggestions(entries.map((e) => e.name));
      } catch (e) {
        console.error("Catalog fetch failed", e);
        setSuggestions([]);
      } finally {
        setLoading(false);
      }
    }, 400); // ⏱ debounce delay

    return () => clearTimeout(timeout);
  }, [searchTerm, office]);

  return (
    <div className="flex w-3/4 m-auto">
      <label className="my-4">Select a timeseries:</label>
      <div className="flex flex-col my-4 w-3/4 m-auto">
        <Combobox
          value={tsids[0] || ""}
          onChange={(value) => {
            if (!value) {
              setTsids([]);
              return;
            }
            if ((value.match(/\./g) || []).length === 5) {
              setTsids([value]);
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
              suggestions.map((s, idx) => (
                <ComboboxOption key={idx} value={s} as={Fragment}>
                  {({ active }) => (
                    <li
                      className={`${
                        active ? "bg-blue-100" : ""
                      } p-2 cursor-pointer`}
                    >
                      {s}
                    </li>
                  )}
                </ComboboxOption>
              ))
            )}
          </ComboboxOptions>
        </Combobox>
      </div>
    </div>
  );
}
