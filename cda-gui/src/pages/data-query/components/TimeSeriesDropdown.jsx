import {
  Combobox,
  ComboboxInput,
  ComboboxOptions,
  ComboboxOption,
} from "@headlessui/react";
import { CatalogApi, Configuration } from "cwmsjs";
import { useState, Fragment } from "react";

export default function TimeSeriesDropdown({ office, tsids, setTsids }) {
  const [suggestions, setSuggestions] = useState([]);
  const catalogApi = new CatalogApi(
    new Configuration({
      basePath: import.meta.env.CDA_URL,
      headers: {
        accept: "application/json;version=2",
      },
    })
  );
  //   const [selected, setSelected] = useState(tsids[0] || "");

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
              alert("TSID must have 5 periods.");
            }
          }}
        >
          <ComboboxInput
            onChange={async (event) => {
              const query = event.target.value;
              if (query.length > 2) {
                try {
                  const { entries } = await catalogApi.getCatalogWithDataset({
                    dataset: "TIMESERIES",
                    excludeEmpty: true,
                    like: `*${query}*`,
                    office: office,
                    pageSize: 20,
                  });
                  setSuggestions(entries.map((e) => e.name));
                } catch (e) {
                  console.error("Catalog fetch failed", e);
                }
              }
            }}
            className="px-3 py-2 border rounded w-full"
            placeholder="Search TSID (e.g. Location.Elev.Inst.1Hour.0.Version)"
          />
          <ComboboxOptions className="bg-white border mt-1 max-h-60 overflow-auto">
            {suggestions?.map((s, idx) => (
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
            ))}
          </ComboboxOptions>
        </Combobox>
      </div>
    </div>
  );
}
