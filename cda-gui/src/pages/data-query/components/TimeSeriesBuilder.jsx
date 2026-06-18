import { useState, useEffect, useMemo } from "react";
import { Badge, H4 } from "@usace/groundwork";
import {
  Combobox,
  ComboboxInput,
  ComboboxOption,
  ComboboxOptions,
} from "@headlessui/react";
import useAliases from "../hooks/useAliases";
import useDescriptors from "../hooks/useDescriptors";
import PropTypes from "prop-types";
import { FiHelpCircle, FiX } from "react-icons/fi";
import { useDebounce } from "use-debounce";

function getOptions(values = []) {
  return values.map((value) => ({
    value,
    label: value,
  }));
}

const DESCRIPTOR_FIELDS = ["parameter", "type", "interval", "duration", "version"];
const TSID_PART_HELP = {
  Duration:
    "Duration identifies the length of time represented by each value. Instantaneous values commonly use 0.",
  Interval:
    "Interval is the time step for regular data, such as 15Minutes, 1Hour, or 1Day. A leading ~ means irregular.",
  Location:
    "Location is the CWMS location identifier where the time series is measured or computed.",
  "Location Kind":
    "Location Kind narrows the location list to projects, gages, locks, sites, outlets, or all locations.",
  Parameter:
    "Parameter is what the time series measures, such as Elev, Flow, Stage, Precip, or Stor.",
  Type: "Type describes how values are produced or summarized, such as Inst, Ave, Total, Min, or Max.",
  Version:
    "Version identifies the source or processing path for the time series, such as Raw, Rev, Best, or a district-specific version.",
};

function getFilteredValues(entries, selectedFilters, field) {
  const values = new Set();

  entries
    .filter((entry) =>
      DESCRIPTOR_FIELDS.every(
        (filterField) =>
          filterField === field ||
          !selectedFilters[filterField] ||
          entry[filterField] === selectedFilters[filterField],
      ),
    )
    .forEach((entry) => {
      if (entry[field]) values.add(entry[field]);
    });

  return Array.from(values);
}

export default function TimeSeriesBuilder({
  includeMissingTimeseries,
  office,
  setOffice,
  setTsidOffices,
  setTsids,
}) {
  const [locationKind, setLocationKind] = useState("");
  const [locationSearchTerm, setLocationSearchTerm] = useState("");
  const [debouncedLocationSearchTerm] = useDebounce(locationSearchTerm, 350);
  const aliases = useAliases({
    office: "",
    kind: locationKind,
    searchTerm: debouncedLocationSearchTerm,
    props: {
      enabled: Boolean(locationKind) && debouncedLocationSearchTerm.trim().length >= 2,
    },
  });

  const [locationKey, setLocationKey] = useState("");
  const [parameter, setParameter] = useState("");
  const [type, setType] = useState("");
  const [interval, setInterval] = useState("");
  const [duration, setDuration] = useState("");
  const [version, setVersion] = useState("");
  const [clearedFields, setClearedFields] = useState({});
  const [selectedLocation, setSelectedLocation] = useState(null);
  const location = selectedLocation?.name || "";
  const descriptorOffice = office || selectedLocation?.office || "";

  const descriptors = useDescriptors({
    includeMissingTimeseries,
    office: descriptorOffice,
    location,
  });

  useEffect(() => {
    if (office && selectedLocation?.office && selectedLocation.office !== office) {
      setLocationKey("");
      setSelectedLocation(null);
    }
  }, [office, selectedLocation]);

  const descriptorEntries = useMemo(
    () => descriptors.data?.entries || [],
    [descriptors.data?.entries],
  );
  const selectedFilters = useMemo(
    () => ({
      duration,
      interval,
      parameter,
      type,
      version,
    }),
    [duration, interval, parameter, type, version],
  );
  const parameters = useMemo(
    () => getFilteredValues(descriptorEntries, selectedFilters, "parameter"),
    [descriptorEntries, selectedFilters],
  );
  const types = useMemo(
    () => getFilteredValues(descriptorEntries, selectedFilters, "type"),
    [descriptorEntries, selectedFilters],
  );
  const intervals = useMemo(
    () => getFilteredValues(descriptorEntries, selectedFilters, "interval"),
    [descriptorEntries, selectedFilters],
  );
  const durations = useMemo(
    () => getFilteredValues(descriptorEntries, selectedFilters, "duration"),
    [descriptorEntries, selectedFilters],
  );
  const versions = useMemo(
    () => getFilteredValues(descriptorEntries, selectedFilters, "version"),
    [descriptorEntries, selectedFilters],
  );
  const locationOptions = useMemo(
    () =>
      Object.keys(aliases.data || {}).map((key) => ({
        value: key,
        label: `${aliases.data[key].office} / ${
          aliases.data[key].publicName || aliases.data[key].name
        }`,
        location: aliases.data[key],
      })),
    [aliases.data],
  );
  const filteredLocationOptions = useMemo(() => {
    const search = debouncedLocationSearchTerm.trim().toLowerCase();
    if (search.length < 2) return [];

    return locationOptions
      .filter((option) => {
        const location = option.location;
        const searchableText = [
          option.label,
          location.name,
          location.office,
          location.publicName,
          JSON.stringify(location.aliases || []),
        ]
          .filter(Boolean)
          .join(" ")
          .toLowerCase();
        return searchableText.includes(search);
      })
      .slice(0, 100);
  }, [debouncedLocationSearchTerm, locationOptions]);
  const selectedLocationOption = useMemo(
    () =>
      locationOptions.find((option) => option.value === locationKey) ||
      (selectedLocation
        ? {
            value: locationKey,
            label: `${selectedLocation.office} / ${
              selectedLocation.publicName || selectedLocation.name
            }`,
            location: selectedLocation,
          }
        : null),
    [locationKey, locationOptions, selectedLocation],
  );
  const hasSelectedLocation = Boolean(location);
  const hasNoTimeseries =
    hasSelectedLocation && !descriptors.isLoading && descriptors.data?.count === 0;

  useEffect(() => {
    setParameter("");
    setType("");
    setInterval("");
    setDuration("");
    setVersion("");
    setClearedFields({});
  }, [location]);

  useEffect(() => {
    if (descriptors.isLoading) return;
    if (!clearedFields.parameter && parameters.length === 1 && !parameter) {
      setParameter(parameters[0]);
    } else if (parameter && !parameters.includes(parameter)) {
      setParameter("");
    }
  }, [clearedFields.parameter, descriptors.isLoading, parameter, parameters]);

  useEffect(() => {
    if (descriptors.isLoading) return;
    if (!clearedFields.type && types.length === 1 && !type) {
      setType(types[0]);
    } else if (type && !types.includes(type)) {
      setType("");
    }
  }, [clearedFields.type, descriptors.isLoading, type, types]);

  useEffect(() => {
    if (descriptors.isLoading) return;
    if (!clearedFields.interval && intervals.length === 1 && !interval) {
      setInterval(intervals[0]);
    } else if (interval && !intervals.includes(interval)) {
      setInterval("");
    }
  }, [clearedFields.interval, descriptors.isLoading, interval, intervals]);

  useEffect(() => {
    if (descriptors.isLoading) return;
    if (!clearedFields.duration && durations.length === 1 && !duration) {
      setDuration(durations[0]);
    } else if (duration && !durations.includes(duration)) {
      setDuration("");
    }
  }, [clearedFields.duration, descriptors.isLoading, duration, durations]);

  useEffect(() => {
    if (descriptors.isLoading) return;
    if (!clearedFields.version && versions.length === 1 && !version) {
      setVersion(versions[0]);
    } else if (version && !versions.includes(version)) {
      setVersion("");
    }
  }, [clearedFields.version, descriptors.isLoading, version, versions]);

  // Compose TSID
  useEffect(() => {
    const parts = [location, parameter, type, interval, duration, version];
    if (parts.every(Boolean)) {
      const fullTsid =
        location +
        "." +
        parameter +
        "." +
        type +
        "." +
        interval +
        "." +
        duration +
        "." +
        version;
      const tsidOffice = selectedLocation?.office || office;
      setTsids((prev) => (prev.includes(fullTsid) ? prev : [...prev, fullTsid]));
      if (tsidOffice) {
        setTsidOffices?.((current) => ({
          ...current,
          [fullTsid]: tsidOffice,
        }));
      }
    }
  }, [
    location,
    parameter,
    type,
    interval,
    duration,
    version,
    office,
    selectedLocation?.office,
    setTsidOffices,
    setTsids,
  ]);

  const errors = [aliases.error, descriptors.error].filter(Boolean);
  const setDescriptorValue = (field, setter, value) => {
    setter(value);
    setClearedFields((current) => ({
      ...current,
      [field]: value === "",
    }));
  };
  const clearLocationSelection = () => {
    setLocationKey("");
    setLocationSearchTerm("");
    setSelectedLocation(null);
    setOffice?.("");
  };

  if (errors.length > 0) {
    return (
      <div className="mt-5 flex flex-col gap-2">
        <H4>Failed to load Timeseries metadata</H4>
        <Badge color="red">
          {errors.map((e, i) => (
            <div key={i}>{e.message}</div>
          ))}
        </Badge>
      </div>
    );
  }

  return (
    <div className="flex flex-wrap gap-4 my-4">
      <Dropdown
        label="Location Kind"
        value={locationKind}
        onChange={(value) => {
          setLocationKind(value);
          setLocationKey("");
          setLocationSearchTerm("");
          setSelectedLocation(null);
          setOffice?.("");
        }}
        options={[
          { value: "*", label: "All" },
          { value: "PROJECT", label: "Project" },
          { value: "LOCK", label: "Lock" },
          { value: "GAGE", label: "Gage" },
          { value: "SITE", label: "Site" },
          { value: "OUTLET", label: "Outlet" },
        ]}
        helpText={TSID_PART_HELP["Location Kind"]}
        loading={aliases.isLoading}
      />
      <LocationCombobox
        disabled={!locationKind}
        helpText={TSID_PART_HELP.Location}
        locationKind={locationKind}
        loading={aliases.isLoading}
        onClear={clearLocationSelection}
        onChange={(option) => {
          const selected = option?.location;
          setSelectedLocation(selected || null);
          setLocationKey(option?.value || "");
          setLocationSearchTerm(option?.label || "");
          setOffice?.(selected?.office || "");
        }}
        onSearchChange={(value) => {
          setLocationSearchTerm(value);
          if (selectedLocationOption && value !== selectedLocationOption.label) {
            setSelectedLocation(null);
            setLocationKey("");
            setOffice?.("");
          }
        }}
        noOptionsMessage={
          locationKind
            ? "No locations found for this kind."
            : "Choose a location kind to load locations."
        }
        options={filteredLocationOptions}
        searchTerm={locationSearchTerm}
        value={selectedLocationOption}
      />
      <Dropdown
        label="Parameter"
        value={parameter}
        onChange={(value) => setDescriptorValue("parameter", setParameter, value)}
        options={getOptions(parameters)}
        disabled={!hasSelectedLocation || hasNoTimeseries}
        helpText={TSID_PART_HELP.Parameter}
        loading={descriptors.isLoading}
        noOptionsMessage="No parameters found for this location."
      />
      <Dropdown
        label="Type"
        value={type}
        onChange={(value) => setDescriptorValue("type", setType, value)}
        options={getOptions(types)}
        disabled={!hasSelectedLocation || hasNoTimeseries}
        helpText={TSID_PART_HELP.Type}
        loading={descriptors.isLoading}
        noOptionsMessage="No types found for this selection."
      />
      <Dropdown
        label="Interval"
        value={interval}
        onChange={(value) => setDescriptorValue("interval", setInterval, value)}
        options={getOptions(intervals)}
        disabled={!hasSelectedLocation || hasNoTimeseries}
        helpText={TSID_PART_HELP.Interval}
        loading={descriptors.isLoading}
        noOptionsMessage="No intervals found for this selection."
      />
      <Dropdown
        label="Duration"
        value={duration}
        onChange={(value) => setDescriptorValue("duration", setDuration, value)}
        options={getOptions(durations)}
        disabled={!hasSelectedLocation || hasNoTimeseries}
        helpText={TSID_PART_HELP.Duration}
        loading={descriptors.isLoading}
        noOptionsMessage="No durations found for this selection."
      />
      <Dropdown
        label="Version"
        value={version}
        onChange={(value) => setDescriptorValue("version", setVersion, value)}
        options={getOptions(versions)}
        disabled={!hasSelectedLocation || hasNoTimeseries}
        helpText={TSID_PART_HELP.Version}
        loading={descriptors.isLoading}
        noOptionsMessage="No versions found for this selection."
      />
      {hasNoTimeseries && (
        <div className="basis-full rounded border border-amber-300 bg-amber-50 p-3 text-sm text-amber-950">
          No time series were found for {selectedLocation.publicName || location}.
          {includeMissingTimeseries
            ? " The catalog also did not return missing time series for this location."
            : " Turn on Include missing time series in Query Settings to show catalog entries without current data."}
        </div>
      )}
    </div>
  );
}

function LabelWithHelp({ helpText, label }) {
  const [helpOpen, setHelpOpen] = useState(false);

  return (
    <>
      <div className="mb-1 flex items-center gap-1">
        <label className="text-sm font-medium">{label}</label>
        {helpText && (
          <button
            type="button"
            aria-expanded={helpOpen}
            aria-label={`${label} help`}
            className="rounded text-slate-500 hover:text-slate-800 focus:outline-none focus:ring-2 focus:ring-indigo-500"
            onClick={() => setHelpOpen((current) => !current)}
            title={`${label} help`}
          >
            <FiHelpCircle aria-hidden="true" size={14} />
          </button>
        )}
      </div>
      {helpOpen && helpText && (
        <div className="mb-2 max-w-64 rounded border border-slate-200 bg-slate-50 p-2 text-xs text-slate-700 shadow-sm">
          {helpText}
        </div>
      )}
    </>
  );
}

LabelWithHelp.propTypes = {
  helpText: PropTypes.string,
  label: PropTypes.string.isRequired,
};

function LocationCombobox({
  disabled,
  helpText,
  loading,
  locationKind,
  noOptionsMessage,
  onClear,
  onChange,
  onSearchChange,
  options,
  searchTerm,
  value,
}) {
  const hasSearch = searchTerm.trim().length >= 2;
  const showNoOptionsMessage =
    !loading && (!locationKind || (hasSearch && !options.length));

  return (
    <div className="flex min-w-[280px] flex-col">
      <LabelWithHelp helpText={helpText} label="Location" />
      <Combobox value={value} onChange={onChange} disabled={disabled}>
        <div className="relative">
          <ComboboxInput
            className={`w-full rounded border border-gray-300 py-1 pl-3 pr-9 ${
              disabled ? "cursor-not-allowed bg-gray-100" : ""
            }`}
            displayValue={(option) => option?.label || searchTerm}
            onChange={(event) => onSearchChange(event.target.value)}
            placeholder={
              locationKind
                ? "Search locations by name or ID"
                : "Choose location kind first"
            }
          />
          {(value || searchTerm) && !disabled && (
            <button
              type="button"
              aria-label="Clear location"
              title="Clear location"
              className="absolute inset-y-0 right-1 my-auto inline-flex h-7 w-7 items-center justify-center rounded text-slate-500 hover:bg-slate-100 hover:text-slate-800 focus:outline-none focus:ring-2 focus:ring-indigo-500"
              onClick={onClear}
            >
              <FiX aria-hidden="true" size={16} />
            </button>
          )}
        </div>
        <ComboboxOptions className="z-20 mt-1 max-h-72 overflow-auto rounded border border-slate-200 bg-white shadow-lg">
          {loading ? (
            <div className="p-2 text-sm italic text-slate-500">
              Searching locations...
            </div>
          ) : hasSearch && options.length ? (
            options.map((option) => (
              <ComboboxOption key={option.value} value={option}>
                {({ active }) => (
                  <div
                    className={`cursor-pointer px-3 py-2 text-sm ${
                      active ? "bg-blue-100" : ""
                    }`}
                  >
                    <div className="font-medium">{option.label}</div>
                    <div className="text-xs text-slate-500">
                      {option.location.office} / {option.location.name}
                    </div>
                  </div>
                )}
              </ComboboxOption>
            ))
          ) : hasSearch ? (
            <div className="p-2 text-sm text-slate-500">No locations found.</div>
          ) : (
            <div className="p-2 text-sm text-slate-500">
              Type at least 2 characters to search.
            </div>
          )}
        </ComboboxOptions>
      </Combobox>
      {showNoOptionsMessage && noOptionsMessage && (
        <span className="mt-1 text-xs text-slate-500">{noOptionsMessage}</span>
      )}
    </div>
  );
}

LocationCombobox.propTypes = {
  disabled: PropTypes.bool,
  helpText: PropTypes.string,
  loading: PropTypes.bool,
  locationKind: PropTypes.string.isRequired,
  noOptionsMessage: PropTypes.string,
  onClear: PropTypes.func.isRequired,
  onChange: PropTypes.func.isRequired,
  onSearchChange: PropTypes.func.isRequired,
  options: PropTypes.arrayOf(
    PropTypes.shape({
      label: PropTypes.string.isRequired,
      location: PropTypes.shape({
        name: PropTypes.string.isRequired,
        office: PropTypes.string.isRequired,
      }).isRequired,
      value: PropTypes.string.isRequired,
    }),
  ).isRequired,
  searchTerm: PropTypes.string.isRequired,
  value: PropTypes.shape({
    label: PropTypes.string.isRequired,
    value: PropTypes.string.isRequired,
  }),
};

function Dropdown({
  label,
  value,
  onChange,
  options,
  disabled = false,
  helpText,
  loading = false,
  noOptionsMessage,
}) {
  const hasOptions = options.length > 0;
  const showNoOptionsMessage = !loading && !disabled && !hasOptions;

  return (
    <div className="flex flex-col min-w-[150px]">
      <LabelWithHelp helpText={helpText} label={label} />
      <select
        disabled={disabled || loading || !hasOptions}
        className={`px-3 py-1 rounded border border-gray-300 ${
          disabled || loading || !hasOptions ? "bg-gray-100 cursor-not-allowed" : ""
        }`}
        value={value}
        onChange={(e) => onChange(e.target.value)}
      >
        <option value="">
          {loading
            ? `Loading ${label}...`
            : hasOptions
              ? `Select ${label}`
              : `No ${label.toLowerCase()} options`}
        </option>
        {!loading &&
          options.map((opt) => (
            <option key={opt.value} value={opt.value}>
              {opt.label}
            </option>
          ))}
      </select>
      {showNoOptionsMessage && noOptionsMessage && (
        <span className="mt-1 text-xs text-slate-500">{noOptionsMessage}</span>
      )}
    </div>
  );
}

Dropdown.propTypes = {
  label: PropTypes.string.isRequired,
  value: PropTypes.string.isRequired,
  onChange: PropTypes.func.isRequired,
  options: PropTypes.arrayOf(
    PropTypes.shape({
      value: PropTypes.string.isRequired,
      label: PropTypes.string.isRequired,
    }),
  ).isRequired,
  disabled: PropTypes.bool,
  helpText: PropTypes.string,
  loading: PropTypes.bool,
  noOptionsMessage: PropTypes.string,
};

TimeSeriesBuilder.propTypes = {
  includeMissingTimeseries: PropTypes.bool.isRequired,
  office: PropTypes.string.isRequired,
  setOffice: PropTypes.func,
  setTsidOffices: PropTypes.func,
  setTsids: PropTypes.func.isRequired,
};
