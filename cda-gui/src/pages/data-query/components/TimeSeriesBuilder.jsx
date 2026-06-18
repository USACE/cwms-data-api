import { useState, useEffect, useMemo } from "react";
import { Badge, H4 } from "@usace/groundwork";
import useAliases from "../hooks/useAliases";
import useDescriptors from "../hooks/useDescriptors";
import PropTypes from "prop-types";
import { FiHelpCircle } from "react-icons/fi";

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
  setTsids,
}) {
  const [locationKind, setLocationKind] = useState("");
  const aliases = useAliases({
    office,
    kind: locationKind,
    props: { enabled: Boolean(locationKind) },
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
      setTsids((prev) => (prev.includes(fullTsid) ? prev : [...prev, fullTsid]));
    }
  }, [location, parameter, type, interval, duration, version, setTsids]);

  const errors = [aliases.error, descriptors.error].filter(Boolean);
  const setDescriptorValue = (field, setter, value) => {
    setter(value);
    setClearedFields((current) => ({
      ...current,
      [field]: value === "",
    }));
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
      <Dropdown
        label="Location"
        value={locationKey}
        onChange={(value) => {
          const selected = aliases.data?.[value];
          setSelectedLocation(selected || null);
          setLocationKey(value);
          setOffice?.(selected?.office || "");
        }}
        options={Object.keys(aliases.data || {}).map((key) => ({
          value: key,
          label: office
            ? aliases.data[key].publicName || aliases.data[key].name
            : `${aliases.data[key].office} / ${
                aliases.data[key].publicName || aliases.data[key].name
              }`,
        }))}
        helpText={TSID_PART_HELP.Location}
        loading={aliases.isLoading}
        noOptionsMessage={
          locationKind
            ? "No locations found for this office and kind."
            : "Choose a location kind to load locations."
        }
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
  const [helpOpen, setHelpOpen] = useState(false);
  const showNoOptionsMessage = !loading && !disabled && !hasOptions;

  return (
    <div className="flex flex-col min-w-[150px]">
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
  setTsids: PropTypes.func.isRequired,
};
