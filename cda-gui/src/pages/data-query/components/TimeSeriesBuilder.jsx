import { useState, useEffect, useMemo } from "react";
import { Badge, H4, Skeleton } from "@usace/groundwork";
import useAliases from "../hooks/useAliases";
import useDescriptors from "../hooks/useDescriptors";
import PropTypes from "prop-types";

function getOptions(values = []) {
  return values.map((value) => ({
    value,
    label: value,
  }));
}

export default function TimeSeriesBuilder({
  includeMissingTimeseries,
  office,
  setTsids,
}) {
  const [locationKind, setLocationKind] = useState("*");
  const aliases = useAliases({ office, kind: locationKind });

  const [locationKey, setLocationKey] = useState("");
  const [parameter, setParameter] = useState("");
  const [type, setType] = useState("");
  const [interval, setInterval] = useState("");
  const [duration, setDuration] = useState("");
  const [version, setVersion] = useState("");
  const [selectedLocation, setSelectedLocation] = useState(null);
  const location = selectedLocation?.name || "";

  const descriptors = useDescriptors({
    includeMissingTimeseries,
    office,
    location,
    parameter,
    type,
    interval,
    duration,
  });

  useEffect(() => {
    if (office && selectedLocation?.office && selectedLocation.office !== office) {
      setLocationKey("");
      setSelectedLocation(null);
    }
  }, [office, selectedLocation]);

  const parameters = useMemo(
    () => descriptors.data?.parameters || [],
    [descriptors.data?.parameters],
  );
  const types = useMemo(() => descriptors.data?.types || [], [descriptors.data?.types]);
  const intervals = useMemo(
    () => descriptors.data?.intervals || [],
    [descriptors.data?.intervals],
  );
  const durations = useMemo(
    () => descriptors.data?.durations || [],
    [descriptors.data?.durations],
  );
  const versions = useMemo(
    () => descriptors.data?.versions || [],
    [descriptors.data?.versions],
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
  }, [location]);

  useEffect(() => {
    if (descriptors.isLoading) return;
    if (parameters.length === 1 && parameter !== parameters[0]) {
      setParameter(parameters[0]);
    } else if (parameter && !parameters.includes(parameter)) {
      setParameter("");
    }
  }, [descriptors.isLoading, parameter, parameters]);

  useEffect(() => {
    if (descriptors.isLoading) return;
    if (types.length === 1 && type !== types[0]) {
      setType(types[0]);
    } else if (type && !types.includes(type)) {
      setType("");
    }
  }, [descriptors.isLoading, type, types]);

  useEffect(() => {
    if (descriptors.isLoading) return;
    if (intervals.length === 1 && interval !== intervals[0]) {
      setInterval(intervals[0]);
    } else if (interval && !intervals.includes(interval)) {
      setInterval("");
    }
  }, [descriptors.isLoading, interval, intervals]);

  useEffect(() => {
    if (descriptors.isLoading) return;
    if (durations.length === 1 && duration !== durations[0]) {
      setDuration(durations[0]);
    } else if (duration && !durations.includes(duration)) {
      setDuration("");
    }
  }, [descriptors.isLoading, duration, durations]);

  useEffect(() => {
    if (descriptors.isLoading) return;
    if (versions.length === 1 && version !== versions[0]) {
      setVersion(versions[0]);
    } else if (version && !versions.includes(version)) {
      setVersion("");
    }
  }, [descriptors.isLoading, version, versions]);

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
  if (aliases.isLoading) {
    return (
      <div className="flex flex-col gap-2">
        <H4>Loading Location Aliases...</H4>
        <Skeleton className="w-full h-10" />
      </div>
    );
  }
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
        }}
        options={[
          { value: "*", label: "All" },
          { value: "PROJECT", label: "Project" },
          { value: "LOCK", label: "Lock" },
          { value: "GAGE", label: "Gage" },
          { value: "SITE", label: "Site" },
          { value: "OUTLET", label: "Outlet" },
        ]}
        loading={aliases.isLoading}
      />
      <Dropdown
        label="Location"
        value={locationKey}
        onChange={(value) => {
          const selected = aliases.data?.[value];
          setSelectedLocation(selected || null);
          setLocationKey(value);
        }}
        options={Object.keys(aliases.data || {}).map((key) => ({
          value: key,
          label: office
            ? aliases.data[key].publicName || aliases.data[key].name
            : `${aliases.data[key].office} / ${
                aliases.data[key].publicName || aliases.data[key].name
              }`,
        }))}
        loading={aliases.isLoading}
        noOptionsMessage="No locations found for this office and kind."
      />
      <Dropdown
        label="Parameter"
        value={parameter}
        onChange={(value) => {
          setParameter(value);
          setType("");
          setInterval("");
          setDuration("");
          setVersion("");
        }}
        options={getOptions(parameters)}
        disabled={!hasSelectedLocation || hasNoTimeseries}
        loading={descriptors.isLoading}
        noOptionsMessage="No parameters found for this location."
      />
      <Dropdown
        label="Type"
        value={type}
        onChange={(value) => {
          setType(value);
          setInterval("");
          setDuration("");
          setVersion("");
        }}
        options={getOptions(types)}
        disabled={!hasSelectedLocation || hasNoTimeseries}
        loading={descriptors.isLoading}
        noOptionsMessage="No types found for this selection."
      />
      <Dropdown
        label="Interval"
        value={interval}
        onChange={(value) => {
          setInterval(value);
          setDuration("");
          setVersion("");
        }}
        options={getOptions(intervals)}
        disabled={!hasSelectedLocation || hasNoTimeseries}
        loading={descriptors.isLoading}
        noOptionsMessage="No intervals found for this selection."
      />
      <Dropdown
        label="Duration"
        value={duration}
        onChange={(value) => {
          setDuration(value);
          setVersion("");
        }}
        options={getOptions(durations)}
        disabled={!hasSelectedLocation || hasNoTimeseries}
        loading={descriptors.isLoading}
        noOptionsMessage="No durations found for this selection."
      />
      <Dropdown
        label="Version"
        value={version}
        onChange={setVersion}
        options={getOptions(versions)}
        disabled={!hasSelectedLocation || hasNoTimeseries}
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
  loading = false,
  noOptionsMessage,
}) {
  const hasOptions = options.length > 0;
  const showNoOptionsMessage = !loading && !disabled && !hasOptions;

  return (
    <div className="flex flex-col min-w-[150px]">
      <label className="text-sm font-medium mb-1">{label}</label>
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
  loading: PropTypes.bool,
  noOptionsMessage: PropTypes.string,
};

TimeSeriesBuilder.propTypes = {
  includeMissingTimeseries: PropTypes.bool.isRequired,
  office: PropTypes.string.isRequired,
  setTsids: PropTypes.func.isRequired,
};
