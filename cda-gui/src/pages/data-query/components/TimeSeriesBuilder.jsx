import { useState, useEffect } from "react";
import { Badge, H4, Skeleton } from "@usace/groundwork";
import useAliases from "../hooks/useAliases";
import useDescriptors from "../hooks/useDescriptors";
import PropTypes from "prop-types";

export default function TimeSeriesBuilder({ office, setTsids }) {
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
      />
      <Dropdown
        label="Parameter"
        value={parameter}
        onChange={setParameter}
        options={(descriptors.data?.parameters || []).map((p) => ({
          value: p,
          label: p,
        }))}
        loading={descriptors.isLoading}
      />
      <Dropdown
        label="Type"
        value={type}
        onChange={setType}
        options={(descriptors.data?.types || []).map((t) => ({
          value: t,
          label: t,
        }))}
        loading={descriptors.isLoading}
      />
      <Dropdown
        label="Interval"
        value={interval}
        onChange={setInterval}
        options={(descriptors.data?.intervals || []).map((i) => ({
          value: i,
          label: i,
        }))}
        loading={descriptors.isLoading}
      />
      <Dropdown
        label="Duration"
        value={duration}
        onChange={setDuration}
        options={(descriptors.data?.durations || []).map((d) => ({
          value: d,
          label: d,
        }))}
        loading={descriptors.isLoading}
      />
      <Dropdown
        label="Version"
        value={version}
        onChange={setVersion}
        options={(descriptors.data?.versions || []).map((v) => ({
          value: v,
          label: v,
        }))}
        loading={descriptors.isLoading}
      />
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
}) {
  return (
    <div className="flex flex-col min-w-[150px]">
      <label className="text-sm font-medium mb-1">{label}</label>
      <select
        disabled={disabled || loading}
        className={`px-3 py-1 rounded border border-gray-300 ${
          disabled || loading ? "bg-gray-100 cursor-not-allowed" : ""
        }`}
        value={value}
        onChange={(e) => onChange(e.target.value)}
      >
        <option value="">{loading ? `Loading ${label}...` : `Select ${label}`}</option>
        {!loading &&
          options.map((opt) => (
            <option key={opt.value} value={opt.value}>
              {opt.label}
            </option>
          ))}
      </select>
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
};

TimeSeriesBuilder.propTypes = {
  office: PropTypes.string.isRequired,
  setTsids: PropTypes.func.isRequired,
};
