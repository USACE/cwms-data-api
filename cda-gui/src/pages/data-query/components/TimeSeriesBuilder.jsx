import { useState, useEffect } from "react";
import { Badge, H4, Skeleton } from "@usace/groundwork";
import useAliases from "../hooks/useAliases";
import useParams from "../hooks/useParams";
import useDescriptors from "../hooks/useDescriptors";

export default function TimeSeriesBuilder({ office, setTsids }) {
  const aliases = useAliases({ office, kind: "PROJECT" });

  const [location, setLocation] = useState("");
  const [parameter, setParameter] = useState("");
  const [type, setType] = useState("");
  const [interval, setInterval] = useState("");
  const [duration, setDuration] = useState("");
  const [version, setVersion] = useState("");

  const descriptors = useDescriptors({
    office,
    location,
    parameter,
    type,
    interval,
    duration,
  });

  // Reset fields when an earlier selection changes
  useEffect(() => {
    setParameter("");
    setType("");
    setInterval("");
    setDuration("");
    setVersion("");
  }, [location]);

  useEffect(() => {
    setType("");
    setInterval("");
    setDuration("");
    setVersion("");
  }, [parameter]);

  useEffect(() => {
    setInterval("");
    setDuration("");
    setVersion("");
  }, [type]);

  useEffect(() => {
    setDuration("");
    setVersion("");
  }, [interval]);

  useEffect(() => {
    setVersion("");
  }, [duration]);

  // Compose TSID
  useEffect(() => {
    const parts = [location, parameter, type, interval, duration, version];
    if (parts.every(Boolean)) {
      setTsids([parts.join(".")]);
    }
  }, [location, parameter, type, interval, duration, version]);

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
        label="Location"
        value={location}
        onChange={setLocation}
        options={Object.keys(aliases.data || {}).map((key) => ({
          value: key,
          label: aliases.data[key].publicName,
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
        disabled={!location}
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
        disabled={!parameter}
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
        disabled={!type}
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
        disabled={!interval}
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
        disabled={!duration}
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
        <option value="">
          {loading ? `Loading ${label}...` : `Select ${label}`}
        </option>
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
