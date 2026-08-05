import { UsaceBox, Skeleton, Badge, H3, Button } from "@usace/groundwork";
import { useState, useMemo, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import dayjs from "dayjs";
import PropTypes from "prop-types";
import { FiDownload, FiRefreshCw } from "react-icons/fi";
import Controls from "./components/Controls";
import { CatalogApi, Configuration, OfficesApi, TimeSeriesApi } from "cwmsjs";
import { getPrecision } from "../../utils/timeseries";
// import useConfigList from "./hooks/useConfigList";
import TimeSeriesDropdown from "./components/TimeSeriesDropdown";
import DataTabs from "./components/DataTabs";
import TimeSeriesBuilder from "./components/TimeSeriesBuilder";
import TimeSeriesManager from "./components/TimeSeriesManager";
import SettingsMenu from "./components/SettingsMenu";
import { buildCsvContent, buildTableRows, downloadBlob } from "./utils/tableData";
const CDA_DATE_FORMAT = "YYYY-MM-DDTHH:mm:ssZ";

const v2_config = new Configuration({
  basePath: import.meta.env.VITE_CDA_API_ROOT,
  headers: {
    accept: "application/json;version=2",
  },
});
const ts_api = new TimeSeriesApi(v2_config);
const offices_api = new OfficesApi(v2_config);
const catalog_api = new CatalogApi(v2_config);
const DATA_QUERY_CACHE_KEY = "data-query-cache-enabled";
const DATA_QUERY_SORT_ASC_KEY = "data-query-sort-ascending";
const DATA_QUERY_INCLUDE_MISSING_TS_KEY = "data-query-include-missing-timeseries";
const DEFAULT_CACHE_ENABLED = true;
const DEFAULT_SORT_ASCENDING = false;
const DEFAULT_INCLUDE_MISSING_TIMESERIES = false;
const DEFAULT_LOOKBACK = { amount: 1, unit: "day" };
const CHUNK_PAGE_SIZE = 5000;
const TARGET_VALUES_PER_CHUNK = 3000;
const MAX_CHUNK_DAYS = 365;
const MAX_PARALLEL_CHUNK_REQUESTS = 6;
const MINIMUM_INTERVAL_LOOKBACKS = {
  minute: DEFAULT_LOOKBACK,
  hour: DEFAULT_LOOKBACK,
};
const INTERVAL_MINUTES = {
  minute: 1,
  hour: 60,
  day: 60 * 24,
  week: 60 * 24 * 7,
  month: 60 * 24 * 30,
  year: 60 * 24 * 365,
};

function summarizeTimeseries(timeseriesList) {
  return timeseriesList.reduce(
    (summary, series) => {
      if (series?.values?.length) {
        summary.tsids.push({
          name: series.name,
          office: series.office,
          units: series.units,
        });
        summary.hasValues = true;
      } else if (series?.name) {
        summary.failed.push(series.name);
      }
      return summary;
    },
    { failed: [], hasValues: false, tsids: [] },
  );
}

function parseInterval(interval) {
  if (!interval || interval === "0") return { amount: 1, unit: "hour" };

  const match = interval.replace(/^~/, "").match(/^(\d+)([A-Za-z]+)$/);
  if (!match) return { amount: 1, unit: "hour" };

  return {
    amount: Number(match[1]),
    unit: match[2].toLowerCase().replace(/s$/, ""),
  };
}

function getLookbackForInterval(interval) {
  if (!interval || interval === "0") return DEFAULT_LOOKBACK;

  const { amount, unit } = parseInterval(interval);

  return MINIMUM_INTERVAL_LOOKBACKS[unit] || { amount, unit };
}

function getIntervalMinutes(tsid) {
  const interval = tsid.split(".")[3];
  const { amount, unit } = parseInterval(interval);
  return amount * (INTERVAL_MINUTES[unit] || INTERVAL_MINUTES.hour);
}

function getLookbackForTsids(tsids, endDateTime) {
  return tsids.reduce(
    (earliestBegin, tsid) => {
      const interval = tsid.split(".")[3];
      const lookback = getLookbackForInterval(interval);
      const begin = endDateTime.subtract(lookback.amount, lookback.unit);
      return begin.isBefore(earliestBegin) ? begin : earliestBegin;
    },
    endDateTime.subtract(DEFAULT_LOOKBACK.amount, DEFAULT_LOOKBACK.unit),
  );
}

function getChunkMinutes(tsid) {
  const estimatedIntervalMinutes = getIntervalMinutes(tsid);
  return Math.min(
    estimatedIntervalMinutes * TARGET_VALUES_PER_CHUNK,
    MAX_CHUNK_DAYS * INTERVAL_MINUTES.day,
  );
}

function getDateChunks(tsid, beginDateTime, endDateTime) {
  const chunkMinutes = getChunkMinutes(tsid);
  const chunks = [];
  let chunkEnd = endDateTime;

  while (chunkEnd.isAfter(beginDateTime)) {
    let chunkBegin = chunkEnd.subtract(chunkMinutes, "minute");
    if (chunkBegin.isBefore(beginDateTime)) chunkBegin = beginDateTime;
    chunks.unshift({ begin: chunkBegin, end: chunkEnd });
    chunkEnd = chunkBegin;
  }

  return chunks;
}

function getLoadPlan(
  tsids,
  beginDateTime,
  endDateTime,
  extentsByTsid = {},
  tsidOffices = {},
  useExtentStartDates = true,
) {
  const items = tsids.map((tsid) => {
    const extentBegin = extentsByTsid[tsid]?.earliestDateTime;
    const effectiveBegin =
      useExtentStartDates &&
      extentBegin?.isValid() &&
      extentBegin.isAfter(beginDateTime)
        ? extentBegin
        : beginDateTime;
    const chunks = effectiveBegin.isBefore(endDateTime)
      ? getDateChunks(tsid, effectiveBegin, endDateTime)
      : [];

    return {
      tsid,
      office: tsidOffices[tsid],
      effectiveBegin,
      chunks,
    };
  });

  return {
    items,
    totalChunks: items.reduce((total, item) => total + item.chunks.length, 0),
  };
}

async function runLimited(tasks, limit) {
  const results = [];
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < tasks.length) {
      const taskIndex = nextIndex;
      nextIndex += 1;
      results[taskIndex] = await tasks[taskIndex]();
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(limit, tasks.length) }, () => worker()),
  );
  return results;
}

function mergeTimeSeriesChunks(name, begin, end, chunks) {
  const valuesByTime = new Map();
  let units;
  let office;
  let total = 0;

  chunks.forEach((chunk) => {
    units ||= chunk?.units;
    office ||= chunk?.office || chunk?.officeId || chunk?.["office-id"];
    total += chunk?.total || 0;
    chunk?.values?.forEach((value) => {
      valuesByTime.set(value[0], value);
    });
  });

  return {
    begin,
    end,
    values: [...valuesByTime.values()].sort((a, b) => a[0] - b[0]),
    name,
    office,
    units,
    total,
  };
}

function getExtentEarliestTime(extent) {
  return extent?.earliestTime || extent?.["earliest-time"];
}

function formatFriendlyDate(dateTime) {
  return dateTime.format("MMM D, YYYY h:mm A");
}

function getProgressPercent(progress) {
  if (!progress?.total) return 0;
  return Math.round((progress.completed / progress.total) * 100);
}

function getSeriesMessage(series) {
  if (!series) return "Waiting for data";
  if (series.message) {
    try {
      const parsedMessage = JSON.parse(series.message);
      const message = String(
        parsedMessage.message ||
          parsedMessage.reason ||
          parsedMessage.error ||
          series.message,
      );
      return message.length > 180 ? `${message.slice(0, 177)}...` : message;
    } catch {
      return series.message.length > 180
        ? `${series.message.slice(0, 177)}...`
        : series.message;
    }
  }
  if (!series.values?.length) return "No values found for the selected date range";
  return `${series.values.length.toLocaleString()} values loaded`;
}

function QueryProgress({ progress }) {
  const percent = getProgressPercent(progress);

  return (
    <div className="mt-4 rounded border border-slate-200 bg-white p-4 shadow-sm">
      <div className="mb-2 flex flex-wrap items-center justify-between gap-2 text-sm font-semibold text-slate-700">
        <span>Loading time series data</span>
        <span>
          {progress.completed} / {progress.total}
        </span>
      </div>
      <div className="h-3 w-full overflow-hidden rounded bg-slate-200">
        <div
          className="h-full bg-blue-600 transition-all"
          style={{ width: `${percent}%` }}
        />
      </div>
      <div className="mt-3 grid gap-2 text-xs text-slate-600">
        {progress.byTsid.map((item) => {
          const itemPercent = item.total
            ? Math.round((item.completed / item.total) * 100)
            : 100;
          return (
            <div key={item.tsid}>
              <div className="mb-1">
                <span className="truncate">{item.tsid}</span>
              </div>
              <div className="relative h-4 overflow-hidden rounded bg-slate-100">
                <div
                  className="h-full bg-emerald-500 transition-all"
                  style={{ width: `${itemPercent}%` }}
                />
                <div className="absolute inset-0 flex items-center justify-center text-[10px] font-semibold text-slate-700">
                  {itemPercent}%
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function ModeSelector({ mode, setMode, setOffice }) {
  const setQueryMode = (nextMode) => {
    setMode(nextMode);
    if (nextMode === "guided") setOffice("");
  };

  return (
    <div className="flex flex-col gap-2">
      <div className="text-sm font-semibold text-slate-700">Query mode</div>
      <div className="inline-flex w-fit rounded border border-slate-300 bg-white p-1 shadow-sm">
        {[
          {
            description: "Build a TSID from catalog filters.",
            label: "Guided",
            value: "guided",
          },
          {
            description: "Search or paste a known TSID.",
            label: "Manual",
            value: "manual",
          },
        ].map((option) => {
          const selected = mode === option.value;
          return (
            <button
              key={option.value}
              type="button"
              aria-pressed={selected}
              className={`rounded px-4 py-2 text-left text-sm font-semibold transition ${
                selected
                  ? "bg-indigo-600 text-white shadow"
                  : "text-slate-700 hover:bg-slate-100"
              }`}
              onClick={() => setQueryMode(option.value)}
              title={option.description}
            >
              {option.label}
            </button>
          );
        })}
      </div>
    </div>
  );
}

ModeSelector.propTypes = {
  mode: PropTypes.oneOf(["guided", "manual"]).isRequired,
  setMode: PropTypes.func.isRequired,
  setOffice: PropTypes.func.isRequired,
};

function OfficeSelect({ office, offices, setOffice, setTsidOffices, setTsids }) {
  return (
    <div className="flex flex-col gap-1 sm:w-56">
      <label htmlFor="office" className="text-sm font-semibold text-slate-700">
        Office
      </label>
      <select
        id="office"
        value={office}
        disabled={offices.isLoading}
        onChange={(e) => {
          const _office = e.target.value;
          if (!_office) {
            setOffice("");
            setTsidOffices({});
            setTsids([]);
            return;
          }
          setOffice(_office);
          setTsidOffices({});
          setTsids([]);
        }}
        className="min-w-[150px] rounded border border-slate-300 px-3 py-2 disabled:cursor-not-allowed disabled:bg-slate-100"
      >
        <option key="select" value="">
          {offices.isLoading ? "Loading offices..." : "Select Office"}
        </option>
        {offices.data?.map((key) => (
          <option key={key} value={key}>
            {key}
          </option>
        ))}
      </select>
    </div>
  );
}

OfficeSelect.propTypes = {
  office: PropTypes.string.isRequired,
  offices: PropTypes.shape({
    data: PropTypes.arrayOf(PropTypes.string),
    isLoading: PropTypes.bool.isRequired,
  }).isRequired,
  setOffice: PropTypes.func.isRequired,
  setTsidOffices: PropTypes.func.isRequired,
  setTsids: PropTypes.func.isRequired,
};

QueryProgress.propTypes = {
  progress: PropTypes.shape({
    byTsid: PropTypes.arrayOf(
      PropTypes.shape({
        completed: PropTypes.number.isRequired,
        total: PropTypes.number.isRequired,
        tsid: PropTypes.string.isRequired,
      }),
    ).isRequired,
    completed: PropTypes.number.isRequired,
    total: PropTypes.number.isRequired,
  }).isRequired,
};

// const config = cwmsConfigs["SWF"];
// async function fetchConfig(configUrl) {
//   return fetch(configUrl)
//     .then((response) => response.json())
//     .then((d) => d)
// }

export default function DataQuery() {
  const [tsids, setTsids] = useState([]);
  const [tsidOffices, setTsidOffices] = useState({});
  const [visibleTSIDs, setVisibleTSIDs] = useState(tsids);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [cacheEnabled, setCacheEnabled] = useState(() => {
    if (typeof window === "undefined") return DEFAULT_CACHE_ENABLED;
    const storedValue = window.localStorage.getItem(DATA_QUERY_CACHE_KEY);
    return storedValue === null ? DEFAULT_CACHE_ENABLED : storedValue === "true";
  });
  const [sortAscending, setSortAscending] = useState(() => {
    if (typeof window === "undefined") return DEFAULT_SORT_ASCENDING;
    const storedValue = window.localStorage.getItem(DATA_QUERY_SORT_ASC_KEY);
    return storedValue === null ? DEFAULT_SORT_ASCENDING : storedValue === "true";
  });
  const [includeMissingTimeseries, setIncludeMissingTimeseries] = useState(() => {
    if (typeof window === "undefined") return DEFAULT_INCLUDE_MISSING_TIMESERIES;
    const storedValue = window.localStorage.getItem(DATA_QUERY_INCLUDE_MISSING_TS_KEY);
    return storedValue === null
      ? DEFAULT_INCLUDE_MISSING_TIMESERIES
      : storedValue === "true";
  });
  //   const [location, setLocation] = useState(null);
  //   const [parameter, setParameter] = useState(null);
  //   const [interval, setInterval] = useState(null);
  const [office, setOffice] = useState("");
  const [mode, setMode] = useState("manual");
  useEffect(() => {
    // Reset visible list when tsids change
    setVisibleTSIDs(tsids);
    setTsidOffices((current) =>
      Object.fromEntries(
        tsids.map((tsid) => [tsid, current[tsid]]).filter((entry) => entry[1]),
      ),
    );
  }, [tsids]);
  useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(DATA_QUERY_CACHE_KEY, String(cacheEnabled));
    }
  }, [cacheEnabled]);
  useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(DATA_QUERY_SORT_ASC_KEY, String(sortAscending));
    }
  }, [sortAscending]);
  useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(
        DATA_QUERY_INCLUDE_MISSING_TS_KEY,
        String(includeMissingTimeseries),
      );
    }
  }, [includeMissingTimeseries]);

  const toggleTSID = (tsid) =>
    setVisibleTSIDs((prev) =>
      prev.includes(tsid) ? prev.filter((t) => t !== tsid) : [...prev, tsid],
    );
  const tsidOfficeEntries = useMemo(
    () => tsids.map((tsid) => [tsid, tsidOffices[tsid] || office || ""]),
    [office, tsidOffices, tsids],
  );
  const officesByTsid = useMemo(
    () => Object.fromEntries(tsidOfficeEntries),
    [tsidOfficeEntries],
  );

  const offices = useQuery({
    queryKey: ["offices"],
    queryFn: async () => {
      const entries = await offices_api.getOffices({
        hasData: true,
      });
      return [...new Set(entries.map((e) => e.name))];
    },
    retry: 1,
    staleTime: 1000 * 60 * 60 * 24,
  });
  const [beginDateTime, setBeginDateTime] = useState(dayjs().subtract(1, "day"));
  const [endDateTime, setEndDateTime] = useState(dayjs());
  const [loadProgress, setLoadProgress] = useState(null);
  const [acceptedExtentSuggestionKey, setAcceptedExtentSuggestionKey] = useState("");
  const [acceptedUserDateKey, setAcceptedUserDateKey] = useState("");

  const validTsids = useMemo(
    () =>
      tsids.length > 0 &&
      tsids.every(
        (tsid) =>
          tsid.split(".").length === 6 &&
          tsid.split(".").every((part) => part.trim() !== ""),
      ),
    [tsids],
  );

  useEffect(() => {
    if (!tsids.length) return;

    const recommendedBegin = getLookbackForTsids(tsids, endDateTime);
    setBeginDateTime((currentBegin) =>
      currentBegin.isAfter(recommendedBegin) ? recommendedBegin : currentBegin,
    );
  }, [endDateTime, tsids]);

  const extents = useQuery({
    queryKey: ["data-query-extents", tsids, tsidOfficeEntries],
    queryFn: async () => {
      const entries = await Promise.all(
        tsids.map(async (tsid) => {
          const tsidOffice = officesByTsid[tsid];
          const { entries: catalogEntries = [] } =
            await catalog_api.getCatalogWithDataset({
              dataset: "TIMESERIES",
              excludeEmpty: false,
              includeAliases: true,
              like: tsid,
              office: tsidOffice || undefined,
              pageSize: 10,
            });
          const entry =
            catalogEntries.find((catalogEntry) => catalogEntry.name === tsid) ||
            catalogEntries[0];
          const earliestTime = getExtentEarliestTime(entry?.extents?.[0]);
          const earliestDateTime = earliestTime ? dayjs(earliestTime) : null;

          return [
            tsid,
            {
              earliestDateTime:
                earliestDateTime?.isValid() === true ? earliestDateTime : null,
              entry,
            },
          ];
        }),
      );

      return Object.fromEntries(entries);
    },
    enabled: validTsids && tsids.every((tsid) => Boolean(officesByTsid[tsid])),
    retry: 1,
    staleTime: 1000 * 60 * 5,
  });

  const extentAdjustments = useMemo(() => {
    return tsids
      .map((tsid) => ({
        tsid,
        office: officesByTsid[tsid],
        earliestDateTime: extents.data?.[tsid]?.earliestDateTime,
      }))
      .filter(
        (item) =>
          item.earliestDateTime?.isValid() &&
          item.earliestDateTime.isAfter(beginDateTime),
      );
  }, [beginDateTime, extents.data, officesByTsid, tsids]);

  const extentSuggestionKey = useMemo(
    () =>
      JSON.stringify({
        begin: beginDateTime.valueOf(),
        end: endDateTime.valueOf(),
        starts: extentAdjustments.map((item) => [
          item.tsid,
          item.office,
          item.earliestDateTime.valueOf(),
        ]),
      }),
    [beginDateTime, endDateTime, extentAdjustments],
  );
  const acceptedExtentSuggestions = acceptedExtentSuggestionKey === extentSuggestionKey;
  const acceptedUserDates = acceptedUserDateKey === extentSuggestionKey;
  const requestedDateBeforeExtent =
    extentAdjustments.length > 0 &&
    !acceptedExtentSuggestions &&
    !acceptedUserDates &&
    !extents.isLoading &&
    !extents.isError;
  const useExtentStartDates = !acceptedUserDates;

  const loadPlan = useMemo(
    () =>
      getLoadPlan(
        tsids,
        beginDateTime,
        endDateTime,
        extents.data,
        officesByTsid,
        useExtentStartDates,
      ),
    [
      beginDateTime,
      endDateTime,
      extents.data,
      officesByTsid,
      tsids,
      useExtentStartDates,
    ],
  );

  const applyExtentSuggestedStartDates = () => {
    setAcceptedExtentSuggestionKey(extentSuggestionKey);
    setAcceptedUserDateKey("");
  };

  const applyUserProvidedStartDates = () => {
    setAcceptedUserDateKey(extentSuggestionKey);
    setAcceptedExtentSuggestionKey("");
  };

  async function fetchTimeSeriesChunkPages(data, request, requestOverrides) {
    let values = data?.values || [];
    let nextPage = data?.["next-page"];
    const maxPages = 200;
    let pageCount = 0;

    while (nextPage) {
      const result = await ts_api.getTimeSeriesRaw(
        {
          ...request,
          page: nextPage,
        },
        requestOverrides,
      );
      const pageData = await result.raw.json();
      values = values.concat(pageData?.values || []);
      nextPage = pageData?.["next-page"];
      pageCount++;
      if (pageCount > maxPages) {
        alert("Max recursion reached, stopping");
        break;
      }
    }

    return { ...data, values };
  }

  async function fetchTimeSeriesChunk(request, requestOverrides) {
    const result = await ts_api.getTimeSeriesRaw(request, requestOverrides);

    if (!result.raw.ok) {
      return {
        name: request.name,
        office: request.office,
        values: [],
        message: await result.raw.text(),
      };
    }

    const data = await result.raw.json();
    const pagedData = await fetchTimeSeriesChunkPages(data, request, requestOverrides);
    return { ...pagedData, office: request.office };
  }

  async function fetchTimeSeries(tsid, requestOverrides, planItem) {
    if (!planItem.effectiveBegin.isBefore(endDateTime)) {
      return {
        begin: beginDateTime.format(CDA_DATE_FORMAT),
        end: endDateTime.format(CDA_DATE_FORMAT),
        values: [],
        name: tsid,
        office: planItem.office,
      };
    }

    const tasks = planItem.chunks.map((chunk) => {
      const request = {
        name: tsid,
        office: planItem.office || undefined,
        begin: chunk.begin.format(CDA_DATE_FORMAT),
        end: chunk.end.format(CDA_DATE_FORMAT),
        pageSize: CHUNK_PAGE_SIZE,
      };
      return async () => {
        try {
          return await fetchTimeSeriesChunk(request, requestOverrides);
        } finally {
          setLoadProgress((current) => {
            if (!current) return current;
            return {
              ...current,
              completed: current.completed + 1,
              byTsid: current.byTsid.map((item) =>
                item.tsid === tsid ? { ...item, completed: item.completed + 1 } : item,
              ),
            };
          });
        }
      };
    });

    const chunkResults = await runLimited(tasks, MAX_PARALLEL_CHUNK_REQUESTS);
    const failedChunk = chunkResults.find((chunk) => chunk?.message);
    if (failedChunk) return failedChunk;

    return mergeTimeSeriesChunks(
      tsid,
      beginDateTime.format(CDA_DATE_FORMAT),
      endDateTime.format(CDA_DATE_FORMAT),
      chunkResults,
    );
  }

  const {
    data: timeseriesData,
    isLoading: timeseriesLoading,
    refetch: refetchTimeseries,
    error,
  } = useQuery({
    queryKey: [
      "cdaTimeSeries",
      tsids,
      tsidOfficeEntries,
      beginDateTime,
      endDateTime,
      cacheEnabled,
    ],

    queryFn: async () => {
      const requestOverrides = cacheEnabled
        ? undefined
        : {
            cache: "no-store",
          };
      setLoadProgress({
        completed: 0,
        total: loadPlan.totalChunks,
        byTsid: loadPlan.items.map((item) => ({
          completed: 0,
          total: item.chunks.length,
          tsid: item.tsid,
        })),
      });

      const promises = loadPlan.items.map((item) => {
        return fetchTimeSeries(item.tsid, requestOverrides, item).catch((e) => {
          console.error(e);
          return {
            name: item.tsid,
            office: item.office,
            values: [],
            message: e?.message,
          };
        });
      });
      const data = await Promise.all(promises);
      setLoadProgress((current) =>
        current ? { ...current, completed: current.total } : current,
      );
      return data;
    },
    select: (data) => {
      return { ...summarizeTimeseries(data), raw: data };
    },
    enabled:
      validTsids &&
      tsids.every((tsid) => Boolean(officesByTsid[tsid])) &&
      !extents.isLoading &&
      !requestedDateBeforeExtent,
    staleTime: cacheEnabled ? 1000 * 60 * 5 : 0,
    gcTime: cacheEnabled ? 1000 * 60 * 30 : 0,
  });

  const timeseriesParams = useMemo(() => {
    // Build table params from timeseriesData
    if (!timeseriesData) return [];
    return timeseriesData.tsids
      .map((series) => {
        const [location, parameter] = series.name.split(".");
        return {
          tsid: series.name,
          header: `${location} ${parameter} (${series.units})`,
          rounding: getPrecision(series.units),
          units: series.units,
        };
      })
      .filter((p) => visibleTSIDs.includes(p.tsid));
  }, [timeseriesData, visibleTSIDs]);
  const visibleLoadedTsids = useMemo(
    () => timeseriesParams.map((param) => param.tsid),
    [timeseriesParams],
  );
  const visibleRawSeries = useMemo(() => {
    const visibleSet = new Set(visibleTSIDs);
    return (timeseriesData?.raw || []).filter((series) => visibleSet.has(series.name));
  }, [timeseriesData?.raw, visibleTSIDs]);
  const visibleSeriesHaveNoData =
    tsids.length > 0 &&
    !timeseriesLoading &&
    visibleRawSeries.length > 0 &&
    visibleRawSeries.every((series) => !series?.values?.length);

  const handleDownloadCSV = () => {
    if (!timeseriesData?.hasValues) {
      console.warn("No data to export");
      return;
    }
    const rows = buildTableRows({
      dateFormat: "YYYY-MM-DD HH:mm:ss",
      missingString: "",
      rawSeries: timeseriesData.raw,
      sortAscending,
      timeseriesParams,
    });
    const csvContent = buildCsvContent({ rows, timeseriesParams });
    const locName = tsids[0].split(".")[0];
    downloadBlob({
      content: csvContent,
      fileName: `${locName}_${timeseriesParams.length}_params_${beginDateTime.format(
        "YYYY-MM-DD",
      )}_${endDateTime.format("YYYY-MM-DD")}.csv`,
      type: "text/csv;charset=utf-8;",
    });
  };
  const handleDownloadJSON = () => {
    if (!timeseriesData?.hasValues) {
      console.warn("No data to export");
      return;
    }

    const visibleSet = new Set(visibleTSIDs);
    const jsonContent = JSON.stringify(
      timeseriesData.raw.filter((series) => visibleSet.has(series.name)),
      null,
      2,
    );
    const parameter = tsids[0].split(".")[1];
    const locName = tsids[0].split(".")[0];
    const paramName = parameter.split("-")[0].split(".")[0];
    downloadBlob({
      content: jsonContent,
      fileName: `${locName}_${paramName}_${beginDateTime.format(
        "YYYY-MM-DD",
      )}_${endDateTime.format("YYYY-MM-DD")}.json`,
      type: "application/json",
    });
  };
  const handleRefreshTimeseries = async () => {
    setIsRefreshing(true);
    try {
      await refetchTimeseries();
    } finally {
      setIsRefreshing(false);
    }
  };
  const hasActiveSettings =
    cacheEnabled !== DEFAULT_CACHE_ENABLED ||
    sortAscending !== DEFAULT_SORT_ASCENDING ||
    includeMissingTimeseries !== DEFAULT_INCLUDE_MISSING_TIMESERIES;
  const canExportTimeseries =
    timeseriesData?.tsids.length && !timeseriesLoading && !requestedDateBeforeExtent;
  const canRefreshTimeseries =
    tsids.length > 0 && !isRefreshing && !requestedDateBeforeExtent;
  const statusByTsid = useMemo(() => {
    if (!tsids.length) return {};
    if (timeseriesLoading) {
      return Object.fromEntries(
        tsids.map((tsid) => [
          tsid,
          {
            message: "Loading data",
            status: "pending",
          },
        ]),
      );
    }
    if (!timeseriesData?.raw) {
      return Object.fromEntries(
        tsids.map((tsid) => [
          tsid,
          {
            message: "Waiting for data",
            status: "pending",
          },
        ]),
      );
    }

    return Object.fromEntries(
      tsids.map((tsid) => {
        const series = timeseriesData.raw.find((item) => item.name === tsid);
        if (!series || series.message || !series.values?.length) {
          return [
            tsid,
            {
              message: getSeriesMessage(series),
              status: "error",
            },
          ];
        }
        return [
          tsid,
          {
            message: getSeriesMessage(series),
            status: "success",
          },
        ];
      }),
    );
  }, [timeseriesData?.raw, timeseriesLoading, tsids]);

  if (error)
    return (
      <div>
        <Badge color="red" className="me-2">
          Error:
        </Badge>
        {error.message}
      </div>
    );
  return (
    <div className="px-5">
      <UsaceBox title="Hydrologic Query">
        <div className="mb-4 flex justify-end">
          <SettingsMenu
            cacheEnabled={cacheEnabled}
            setCacheEnabled={setCacheEnabled}
            sortAscending={sortAscending}
            setSortAscending={setSortAscending}
            includeMissingTimeseries={includeMissingTimeseries}
            setIncludeMissingTimeseries={setIncludeMissingTimeseries}
            active={hasActiveSettings}
          />
        </div>
        <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(320px,2fr)]">
          <div className="flex w-full min-w-0 flex-col gap-4 lg:flex-1">
            <ModeSelector mode={mode} setMode={setMode} setOffice={setOffice} />
            {mode === "manual" ? (
              <>
                <OfficeSelect
                  office={office}
                  offices={offices}
                  setOffice={setOffice}
                  setTsidOffices={setTsidOffices}
                  setTsids={setTsids}
                />
                {office ? (
                  <TimeSeriesDropdown
                    office={office}
                    setOffice={setOffice}
                    setTsidOffices={setTsidOffices}
                    setTsids={setTsids}
                    tsids={tsids}
                    includeMissingTimeseries={includeMissingTimeseries}
                  />
                ) : (
                  <H3 className="text-center mt-4">Select an office to begin</H3>
                )}
              </>
            ) : (
              <TimeSeriesBuilder
                includeMissingTimeseries={includeMissingTimeseries}
                office={office}
                setOffice={setOffice}
                setTsidOffices={setTsidOffices}
                setTsids={setTsids}
                tsids={tsids}
              />
            )}

            <Controls
              setBeginDateTime={setBeginDateTime}
              setEndDateTime={setEndDateTime}
              beginDateTime={beginDateTime}
              endDateTime={endDateTime}
            />
            {requestedDateBeforeExtent && (
              <div className="rounded border border-amber-300 bg-amber-50 p-4 text-sm text-amber-950">
                <div className="font-semibold">
                  Some selected time series start after your requested begin date.
                </div>
                <div className="mt-1">
                  Use the suggested start date for each affected time series?
                </div>
                <div className="mt-3 max-h-36 overflow-auto text-xs">
                  {extentAdjustments.map((item) => (
                    <div key={item.tsid} className="flex flex-col gap-1 py-1">
                      <span className="font-medium">
                        {item.office ? `${item.office} / ` : ""}
                        {item.tsid}
                      </span>
                      <span>
                        Suggested start: {formatFriendlyDate(item.earliestDateTime)}
                      </span>
                    </div>
                  ))}
                </div>
                <div className="mt-3 flex flex-wrap gap-2">
                  <Button
                    className="bg-amber-600 px-3 py-2 text-white"
                    onClick={applyExtentSuggestedStartDates}
                  >
                    Use Suggested Start Dates
                  </Button>
                  <Button
                    className="border border-slate-300 bg-slate-100 px-3 py-2 text-slate-800 hover:bg-slate-200"
                    onClick={applyUserProvidedStartDates}
                  >
                    Use My Dates
                  </Button>
                </div>
              </div>
            )}
          </div>
          <TimeSeriesManager
            statusByTsid={statusByTsid}
            tsidOffices={officesByTsid}
            tsids={tsids}
            visibleTSIDs={visibleTSIDs}
            setTsidOffices={setTsidOffices}
            setTsids={setTsids}
            toggleTSID={toggleTSID}
          />
        </div>
        <div className="max-w-full overflow-auto">
          <div className="mt-4 flex flex-wrap items-start gap-2">
            {(canExportTimeseries || tsids.length > 0) && (
              <div className="mb-3 ms-auto flex flex-wrap items-center justify-end gap-2">
                {canExportTimeseries && (
                  <>
                    <Button
                      aria-label="Download CSV"
                      title="Download CSV"
                      onClick={handleDownloadCSV}
                      className="inline-flex h-9 items-center gap-2 rounded border border-slate-300 bg-white px-3 text-sm font-medium text-slate-700 shadow-sm hover:bg-slate-50"
                    >
                      <FiDownload aria-hidden="true" />
                      CSV
                    </Button>
                    <Button
                      aria-label="Download JSON"
                      title="Download JSON"
                      onClick={handleDownloadJSON}
                      className="inline-flex h-9 items-center gap-2 rounded border border-slate-300 bg-white px-3 text-sm font-medium text-slate-700 shadow-sm hover:bg-slate-50"
                    >
                      <FiDownload aria-hidden="true" />
                      JSON
                    </Button>
                  </>
                )}
                {tsids.length > 0 && (
                  <Button
                    aria-label={isRefreshing ? "Refreshing data" : "Refresh data"}
                    title={isRefreshing ? "Refreshing data" : "Refresh data"}
                    onClick={handleRefreshTimeseries}
                    disabled={!canRefreshTimeseries}
                    className="inline-flex h-9 items-center gap-2 rounded border border-slate-300 bg-white px-3 text-sm font-medium text-slate-700 shadow-sm hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    <FiRefreshCw
                      aria-hidden="true"
                      className={isRefreshing ? "animate-spin" : ""}
                    />
                    <span className="hidden sm:inline">
                      {isRefreshing ? "Refreshing" : "Refresh"}
                    </span>
                  </Button>
                )}
              </div>
            )}
          </div>

          {requestedDateBeforeExtent ? null : timeseriesLoading ? (
            loadProgress ? (
              <QueryProgress progress={loadProgress} />
            ) : (
              <Skeleton type="card" className="w-full h-[500px]" />
            )
          ) : visibleSeriesHaveNoData ? (
            <>
              <div className="text-center text-red-600 font-semibold mt-4">
                No TimeSeries values found for the selected parameters, office, or date
                range.
              </div>
              <Badge color="blue" className="my-2 mx-auto block w-1/2 text-center">
                Try expanding the date range if querying daily, monthly, or yearly data.
              </Badge>
            </>
          ) : (
            <DataTabs
              begin={beginDateTime}
              end={endDateTime}
              office={office}
              officesByTsid={officesByTsid}
              tsids={visibleLoadedTsids}
              timeseriesData={timeseriesData}
              isLoading={timeseriesLoading}
              timeseriesParams={timeseriesParams}
              sortAscending={sortAscending}
            />
          )}
        </div>
      </UsaceBox>
    </div>
  );
}
