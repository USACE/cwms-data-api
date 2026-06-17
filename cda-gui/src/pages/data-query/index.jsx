import { UsaceBox, Skeleton, Badge, H3, Button } from "@usace/groundwork";
import { useState, useMemo, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import dayjs from "dayjs";
import PropTypes from "prop-types";
import Controls from "./components/Controls";
import { CatalogApi, Configuration, OfficesApi, TimeSeriesApi } from "cwmsjs";
import { getPrecision, mergeTimeseries } from "../../utils/timeseries";
import FailedTimeSeries from "./components/FailedTimeSeries";
// import useConfigList from "./hooks/useConfigList";
import TimeSeriesDropdown from "./components/TimeSeriesDropdown";
import DataTabs from "./components/DataTabs";
import Toggle from "./components/Toggle";
import TimeSeriesBuilder from "./components/TimeSeriesBuilder";
import TimeSeriesManager from "./components/TimeSeriesManager";
import SettingsMenu from "./components/SettingsMenu";
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

function getLoadPlan(tsids, beginDateTime, endDateTime, extentsByTsid = {}) {
  const items = tsids.map((tsid) => {
    const extentBegin = extentsByTsid[tsid]?.earliestDateTime;
    const effectiveBegin =
      extentBegin?.isValid() && extentBegin.isAfter(beginDateTime)
        ? extentBegin
        : beginDateTime;
    const chunks = effectiveBegin.isBefore(endDateTime)
      ? getDateChunks(tsid, effectiveBegin, endDateTime)
      : [];

    return {
      tsid,
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
  let total = 0;

  chunks.forEach((chunk) => {
    units ||= chunk?.units;
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

function QueryProgress({ progress }) {
  const percent = getProgressPercent(progress);

  return (
    <div className="mt-4 rounded border border-slate-200 bg-white p-4 shadow-sm">
      <div className="mb-2 flex flex-wrap items-center justify-between gap-2 text-sm font-semibold text-slate-700">
        <span>Loading time series data</span>
        <span>
          {progress.completed} / {progress.total} chunks
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
              <div className="mb-1 flex justify-between gap-4">
                <span className="truncate">{item.tsid}</span>
                <span>
                  {item.completed} / {item.total}
                </span>
              </div>
              <div className="h-2 overflow-hidden rounded bg-slate-100">
                <div
                  className="h-full bg-emerald-500 transition-all"
                  style={{ width: `${itemPercent}%` }}
                />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

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
  const [mode, setMode] = useState("advanced");
  useEffect(() => {
    // Reset visible list when tsids change
    setVisibleTSIDs(tsids);
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
    queryKey: ["data-query-extents", tsids, office],
    queryFn: async () => {
      const entries = await Promise.all(
        tsids.map(async (tsid) => {
          const { entries: catalogEntries = [] } =
            await catalog_api.getCatalogWithDataset({
              dataset: "TIMESERIES",
              excludeEmpty: false,
              includeAliases: true,
              like: tsid,
              office: office || undefined,
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
    enabled: validTsids && office !== undefined,
    retry: 1,
    staleTime: 1000 * 60 * 5,
  });

  const loadPlan = useMemo(
    () => getLoadPlan(tsids, beginDateTime, endDateTime, extents.data),
    [beginDateTime, endDateTime, extents.data, tsids],
  );

  const extentAdjustments = useMemo(() => {
    return loadPlan.items
      .map((item) => ({
        tsid: item.tsid,
        earliestDateTime: extents.data?.[item.tsid]?.earliestDateTime,
      }))
      .filter(
        (item) =>
          item.earliestDateTime?.isValid() &&
          item.earliestDateTime.isAfter(beginDateTime),
      );
  }, [beginDateTime, extents.data, loadPlan.items]);

  const requestedDateBeforeExtent =
    extentAdjustments.length > 0 && !extents.isLoading && !extents.isError;

  const adjustedExtentBeginDateTime = useMemo(() => {
    return extentAdjustments.reduce(
      (latestBegin, item) =>
        item.earliestDateTime.isAfter(latestBegin)
          ? item.earliestDateTime
          : latestBegin,
      beginDateTime,
    );
  }, [beginDateTime, extentAdjustments]);

  const applyExtentStartDate = () => {
    setBeginDateTime(adjustedExtentBeginDateTime);
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
        values: [],
        message: await result.raw.text(),
      };
    }

    const data = await result.raw.json();
    return await fetchTimeSeriesChunkPages(data, request, requestOverrides);
  }

  async function fetchTimeSeries(tsid, requestOverrides, planItem) {
    if (!planItem.effectiveBegin.isBefore(endDateTime)) {
      return {
        begin: beginDateTime.format(CDA_DATE_FORMAT),
        end: endDateTime.format(CDA_DATE_FORMAT),
        values: [],
        name: tsid,
      };
    }

    const tasks = planItem.chunks.map((chunk) => {
      const request = {
        name: tsid,
        office: office || undefined,
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
      office,
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
          return { name: item.tsid, values: [], message: e?.message };
        });
      });
      const data = await Promise.all(promises);
      setLoadProgress((current) =>
        current ? { ...current, completed: current.total } : current,
      );
      return data;
    },
    select: (data) => {
      return { ...mergeTimeseries(data), raw: data };
    },
    enabled:
      validTsids &&
      office !== undefined &&
      !extents.isLoading &&
      !requestedDateBeforeExtent,
    staleTime: cacheEnabled ? 1000 * 60 * 5 : 0,
    gcTime: cacheEnabled ? 1000 * 60 * 30 : 0,
  });

  const timeseriesParams = useMemo(() => {
    // Build table params from timeseriesData
    if (!timeseriesData) return [];
    return timeseriesData.tsids
      .map((series, index) => ({
        tsid: tsids[index],
        header: `${tsids[index].split(".")[1]} (${series.units})`,
        rounding: getPrecision(series.units),
      }))
      .filter((p) => visibleTSIDs.includes(p.tsid));
  }, [timeseriesData, tsids, visibleTSIDs]);

  const cdaParams = useMemo(
    () => ({
      begin: beginDateTime.format("YYYY-MM-DDTHH:mm:ssZZ"),
      end: endDateTime.format("YYYY-MM-DDTHH:mm:ssZZ"),
      office: office || undefined,
    }),
    [beginDateTime, endDateTime, office],
  );
  const handleDownloadCSV = () => {
    if (!timeseriesData || timeseriesData.dates.length === 0) {
      console.warn("No data to export");
      return;
    }
    const parameters = visibleTSIDs.map((ts) => ts.split(".")[1]);
    const header = ["Date", ...parameters];
    const rows = timeseriesData.dates.map((date) => {
      const formattedDate = dayjs(date).format("YYYY-MM-DD HH:mm:ss");
      const values = timeseriesData.values[date] || [];
      const paddedValues = visibleTSIDs.map((_, i) => {
        const val = values[i];
        return val === null || val === undefined ? "" : val;
      });
      return [formattedDate, ...paddedValues];
    });
    const csvContent = [header, ...rows].map((row) => row.join(",")).join("\n");
    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);

    const link = document.createElement("a");
    const locName = tsids[0].split(".")[0];
    link.setAttribute("href", url);
    link.setAttribute(
      "download",
      `${locName}_${parameters.length}_params_${beginDateTime.format(
        "YYYY-MM-DD",
      )}_${endDateTime.format("YYYY-MM-DD")}.csv`,
    );
    link.style.visibility = "hidden";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };
  const handleDownloadJSON = () => {
    if (!timeseriesData || timeseriesData.dates.length === 0) {
      console.warn("No data to export");
      return;
    }

    const jsonContent = JSON.stringify(timeseriesData.raw, null, 2);
    const blob = new Blob([jsonContent], { type: "application/json" });
    const url = URL.createObjectURL(blob);

    const parameter = tsids[0].split(".")[1];
    const locName = tsids[0].split(".")[0];
    const paramName = parameter.split("-")[0].split(".")[0];

    const link = document.createElement("a");
    link.setAttribute("href", url);
    link.setAttribute(
      "download",
      `${locName}_${paramName}_${beginDateTime.format(
        "YYYY-MM-DD",
      )}_${endDateTime.format("YYYY-MM-DD")}.json`,
    );
    link.style.visibility = "hidden";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
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

  if (error)
    return (
      <div>
        <Badge color="red" className="me-2">
          Error:
        </Badge>
        {error.message}
      </div>
    );
  if (offices.isLoading)
    return <Skeleton type="card" className="w-full h-[500px] mb-5" />;
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
        <div className="flex flex-col lg:flex-row gap-4">
          <div className="flex w-full min-w-0 flex-col gap-4 lg:flex-1">
            <div className="flex flex-col sm:flex-row gap-4 sm:items-center">
              <label htmlFor="office">Office: </label>
              <select
                id="office"
                value={office}
                onChange={(e) => {
                  const _office = e.target.value;
                  if (!_office) {
                    setOffice("");
                    setTsids([]);
                    return;
                  }
                  setOffice(_office);
                  setTsids([]);
                }}
                className="px-3 min-w-[150px] w-auto"
              >
                <option key="select" value="">
                  Select Office
                </option>
                {offices.data?.map((key) => (
                  <option key={key} value={key}>
                    {key}
                  </option>
                ))}
              </select>

              <Toggle
                checked={mode === "advanced"}
                onChange={() =>
                  setMode((prev) => (prev === "basic" ? "advanced" : "basic"))
                }
                label={mode === "basic" ? "Guided Mode" : "Manual Mode"}
              />
            </div>
            {mode == "advanced" ? (
              <TimeSeriesDropdown
                office={office}
                setOffice={setOffice}
                setTsids={setTsids}
                tsids={tsids}
                includeMissingTimeseries={includeMissingTimeseries}
              />
            ) : !office ? (
              <H3 className="text-center mt-4">Select an office to begin</H3>
            ) : (
              <TimeSeriesBuilder office={office} setTsids={setTsids} tsids={tsids} />
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
                  Data starts on {formatFriendlyDate(adjustedExtentBeginDateTime)}.
                </div>
                <div className="mt-1">Query from that date instead?</div>
                <div className="mt-3 max-h-36 overflow-auto text-xs">
                  {extentAdjustments.map((item) => (
                    <div key={item.tsid} className="flex flex-col gap-1 py-1">
                      <span className="font-medium">{item.tsid}</span>
                      <span>Starts {formatFriendlyDate(item.earliestDateTime)}</span>
                    </div>
                  ))}
                </div>
                <div className="mt-3 flex flex-wrap gap-2">
                  <Button
                    className="bg-amber-600 px-3 py-2 text-white"
                    onClick={applyExtentStartDate}
                  >
                    Query from {formatFriendlyDate(adjustedExtentBeginDateTime)}
                  </Button>
                </div>
              </div>
            )}
          </div>
          <TimeSeriesManager
            tsids={tsids}
            visibleTSIDs={visibleTSIDs}
            setTsids={setTsids}
            toggleTSID={toggleTSID}
          />
        </div>
        <div className="overflow-auto max-w-[85vw]">
          <div className="mt-4">
            <FailedTimeSeries
              failedTS={timeseriesData?.failed}
              className="w-3/4 mx-auto"
            />

            <Button
              onClick={handleDownloadCSV}
              className={`mb-4 bg-blue-500 text-white px-4 py-2 rounded ${
                !timeseriesData?.tsids.length ||
                timeseriesLoading ||
                requestedDateBeforeExtent
                  ? "hidden"
                  : ""
              }`}
            >
              Download CSV
            </Button>
            <Button
              onClick={handleDownloadJSON}
              className={`mb-4 bg-green-600 text-white px-4 py-2 rounded ms-2 ${
                !timeseriesData?.tsids.length ||
                timeseriesLoading ||
                requestedDateBeforeExtent
                  ? "hidden"
                  : ""
              }`}
            >
              Download JSON
            </Button>
            <Button
              onClick={handleRefreshTimeseries}
              disabled={!tsids.length || isRefreshing || requestedDateBeforeExtent}
              className={`mb-4 bg-slate-700 text-white px-4 py-2 rounded ms-2 ${
                !tsids.length ? "hidden" : ""
              }`}
            >
              {isRefreshing ? "Refreshing..." : "Refresh Data"}
            </Button>
          </div>

          {requestedDateBeforeExtent ? null : timeseriesLoading ? (
            loadProgress ? (
              <QueryProgress progress={loadProgress} />
            ) : (
              <Skeleton type="card" className="w-full h-[500px]" />
            )
          ) : tsids.length > 0 &&
            timeseriesData?.raw?.every((ts) => ts?.values?.length === 0) ? (
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
              tsids={visibleTSIDs}
              timeseriesData={timeseriesData}
              isLoading={timeseriesLoading}
              cdaParams={cdaParams}
              timeseriesParams={timeseriesParams}
              sortAscending={sortAscending}
            />
          )}
        </div>
      </UsaceBox>
    </div>
  );
}
