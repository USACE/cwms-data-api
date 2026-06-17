import { UsaceBox, Skeleton, Badge, H3, Button } from "@usace/groundwork";
import { useState, useMemo, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import dayjs from "dayjs";
import Controls from "./components/Controls";
import { Configuration, OfficesApi, TimeSeriesApi } from "cwmsjs";
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
const DATA_QUERY_CACHE_KEY = "data-query-cache-enabled";
const DATA_QUERY_SORT_ASC_KEY = "data-query-sort-ascending";
const DATA_QUERY_INCLUDE_MISSING_TS_KEY = "data-query-include-missing-timeseries";
const DEFAULT_CACHE_ENABLED = true;
const DEFAULT_SORT_ASCENDING = false;
const DEFAULT_INCLUDE_MISSING_TIMESERIES = false;
const DEFAULT_LOOKBACK = { amount: 1, unit: "day" };
const MINIMUM_INTERVAL_LOOKBACKS = {
  minute: DEFAULT_LOOKBACK,
  hour: DEFAULT_LOOKBACK,
};

function getLookbackForInterval(interval) {
  if (!interval || interval === "0") return DEFAULT_LOOKBACK;

  const match = interval.replace(/^~/, "").match(/^(\d+)([A-Za-z]+)$/);
  if (!match) return DEFAULT_LOOKBACK;

  const amount = Number(match[1]);
  const unit = match[2].toLowerCase().replace(/s$/, "");

  return MINIMUM_INTERVAL_LOOKBACKS[unit] || { amount, unit };
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

  useEffect(() => {
    if (!tsids.length) return;

    const recommendedBegin = getLookbackForTsids(tsids, endDateTime);
    setBeginDateTime((currentBegin) =>
      currentBegin.isAfter(recommendedBegin) ? recommendedBegin : currentBegin,
    );
  }, [endDateTime, tsids]);

  async function fetchAllTSData(data, requestOverrides) {
    let startDate = data?.begin;
    let endDate = data?.end;
    let values = data?.values;
    let { name, "office-id": office, "next-page": nextPage } = data;
    const maxPages = 200;
    let pageCount = 0;
    while (nextPage) {
      let _result = await ts_api.getTimeSeries(
        {
          begin: startDate,
          end: endDate,
          name,
          office,
          page: nextPage,
          pageSize: 25000,
          // begin: beginDateTime.format(CDA_DATE_FORMAT),
          // end: endDateTime.format(CDA_DATE_FORMAT),
        },
        requestOverrides,
      );
      // if (!_result?.page) page = false
      nextPage = _result?.nextPage;
      endDate = _result?.end;
      values = values.concat(_result?.values);
      pageCount++;
      if (pageCount > maxPages) {
        alert("Max recursion reached, stopping");
        break;
      }
    }
    return {
      begin: startDate,
      end: endDate,
      values,
      name,
      units: data?.units,
      total: data?.total,
    };
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
      const promises = tsids.map((tsid) => {
        return ts_api
          .getTimeSeriesRaw(
            {
              name: tsid,
              office: office || undefined,
              begin: beginDateTime.format(CDA_DATE_FORMAT),
              end: endDateTime.format(CDA_DATE_FORMAT),
              pageSize: 25000,
            },
            requestOverrides,
          )
          .then(async (r) => {
            if (r.raw.ok) {
              let _data = await r.raw.json();
              return await fetchAllTSData(_data, requestOverrides);
            } else return { name: tsid, values: [], message: r.raw.text };
          })
          .catch((e) => {
            console.error(e);
            return { name: tsid, values: [], message: e?.message };
          });
      });
      const data = await Promise.all(promises);
      return data;
    },
    select: (data) => {
      return { ...mergeTimeseries(data), raw: data };
    },
    enabled:
      tsids.length > 0 &&
      tsids.every(
        (tsid) =>
          tsid.split(".").length === 6 &&
          tsid.split(".").every((part) => part.trim() !== ""),
      ) &&
      office !== undefined,
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
                !timeseriesData?.tsids.length || timeseriesLoading ? "hidden" : ""
              }`}
            >
              Download CSV
            </Button>
            <Button
              onClick={handleDownloadJSON}
              className={`mb-4 bg-green-600 text-white px-4 py-2 rounded ms-2 ${
                !timeseriesData?.tsids.length || timeseriesLoading ? "hidden" : ""
              }`}
            >
              Download JSON
            </Button>
            <Button
              onClick={handleRefreshTimeseries}
              disabled={!tsids.length || isRefreshing}
              className={`mb-4 bg-slate-700 text-white px-4 py-2 rounded ms-2 ${
                !tsids.length ? "hidden" : ""
              }`}
            >
              {isRefreshing ? "Refreshing..." : "Refresh Data"}
            </Button>
          </div>

          {timeseriesLoading ? (
            <Skeleton type="card" className="w-full h-[500px]" />
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
