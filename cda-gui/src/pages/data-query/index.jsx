import { UsaceBox, Skeleton, Button, Badge, H3 } from "@usace/groundwork";
import { useState, useMemo, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
// import { CWMSPlot, CWMSTable} from "@usace-watermanagement/groundwork-water";
import { CWMSTable } from "./components/CWMSTable";
import { CWMSPlot } from "./components/CWMSPlot";
// import { AllCommunityModule, ModuleRegistry } from "ag-grid-community";
// ModuleRegistry.registerModules([AllCommunityModule]);
import dayjs from "dayjs";
import Controls from "./components/Controls";
import { Configuration, TimeSeriesApi } from "cwmsjs";
import { getPrecision, mergeTimeseries } from "../../utils/timeseries";
import FailedTimeSeries from "./components/FailedTimeSeries";
import { FaInfoCircle } from "react-icons/fa";
import useConfigList from "./hooks/useConfigList";
const CDA_DATE_FORMAT = "YYYY-MM-DDTHH:mm:ssZ";

const v2_config = new Configuration({
  headers: {
    accept: "application/json;version=2",
  },
});
const ts_api = new TimeSeriesApi(v2_config);

// const config = cwmsConfigs["SWF"];
async function fetchConfig(configUrl) {
  return fetch(configUrl)
    .then((response) => response.json())
    .then((d) => d)
}

export default function HydrologicQuery() {
  const [tsids, setTsids] = useState([]);
  const [location, setLocation] = useState(null);
  const [parameter, setParameter] = useState(null);
  const [interval, setInterval] = useState(null);
  const [office, setOffice] = useState(null);

  //   const {data: offices, isLoading: officesLoading, error: officesError} = useOffices({cacheDuration: 1000 * 60 * 60 * 24});
  const {
    data: offices,
    isLoading: officesLoading,
    error: officesError,
  } = useConfigList({
    like: "CDA_QUERY_TOOL.*JSON",
    cacheDuration: 1000 * 60 * 60 * 24,
    props: {
      select: (data) => data?.blobs?.map((blob) => blob?.officeId) || null,
    },
  });
  const [configUrl, setConfigUrl] = useState(null);
  const [beginDateTime, setBeginDateTime] = useState(
    dayjs().subtract(1, "day")
  );
  const [endDateTime, setEndDateTime] = useState(dayjs());
  const [view, setView] = useState("table");
  const {
    data: config,
    isLoading: configLoading,
    error: configError,
    refetch: refetchConfig,
  } = useQuery({
    queryKey: ["config", configUrl, office],
    staleTime: 1000 * 60 * 60 * 24, // 1 day
    queryFn: () => fetchConfig(configUrl),
    onSuccess: (data) => {
      return data;
    },
    enabled: !!configUrl && !!office,
  });

  async function fetchAllTSData(data) {
    let startDate = data?.begin;
    let endDate = data?.end;
    let values = data?.values;
    let { name, "office-id": office, "next-page": nextPage } = data;
    const maxPages = 200;
    let pageCount = 0;
    while (nextPage) {
      let _result = await ts_api.getTimeSeries({
        begin: startDate,
        end: endDate,
        name,
        office,
        page: nextPage,
        pageSize: 25000,
        // begin: beginDateTime.format(CDA_DATE_FORMAT),
        // end: endDateTime.format(CDA_DATE_FORMAT),
      });
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

  useEffect(() => {
    if (!config || !parameter || !location || !interval) return;
    const parameterIntervalMapping = config.parameterIntervalMapping;
    const intervalValue = parameterIntervalMapping[parameter]?.[interval];
    setTsids([`${location}.${parameter}.${intervalValue}`]);
  }, [location, parameter, interval, config]);

  useEffect(() => {
    if (!config) return;

    if (config?.locationOptions?.length > 0) {
      setLocation(config.locationOptions[0].value);
    }

    if (config?.parameterIntervalMapping && Object.keys(config?.parameterIntervalMapping).length > 0) {
      setParameter(Object.keys(config.parameterIntervalMapping)[0]);
    }

    if (parameter && config.parameterIntervalMapping[parameter]) {
      const intervals = Object.keys(config.parameterIntervalMapping[parameter]);
      if (intervals.length > 0) {
        setInterval(intervals[0]);
      }
    }
  }, [config, location, parameter, interval]);

  const {
    data: timeseriesData,
    isLoading: timeseriesLoading,
    error,
  } = useQuery({
    queryKey: ["cdaTimeSeries", tsids, office, beginDateTime, endDateTime],

    queryFn: async () => {
      const promises = tsids.map((tsid) => {
        return ts_api
          .getTimeSeriesRaw({
            name: tsid,
            office: office,
            begin: beginDateTime.format(CDA_DATE_FORMAT),
            end: endDateTime.format(CDA_DATE_FORMAT),
            pageSize: 25000,
          })
          .then(async (r) => {
            if (r.raw.ok) {
              let _data = await r.raw.json();
              return await fetchAllTSData(_data);
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
    enabled: tsids.length > 0 && office !== undefined,
  });

  const timeseriesParams = useMemo(() => {
    if (!timeseriesData) return [];
    return timeseriesData.tsids.map((series, index) => ({
      tsid: tsids[index],
      header: `${tsids[index].split(".")[1]} (${series.units})`,
      rounding: getPrecision(series.units),
    }));
  }, [timeseriesData, tsids]);

  const cdaParams = useMemo(
    () => ({
      begin: beginDateTime.format("YYYY-MM-DDTHH:mm:ssZZ"),
      end: endDateTime.format("YYYY-MM-DDTHH:mm:ssZZ"),
      office: office,
    }),
    [beginDateTime, endDateTime, office]
  );
  const handleDownloadCSV = () => {
    if (!timeseriesData || timeseriesData.dates.length === 0) {
      console.warn("No data to export");
      return;
    }

    const header = ["Date", parameter];
    const rows = timeseriesData.dates.map((date) => {
      const formattedDate = dayjs(date).format("YYYY-MM-DD HH:mm:ss");
      const value = timeseriesData.values[date]?.[0] ?? "";
      return [formattedDate, value];
    });

    const csvContent = [header, ...rows].map((row) => row.join(",")).join("\n");

    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);

    const link = document.createElement("a");
    const locName = config?.locationNames?.[location] ?? location;
    const paramName = parameter.split("-")[0].split(".")[0];
    link.setAttribute("href", url);
    link.setAttribute("download", `${locName} ${paramName} Date.csv`);
    link.style.visibility = "hidden";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };
  if (configError || officesError || error)
    return (
      <div><Badge color="red" className="me-2">Error:</Badge>{configError?.message || officesError?.message || error.message}</div>
    );
  if (officesLoading || configLoading)
    return <Skeleton type="card" className="w-full h-[500px]" />;
  return (
    <div className="px-5">
      <UsaceBox title="Hydrologic Query">
        <div className={!office ? "text-lg m-auto text-center": ""}>
        <Badge color="blue" className={!office ? "text-lg m-auto text-center mb-4": "mb-2"}>
          <FaInfoCircle /> If Office ID is not present in dropdown. Office has
          not created a configuration file.
        </Badge>
        </div>

        <div className="flex gap-4">
          <div className={!office ? "text-lg m-auto": ""}>
            <label htmlFor="office">Select Office: </label>
            <select
              id="office"
              value={office}
              onChange={(e) => {
                const _office = e.target.value;
                if (!_office) {
                    setConfigUrl(null);
                    setOffice(null);
                    setLocation(null);
                    setParameter(null);
                    setInterval(null);
                    setTsids([]);
                    return;
                }
                setOffice(_office);
                setConfigUrl(
                  `https://cwms-data.usace.army.mil/cwms-data/blobs/CDA_QUERY_TOOL_${_office}.JSON?office=${_office}`
                );
                refetchConfig();
                setTsids([]);
                // cwmsConfigs[e.target.value].locationOptions[0].value
              }}
              className="px-3 min-w-[150px] w-auto"
            >
              <option key="select" value="">
                Select Office
              </option>
              {offices.map((key) => (
                <option key={key} value={key}>
                  {key}
                </option>
              ))}
            </select>
          </div>
          {config && office && (
            <>
              <div>
                <label htmlFor="location">Select Lake Location: </label>
                <select
                  id="location"
                  value={location}
                  onChange={(e) => setLocation(e.target.value)}
                  style={{
                    paddingLeft: "10px",
                    paddingRight: "10px",
                    minWidth: "150px",
                    width: "auto",
                  }}
                >
                  {config?.locationOptions.map(({ value, label }) => (
                    <option key={value} value={value}>
                      {label}
                    </option>
                  ))}
                </select>
              </div>

              <div>
                <label htmlFor="parameter">Select Parameter: </label>
                <select
                  id="parameter"
                  value={parameter}
                  onChange={(e) => setParameter(e.target.value)}
                  style={{
                    paddingLeft: "10px",
                    paddingRight: "10px",
                    minWidth: "120px",
                    width: "auto",
                  }}
                >
                  {config?.parameterIntervalMapping &&
                    Object.keys(config?.parameterIntervalMapping).map((key) => (
                      <option key={key} value={key}>
                        {key}
                      </option>
                    ))}
                </select>
              </div>

              <div>
                <label htmlFor="interval">Select Interval: </label>
                <select
                  id="interval"
                  value={interval}
                  onChange={(e) => setInterval(e.target.value)}
                  style={{
                    paddingLeft: "10px",
                    paddingRight: "10px",
                    minWidth: "100px",
                    width: "auto",
                  }}
                >
                  {config?.parameterIntervalMapping &&
                    Object.keys(
                      config?.parameterIntervalMapping[parameter] || {}
                    ).map((key) => (
                      <option key={key} value={key}>
                        {key}
                      </option>
                    ))}
                </select>
              </div>

              <div>
                <label htmlFor="view">Select View: </label>
                <select
                  id="view"
                  value={view}
                  onChange={(e) => setView(e.target.value)}
                  style={{
                    paddingLeft: "10px",
                    paddingRight: "10px",
                    minWidth: "90px",
                    width: "auto",
                  }}
                >
                  <option value="table">Table</option>
                  <option value="graph">Graph</option>
                </select>
              </div>
            </>
          )}
        </div>
        {config && office && (
        <>
        <Controls
          setBeginDateTime={setBeginDateTime}
          setEndDateTime={setEndDateTime}
          beginDateTime={beginDateTime}
          endDateTime={endDateTime}
          />
          <FailedTimeSeries failedTS={timeseriesData?.failed} />
          </>
        )}
        {!config && !office && (
            <H3 className="text-center mt-4">
                Select an office to begin
            </H3>
        )}
        {view === "graph" ? (
          <div className="mt-2">
            {timeseriesLoading ? (
              <Skeleton type="card" className="w-full h-[500px]" />
            ) : (
              <CWMSPlot
                inputTSValues={timeseriesData?.raw}
                timeSeries={tsids.map((tsid, index) => ({
                  id: tsid,
                  traceOptions: {
                    name: `${tsid.split(".")[1]} (${
                      timeseriesData?.tsids?.[index]?.units || "unit"
                    })`,
                    yaxis: "y1",
                  },
                }))}
                locationLevels={[]} // Optional static levels like top of flood, etc.
                layoutOptions={{
                  height: 500,
                  yaxis: {
                    title: {
                      text: "Value",
                    },
                  },
                  showlegend: true,
                  legend: {
                    font: {
                      family: "Arial, sans-serif",
                      size: 10,
                    },
                  },
                  responsive: true,
                }}
                unit="EN"
                office={office}
                begin={beginDateTime.format("YYYY-MM-DDTHH:mm:ssZZ")}
                end={endDateTime.format("YYYY-MM-DDTHH:mm:ssZZ")}
              />
            )}
          </div>
        ) : (
          <>
            <Button
              onClick={handleDownloadCSV}
              className={`mb-4 bg-blue-500 text-white px-4 py-2 rounded ${
                !timeseriesData?.tsids.length || timeseriesLoading ? "hidden" : ""
              }`}
            >
              Download CSV
            </Button>
            <div className="ag-theme-quartz w-full">
              {timeseriesLoading ? (
                <Skeleton type="card" className="w-full h-full" />
              ) : (
                <>
                  {timeseriesData?.tsids.length > 0 && (
                    <div
                      key={`cwms-${tsids.join(
                        ","
                      )}-${beginDateTime.toISOString()}-${endDateTime.toISOString()}`}
                      className="relative z-10 bg-white"
                    >
                      <CWMSTable
                        begin={cdaParams.begin}
                        end={cdaParams.end}
                        office={cdaParams.office}
                        tsids={timeseriesParams.map((item) => item.tsid)}
                        timeseriesParams={timeseriesParams}
                        dateFormat="YYYY-MM-DD HH:mm:ss"
                        interval="5"
                        missingString="---"
                        sortAscending={true}
                        trim={true}
                        inputTSValues={timeseriesData?.raw}
                      />
                    </div>
                  )}
                </>
              )}
            </div>
          </>
        )}
      </UsaceBox>
    </div>
  );
}
