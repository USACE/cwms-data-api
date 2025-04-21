import {
  UsaceBox,
  Skeleton,
  Badge,
  Accordion,
  Button,
} from "@usace/groundwork";
import { useState, useMemo, useEffect, useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import { CWMSTable, CWMSPlot } from "@usace-watermanagement/groundwork-water";
// import { AllCommunityModule, ModuleRegistry } from "ag-grid-community";
// ModuleRegistry.registerModules([AllCommunityModule]);
import dayjs from "dayjs";
import Controls from "./components/Controls";
import { Configuration, TimeSeriesApi } from "cwmsjs";
import { getPrecision, mergeTimeseries } from "../../utils/timeseries";
import { IoWarning } from "react-icons/io5";
const OFFICES = ["SWT", "SWF"];
const CDA_DATE_FORMAT = "YYYY-MM-DDTHH:mm:ssZ";
// const config = cwmsConfigs["SWF"];
async function fetchConfig(configUrl) {
  return fetch(configUrl)
    .then((response) => response.json())
    .then((d) => d)
    .catch((error) => {
      console.error("Error fetching config:", error);
      return null; // Return null or handle the error as needed
    });
}

export default function HydrologicQuery() {
  const [tsids, setTsids] = useState([]);
  const [location, setLocation] = useState(null);
  const [parameter, setParameter] = useState(null);
  const [interval, setInterval] = useState(null);
  const [office, setOffice] = useState("SWF");
  const [configUrl, setConfigUrl] = useState(`https://cwms-data.usace.army.mil/cwms-data/blobs/CDA_QUERY_TOOL_${office}.JSON?office=${office}`);
  const [beginDateTime, setBeginDateTime] = useState(
    dayjs().subtract(1, "day")
  );
  const [endDateTime, setEndDateTime] = useState(dayjs());
  const [view, setView] = useState("table");
  const { data: config, isPending:configPending, error: configError, refetch: refetchConfig } = useQuery({
    queryKey: ["config"],
    staleTime: 1000 * 60 * 60 * 24, // 1 day
    queryFn: () => fetchConfig(configUrl),
    onSuccess: (data) => {
      return data;
    },
    enabled: !!configUrl && office !== null,
  });
  console.log("config", config);
  const v2_config = new Configuration({
    headers: {
      accept: "application/json;version=2",
    },
  });
  const ts_api = new TimeSeriesApi(v2_config);


  

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
    if (!config) return;
    const parameterIntervalMapping = config.parameterIntervalMapping;
    const intervalValue =
      parameterIntervalMapping[parameter]?.[interval] || "1Hour.0.Decodes-Rev";
    setTsids([`${location}.${parameter}.${intervalValue}`]);
  }, [location, parameter, interval, config]);

  useEffect(() => {
    if (!config) return;

    if (!location && config.locationOptions?.length > 0) {
      setLocation(config.locationOptions[0].value);
    }

    if (!parameter && Object.keys(config.parameterIntervalMapping).length > 0) {
      setParameter(Object.keys(config.parameterIntervalMapping)[0]);
    }

    if (!interval && parameter && config.parameterIntervalMapping[parameter]) {
      const intervals = Object.keys(config.parameterIntervalMapping[parameter]);
      if (intervals.length > 0) {
        setInterval(intervals[0]);
      }
    }
  }, [config, location, parameter, interval]);


  const {
    data: timeseriesData,
    isPending,
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
    select: (data) => mergeTimeseries(data),
    enabled: tsids.length > 0 && office !== undefined,
  });

  useEffect(() => {
    console.log("timeseriesData updated", timeseriesData);
  }, [timeseriesData]);


  const timeseriesParams = useMemo(() => {
    if (!timeseriesData) return [];
    return timeseriesData.tsids.map((series, index) => ({
      tsid: tsids[index],
      header: `${tsids[index].split(".")[1]} (${series.units})`,
      rounding: getPrecision(series.units),
    }));
  }, [timeseriesData, tsids, beginDateTime, endDateTime]);

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

  if (error) return <div>Error: {error.message}</div>;
  if (configError) return <div>Configuration File Error: {configError?.message}</div>;
  if (configPending) return <Skeleton type="card" className="w-full h-[500px]" />;
  return (
    <div className="px-5">
      <UsaceBox title="Hydrologic Query">
        <div className="flex gap-4">
          <div>
            <label htmlFor="office">Select Office: </label>
            <select
              id="office"
              value={office}
              onChange={(e) => {
                const office = e.target.value;
                setOffice(office);
                setConfigUrl(
                  `https://cwms-data.usace.army.mil/cwms-data/blobs/CDA_QUERY_TOOL_${office}.JSON?office=${office}`)
                  refetchConfig();
                setLocation(
                  // cwmsConfigs[e.target.value].locationOptions[0].value
                );
              }}
              style={{
                paddingLeft: "10px",
                paddingRight: "10px",
                minWidth: "120px",
                width: "auto",
              }}
            >
              {OFFICES.map((key) => (
                <option key={key} value={key}>
                  {key}
                </option>
              ))}
            </select>
          </div>

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
              <option value="Elev">Elevation</option>
              <option value="Precip-INC">Precipitation</option>
              <option value="Evap-Project">Evaporation</option>
              <option value="Flow-In">Inflow</option>
              <option value="Flow-Out">Outflow</option>
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
              <option value="Hourly">Hourly</option>
              <option value="Daily">Daily</option>
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
        </div>

        <Controls
          setBeginDateTime={setBeginDateTime}
          setEndDateTime={setEndDateTime}
          beginDateTime={beginDateTime}
          endDateTime={endDateTime}
        />

        {timeseriesData?.failed.length > 0 && (
          <div className="flex flex-col gap-2 mx-2">
            <Accordion
              heading={
                <div className="flex justify-between items-center w-full">
                  <div className="text-xl font-bold">
                    <IoWarning className="inline" /> Failed Timeseries
                  </div>
                  <Badge color="red">
                    <b>{timeseriesData?.failed.length} Failed</b>
                  </Badge>
                </div>
              }
            >
              <div className="py-3">
                {timeseriesData?.failed.map((tsid) => (
                  <Badge key={"failed-" + tsid} color="yellow" className="ms-5">
                    <b>{tsid}</b>
                  </Badge>
                ))}
              </div>
            </Accordion>
          </div>
        )}
        {view === "graph" ? (
          <div className="mt-2">
            {isPending ? (
              <Skeleton type="card" className="w-full h-[500px]" />
            ) : (
              <CWMSPlot
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
                !timeseriesData?.tsids.length || isPending ? "hidden" : ""
              }`}
            >
              Download CSV
            </Button>
            <div className="ag-theme-quartz w-full">
              {isPending ? (
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
