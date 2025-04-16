import { UsaceBox, Skeleton, Badge, Accordion, Button } from "@usace/groundwork";
import { useState, useMemo, useEffect, useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import { CWMSTable, CWMSPlot } from "@usace-watermanagement/groundwork-water";
import { cwmsConfigs } from "./components/cwmsConfig";
import { AllCommunityModule, ModuleRegistry } from 'ag-grid-community'; 
ModuleRegistry.registerModules([AllCommunityModule]);
import dayjs from "dayjs";
import Controls from "./components/Controls";
import { Configuration, TimeSeriesApi } from "cwmsjs";
import { getPrecision, mergeTimeseries } from "../../utils/timeseries";
import { IoWarning } from "react-icons/io5";

const CDA_DATE_FORMAT = "YYYY-MM-DDTHH:mm:ssZ";
const config = cwmsConfigs["SWF"];


export default function HydrologicQuery() {
  const [failedTsids, setFailedTsids] = useState([]);
  const [tsids, setTsids] = useState([ "ALAT2.Elev.Inst.1Hour.0.Decodes-Rev" ]);
  const [location, setLocation] = useState("ALAT2");
  const [parameter, setParameter] = useState("Elev.Inst");
  const [interval, setInterval] = useState("Hourly"); 
  const [office, setOffice] = useState("SWF");
  const [beginDateTime, setBeginDateTime] = useState(dayjs().subtract(1, "day"));
  const [endDateTime, setEndDateTime] = useState(dayjs());
  const [view, setView] = useState("table"); 

  const v2_config = new Configuration({
    headers: {
      accept: "application/json;version=2",
    },
  });

  const gridApi = useRef(null);

  const ts_api = new TimeSeriesApi(v2_config);

  const locationNames = config.locationNames;
  

  const parameterIntervalMapping = config.parameterIntervalMapping;

    async function fetchAllTSData(data) {
        let startDate = data?.begin
        let endDate = data?.end
        let values = data?.values
        let { name, "office-id": office, "next-page": nextPage } = data
        const maxPages = 1000
        let pageCount = 0
        while (nextPage) {
            let _result = await ts_api.getTimeSeries({
                begin: startDate,
                end: endDate,
                name,
                office,
                page: nextPage,
                pageSize: 1500
                // begin: beginDateTime.format(CDA_DATE_FORMAT),
                // end: endDateTime.format(CDA_DATE_FORMAT),
            })
            // if (!_result?.page) page = false
            nextPage = _result?.nextPage
            endDate = _result?.end
            values = values.concat(_result?.values)
            pageCount++
            if (pageCount > maxPages) {
                alert("Max recursion reached, stopping")
                break
            }
        }
        return {
            begin: startDate,
            end: endDate,
            values,
            name,
            units: data?.units,
            total: data?.total,
        }

    }
  useEffect(() => {
    const intervalValue = parameterIntervalMapping[parameter]?.[interval] || "1Hour.0.Decodes-Rev";
    setTsids([`${location}.${parameter}.${intervalValue}`]);
  }, [location, parameter, interval]);

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
            pageSize: 1500
          })
          .then(async (r) => {
            if (r.raw.ok) {
                let _data = await r.raw.json();
                return await fetchAllTSData(_data)
            }
            else return { name: tsid, values: [], message: r.raw.text };
          })
          .catch((e) => {
            console.error(e)
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
  

  // Build the table columns
  const columns = useMemo(() => {
    if (!timeseriesData || timeseriesData.length === 0) return [];

    const columnDefs = [
      {
        headerName: "Date",
        field: "date",
        sortable: true,
        filter: true,
        chartDataType: "time",
        editable: true,
        pinned: "left",
      },
    ];

    

    timeseriesData.tsids.forEach((series, index) => {
      const seriesName = tsids[index];
      columnDefs.push({
        headerName: `${seriesName.split(".")[1]} (${series.units})`,
        field: String(index),
        sortable: true,
        filter: true,
        editable: true,
        valueFormatter: ({ value }) => value?.toFixed(getPrecision(series.units)),
      });
    });
    console.log({columnDefs})
    return columnDefs;
  }, [timeseriesData, tsids, beginDateTime, endDateTime]);

  // Build the table row data
  const rowData = useMemo(() => {
    if (!timeseriesData || timeseriesData.length === 0) return [];
    let tableDates = [];
    for (let i = 0; i < timeseriesData.dates.length; i++) {
      const tsDate = timeseriesData.dates[i];
      tableDates.push({
        date: dayjs(tsDate).format("YYYY-MM-DD HH:mm:ss"),
        ...timeseriesData.values[tsDate],
      });
    }
    return tableDates;
  }, [timeseriesData, tsids, beginDateTime, endDateTime]);

  // const graphData = useMemo(() => {
  //   if (!timeseriesData) return [];
  //   return timeseriesData.tsids.map((series, index) => {
  //     return {
  //       x: timeseriesData.dates.map((date) => dayjs(date).format("YYYY-MM-DD HH:mm:ss")),
  //       y: timeseriesData.dates.map((date) => timeseriesData.values[date]?.[index]),
  //       type: 'scatter',
  //       mode: 'lines+markers',
  //       name: `${tsids[index].split(".")[1]} (${series.units})`,
  //     };
  //   });
  // }, [timeseriesData, tsids, beginDateTime, endDateTime]);

  const timeseriesParams = useMemo(() => {
    if (!timeseriesData) return [];
    return timeseriesData.tsids.map((series, index) => ({
      tsid: tsids[index],
      header: `${tsids[index].split(".")[1]} (${series.units})`,
      rounding: getPrecision(series.units),
    }));
  }, [timeseriesData, tsids, beginDateTime, endDateTime]);
  

  const cdaParams = useMemo(() => ({
    begin: beginDateTime.format("YYYY-MM-DDTHH:mm:ssZZ"),
    end: endDateTime.format("YYYY-MM-DDTHH:mm:ssZZ"),
    office: office,
  }), [beginDateTime, endDateTime, office]);
  
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
  
    const csvContent = [header, ...rows]
      .map((row) => row.join(","))
      .join("\n");
  
    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
  
    const link = document.createElement("a");
    const locName = cwmsConfigs[office]?.locationNames?.[location] ?? location;
    const paramName = parameter.split("-")[0].split(".")[0];
    link.setAttribute("href", url);
    link.setAttribute("download", `${locName} ${paramName} Date.csv`);
    link.style.visibility = "hidden";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);

  };
  

  if (error) return <div>Error: {error.message}</div>;

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
            setOffice(e.target.value);
            setLocation(cwmsConfigs[e.target.value].locationOptions[0].value);
          }}
          style={{ paddingLeft: '10px', paddingRight: '10px', minWidth: '120px', width: 'auto' }}
        >
          {Object.keys(cwmsConfigs).map((key) => (
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
          style={{ paddingLeft: '10px', paddingRight: '10px', minWidth: '150px', width: 'auto' }}
        >
          {cwmsConfigs[office]?.locationOptions.map(({ value, label }) => (
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
              style={{ paddingLeft: '10px', paddingRight: '10px', minWidth: '120px', width: 'auto' }}
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
              style={{ paddingLeft: '10px', paddingRight: '10px', minWidth: '100px', width: 'auto' }}
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
              style={{ paddingLeft: '10px', paddingRight: '10px', minWidth: '90px', width: 'auto' }}
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
                    name: `${tsid.split(".")[1]} (${timeseriesData?.tsids?.[index]?.units || "unit"})`,
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
                  className={`mb-4 bg-blue-500 text-white px-4 py-2 rounded ${!timeseriesData?.tsids.length || isPending ? "hidden": ""}`}
                >
                  Download CSV
             </Button>
          <div className="ag-theme-quartz w-full">
            {isPending ? (
              <Skeleton type="card" className="w-full h-full" />
            ) : (
              <>
                {timeseriesData && (
                  <div
                    key={`cwms-${tsids.join(",")}-${beginDateTime.toISOString()}-${endDateTime.toISOString()}`}
                    className="relative z-10 bg-white"
                  >
                    <CWMSTable
                      begin={cdaParams.begin}
                      end={cdaParams.end}
                      office={cdaParams.office}
                      tsids={timeseriesParams.map((item) => item.tsid)}
                      timeseriesParams={timeseriesParams}
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
