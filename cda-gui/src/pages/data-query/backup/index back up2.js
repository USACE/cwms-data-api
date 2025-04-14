import { UsaceBox, Skeleton, Badge, Accordion } from "@usace/groundwork";
import { useState, useMemo, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { AgGridReact } from "ag-grid-react";
import { ClientSideRowModelModule } from "ag-grid-community";

import dayjs from "dayjs";
import Controls from "../components/Controls";
import { Configuration, TimeSeriesApi } from "cwmsjs";
import { getPrecision, mergeTimeseries } from "../../../utils/timeseries";
import { IoWarning } from "react-icons/io5";

const CDA_DATE_FORMAT = "YYYY-MM-DDTHH:mm:ssZ";

export default function HydrologicQuery() {
  const [failedTsids, setFailedTsids] = useState([]);
  const [tsids, setTsids] = useState([ "KEYS.Elev-Tailwater.Inst.15Minutes.0.Rev-SCADA" ]);
  const [location, setLocation] = useState("KEYS");
  const [parameter, setParameter] = useState("Elev.Inst");
  const [interval, setInterval] = useState("Hourly"); 
  
  const [office, setOffice] = useState("SWT");
  const [beginDateTime, setBeginDateTime] = useState(dayjs().subtract(1, "day"));
  const [endDateTime, setEndDateTime] = useState(dayjs());

  const v2_config = new Configuration({
    headers: {
      accept: "application/json;version=2",
    },
  });

  const ts_api = new TimeSeriesApi(v2_config);

  // Define how different parameters should change based on the selected interval
  const parameterIntervalMapping = {
    "Elev": {
      Hourly: "Inst.1Hour.0.Ccp-Rev",
      Daily: "Inst.~1Day.0.Ccp-Rev",
    },
    "Precip-Cuml": {
      Hourly: "Inst.1Hour.0.Ccp-Rev",
      Daily: "Inst.1Day.0.Ccp-Rev",  // For Precipitation, Daily is just 1Day, not ~1Day
    },
    "%-Humidity": {
      Hourly: "Ave.1Hour.0.Ccp-Rev",
      Daily: "Ave.~1Day.1Day.Ccp-Rev",
    },
    "Elev-Tailwater": {
      Hourly: "Inst.1Hour.0.Ccp-Rev",
      Daily: "Inst.0.Rev-SCADA",
    },
    // Add more mappings as necessary
  };

  // Dynamically update the tsids when location, parameter, or interval changes
  useEffect(() => {
    // Get the interval part from parameterIntervalMapping
    const intervalValue = parameterIntervalMapping[parameter]?.[interval] || "1Hour.0.Ccp-Rev";

    // Construct the correct time series string
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
          })
          .then((r) => {
            if (r.raw.ok) return r.raw.json();
            else return { name: tsid, values: [], message: r.raw.text };
          })
          .catch((e) => {
            return { name: tsid, values: [], message: e?.message };
          });
      });
      const data = await Promise.all(promises);
      return data;
    },
    select: (data) => mergeTimeseries(data),
    enabled: tsids.length > 0 && office !== undefined,
  });

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

    return columnDefs;
  }, [timeseriesData, tsids]);

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
  }, [timeseriesData, tsids]);

  if (error) return <div>Error: {error.message}</div>;

  return (
    <div className="px-5">
      <UsaceBox title="Hydrologic Query">
        <div className="flex gap-4">
          <div>
            <label htmlFor="location">Select Lake Location: </label>
            <select
              id="location"
              value={location}
              onChange={(e) => setLocation(e.target.value)}
            >
              <option value="KEYS">KEYS</option>
              <option value="LAKE1">LAKE1</option>
              <option value="LAKE2">LAKE2</option>
            </select>
          </div>

          <div>
            <label htmlFor="parameter">Select Parameter: </label>
            <select
              id="parameter"
              value={parameter}
              onChange={(e) => setParameter(e.target.value)}
            >
              <option value="Elev">Elevation</option>
              <option value="Precip-Cuml">Precipitation</option>
              <option value="%-Humidity">Humidity</option>
              <option value="Elev-Tailwater">Tailwater</option>
            </select>
          </div>

          <div>
            <label htmlFor="interval">Select Interval: </label>
            <select
              id="interval"
              value={interval}
              onChange={(e) => setInterval(e.target.value)}
            >
              <option value="Hourly">Hourly</option>
              <option value="Daily">Daily</option>
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

        <div className="mt-2 ag-theme-quartz w-full" style={{ height: 500 }}>
          {isPending ? (
            <Skeleton type="card" className="w-full h-full" />
          ) : (
            <AgGridReact columnDefs={columns} rowData={rowData} modules={[ClientSideRowModelModule]} />
          )}
        </div>
      </UsaceBox>
      <br />
    </div>
  );
}
