import { UsaceBox, Skeleton, Badge, Accordion } from "@usace/groundwork";
import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { AgGridReact } from "ag-grid-react";
import { ClientSideRowModelModule } from "ag-grid-community";

import dayjs from "dayjs";
import Controls from "./components/Controls";
import { Configuration, TimeSeriesApi } from "cwmsjs";
import { getPrecision, mergeTimeseries } from "../../utils/timeseries";
import { IoWarning } from "react-icons/io5";

const CDA_DATE_FORMAT = "YYYY-MM-DDTHH:mm:ssZ";

export default function HydrologicQuery() {
  const [failedTsids, setFailedTsids] = useState([]);
  const [tsids, setTsids] = useState([
    "KEYS.Elev-Tailwater.Inst.0.Rev-SCADA",
    "KEYS.Elev.Inst.1Hour.0.Ccp-Rev",
    "KEYS.Elev-Tailwater.Inst.1Hour.0.Ccp-Rev",
    "KEYS.%-Conservation Pool Full.Inst.1Hour.0.Ccp-Rev",
    "KEYS.Precip-Cuml.Inst.1Hour.0.Ccp-Rev",
    "KEYS.%-Humidity.Ave.~1Day.1Day.Ccp-Rev",
  ]);

  const [office, setOffice] = useState("SWT"); // Added state for office
  const [beginDateTime, setBeginDateTime] = useState(dayjs().subtract(1, "day"));
  const [endDateTime, setEndDateTime] = useState(dayjs());

  const v2_config = new Configuration({
    headers: {
      accept: "application/json;version=2",
    },
  });

  const ts_api = new TimeSeriesApi(v2_config);

  // Fetch timeseries data
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
            office: office, // Using dynamic office
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
    select: (data) => {
      // Merge the timeseries data by epoch
      return mergeTimeseries(data);
    },
    enabled: tsids.length > 0 && office !== undefined,
  });

  // Define column structure dynamically based on the timeseries
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
        valueFormatter: ({ value }) => {
          return value?.toFixed(getPrecision(series.units));
        },
      });
    });

    return columnDefs;
  }, [timeseriesData, tsids]);

  // Prepare row data based on the timeseries data
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
            <label htmlFor="office">Select Office: </label>
            <select
              id="office"
              value={office}
              onChange={(e) => setOffice(e.target.value)}
            >
              <option value="SWT">SWT</option>
              <option value="SWF">SWF</option>
            </select>
          </div>

          <div>
            <label htmlFor="tsids">Select Time Series: </label>
            <select
              id="tsids"
              multiple
              value={tsids}
              onChange={(e) =>
                setTsids(Array.from(e.target.selectedOptions, (option) => option.value))
              }
            >
              <option value="KEYS.Elev-Tailwater.Inst.15Minutes.0.Rev-SCADA">
                Elev-Tailwater
              </option>
              <option value="KEYS.Elev.Inst.1Hour.0.Ccp-Rev">Elevation 1Hour</option>
              <option value="KEYS.Elev-Tailwater.Inst.1Hour.0.Ccp-Rev">
                Elev-Tailwater 1Hour
              </option>
              <option value="KEYS.%-Conservation Pool Full.Inst.1Hour.0.Ccp-Rev">
                % Conservation Pool Full
              </option>
              <option value="KEYS.Precip-Cuml.Inst.1Hour.0.Ccp-Rev">
                Precipitation
              </option>
              <option value="KEYS.%-Humidity.Ave.~1Day.1Day.Ccp-Rev">
                % Humidity
              </option>
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
                {timeseriesData?.failed.map((tsid) => {
                  return (
                    <Badge
                      key={"failed-" + tsid}
                      color="yellow"
                      className="ms-5"
                    >
                      <b>{tsid}</b>
                    </Badge>
                  );
                })}
              </div>
            </Accordion>
          </div>
        )}

        <div className="mt-2 ag-theme-quartz w-full" style={{ height: 500 }}>
          {isPending ? (
            <Skeleton type="card" className="w-full h-full" />
          ) : (
            <AgGridReact
              columnDefs={columns}
              rowData={rowData}
              modules={[ClientSideRowModelModule]}
            />
          )}
        </div>
      </UsaceBox>
      <br />
    </div>
  );
}
