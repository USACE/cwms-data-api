import { UsaceBox, Skeleton, Badge, Accordion } from "@usace/groundwork";
import { useState, useMemo, useEffect } from "react";
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
  const [tsids, setTsids] = useState([ "ALAT2.Elev.Inst.1Hour.0.Decodes-Rev" ]);
  const [location, setLocation] = useState("ALAT2");
  const [parameter, setParameter] = useState("Elev.Inst");
  const [interval, setInterval] = useState("Hourly"); 
  
  const [office, setOffice] = useState("SWF");
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
      Hourly: "Inst.1Hour.0.Decodes-Rev",
      Daily: "Inst.~1Day.0.Decodes-Rev",
    },
    "Precip-INC": {
      Hourly: "Total.1Hour.1Hour.Decodes-Rev",
      Daily: "Total.~1Day.1Day.Best-SWF",  
    },
    "Evap-Project": {
      Hourly: "Total.1Hour.1Hour.Decodes-Rev",
      Daily: "Total.~1Day.1Day.Best-SWF",  
    },
    "Flow-In": {
      Hourly: "Inst.1Hour.0.Decodes-Comp",
      Daily: "Ave.~1Day.1Day.Computed-SWF-REGI",
    },
    "Flow-Out": {
      Hourly: "Inst.1Hour.0.Rev-SWF-REGI",
      Daily: "Ave.~1Day.1Day.Rev-SWF-REGI",
    },
    "Gated-Out": {
      Hourly: "Inst.1Hour.0.Decodes-Comp",
      Daily: "Ave.~1Day.1Day.Computed-SWF-REGI",
    },
    "%-Humidity": {
      Hourly: "Ave.1Hour.0.Decodes-Rev",
      Daily: "Ave.~1Day.1Day.Decodes-Rev",
    },
    "Elev-Tailwater": {
      Hourly: "Inst.1Hour.0.Decodes-Rev",
      Daily: "Inst.0.Rev-SCADA",
    },
    // Add more mappings as necessary
  };

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
              
              <option value="ALAT2">Aquilla  </option>
              <option value="TBLT2">  BA Steinhagen</option>
              <option value="BDWT2">  Bardwell</option>
              <option value="BLNT2">  Belton</option> 
              <option value="BSLT2">  Bob Sandlin</option>  
              <option value="SMCT2">  Canyon</option>    
              <option value="CLDL1">  Caddo</option>    
              <option value="SCLT2">  Cooper</option>    
              <option value="GGLT2">  Georgetown</option>   
              <option value="GNGT2">  Granger</option>   
              <option value="GPVT2">  Grapevine</option>  
              <option value="HORT2">  Hords Creek</option> 
              <option value="JPLT2">  Joe Pool</option>   
              <option value="JFNT2">  Lake O Pines</option>  
              <option value="LVNT2">  Lavon</option>   
              <option value="LEWT2">  Lewisville</option>   
              <option value="DAWT2">  Navarro Mills</option> 
              <option value="SAGT2">  O.C. Fisher</option>    
              <option value="PCTT2">  Proctor</option>    
              <option value="RRLT2">  Ray Roberts</option>  
              <option value="FFLT2">  Richland Chambers</option>
              <option value="JSPT2">  Sam Rayburn</option>                                                            
              <option value="SOMT2">  Somerville</option>    
              <option value="STIT2">  Stillhouse Hollow</option>  
              <option value="TBRT2">  Twin Buttes</option>   
              <option value="ACTT2">  Waco</option>    
              <option value="WTYT2">  Whitney</option>   
              <option value="TXKT2">  Wright Patman</option>   
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
              <option value="Precip-INC">Precipitation</option>
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
