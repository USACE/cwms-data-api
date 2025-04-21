import { useQuery } from "@tanstack/react-query";
import { Skeleton } from "@usace/groundwork";
import { Configuration, TimeSeriesApi } from "cwmsjs";
import dayjs from "dayjs";
import { parse } from "ol/expr/expression";
import { useEffect, useState } from "react";

const intervalRegex = RegExp(/~?(\d+)(\w+)/g)
const intervalWeights = [ "year", "month", "week", "day", "hour", "minutes", "seconds" ]
  

function getMinInterval(tsids) {
  // Map time intervals to a comparable value in minutes
  const timeMap = {
      "Minutes": 1,
      "Hour": 60,
      "Day": 1440,    //  24 * 60
      "Week": 10080,  //   7 * 24 * 60
      "Month": 43200, //  30 * 1440
      "Year": 525600  // 365 * 24 * 60
  };

  // Helper function to parse the time interval from a string
  function parseInterval(str) {
      const timeString = str.split(".")[3]; // Get the 3rd part
      const timeValue = timeString.replace("~", "").replace(/[^0-9]/g, ""); // Extract the number
      const timeUnit = timeString.replace(/[^a-zA-Z]/g, ""); // Extract the unit (Minutes, Hours, Day, Month)

      return {
          value: timeValue ? parseInt(timeValue) : 1, // Fallback to 1 if the timeValue is empty
          unit: timeUnit
      };
  }

  let intervalCompare = Infinity;
  let minValue = "";
  let minInterval = Infinity;

  // Iterate over each value in the array
  let tsIdx;
  for (let i = 0; i < tsids.length; i++) {
      const { value, unit } = parseInterval(tsids[i]);
      const intervalInMinutes = value * (timeMap[unit] || Infinity); // Convert to minutes

      // Check for the minimum interval
      if (intervalInMinutes < intervalCompare) {
          intervalCompare = intervalInMinutes;
          minValue = unit;
          minInterval = value
          tsIdx = i
      }
  }
  return {
      tsIdx,
      minValue,
      minInterval
  };
}

export default function MultiTSTable({ basePath, cdaParams, cdaHeaders, className }) {
  const [dates, setDates] = useState([])
  const [tableDates, setTableDates] = useState([])

  const v2_config = new Configuration({
    basePath: basePath,
    headers: cdaHeaders
      ? cdaHeaders
      : {
          accept: "application/json;version=2"
        }
  });
  
function combineData(tsData) {
  const {tsIdx, minValue, minInterval} = getMinInterval(cdaParams?.tsids)
  const minTs = tsData[tsIdx]
  const minTSValues = minTs?.values

  // Loop the minimum TS values and create an array of dates with the values for each of the other arrays
  // in the row with their rsepective timestamps
  let tableDates = []
  for (let i = 0; i < minTSValues.length; i++) {
    const date = dayjs(minTSValues[i][0])
    const rowValues = []
    for (let j = 0; j < tsData.length; j++) {
      if (j == tsIdx) continue
      const ts = tsData[j]
      const tsValues = ts?.values
      if (!tsValues[i]) continue
      const value = tsValues[i][1]
      rowValues.push(value)
    }
    tableDates.push({date, rowValues})
  }
  return tableDates
}

  const ts_api = new TimeSeriesApi(v2_config);
  // Loop over the tsids and create a Promise.all to fetch all the data
  // Then, loop over the data and create a table
  const { data, isPending,  error } = useQuery({
    queryKey: ["cdaTimeSeries", cdaParams],
    queryFn: async () => {
      const promises = cdaParams?.tsids.map((tsid) => {
        return ts_api.getCwmsDataTimeseries({
          office: cdaParams?.office,
          name: tsid,
          begin: cdaParams?.begin,
          end: cdaParams?.end,
        }).catch((e)=> {e?.message});
      });
      const data = await Promise.all(promises);
      return data;
    },
    select: (data) => {
      data = combineData(data)
      return data
    },
    enabled: cdaParams?.tsids.length > 0 && cdaParams?.office !== undefined,
  });
  // If the user passes in a single tsid, convert it to an array
  if (cdaParams?.tsid) cdaParams.tsids = [cdaParams.tsid];
  if (!cdaParams?.office) 
    throw Error("You must specify the office in the cdaParams");
  if (!cdaParams?.tsids) return <div>No tsids provided</div>;
  if (error) return <div>Error: {error?.message}</div>;
  if (isPending) return <Skeleton className="h-[50vh] w-[100%]" />;
  return (
    <table className={`table-auto text-sm ${className}`}>
      <thead>
        <tr>
          <th>Time</th>
        </tr>
      </thead>
      <tbody>
        {
          data.map((d, r_idx)=>{
            return <tr key={d?.date.unix()}>
              <td>{d?.date.format("YYYY-MM-DD HH:mm:ss")}</td>
              {
                d?.rowValues.map((v, c_idx)=>{
                  return <td key={c_idx}>{v}</td>
                })
              }
            </tr> 
          })
        }
      </tbody>
    </table>
  );
}
