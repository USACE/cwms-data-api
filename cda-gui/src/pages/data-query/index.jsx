import { UsaceBox, Skeleton, Badge, H3, Button } from "@usace/groundwork";
import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import dayjs from "dayjs";
import Controls from "./components/Controls";
import { Configuration, OfficesApi, TimeSeriesApi } from "cwmsjs";
import { getPrecision, mergeTimeseries } from "../../utils/timeseries";
import FailedTimeSeries from "./components/FailedTimeSeries";
// import useConfigList from "./hooks/useConfigList";
import TimeSeriesDropdown from "./components/TimeSeriesDropdown";
import DataTabs from "./components/DataTabs";
const CDA_DATE_FORMAT = "YYYY-MM-DDTHH:mm:ssZ";


const v2_config = new Configuration({
  headers: {
    accept: "application/json;version=2",
  },
});
const ts_api = new TimeSeriesApi(v2_config);
const offices_api = new OfficesApi();

// const config = cwmsConfigs["SWF"];
// async function fetchConfig(configUrl) {
//   return fetch(configUrl)
//     .then((response) => response.json())
//     .then((d) => d)
// }

export default function HydrologicQuery() {
  const [tsids, setTsids] = useState([]);
  //   const [location, setLocation] = useState(null);
  //   const [parameter, setParameter] = useState(null);
  //   const [interval, setInterval] = useState(null);
  const [office, setOffice] = useState(null);

  const offices = useQuery({
    queryKey: ["offices"],
    queryFn: async () => {
        const entries = await offices_api.getOffices({
            hasData: true
        }) 
        console.log(entries)

        return [...new Set(entries.map((e) => e.name))];
        },
    retry: 1,
    staleTime: 1000 * 60 * 60 * 24
  })
  const [beginDateTime, setBeginDateTime] = useState(
    dayjs().subtract(1, "day")
  );
  const [endDateTime, setEndDateTime] = useState(dayjs());
  const [view, setView] = useState("table");

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
    // Build table params from timeseriesData
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

    const parameter = tsids[0].split(".")[1];
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
    const locName = tsids[0].split(".")[0];
    const paramName = parameter.split("-")[0].split(".")[0];
    link.setAttribute("href", url);
    link.setAttribute("download", `${locName}_${paramName}_${beginDateTime.format("YYYY-MM-DD")}_${endDateTime.format("YYYY-MM-DD")}.csv`);
    link.style.visibility = "hidden";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };
  if (error)
    return (
      <div>
        <Badge color="red" className="me-2">
          Error:
        </Badge>
        {error.message}
      </div>
    );
console.log(offices.data)
  if (offices.isLoading) return <Skeleton type="card" className="w-full h-[500px] mb-5" />;
  return (
    <div className="px-5">
      <UsaceBox title="Hydrologic Query">
        <div className="flex gap-4">
          <div className={!office ? "text-lg m-auto" : ""}>
            <label htmlFor="office">Select Office: </label>
            <select
              id="office"
              value={office}
              onChange={(e) => {
                const _office = e.target.value;
                if (!_office) {
                  setOffice(null);
                  setInterval(null);
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
              {offices.data.map((key) => (
                <option key={key} value={key}>
                  {key}
                </option>
              ))}
            </select>
          </div>
        </div>
        {office && (
          <>
            <TimeSeriesDropdown
              office={office}
              setTsids={setTsids}
              tsids={tsids}
            />

            <Controls
              setBeginDateTime={setBeginDateTime}
              setEndDateTime={setEndDateTime}
              beginDateTime={beginDateTime}
              endDateTime={endDateTime}
            />
            <FailedTimeSeries failedTS={timeseriesData?.failed} />
          </>
        )}
        {!office && (
          <H3 className="text-center mt-4">Select an office to begin</H3>
        )}
        <div className="mt-4">
          <Button
            onClick={handleDownloadCSV}
            className={`mb-4 bg-blue-500 text-white px-4 py-2 rounded ${
              !timeseriesData?.tsids.length || timeseriesLoading ? "hidden" : ""
            }`}
          >
            Download CSV
          </Button>
          <DataTabs
            begin={beginDateTime}
            end={endDateTime}
            office={office}
            tsids={tsids}
            timeseriesData={timeseriesData}
            isLoading={timeseriesLoading}
            cdaParams={cdaParams}
            timeseriesParams={timeseriesParams}
          />
        </div>
      </UsaceBox>
    </div>
  );
}
