import { useEffect, useState, useRef } from "react";
import { Configuration, LevelsApi, TimeSeriesApi } from "cwmsjs";
import Plotly from "plotly.js-basic-dist";
import { gwMerge, Skeleton } from "@usace/groundwork";
import deepmerge from "deepmerge";
import { useMemo } from "react";

/**
 * Normalize a data prop to an array of objects
 *
 * This function provides support for data props entered as a single element
 * or as an array as well as support for data points entered as strings or as
 * objects.  Data is adjusted as necessary to return a normalized prop formatted
 * as an array of data objects in the format {id, ...additionalOptions}.
 * @param prop The data prop to be normalized.
 * @returns An array of data objects in the format {id, ...additionalOptions}
 */
const normalizeDataProp = (prop) => {
  if (!prop) return [];
  if (Array.isArray(prop))
    return prop.map((datum) => {
      if (Array.isArray(datum))
        console.error("Nested array not valid in plot data props");
      else if (typeof datum === "string") return { id: datum };
      else if (typeof datum === "object") return datum;
      else console.error(`Unrecognized data format: ${datum}`);
    });
  else if (typeof prop === "string") return [{ id: prop }];
  else if (typeof prop === "object") return [prop];
  else console.error(`Unrecognized data format: ${prop}`);
};

const getYAxisId = (timeseriesParam) => {
  const yaxis = timeseriesParam?.traceOptions?.yaxis;
  if (!yaxis) return undefined;
  if ((yaxis == "y") | (yaxis == "y1")) return "yaxis";
  const re = /^y(\d+)$/;
  const match = yaxis.match(re);
  if (match) {
    const axisInt = match[1];
    return `yaxis${axisInt}`;
  } else {
    return undefined;
  }
};

function CWMSPlot({
  begin,
  end,
  unit,
  office,
  timeSeries,
  pageSize,
  locationLevels,
  inputTSValues,
  layoutOptions = {},
  className = "",
  responsive = true,
  datum,
  timezone,
  trim,
  staticTraces,
  cdaUrl,
}) {
  const [isLoading, setIsLoading] = useState(true);
  const [tsData, setTsData] = useState(null);
  const plotElement = useRef(null);
  const [error, setError] = useState(null);

  const timeSeriesArray = useMemo(
    () => normalizeDataProp(timeSeries),
    [timeSeries]
  );

  const locationLevelsArray = useMemo(
    () => normalizeDataProp(locationLevels),
    [locationLevels]
  );

  const config_v2 = new Configuration({
    basePath: cdaUrl,
    headers: {
      accept: "application/json;version=2",
    },
  });
  const ts_api = new TimeSeriesApi(config_v2);

  const config_level = new Configuration({
    basePath: cdaUrl,
    headers: {
      accept: "*/*",
    },
  });
  const level_api = new LevelsApi(config_level);

  const defaultLayout = {
    height: 750,
    grid: {
      columns: 1,
    },
    xaxis: {
      showgrid: true,
      showline: true,
      mirror: "all",
      linecolor: "black",
      linewidth: 1,
    },
  };

  timeSeriesArray.forEach((item, index) => {
    const yaxis_id =
      getYAxisId(item) ?? (index == 0 ? "yaxis" : "yaxis" + index);
    if (!(yaxis_id in defaultLayout)) {
      defaultLayout[yaxis_id] = {
        title: {
          text: item.id.split(".")[1] + (item?.traceOptions?.units ? " (" + item.traceOptions.units + ")" : ""),
          font: {
            family: "Arial, sans-serif",
            size: 14,
          },
        },
      };
    }
  });

  defaultLayout.grid.rows = Object.keys(defaultLayout).filter((key) =>
    key.includes("yaxis")
  ).length;

  const layout = deepmerge(defaultLayout, layoutOptions);

  useEffect(() => {
    const tsids = timeSeriesArray.map((ts) => ts.id);
    const levels = locationLevelsArray.map((level) => level.id);

    if (!tsids?.length) {
      setError("You must specify one or more Timeseries IDs to plot.");
      return;
    }

    if (!office) setError("You must specify a 3 letter ID for the office");

    const fetchData = async () => {
      let values = [];
      let _data = { ts: {} };

      if (!inputTSValues) {
        let ts_promises = tsids.map(async (name) => {
          try {
            // Currently, large page size calls are blocked, so the default of 500 is used
            // let pageSize = 500;
            // const delta = dayjs(end.value).diff(dayjs(start.value), "day", true)
            // let interval = tsid.split(".")[3]
            // if (interval.includes("Minute")) {
            //   if (interval.includes("15")) { pageSize = delta * 100 }
            //   if (interval.includes("1Minute")) { pageSize = delta * 1500 }
            // }
            // if (interval.includes("Hour")) { pageSize = delta * 10 }
            // if (interval.includes("Day")) { pageSize = delta * 1 }
            return await ts_api.getTimeSeries({
              name,
              office,
              unit,
              datum,
              begin,
              end,
              pageSize,
              timezone,
              trim,
            });
          } catch (error) {
            console.error("Error fetching timeseries data:", error);
          }
        });

        values = await Promise.all(ts_promises);
      } else {
        values = inputTSValues;
      }
      let lev_promises = levels?.map(async (item) => {
        let level;
        // The Level API doesn't accept the same date format
        try {
          level = await level_api.getLevels({
            levelIdMask: item,
            begin: begin?.slice(0, 10),
            end: end?.slice(0, 10),
            office: office,
            format: "json",
            unit: unit,
          });
        } catch (error) {
          console.error("Error fetching location level data:", error);
        }
        return level;
      });
      values.forEach((result) => {
        if (result && result.name) {
          if (!_data.ts[result.name]) {
            _data.ts[result.name] = [];
          }
          _data.ts[result.name].push(result);
        } else if (result === null) {
          console.warn(`Skipping as no data was found.`);
        } else {
          console.warn(`No timeseries data found for ${result?.name}`);
        }
      });

      let lev_values;
      if (lev_promises) {
        lev_values = await Promise.all(lev_promises);
      }

      lev_values?.forEach((result) => {
        const name = result["location-levels"]["location-levels"][0]?.name;
        const levels = result["location-levels"]["location-levels"][0]?.values;
        if (result && name) {
          if (!_data.ts[name]) {
            _data.ts[name] = [];
          }
          _data.ts[name].push(levels);
        } else if (result === null) {
          console.warn(`Skipping as no data was found.`);
        } else {
          console.warn(`No location level data found for ${result?.name}`);
        }
      });

      setTsData(_data);
    };

    fetchData();
  }, [
    begin,
    datum,
    end,
    locationLevelsArray,
    office,
    timeSeriesArray,
    timezone,
    trim,
    unit,
    pageSize,
  ]);

  useEffect(() => {
    const tsids = timeSeriesArray.map((ts) => ts.id);
    const levels = locationLevelsArray.map((level) => level.id);

    if (!plotElement.current || !tsData) {
      return;
    }

    setIsLoading(true);

    let ts_keys = Object.keys(tsData.ts);

    // Create traces keyed to the ts data
    let traces = [];
    let ts;

    // Add Static Data first to the list of traces so they show up behind
    if (staticTraces) {
      staticTraces.map((trace) => traces.push(trace));
    }

    // Loop thru TS Data for timeseries and location levels
    for (let k_idx = 0; k_idx < ts_keys.length; k_idx++) {
      const key = ts_keys[k_idx];

      // Add Timeseries to list of traces
      if (tsids.includes(key)) {
        for (let ts_idx = 0; ts_idx < tsData.ts[key].length; ts_idx++) {
          ts = tsData.ts[key][ts_idx];
          // Defaults for trace
          const trace = {
            x: ts.values.map((value) => new Date(value[0])),
            y: ts.values.map((value) => value[1]),
            showlegend: true,
            legend: { x: 1, xanchor: "right", y: 1 },
          };
          // Add all other parameters
          const tsObj = timeSeriesArray?.filter(
            (item) => item.id === ts.name
          )[0];
          const fullTrace = tsObj?.traceOptions
            ? deepmerge(trace, tsObj?.traceOptions)
            : trace;
          traces.push(fullTrace);
        }
      }

      // Add Location Levels to list of traces
      if (levels?.includes(key)) {
        for (let ts_idx = 0; ts_idx < tsData.ts[key].length; ts_idx++) {
          ts = tsData.ts[key][ts_idx];
          // Defaults for trace
          const trace = {
            x: ts.segments[0].values.map((value) => new Date(value[0])),
            y: ts.segments[0].values.map((value) => value[1]),
            showlegend: true,
            legend: { x: 1, xanchor: "right", y: 1 },
          };
          // Add all other parameters
          const levelObj = locationLevelsArray?.filter(
            (item) => item.id == key
          )[0];
          const fullTrace = levelObj?.traceOptions
            ? deepmerge(trace, levelObj?.traceOptions)
            : trace;
          traces.push(fullTrace);
        }
      }
    }

    setIsLoading(false);

    Plotly.newPlot(plotElement.current, traces, layout, {
      responsive: responsive,
    });
  }, [
    layout,
    locationLevelsArray,
    responsive,
    staticTraces,
    timeSeriesArray,
    tsData,
  ]);

  return (
    <div
      className={gwMerge("gww-h-full gww-w-full", className)}
      style={{ height: layoutOptions.height }}
    >
      <div
        ref={plotElement}
        id="plot"
        className="gww-h-full gww-w-full"
        style={{ height: layoutOptions.height }}
      >
        {error ? (
          <div>Error: {error}</div>
        ) : isLoading ? (
          <div style={{ height: `${layoutOptions.height}px` }}>
            <Skeleton className="gww-h-full gww-w-full" />
          </div>
        ) : (
          <></>
        )}
      </div>
    </div>
  );
}

export default CWMSPlot;
export { CWMSPlot };
