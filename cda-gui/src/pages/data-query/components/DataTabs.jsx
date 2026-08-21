import { Tabs, Skeleton } from "@usace/groundwork";
import CWMSPlot from "./CWMSPlot";
import DataQueryTable from "./DataQueryTable";
import MetaDataTab from "./MetaDataTab";
import PropTypes from "prop-types";

export default function DataTabs({
  office,
  officesByTsid = {},
  tsids,
  timeseriesData,
  isLoading,
  timeseriesParams,
  begin,
  end,
  sortAscending,
}) {
  if (!tsids || !tsids.length) return null;
  if (isLoading) return <Skeleton type="card" className="w-full h-[500px]" />;
  const primaryOffice = officesByTsid[tsids[0]] || office;

  return (
    <Tabs
      tabs={[
        {
          name: "Table",
          content: (
            <div
              key={`cwms-${tsids.join(
                ",",
              )}-${begin.toISOString()}-${end.toISOString()}`}
              className="relative z-10 bg-white"
            >
              {timeseriesParams.length > 0 && (
                <DataQueryTable
                  timeseriesParams={timeseriesParams}
                  dateFormat="YYYY-MM-DD HH:mm:ss"
                  missingString="---"
                  sortAscending={sortAscending}
                  rawSeries={timeseriesData?.raw}
                />
              )}
            </div>
          ),
        },
        {
          name: "Graph",
          content: (
            <CWMSPlot
              inputTSValues={timeseriesData?.raw}
              timeSeries={timeseriesParams.map((param, index) => ({
                id: param.tsid,
                traceOptions: {
                  name: `${param.tsid.split(".").join(" ")}${
                    param.units ? " (" + param.units + ")" : ""
                  }`,
                  units: param.units,
                  yaxis: `y${index + 1}`,
                },
              }))}
              locationLevels={[]} // Optional static levels like top of flood, etc.
              layoutOptions={{
                height: 500 + tsids.length * 100, // Dynamically adjust height based on number of timeseries
                showlegend: true,
                legend: {
                  orientation: "h",
                  y: -0.2,
                  x: 0.5,
                  xanchor: "center",
                  font: {
                    family: "mono, monospace",
                  },
                },
                responsive: true,
                margin: {
                  b: 100, // ensure room for legend
                },
              }}
              unit="EN"
              office={primaryOffice}
              begin={begin.format("YYYY-MM-DDTHH:mm:ssZZ")}
              end={end.format("YYYY-MM-DDTHH:mm:ssZZ")}
            />
          ),
        },
        {
          name: "Metadata",
          content: <MetaDataTab tsids={tsids} office={primaryOffice} />,
        },
      ]}
    />
  );
}

DataTabs.propTypes = {
  office: PropTypes.string.isRequired,
  officesByTsid: PropTypes.objectOf(PropTypes.string),
  tsids: PropTypes.array.isRequired,
  timeseriesData: PropTypes.object,
  isLoading: PropTypes.bool,
  timeseriesParams: PropTypes.array,
  begin: PropTypes.object.isRequired,
  end: PropTypes.object.isRequired,
  sortAscending: PropTypes.bool.isRequired,
};
