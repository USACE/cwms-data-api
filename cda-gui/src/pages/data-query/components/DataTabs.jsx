import { Tabs, Skeleton } from "@usace/groundwork";
import CWMSPlot from "./CWMSPlot";
import { CWMSTable } from "@usace-watermanagement/groundwork-water";
import MetaDataTab from "./MetaDataTab";
import PropTypes from "prop-types";

export default function DataTabs({
  office,
  tsids,
  timeseriesData,
  isLoading,
  cdaParams,
  timeseriesParams,
  begin,
  end,
}) {
  if (!tsids || !tsids.length) return null;
  if (isLoading) return <Skeleton type="card" className="w-full h-[500px]" />;

  return (
    <Tabs
      tabs={[
        {
          name: "Table",
          content: (
            <div
              key={`cwms-${tsids.join(
                ","
              )}-${begin.toISOString()}-${end.toISOString()}`}
              className="relative z-10 bg-white"
            >
              {timeseriesParams.length > 0 && (
                <CWMSTable
                  begin={cdaParams.begin}
                  end={cdaParams.end}
                  office={cdaParams.office}
                  timeseriesParams={timeseriesParams}
                  dateFormat="YYYY-MM-DD HH:mm:ss"
                  interval="5"
                  missingString="---"
                  sortAscending
                  trim
                  tableOptions={{
                    bleed: true,
                    dense: true,
                    grid: true,
                    overflow: true,
                    striped: true,
                    stickyHeader: true,
                    overflowHeight: "max-h-[55vh]",
                  }}
                  inputTSValues={timeseriesData?.raw}
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
              timeSeries={tsids.map((tsid, index) => ({
                id: tsid,
                traceOptions: {
                  name: `${tsid.split(".").join(" ")}${
                    timeseriesData?.tsids?.[index]?.units
                      ? " (" + timeseriesData?.tsids?.[index]?.units + ")"
                      : ""
                  }`,
                  units: timeseriesData?.tsids?.[index]?.units,
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
              office={office}
              begin={begin.format("YYYY-MM-DDTHH:mm:ssZZ")}
              end={end.format("YYYY-MM-DDTHH:mm:ssZZ")}
            />
          ),
        },
        {
          name: "Metadata",
          content: <MetaDataTab tsids={tsids} office={office} />,
        },
      ]}
    />
  );
}

DataTabs.propTypes = {
  office: PropTypes.string.isRequired,
  tsids: PropTypes.array.isRequired,
  timeseriesData: PropTypes.object,
  isLoading: PropTypes.bool,
  cdaParams: PropTypes.object,
  timeseriesParams: PropTypes.array,
  begin: PropTypes.object.isRequired,
  end: PropTypes.object.isRequired,
};
