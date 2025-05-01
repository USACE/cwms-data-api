import { Tabs, Skeleton } from "@usace/groundwork";
import CWMSPlot from "./CWMSPlot";
import CWMSTable from "./CWMSTable";
import MetaDataTab from "./MetaDataTab";

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
                inputTSValues={timeseriesData?.raw}
              />
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
              begin={begin.format("YYYY-MM-DDTHH:mm:ssZZ")}
              end={end.format("YYYY-MM-DDTHH:mm:ssZZ")}
            />
          ),
        },
        {
          name: "Metadata",
          content: <MetaDataTab tsids={tsids}  office={office} />,
        },
      ]}
    />
  );
}
