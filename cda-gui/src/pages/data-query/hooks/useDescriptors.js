import { useQuery } from "@tanstack/react-query";
import { Configuration, CatalogApi } from "cwmsjs";

const config = new Configuration({
  basePath: import.meta.env.VITE_CDA_API_ROOT,
});
const cataApi = new CatalogApi(config);

export default function useDescriptors({ includeMissingTimeseries, office, location }) {
  return useQuery({
    queryKey: ["tsid-descriptors", includeMissingTimeseries, office, location],
    queryFn: async () => {
      const like = [location || "*", "*", "*", "*", "*", "*"].join(".");
      const request = {
        dataset: "TIMESERIES",
        excludeEmpty: !includeMissingTimeseries,
        like,
      };
      if (office) request.office = office;
      const all = await cataApi.getCatalogWithDataset(request);
      return all;
    },
    enabled: !!location,
    select: (descriptors) => {
      const types = new Set();
      const intervals = new Set();
      const durations = new Set();
      const versions = new Set();
      const parameters = new Set();
      const entries = (descriptors?.entries || []).map((d) => {
        const [entryLocation, parameter, type, interval, duration, version] =
          d.name.split(".");
        // const locParts = parts[0].split("-");
        parameters.add(parameter);
        types.add(type);
        intervals.add(interval);
        durations.add(duration);
        versions.add(version);
        return {
          duration,
          interval,
          location: entryLocation,
          name: d.name,
          parameter,
          type,
          version,
        };
      });

      return {
        count: descriptors?.entries?.length || 0,
        entries,
        parameters: Array.from(parameters),
        types: Array.from(types),
        intervals: Array.from(intervals),
        durations: Array.from(durations),
        versions: Array.from(versions),
      };
    },
    staleTime: 0,
  });
}
