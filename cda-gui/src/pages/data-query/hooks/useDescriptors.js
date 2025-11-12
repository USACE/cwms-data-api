import { useQuery } from "@tanstack/react-query";
import { Configuration, CatalogApi } from "cwmsjs";

const config = new Configuration({
  basePath: import.meta.env.CDA_URL,
});
const cataApi = new CatalogApi(config);

export default function useDescriptors({
  office,
  location,
  parameter,
  type,
  interval,
  duration,
}) {
  return useQuery({
    queryKey: [
      "tsid-descriptors",
      office,
      location,
      parameter,
      type,
      interval,
      duration,
    ],
    queryFn: async () => {
      if (!office) return [];
      const like = [
        location || "*",
        parameter || "*",
        type || "*",
        interval || "*",
        duration || "*",
      ].join(".");
      const all = await cataApi.getCatalogWithDataset({
        dataset: "TIMESERIES",
        excludeEmpty: true,
        office,
        like,
      });
      return all;
    },
    enabled: !!office,
    select: (descriptors) => {
      const types = new Set();
      const intervals = new Set();
      const durations = new Set();
      const versions = new Set();
      const parameters = new Set();
      descriptors?.entries.forEach((d) => {
        const parts = d.name.split(".");
        // const locParts = parts[0].split("-");
        parameters.add(parts[1]);
        types.add(parts[2]);
        intervals.add(parts[3]);
        durations.add(parts[4]);
        versions.add(parts[5]);
      });

      return {
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
