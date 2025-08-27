import { useQuery } from "@tanstack/react-query";
import { CatalogApi, Configuration } from "cwmsjs";

const CDA_URL = import.meta.env.CDA_URL;
const config = new Configuration({
  basePath: CDA_URL,
});
const cataApi = new CatalogApi(config);

export default function useAliases({ office, kind, cacheDuration, props }) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["aliases", office, kind],
    queryFn: async () =>
      cataApi.getCatalogWithDataset({
        dataset: "LOCATIONS",
        locationKindLike: kind,
        office,
      }),
    staleTime: cacheDuration,
    select: (data) => {
      const aliasMap = {};

      data?.entries.sort((a, b) => (
        a.name.localeCompare(b.name, undefined, { sensitivity: "base" })
      )).forEach((loc) => {
        aliasMap[loc.name] = {
          name: loc.name,
          publicName: loc.publicName,
          office: loc.office,
          aliases: loc.aliases,
        };
      });
      return aliasMap;
    },
    ...props,
  });

  return { data, isLoading, error };
}
