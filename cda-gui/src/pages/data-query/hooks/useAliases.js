import { useQuery } from "@tanstack/react-query";
import { CatalogApi, Configuration } from "cwmsjs";

const CDA_URL = import.meta.env.VITE_CDA_API_ROOT;
const config = new Configuration({
  basePath: CDA_URL,
});
const cataApi = new CatalogApi(config);

export default function useAliases({ office, kind, cacheDuration, props }) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["aliases", office, kind],
    queryFn: async () => {
      const request = {
        dataset: "LOCATIONS",
        includeAliases: true,
      };
      if (kind && kind !== "*") request.locationKindLike = kind;
      if (office) request.office = office;
      return cataApi.getCatalogWithDataset(request);
    },
    staleTime: cacheDuration,
    select: (data) => {
      const aliasMap = {};

      data?.entries
        .sort((a, b) =>
          a.name.localeCompare(b.name, undefined, { sensitivity: "base" }),
        )
        .forEach((loc) => {
          const key = office ? loc.name : `${loc.office}/${loc.name}`;
          aliasMap[key] = {
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
