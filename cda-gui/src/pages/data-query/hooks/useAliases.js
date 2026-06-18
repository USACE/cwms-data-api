import { useQuery } from "@tanstack/react-query";
import { CatalogApi, Configuration } from "cwmsjs";

const CDA_URL = import.meta.env.VITE_CDA_API_ROOT;
const config = new Configuration({
  basePath: CDA_URL,
});
const cataApi = new CatalogApi(config);
const ALIAS_PAGE_SIZE = 2000;
const MAX_ALIAS_PAGES = 10;

async function getCatalogPages(request) {
  const firstPage = await cataApi.getCatalogWithDataset(request);
  const entries = firstPage?.entries || [];
  let nextPage = firstPage?.["next-page"];
  let pageCount = 1;

  while (nextPage && pageCount < MAX_ALIAS_PAGES) {
    const page = await cataApi.getCatalogWithDataset({
      ...request,
      page: nextPage,
    });
    entries.push(...(page?.entries || []));
    nextPage = page?.["next-page"];
    pageCount += 1;
  }

  return { ...firstPage, entries };
}

export default function useAliases({ office, kind, cacheDuration, props }) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["aliases", office, kind],
    queryFn: async () => {
      const request = {
        dataset: "LOCATIONS",
        includeAliases: true,
        pageSize: ALIAS_PAGE_SIZE,
      };
      if (kind && kind !== "*") request.locationKindLike = kind;
      if (office) request.office = office;
      return getCatalogPages(request);
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
