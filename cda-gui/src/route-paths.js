// Routes are defined here to allow building a sitemap dynamically
export const routePaths = [
  {
    id: "home",
    index: true,
    sitemapPath: "",
  },
  {
    id: "swagger-ui",
    path: "swagger-ui",
    sitemapPath: "swagger-ui",
  },
  {
    id: "data-query",
    path: "data-query",
    sitemapPath: "data-query",
  },
  {
    id: "regexp",
    path: "regexp",
    sitemapPath: "regexp",
  },
  {
    id: "filter-expressions",
    path: "filter-expressions",
    sitemapPath: "filter-expressions",
  },
  {
    id: "timestamps",
    path: "timestamps",
    sitemapPath: "timestamps",
  },
  {
    id: "users",
    path: "users",
    sitemapPath: "users",
  },
  {
    id: "user-lists",
    path: "user-lists",
    sitemapPath: "user-lists",
  },
  {
    id: "user-roles",
    path: "user-roles",
    sitemapPath: "user-roles",
  },
  {
    id: "legacy-format",
    path: "legacy-format",
    sitemapPath: "legacy-format",
  },
  {
    id: "location-search",
    path: "location-search",
    sitemapPath: "location-search",
  },
];

export const sitemapPaths = routePaths.map(({ sitemapPath }) => sitemapPath);
