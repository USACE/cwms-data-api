// Routes are defined here to allow building a sitemap dynamically
export const routePaths = [
  { id: "api-keys", path: "api-keys", requiresAuth: true },
  { id: "api-key-help", path: "api-keys/help", requiresAuth: true },
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
    requiresAuth: true,
    path: "users",
    sitemapPath: "users",
  },
  {
    id: "user-lists",
    requiresAuth: true,
    path: "user-lists",
    sitemapPath: "user-lists",
  },
  {
    id: "user-roles",
    requiresAuth: true,
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

export const sitemapPaths = routePaths
  .filter(({ sitemapPath }) => sitemapPath !== undefined)
  .map(({ sitemapPath }) => sitemapPath);
