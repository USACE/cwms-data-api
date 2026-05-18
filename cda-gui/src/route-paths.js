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
    id: "swagger-docs",
    path: "swagger-docs",
    sitemapPath: "swagger-ui",
    // yes, this is a bit odd, but the swagger-docs are "rendered" by the api backend, not client side.
    loader: async ({ params }) => {
      const h = href(getBasePath() + "/swagger-docs");
      globalThis.location.replace(h);
      return null; // we don't get there, the browser should immediately navigate away.
    },
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
    id: "legacy-format",
    path: "legacy-format",
    sitemapPath: "legacy-format",
  },
];

export const sitemapPaths = routePaths.map(({ sitemapPath }) => sitemapPath);
