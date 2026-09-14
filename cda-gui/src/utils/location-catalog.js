// cwmsjs currently predates the catalog search-text parameter. Preserve its
// serialization and response mapping while adding this parameter to the request.
export function getLocationCatalog(api, params) {
  const { searchText, ...catalogParams } = params;
  const text = searchText?.trim();
  const client = text
    ? api.withPreMiddleware(({ url, init }) => ({
        url: `${url}${url.includes("?") ? "&" : "?"}search-text=${encodeURIComponent(text)}`,
        init,
      }))
    : api;
  return client.getCatalogWithDataset(catalogParams);
}
