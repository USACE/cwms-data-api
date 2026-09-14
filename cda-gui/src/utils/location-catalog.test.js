import assert from "node:assert/strict";
import test from "node:test";
import { CatalogApi, Configuration } from "cwmsjs";
import { getLocationCatalog } from "./location-catalog.js";

test("location searches preserve office, filters, and page while encoding search text", async () => {
  const requests = [];
  const api = new CatalogApi(
    new Configuration({
      basePath: "https://example.test/cwms-data",
      fetchApi: async (url) => {
        requests.push(new URL(url));
        return new Response(JSON.stringify({ entries: [], "next-page": "next" }), {
          headers: { "Content-Type": "application/json" },
        });
      },
    }),
  );
  const params = {
    dataset: "LOCATIONS",
    office: "SWT",
    like: "^KEYS",
    searchText: " Keystone & Lake ",
  };
  const first = await getLocationCatalog(api, params);
  await getLocationCatalog(api, { ...params, page: first.nextPage });
  await getLocationCatalog(api, { dataset: "LOCATIONS", office: "SWT" });
  assert.equal(requests[0].pathname, "/cwms-data/catalog/LOCATIONS");
  assert.equal(requests[0].searchParams.get("office"), "SWT");
  assert.equal(requests[0].searchParams.get("like"), "^KEYS");
  assert.equal(requests[0].searchParams.get("search-text"), "Keystone & Lake");
  assert.equal(requests[1].searchParams.get("page"), "next");
  assert.equal(requests[1].searchParams.getAll("search-text").length, 1);
  assert.equal(requests[2].searchParams.has("search-text"), false);
});
