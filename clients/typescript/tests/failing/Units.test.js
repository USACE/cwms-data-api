// !ignore
// Example / test for fetching timeseries from CDA

import { UnitsApi, Configuration } from "cwmsjs";
import fetch from "node-fetch";
global.fetch = fetch;

test("Test Units", async () => {
  const config = new Configuration({
    accept: "*/*",
  });
  const u_api = new UnitsApi(config);
  await u_api
    .getDataUnitsRaw({
      format: "json",
    })
    .then(async (r) => {
      const data = await r.raw.json();
      return data;
    })
    .then((data) => {
      expect(data?.units?.units).toBeDefined();
    });
}, 30000);
