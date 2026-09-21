// Example / test for fetching timeseries from CDA
// !ignore
// As of 2024/08/06 the TimeZones endpoint returns invalid JSON

import { TimeZonesApi, Configuration } from "cwmsjs";
import fetch from "node-fetch";
global.fetch = fetch;

test("Test TimeZones", async () => {
  const tz_config = new Configuration({
    headers: {
      accept: "application/json;version=2",
    },
  });
  const tz_api = new TimeZonesApi(tz_config);
  await tz_api
    .getDataTimezonesRaw({
      format: "json",
    })
    .then(async (r) => {
      const data = await r.raw.text();
      console.log(data);
      return data;
    })
    .then((data) => {
      console.log(data);
      expect(data).toBeDefined();
    });
}, 30000);
