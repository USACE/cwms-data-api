// Example / test for fetching timeseries from CDA

import { TimeSeriesApi, Configuration } from "cwmsjs";
import fetch from "node-fetch";
global.fetch = fetch;

test("Test TimeSeries V1", async () => {
  // ⚠️ NOTE!
  // Do NOT use V1 of the Timeseries endpoint.
  // It is deprecated and will be removed in the future❗
  // This serves as an example of how to use the V1 API for legacy purposes.

  const ts_api = new TimeSeriesApi();
  await ts_api
    .getTimeSeries({
      office: "SWT",
      name: "KEYS.Elev.Inst.1Hour.0.Ccp-Rev",
    })
    .then((data) => {
      expect(data).toBeDefined();
    });
}, 30000);
