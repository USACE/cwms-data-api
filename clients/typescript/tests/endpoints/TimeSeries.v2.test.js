// Example / test for fetching timeseries from CDA

import { TimeSeriesApi, Configuration } from "cwmsjs";
import fetch from "node-fetch";
global.fetch = fetch;

test("Test TimeSeries V2", async () => {
  const v2_config = new Configuration({
    headers: {
      accept: "application/json;version=2",
    },
  });
  const ts_api = new TimeSeriesApi(v2_config);
  await ts_api
    .getTimeSeriesRaw({
      office: "SWT",
      name: "KEYS.Elev.Inst.1Hour.0.Ccp-Rev",
    })
    .then((r) => {
      console.log(r.raw.url);
      return r.raw.json();
    })
    .then((data) => {
      expect(data?.values).toBeDefined();
    })
    .catch((e) => {
      console.log(e);
      throw Error(e);
    });
}, 30000);
