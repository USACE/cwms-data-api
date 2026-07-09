import { PoolsApi, Configuration } from "cwmsjs";
import fetch from "node-fetch";
global.fetch = fetch;

test("Test Pools", async () => {
  const config = new Configuration({
    headers: {
      accept: "application/json;version=2",
    },
  });
  const p_api = new PoolsApi(config);
  await p_api
    .getPoolsRaw({
      office: "SWT",
    })
    .then(async (r) => {
      const data = await r.raw.json();
      return data;
    })
    .then((data) => {
      expect(data).toBeDefined();
      // data.forEach((category) => {
      //     expect(category).toBeDefined()
      //     expect(category["id"]).toBeDefined()
      // })
    });
}, 30000);
