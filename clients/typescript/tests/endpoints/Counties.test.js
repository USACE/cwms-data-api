// Example / test for fetching timeseries from CDA

import { CountiesApi, Configuration } from "cwmsjs";
import fetch from "node-fetch";
global.fetch = fetch;

// TODO: Why does the query fail when you do not specify version 2 in the headers?
const c_config = new Configuration({
  headers: {
    accept: "application/json;version=2",
  },
});
test("Test Counties", async () => {
  const c_api = new CountiesApi(c_config);
  await c_api
    .getCounties()
    .then((data) => {
      expect(data).toBeDefined();
      data.forEach((value) => {
        expect(value?.name).toBeDefined();
      });
    })
    .catch(async (e) => {
      if (e.response) {
        const error_msg = await e.response.json();
        e.message = `${e.response.url}\n${e.message}\n${JSON.stringify(error_msg, null, 2)}`;
        console.error(e);
        throw e;
      } else {
        throw e;
      }
    });
}, 10000);
