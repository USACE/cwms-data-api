import { LevelsApi, Configuration } from "cwmsjs";

import fetch from "node-fetch";
global.fetch = fetch;

test("Test Levels", async () => {
  const l_api = new LevelsApi();
  await l_api
    .getLevels({
      levelIdMask: "*KEYS*",
      office: "SWT",
      format: "json",
    })
    .then((data) => {
      expect(data?.["location-levels"]?.["location-levels"]).toBeDefined();
      data?.["location-levels"]?.["location-levels"].forEach((level) => {
        expect(level?.name).toBeDefined();
        expect(level?.office).toBeDefined();
        expect(level?.values).toBeDefined();
      });
    })
    .catch(async (e) => {
      if (e.response) {
        const error_msg = await e.response.json();
        e.message = `${e.response.url}\n${e.message}\n${JSON.stringify(error_msg, null, 2)}`;
        console.error(e);
      }
      throw e;
    });
}, 10000);
