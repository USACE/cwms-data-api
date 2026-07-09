// !ignore
import { BasinsApi } from "cwmsjs";
import fetch from "node-fetch";
global.fetch = fetch;

test("Test Basins", async () => {
  const bas_api = new BasinsApi();
  let basins = await bas_api.getDataBasins({
    office: "SWT",
  });

  // If this throws an error,
  // then the basin data is not being returned from the API
  expect(basins).toBeDefined();
}, 30000);
