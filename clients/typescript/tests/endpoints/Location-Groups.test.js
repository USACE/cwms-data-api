import { LocationGroupsApi } from "cwmsjs";
import fetch from "node-fetch";
global.fetch = fetch;

test("Test Location Groups", async () => {
  const lg_api = new LocationGroupsApi();
  await lg_api
    .getLocationGroup({
      office: "SWT",
    })
    .then((data) => {
      expect(data).toBeDefined();
      data.forEach((category) => {
        expect(category).toBeDefined();
        expect(category["id"]).toBeDefined();
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
}, 30000);
