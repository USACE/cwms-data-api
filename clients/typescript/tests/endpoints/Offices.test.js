import { OfficesApi } from "cwmsjs";
import fetch from "node-fetch";
global.fetch = fetch;

test("Test Offices", async () => {
  const o_api = new OfficesApi();
  await o_api
    .getOffices()
    .then((data) => {
      expect(data).toBeDefined();
      data.forEach((office) => {
        expect(office.name).toBeDefined();
        expect(office.type).toBeDefined();
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
