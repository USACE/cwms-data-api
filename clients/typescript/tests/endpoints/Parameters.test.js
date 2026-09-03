import { ParametersApi } from "cwmsjs";
import fetch from "node-fetch";
global.fetch = fetch;

test("Test Parameters", async () => {
  const p_api = new ParametersApi();
  await p_api
    .getParameters({
      office: "SWT",
    })
    .then((data) => {
      expect(data).toBeDefined();
      data.forEach((parameter) => {
        expect(parameter.officeId).toBeDefined();
        expect(parameter.name).toBeDefined();
        expect(parameter.baseParameter).toBeDefined();
        expect(parameter.dbUnitId).toBeDefined();
        expect(parameter.unitLongName).toBeDefined();
        expect(parameter.unitDescription).toBeDefined();
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
}, 60000);
