import { TurbinesApi } from "cwmsjs";
import fetch from "node-fetch";
import { Configuration } from "typedoc";
global.fetch = fetch;

// Read the env vars and see if a test host is set
const host = process.env.CDA_TEST_HOST || null;
const api_key = process.env.CDA_TEST_API_KEY || null;

const config = new Configuration({
  basePath: host,
  // headers:{
  //     "Authorization": `apiKey ${api_key}`
  // }
});
test("Test Parameters", async () => {
  const tb_api = new TurbinesApi(config);
  await tb_api
    .getProjectsTurbines({
      projectId: "KEYS",
      office: "SWT",
    })
    .then((r) => r.raw.json())
    .then((data) => {
      expect(data?.parameters).toBeDefined();
      expect(data?.parameters?.parameters).toBeDefined();
      Object.entries(data?.parameters?.parameters).forEach(([key, value]) => {
        expect(value?.["abstract-param"]).toBeDefined();
        expect(value?.["office"]).toBeDefined();
        expect(value?.["default-english-unit"]).toBeDefined();
        expect(value?.["default-si-unit"]).toBeDefined();
        expect(value?.["long-name"]).toBeDefined();
        expect(value?.["description"]).toBeDefined();
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
