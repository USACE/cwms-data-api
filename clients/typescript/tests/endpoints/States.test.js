import { StatesApi, Configuration } from "cwmsjs";
import fetch from "node-fetch";
global.fetch = fetch;

test("Test States", async () => {
  const config = new Configuration({
    headers: {
      accept: "application/json;version=2",
    },
  });
  const s_api = new StatesApi(config);
  s_api.getStates().then((data) => {
    expect(data).toBeDefined();
  });
}, 30000);
