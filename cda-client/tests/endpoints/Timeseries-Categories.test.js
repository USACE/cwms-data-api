import { TimeSeriesCategoriesApi } from "cwmsjs";
import fetch from "node-fetch";
global.fetch = fetch;

test("Test TS Categories", async () => {
  const OFFICE = "SWT";

  // Initialize the timeseries groups api with the default config
  const tsc_api = new TimeSeriesCategoriesApi();
  // Fetch ALL timeseries groups for the given office
  await tsc_api.getTimeSeriesCategory({ office: OFFICE }).then((data) => {
    expect(data[0]).toBeDefined();
    const CATEGORIES = data.map((cat) => cat.id);
    data.forEach((category) => {
      expect(category.id).toBeDefined();
    });
    console.log(`Fetched ${CATEGORIES.length} timeseries for office ${OFFICE}`);
  });
}, 20000);
