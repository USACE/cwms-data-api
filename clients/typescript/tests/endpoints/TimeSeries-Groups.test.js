import { TimeSeriesGroupsApi } from "cwmsjs";
import fetch from "node-fetch";
global.fetch = fetch;

test("Test TS Groups", async () => {
  // ⚠️ NOTE!
  // Prior to V 1.14.0 TSGroups was broken and returned empty data for assignedTimeSeries
  // Please update to V 1.15.0 or higher to fix this issue
  // npm uninstall cwmsjs
  // npm install cwmsjs --save

  // Initialize the timeseries groups api with the default config
  const tsg_api = new TimeSeriesGroupsApi();

  const testOffice = "SWT";

  // Fetch ALL timeseries groups for the given office
  await tsg_api
    .getTimeSeriesGroup({ office: testOffice, includeAssigned: false })
    .then((data) => {
      expect(data[0]).toBeDefined();
      data.forEach((group) => {
        expect(group.officeId).toBeDefined();
      });
      console.log(`Fetched ${data.length} groups for office ${testOffice}`);
    });

  // Fetch a specific timeseries groups given an ID for the given office
  await tsg_api
    .getTimeSeriesGroupWithGroupId({
      groupId: "USGS TS Data Acquisition",
      office: "CWMS",
      categoryId: "Data Acquisition",
    })
    .then((group) => {
      expect(group).toBeDefined();
      expect(group.assignedTimeSeries).toBeDefined();
      const TSIDS = group.assignedTimeSeries.map((ts) => ts.timeseriesId);
      expect(TSIDS).toBeDefined();
      console.log(`Fetched ${TSIDS.length} timeseries for group ${group.id}`);
    });
}, 20000);
