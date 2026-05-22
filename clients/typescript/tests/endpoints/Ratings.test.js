import { RatingsApi, Configuration } from "cwmsjs";
import fetch from "node-fetch";
global.fetch = fetch;

test("Test Ratings", async () => {
  const config_v2 = new Configuration({
    headers: {
      accept: "application/json;version=2",
    },
  });
  const r_api = new RatingsApi(config_v2);
  await r_api
    .getRatings({
      office: "SWT",
    })
    .then((data) => {
      expect(data).toBeDefined();
    });
  //   Fetch the CWMS Rating specification for the SWT OFFICE given a rating mask for keystone lake
  await r_api
    .getRatingsSpec({
      office: "SWT",
      ratingIdMask: "KEYS.*",
    })
    .then((data) => {
      expect(data?.specs).toBeDefined();
    });
  //   Fetch the CWMS Rating templates for the SPK OFFICE
  await r_api
    .getRatingsTemplate({
      office: "SPK",
    })
    .then((data) => {
      expect(data?.templates).toBeDefined();
    });

  //   await r_api
  //     .getRatingsTemplateWithTemplateId({
  //       office: "SWT",
  //       templateId: "Elev-Alt;Stor-Alt.Linear",
  //     })
  //     .then((data) => {
  //       expect(data).toBeDefined();
  //     });
}, 30000);
