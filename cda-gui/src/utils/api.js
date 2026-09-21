import {
  TimeSeriesApi,
  LevelsApi,
  CatalogApi,
  Configuration,
  LocationsApi,
} from "cwmsjs";

const config_v2 = new Configuration({
  basePath: import.meta.env.VITE_CDA_API_ROOT,
  headers: {
    accept: "application/json;version=2",
  },
});
const ts_api = new TimeSeriesApi(config_v2);
const level_api = new LevelsApi(config_v2);
const catalog_api = new CatalogApi(config_v2);
const locations_api = new LocationsApi(config_v2);

const CDA_DATE_FORMAT = "YYYY-MM-DDTHH:mm:ssZ";

export { ts_api, level_api, catalog_api, locations_api, config_v2, CDA_DATE_FORMAT };
