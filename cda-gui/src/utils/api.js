import { TimeSeriesApi, LevelsApi, CatalogApi, Configuration, LocationsApi } from "cwmsjs"

const ts_api = new TimeSeriesApi()
const level_api = new LevelsApi()
const catalog_api = new CatalogApi()
const locations_api = new LocationsApi()
const config_v2 = new Configuration({
    headers: {
        "accept": "application/json;version=2"
    }
})

const CDA_DATE_FORMAT = "YYYY-MM-DDTHH:mm:ssZ"

export { ts_api, level_api, catalog_api, locations_api, config_v2, CDA_DATE_FORMAT }

