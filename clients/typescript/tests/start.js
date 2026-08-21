// Graham - 04/2024
// This file initializes all other tests and collects them

import chalk from "chalk";
import {
  AuthorizationApi,
  BasinsApi,
  BlobApi,
  CatalogApi,
  ClobApi,
  CountiesApi,
  DefaultApi,
  LevelsApi,
  LocationCategoriesApi,
  LocationGroupsApi,
  LocationsApi,
  OfficesApi,
  ParametersApi,
  PoolsApi,
  RatingsApi,
  StatesApi,
  TimeSeriesApi,
  TimeSeriesCategoriesApi,
  TimeSeriesIdentifierApi,
  TimeZonesApi,
  TimeSeriesGroupsApi,
  UnitsApi,
} from "cwmsjs";
// Install fetch for use with testing in the library (fetch is native on browsers)
import fetch from "node-fetch";
global.fetch = fetch;
