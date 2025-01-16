import { createRouteBundle } from "redux-bundler";
import Home from "../pages/Home";
import NotFound from "../pages/NotFound";
import SwaggerUI from "../pages/swagger-ui";
import RegExp from "../pages/regexp";
import DataQuery from "../pages/data-query";

export default createRouteBundle({
    "/cwms-data/": Home,
    "/cwms-data/swagger-ui": SwaggerUI,
    "/cwms-data/swagger-ui.html": SwaggerUI,
    "/cwms-data/regexp.html": RegExp,
    "/cwms-data/regexp": RegExp,
    "/cwms-data/data-query": DataQuery,
    // "/location/:location": Location,
    "*": NotFound,
});