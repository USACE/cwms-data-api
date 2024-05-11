import { createRouteBundle } from "redux-bundler";
import Home from "../pages/Home";
import NotFound from "../pages/NotFound";
import SwaggerUI from "../pages/swagger-ui";
import Decide from "../pages/decide";
import RegExp from "../pages/regexp";

export default createRouteBundle({
    "/": Home,
    "/cwms-data/": Decide,
    "/cwms-data/swagger-ui": SwaggerUI,
    "/cwms-data/swagger-ui.html": SwaggerUI,
    "/cwms-data/regexp.html": RegExp,
    "/cwms-data/regexp": RegExp,
    // "/location/:location": Location,
    "*": NotFound,
});