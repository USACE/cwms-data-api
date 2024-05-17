import { createRouteBundle } from "redux-bundler";
import Home from "../pages/Home";
import NotFound from "../pages/NotFound";
import SwaggerUI from "../pages/swagger-ui";
import RegExp from "../pages/regexp";

export default createRouteBundle({
    "/swf-data/": Home,
    "/swf-data/swagger-ui": SwaggerUI,
    "/swf-data/swagger-ui.html": SwaggerUI,
    "/swf-data/regexp.html": RegExp,
    "/swf-data/regexp": RegExp,
    // "/location/:location": Location,
    "*": NotFound,
});