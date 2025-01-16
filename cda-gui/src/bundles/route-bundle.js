import { createRouteBundle } from "redux-bundler";
import Home from "../pages/Home";
import NotFound from "../pages/NotFound";
import SwaggerUI from "../pages/swagger-ui";
import RegExp from "../pages/regexp";
import DataQuery from "../pages/data-query";

const BASE_PATH = import.meta.env.BASE_PATH
console.log("Base path", BASE_PATH)
export default createRouteBundle({
    [`/${BASE_PATH}/`]: Home,
    [`/${BASE_PATH}/swagger-ui`]: SwaggerUI,
    [`/${BASE_PATH}/swagger-ui.html`]: SwaggerUI,
    [`/${BASE_PATH}/regexp.html`]: RegExp,
    [`/${BASE_PATH}/regexp`]: RegExp,
    [`/${BASE_PATH}/data-query`]: DataQuery,
    // "/location/:location": Location,
    "*": NotFound,
});