import { composeBundles, createUrlBundle } from "redux-bundler";
import routeBundle from "./route-bundle";

export default composeBundles(createUrlBundle(), routeBundle);