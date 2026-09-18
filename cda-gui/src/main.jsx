import React from "react";
// Routing
import ReactDOM from "react-dom/client";
import { Link, createBrowserRouter, RouterProvider } from "react-router-dom";

import { LinkProvider } from "@usace/groundwork";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

// Pages
import Home from "./pages/Home";
import Disclaimer from "./pages/Disclaimer";
import SiteMap from "./pages/SiteMap";
import QuickStart from "./pages/quick-start";
import NotFound from "./pages/NotFound";
import SwaggerUI from "./pages/swagger-ui/index";
import Regexp from "./pages/regexp/index";
import DataQuery from "./pages/data-query";
import Layout from "./components/Layout";
import LocationSearch from "./pages/LocationSearch.jsx";

// Styles
import "@usace/groundwork/dist/groundwork.css";
import "./css/index.css";
import ErrorFallback from "./pages/ErrorFallback";
import FilterExpressions from "./pages/rsql";
import Timestamps from "./pages/timestamps";
import LegacyFormat from "./pages/legacy-format/index.jsx";
import UserLists from "./pages/user-lists/index.jsx";
import { routePaths } from "./route-paths";
import AppAuthProvider from "./components/AppAuthProvider.jsx";
import GlobalErrorBoundary from "./components/GlobalErrorBoundary.jsx";

const queryClient = new QueryClient();
const routeComponents = {
  home: Home,
  disclaimer: Disclaimer,
  "site-map": SiteMap,
  "quick-start": QuickStart,
  "swagger-ui": SwaggerUI,
  "data-query": DataQuery,
  regexp: Regexp,
  "filter-expressions": FilterExpressions,
  timestamps: Timestamps,
  "legacy-format": LegacyFormat,
  "location-search": LocationSearch,
  "user-lists": UserLists,
};

const router = createBrowserRouter(
  [
    {
      path: "/",
      element: <Layout />,
      errorElement: <ErrorFallback />,
      children: [
        ...routePaths.map(({ id, index, path }) => {
          const Component = routeComponents[id];
          return index
            ? { index: true, element: <Component /> }
            : { path, element: <Component /> };
        }),
        { path: "*", element: <NotFound /> },
      ],
    },
  ],
  { basename: "/cwms-data" },
);

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <GlobalErrorBoundary>
      <QueryClientProvider client={queryClient}>
        <AppAuthProvider>
          <LinkProvider component={Link} hrefMap="to">
            <RouterProvider router={router} />
          </LinkProvider>
        </AppAuthProvider>
      </QueryClientProvider>
    </GlobalErrorBoundary>
  </React.StrictMode>,
);
