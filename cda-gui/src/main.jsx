// Routing
import React from "react";
import ReactDOM from "react-dom/client";
import { Link, createBrowserRouter, RouterProvider, href } from "react-router-dom";

import { LinkProvider } from "@usace/groundwork";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

// Pages
import Home from "./pages/Home";
import NotFound from "./pages/NotFound";
import SwaggerUI from "./pages/swagger-ui/index";
import Regexp from "./pages/regexp/index";
import DataQuery from "./pages/data-query";
import Layout from "./components/Layout";

// Styles
import "@usace/groundwork/dist/groundwork.css";
import "./css/index.css";
import ErrorFallback from "./pages/ErrorFallback";
import FilterExpressions from "./pages/rsql";
import Timestamps from "./pages/timestamps";
import LegacyFormat from "./pages/legacy-format/index.jsx";
import { routePaths } from "./route-paths";

const queryClient = new QueryClient();
const routeComponents = {
  home: Home,
  "swagger-ui": SwaggerUI,
  "swagger-docs": () => <div>Hello?</div>,
  "data-query": DataQuery,
  regexp: Regexp,
  "filter-expressions": FilterExpressions,
  timestamps: Timestamps,
  "legacy-format": LegacyFormat,
};

const router = createBrowserRouter(
  [
    {
      path: "/",
      element: <Layout />,
      errorElement: <ErrorFallback />,
      children: [
        ...routePaths.map((route) => {
          const { id, index, path } = route;
          const Component = routeComponents[id];
          return index
            ? { index: true, element: <Component />, loader: route.loader }
            : { path: path, element: <Component />, loader: route.loader };
        }),
        { path: "*", element: <NotFound /> },
      ],
    },
  ],
  { basename: "/cwms-data" },
);

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <LinkProvider component={Link} hrefMap="to">
        <RouterProvider router={router} />
      </LinkProvider>
    </QueryClientProvider>
  </React.StrictMode>,
);
