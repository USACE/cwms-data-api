// Routing
import React from "react";
import ReactDOM from "react-dom/client";
import { Link, createBrowserRouter, RouterProvider } from "react-router-dom";

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
import "@usace/groundwork/dist/style.css";
import "./css/index.css";
import ErrorFallback from "./pages/ErrorFallback";
import FilterExpressions from "./pages/rsql";
import Timestamps from "./pages/timestamps";
import LegacyFormat from "./pages/legacy-format/index.jsx";

const queryClient = new QueryClient();

const router = createBrowserRouter(
  [
    {
      path: "/",
      element: <Layout />,
      errorElement: <ErrorFallback />,
      children: [
        { index: true, element: <Home /> },
        {
          path: "swagger-ui",
          element: <SwaggerUI />,
        },
        { path: "data-query", element: <DataQuery /> },
        { path: "regexp", element: <Regexp /> },
        { path: "filter-expressions", element: <FilterExpressions /> },
        { path: "timestamps", element: <Timestamps /> },
        { path: "legacy-format", element: <LegacyFormat /> },
        { path: "*", element: <NotFound /> },
      ],
    },
  ],
  { basename: "/cwms-data" }
);

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <LinkProvider component={Link} hrefMap="to">
        <RouterProvider router={router} />
      </LinkProvider>
    </QueryClientProvider>
  </React.StrictMode>
);
