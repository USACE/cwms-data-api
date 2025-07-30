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
import ErrorFallback from "./pages/data-query/components/ErrorFallBack";

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
