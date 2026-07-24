// Routing
import React from "react";
import ReactDOM from "react-dom/client";
import { Link, createBrowserRouter, RouterProvider } from "react-router-dom";

import { LinkProvider } from "@usace/groundwork";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  AuthProvider,
  createKeycloakAuthMethod,
} from "@usace-watermanagement/groundwork-water";

// Pages
import Home from "./pages/Home";
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

const queryClient = new QueryClient();

function createLocalAuthMethod() {
  let token;
  return {
    async login() {
      const response = await fetch(
        `${import.meta.env.VITE_AUTH_HOST}/realms/${import.meta.env.VITE_AUTH_REALM}/protocol/openid-connect/token`,
        {
          method: "POST",
          headers: { "Content-Type": "application/x-www-form-urlencoded" },
          body: new URLSearchParams({
            grant_type: "password",
            client_id: "cwms",
            username: import.meta.env.VITE_AUTH_USER,
            password: import.meta.env.VITE_AUTH_PASSWORD,
          }),
        },
      );
      if (!response.ok) {
        throw new Error(`Local Keycloak login failed (${response.status})`);
      }
      token = (await response.json()).access_token;
    },
    async logout() {
      token = undefined;
    },
    async isAuth() {
      return !!token;
    },
    get token() {
      return token;
    },
  };
}

const authMethod =
  import.meta.env.MODE === "dev-cda-compose"
    ? createLocalAuthMethod()
    : createKeycloakAuthMethod({
        host: import.meta.env.VITE_AUTH_HOST,
        realm: import.meta.env.VITE_AUTH_REALM,
        client: "cwms",
        flow: "authorization-code-pkce",
        redirectUri: window.location.href,
        providerHint: "federation-eams",
      });
const routeComponents = {
  home: Home,
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
    <QueryClientProvider client={queryClient}>
      <AuthProvider method={authMethod}>
        <LinkProvider component={Link} hrefMap="to">
          <RouterProvider router={router} />
        </LinkProvider>
      </AuthProvider>
    </QueryClientProvider>
  </React.StrictMode>,
);
