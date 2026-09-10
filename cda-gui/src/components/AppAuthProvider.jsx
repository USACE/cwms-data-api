import {
  AuthProvider,
  createKeycloakAuthMethod,
} from "@usace-watermanagement/groundwork-water";
import PropTypes from "prop-types";
import { useEffect, useState } from "react";

import { getBasePath } from "../utils/base";
import { getKeycloakConfig, normalizeOpenIdConnectUrls } from "../utils/auth-config";
import { AuthConfigurationContext } from "./auth-configuration-context";

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

const unavailableAuthMethod = {
  async login() {},
  async logout() {},
  async isAuth() {
    return false;
  },
  get token() {
    return undefined;
  },
};

async function loadDeployedAuthMethod() {
  const response = await fetch(`${getBasePath()}/swagger-docs`, {
    cache: "no-store",
    headers: { Accept: "application/json" },
  });
  if (!response.ok) {
    throw new Error(`OpenAPI request failed with HTTP ${response.status}`);
  }

  const spec = await response.json();
  normalizeOpenIdConnectUrls(spec, window.location.origin);
  const config = getKeycloakConfig(spec, window.location.href);
  if (!config) {
    throw new Error("The OpenAPI document does not advertise a usable OpenID client.");
  }
  return createKeycloakAuthMethod(config);
}

export default function AppAuthProvider({ children }) {
  const [state, setState] = useState(() =>
    import.meta.env.MODE === "dev-cda-compose"
      ? { method: createLocalAuthMethod(), error: null }
      : { method: null, error: null },
  );

  useEffect(() => {
    if (state.method) return undefined;

    let cancelled = false;
    loadDeployedAuthMethod()
      .then((method) => {
        if (!cancelled) setState({ method, error: null });
      })
      .catch((error) => {
        if (!cancelled) {
          setState({
            method: unavailableAuthMethod,
            error: `Sign-in is unavailable: ${error?.message ?? "invalid authentication configuration"}`,
          });
        }
      });

    return () => {
      cancelled = true;
    };
  }, [state.method]);

  if (!state.method) {
    return (
      <div className="p-6 text-slate-700" role="status">
        Loading authentication configuration…
      </div>
    );
  }

  return (
    <AuthConfigurationContext.Provider value={{ error: state.error }}>
      <AuthProvider method={state.method}>{children}</AuthProvider>
    </AuthConfigurationContext.Provider>
  );
}

AppAuthProvider.propTypes = {
  children: PropTypes.node.isRequired,
};
