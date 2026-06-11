import SwaggerUIBundle from "swagger-ui-dist/swagger-ui-bundle";
import "swagger-ui-dist/swagger-ui.css";
import "../../css/swagger.css";

import {
  createCwmsLoginAuthMethod,
  createKeycloakAuthMethod,
} from "@usace-watermanagement/groundwork-water";
import { useEffect, useMemo, useRef, useState } from "react";
import { getBasePath } from "../../utils/base";

function normalizeOpenIdConnectUrls(spec) {
  const schemes = spec.components?.securitySchemes ?? {};
  for (const scheme of Object.values(schemes)) {
    if (scheme.type === "openIdConnect" && scheme.openIdConnectUrl) {
      const openIdConnectUrl = new URL(scheme.openIdConnectUrl, window.location.origin);
      if (openIdConnectUrl.hostname === "auth") {
        openIdConnectUrl.protocol = window.location.protocol;
        openIdConnectUrl.host = window.location.host;
        scheme.openIdConnectUrl = openIdConnectUrl.toString();
      }
    }
  }
}

function getOpenIdConnectScheme(spec) {
  const schemes = spec.components?.securitySchemes ?? {};
  return Object.values(schemes).find((scheme) => scheme.type === "openIdConnect");
}

function getCwmsLoginScheme(spec) {
  return spec.components?.securitySchemes?.CwmsAAACacAuth;
}

function isLoopbackHost(hostname) {
  return ["localhost", "127.0.0.1", "::1"].includes(hostname);
}

function isLocalOrigin(url) {
  return isLoopbackHost(new URL(url).hostname);
}

async function isCwmsLoginAvailable() {
  try {
    const response = await fetch(`${window.location.origin}/CWMSLogin`, {
      cache: "no-store",
      redirect: "manual",
    });
    return response.type === "opaqueredirect" || response.status < 400;
  } catch {
    return false;
  }
}

function getKeycloakConfig(spec) {
  const scheme = getOpenIdConnectScheme(spec);
  if (!scheme?.openIdConnectUrl) {
    return null;
  }

  const openIdConnectUrl = new URL(scheme.openIdConnectUrl);
  const realmMatch = openIdConnectUrl.pathname.match(/^(.*)\/realms\/([^/]+)\//);
  if (!realmMatch) {
    return null;
  }

  const providerHint = scheme["x-kc_idp_hint"]?.values?.[0];
  const useLocalDevCredentials = isLocalOrigin(openIdConnectUrl);
  return {
    host: `${openIdConnectUrl.origin}${realmMatch[1]}`,
    realm: realmMatch[2],
    client: scheme["x-oidc-client-id"],
    flow: useLocalDevCredentials ? "direct-grant" : "authorization-code-pkce",
    username: useLocalDevCredentials ? "m5hectest" : undefined,
    password: useLocalDevCredentials ? "m5hectest" : undefined,
    redirectUri: window.location.href.split("?")[0],
    postLogoutRedirectUri: window.location.href.split("?")[0],
    providerHint: useLocalDevCredentials ? undefined : providerHint,
  };
}

function removeSwaggerAuthOptions(spec) {
  if (spec.components?.securitySchemes) {
    spec.components.securitySchemes = {};
  }

  for (const path of Object.values(spec.paths ?? {})) {
    for (const operation of Object.values(path ?? {})) {
      if (operation && typeof operation === "object") {
        operation.security = [];
      }
    }
  }
}

export default function SwaggerUI() {
  const [authStatus, setAuthStatus] = useState("checking");
  const [authMode, setAuthMode] = useState("custom");
  const [customAuthType, setCustomAuthType] = useState(null);
  const [isOpenIdAuthReady, setIsOpenIdAuthReady] = useState(false);
  const autoLoginAttemptedRef = useRef(false);
  const openIdAuthMethodRef = useRef(null);
  const openIdAuthConfigSignatureRef = useRef(null);
  const useSwaggerLogin = authMode === "swagger";
  const cwmsAuthMethod = useMemo(() => {
    const basePath = getBasePath();
    return createCwmsLoginAuthMethod({
      authUrl: `${window.location.origin}/CWMSLogin`,
      authCheckUrl: `${basePath}/auth/keys`,
    });
  }, []);
  const openIdAuthMethod = isOpenIdAuthReady ? openIdAuthMethodRef.current : null;
  const authMethod =
    customAuthType === "cwms"
      ? cwmsAuthMethod
      : customAuthType === "openid"
        ? openIdAuthMethod
        : null;
  const customAuthLabel = customAuthType === "cwms" ? "CWMS Login" : "CWBI Login";

  const checkAuth = async () => {
    if (!authMethod) {
      setAuthStatus("anonymous");
      return;
    }

    setAuthStatus("checking");
    try {
      const isAuth = await authMethod.isAuth();
      setAuthStatus(isAuth ? "authenticated" : "anonymous");
    } catch {
      setAuthStatus("anonymous");
    }
  };

  useEffect(() => {
    // document.querySelector("#swagger-ui").prepend(Index)
    // TODO: Add page index to top of page
    // Alter the page title to match the swagger page
    document.title = "CWMS Data API for Data Retrieval - Swagger UI";
    // Begin Swagger UI call region
    // TODO: add endpoint that dynamic returns swagger generated doc

    let cancelled = false;

    async function initSwagger() {
      const response = await fetch(`${getBasePath()}/swagger-docs`, {
        headers: {
          Accept: "application/json",
        },
      });
      const spec = await response.json();
      normalizeOpenIdConnectUrls(spec);
      const keycloakConfig = getKeycloakConfig(spec);
      const hasCwmsLogin = getCwmsLoginScheme(spec) && (await isCwmsLoginAvailable());
      const nextCustomAuthType = hasCwmsLogin
        ? "cwms"
        : keycloakConfig
          ? "openid"
          : null;
      if (customAuthType !== nextCustomAuthType) {
        setCustomAuthType(nextCustomAuthType);
      }
      if (nextCustomAuthType === "openid" && keycloakConfig) {
        const keycloakConfigSignature = JSON.stringify(keycloakConfig);
        if (openIdAuthConfigSignatureRef.current !== keycloakConfigSignature) {
          openIdAuthMethodRef.current = createKeycloakAuthMethod(keycloakConfig);
          openIdAuthConfigSignatureRef.current = keycloakConfigSignature;
        }
        setIsOpenIdAuthReady(true);
      } else {
        openIdAuthMethodRef.current = null;
        openIdAuthConfigSignatureRef.current = null;
        setIsOpenIdAuthReady(false);
      }
      if (!useSwaggerLogin) {
        removeSwaggerAuthOptions(spec);
      }

      if (cancelled) {
        return;
      }

      const ui = SwaggerUIBundle({
        spec,
        dom_id: "#swagger-ui",
        deepLinking: false,
        presets: [SwaggerUIBundle.presets.apis],
        plugins: [SwaggerUIBundle.plugins.DownloadUrl],
        requestInterceptor: (req) => {
          // Add a cache-busting query param... but only if it's to our api. Some
          // external systems, like keycloak, don't allow random unknown parameters.
          const requestUrl = new URL(req.url, window.location.origin);
          if (requestUrl.hostname === "auth") {
            requestUrl.protocol = window.location.protocol;
            requestUrl.host = window.location.host;
            req.url = requestUrl.toString();
          }

          if (
            requestUrl.origin === window.location.origin &&
            requestUrl.pathname.startsWith(getBasePath())
          ) {
            req.credentials = "include";
            requestUrl.searchParams.set("_cb", Date.now());
            req.url = requestUrl.toString();

            if (!useSwaggerLogin && authMethod?.token) {
              req.headers["Authorization"] = `Bearer ${authMethod.token}`;
            }

            // Also ask intermediaries not to serve from cache
            req.headers["Cache-Control"] = "no-cache, no-store, max-age=0";
            req.headers["Pragma"] = "no-cache";
          }
          return req;
        },
        onComplete: () => {
          if (useSwaggerLogin) {
            for (const schemeName in spec.components.securitySchemes) {
              const scheme = spec.components.securitySchemes[schemeName];
              if (scheme.type === "openIdConnect") {
                let additionalParams = null;
                let hints = scheme["x-kc_idp_hint"];
                if (hints) {
                  additionalParams = {
                    // Since getting the interface to allow users to choose
                    // is likely impossible, we will assume the first in the list
                    // is the "primary" auth system
                    kc_idp_hint: hints.values[0],
                  };
                }
                ui.initOAuth({
                  clientId: scheme["x-oidc-client-id"],
                  usePkceWithAuthorizationCodeGrant: true,
                  additionalQueryStringParams: additionalParams,
                });
                break;
              }
            }
          }
        },
      });
    }

    initSwagger();

    return () => {
      cancelled = true;
      document.querySelector("#swagger-ui").innerHTML = "";
    };
  }, [authMethod, customAuthType, useSwaggerLogin]);

  useEffect(() => {
    if (!authMethod) {
      setAuthStatus("anonymous");
      return;
    }

    let mounted = true;
    authMethod
      .isAuth()
      .then((isAuth) => {
        if (mounted) {
          setAuthStatus(isAuth ? "authenticated" : "anonymous");
        }
      })
      .catch(() => {
        if (mounted) {
          setAuthStatus("anonymous");
        }
      });
    return () => {
      mounted = false;
    };
  }, [authMethod]);

  useEffect(() => {
    if (
      customAuthType !== "openid" ||
      !authMethod ||
      !isLoopbackHost(window.location.hostname) ||
      autoLoginAttemptedRef.current
    ) {
      return undefined;
    }

    autoLoginAttemptedRef.current = true;
    let mounted = true;
    setAuthStatus("checking");
    authMethod
      .login()
      .then(() => authMethod.isAuth())
      .then((isAuth) => {
        if (mounted) {
          setAuthStatus(isAuth ? "authenticated" : "anonymous");
        }
      })
      .catch(() => {
        if (mounted) {
          setAuthStatus("anonymous");
        }
      });

    return () => {
      mounted = false;
    };
  }, [authMethod, customAuthType]);

  const startOpenIdLogin = async () => {
    if (!authMethod) {
      return;
    }

    await authMethod.login();
    await checkAuth();
  };

  const isAuthenticated = authStatus === "authenticated";
  const isCheckingAuth = authStatus === "checking";

  return (
    <>
      <div
        className={`swagger-auth-bar ${useSwaggerLogin ? "swagger-login-mode" : ""}`}
      >
        <div>
          <strong>{useSwaggerLogin ? "Swagger Login" : customAuthLabel}</strong>
          <span>
            {customAuthType === "cwms"
              ? isAuthenticated
                ? "Signed in with the shared CWMS session. Swagger requests will include it automatically."
                : "Sign in with the district CWMS AAA login before using secured endpoints."
              : isLoopbackHost(window.location.hostname)
                ? isAuthenticated
                  ? "Signed in with the local Keycloak dev account."
                  : "Local Keycloak dev login uses m5hectest / m5hectest."
                : "Sign in through the OpenID authorization flow for this CDA deployment."}
          </span>
        </div>
        <div className="swagger-auth-actions">
          <label className="swagger-auth-mode-select">
            <span>Login mode</span>
            <select
              onChange={(event) => setAuthMode(event.target.value)}
              value={authMode}
            >
              <option value="custom">{customAuthLabel}</option>
              <option value="swagger">Swagger Login</option>
            </select>
          </label>
          {!useSwaggerLogin && customAuthType === "cwms" ? (
            <>
              <button
                className="swagger-auth-button primary"
                disabled={isCheckingAuth}
                onClick={() => authMethod.login()}
                type="button"
              >
                {isAuthenticated ? "Reauthenticate" : "Sign in"}
              </button>
              {isAuthenticated && (
                <button
                  className="swagger-auth-button danger"
                  disabled={isCheckingAuth}
                  onClick={() => authMethod.logout()}
                  type="button"
                >
                  Sign out
                </button>
              )}
              <button
                className="swagger-auth-button secondary"
                disabled={isCheckingAuth}
                onClick={checkAuth}
                type="button"
              >
                Refresh status
              </button>
            </>
          ) : !useSwaggerLogin ? (
            <>
              <button
                className="swagger-auth-button primary"
                disabled={!authMethod || isCheckingAuth}
                onClick={startOpenIdLogin}
                type="button"
              >
                {isAuthenticated ? "Reauthenticate" : "Sign in"}
              </button>
              {isAuthenticated && (
                <button
                  className="swagger-auth-button danger"
                  disabled={isCheckingAuth}
                  onClick={() => authMethod.logout().then(checkAuth)}
                  type="button"
                >
                  Sign out
                </button>
              )}
            </>
          ) : (
            <span className="swagger-auth-mode-note">
              Swagger authorization controls are enabled.
            </span>
          )}
        </div>
      </div>
      <div
        className={
          useSwaggerLogin ? "swagger-ui-host swagger-login-mode" : "swagger-ui-host"
        }
        id="swagger-ui"
      ></div>
    </>
  );
}
