import SwaggerUIBundle from "swagger-ui-dist/swagger-ui-bundle";
import "swagger-ui-dist/swagger-ui.css";
import "../../css/swagger.css";

import {
  createCwmsLoginAuthMethod,
  useAuth,
} from "@usace-watermanagement/groundwork-water";
import { useEffect, useMemo, useRef, useState } from "react";
import { useAuthConfiguration } from "../../components/auth-configuration-context";
import { getBasePath } from "../../utils/base";
import {
  getKeycloakConfig,
  isLoopbackHost,
  normalizeOpenIdConnectUrls,
} from "../../utils/auth-config";

function getCwmsLoginScheme(spec) {
  return spec.components?.securitySchemes?.CwmsAAACacAuth;
}

function isExternalOpenIdOnLocalhost(keycloakConfig) {
  return (
    keycloakConfig?.flow === "authorization-code-pkce" &&
    isLoopbackHost(window.location.hostname)
  );
}

export default function SwaggerUI() {
  const appAuth = useAuth();
  const { error: appAuthError } = useAuthConfiguration();
  const [authStatus, setAuthStatus] = useState("checking");
  const [customAuthType, setCustomAuthType] = useState(null);
  const [authUiMode, setAuthUiMode] = useState("hidden");
  const [isLocalOpenIdAuth, setIsLocalOpenIdAuth] = useState(false);
  const [authError, setAuthError] = useState(null);
  const [swaggerError, setSwaggerError] = useState(null);
  const autoLoginAttemptedRef = useRef(false);
  const cwmsAuthMethod = useMemo(() => {
    const basePath = getBasePath();
    return createCwmsLoginAuthMethod({
      authUrl: `${basePath}/CWMSLogin`,
      authCheckUrl: `${basePath}/auth/keys`,
    });
  }, []);
  const authMethod = customAuthType === "cwms" ? cwmsAuthMethod : null;
  const hasAuthMethod =
    customAuthType === "openid" ? !appAuthError : Boolean(authMethod);
  const authLabel = authUiMode === "cwms-login" ? "CWMS Login" : "CWBI Login";

  const getUnavailableMessage = () => {
    if (hasAuthMethod) {
      return null;
    }

    if (customAuthType === "openid") {
      if (appAuthError) {
        return appAuthError;
      }
      return isLoopbackHost(window.location.hostname)
        ? "Local sign-in is not available for this OpenID configuration. Start the local CDA/Keycloak stack, or use Swagger's Authorize controls with an API key."
        : "Sign-in is not available because this Swagger spec does not advertise a usable OpenID client.";
    }

    return "No supported sign-in method was advertised by this CDA deployment. You can still use Swagger's Authorize controls for API keys.";
  };

  const checkAuth = async () => {
    if (!authMethod) {
      setAuthStatus("anonymous");
      return;
    }

    setAuthStatus("checking");
    setAuthError(null);
    try {
      const isAuth = await authMethod.isAuth();
      setAuthStatus(isAuth ? "authenticated" : "anonymous");
    } catch (error) {
      setAuthError(error?.message ?? "Unable to check sign-in status.");
      setAuthStatus("anonymous");
    }
  };

  useEffect(() => {
    document.title = "CWMS Data API for Data Retrieval - Swagger UI";

    let cancelled = false;

    async function initSwagger() {
      let spec;
      try {
        const response = await fetch(`${getBasePath()}/swagger-docs`, {
          headers: {
            Accept: "application/json",
          },
        });
        if (!response.ok) {
          throw new Error(`Swagger spec request failed with HTTP ${response.status}`);
        }
        spec = await response.json();
        setSwaggerError(null);
      } catch (error) {
        if (!cancelled) {
          setSwaggerError(
            error?.message ?? "Unable to load the Swagger specification.",
          );
          setCustomAuthType(null);
          setAuthUiMode("hidden");
          setAuthStatus("anonymous");
          setIsLocalOpenIdAuth(false);
          document.querySelector("#swagger-ui").innerHTML = "";
        }
        return;
      }
      normalizeOpenIdConnectUrls(spec, window.location.origin);
      const keycloakConfig = getKeycloakConfig(spec, window.location.href);
      const hasCwmsLogin = Boolean(getCwmsLoginScheme(spec));
      // Some non-T7 deployments advertise CwmsAAACacAuth even though their
      // /CWMSLogin route is only a generic landing page. Prefer a usable
      // OpenID configuration when both schemes are present; T7 deployments
      // that advertise only CWMS AAA still use the shared CWMS session flow.
      const nextCustomAuthType = keycloakConfig
        ? "openid"
        : hasCwmsLogin
          ? "cwms"
          : null;

      if (customAuthType !== nextCustomAuthType) {
        setCustomAuthType(nextCustomAuthType);
      }

      if (nextCustomAuthType === "cwms") {
        setAuthUiMode("cwms-login");
      } else if (nextCustomAuthType === "openid" && keycloakConfig) {
        if (isExternalOpenIdOnLocalhost(keycloakConfig)) {
          setAuthUiMode("hidden");
          setAuthError(null);
          setAuthStatus("anonymous");
          setIsLocalOpenIdAuth(false);
        } else {
          setAuthError(null);
          setIsLocalOpenIdAuth(keycloakConfig.flow === "direct-grant");
          setAuthUiMode(
            keycloakConfig.flow === "direct-grant"
              ? "local-keycloak"
              : "external-openid",
          );
        }
      } else {
        setAuthUiMode("hidden");
        setAuthError(null);
        setIsLocalOpenIdAuth(false);
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
            req.headers = req.headers ?? {};

            const token =
              customAuthType === "openid" ? appAuth.token : authMethod?.token;
            if (token) {
              req.headers.Authorization = `Bearer ${token}`;
            }

            req.cache = "no-store";
            req.headers["Cache-Control"] = "no-cache, no-store, max-age=0";
            req.headers.Pragma = "no-cache";
          }
          return req;
        },
        onComplete: () => {
          for (const schemeName in spec.components?.securitySchemes ?? {}) {
            const scheme = spec.components.securitySchemes[schemeName];
            if (scheme.type === "openIdConnect") {
              if (isExternalOpenIdOnLocalhost(keycloakConfig)) {
                break;
              }

              let additionalParams = null;
              let hints = scheme["x-kc_idp_hint"];
              if (hints) {
                additionalParams = {
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
        },
      });
    }

    initSwagger();

    return () => {
      cancelled = true;
      document.querySelector("#swagger-ui").innerHTML = "";
    };
  }, [appAuth.token, authMethod, customAuthType]);

  useEffect(() => {
    if (customAuthType !== "cwms" || !authMethod) {
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
  }, [authMethod, customAuthType]);

  useEffect(() => {
    if (
      customAuthType !== "openid" ||
      !hasAuthMethod ||
      !isLocalOpenIdAuth ||
      autoLoginAttemptedRef.current
    ) {
      return undefined;
    }

    autoLoginAttemptedRef.current = true;
    let mounted = true;
    setAuthStatus("checking");
    setAuthError(null);
    appAuth
      .login()
      .catch((error) => {
        if (mounted) {
          setAuthError(error?.message ?? "Automatic local sign-in failed.");
          setAuthStatus("anonymous");
        }
      });

    return () => {
      mounted = false;
    };
  }, [appAuth, customAuthType, hasAuthMethod, isLocalOpenIdAuth]);

  const startLogin = async () => {
    if (!hasAuthMethod) {
      setAuthError(getUnavailableMessage());
      return;
    }

    setAuthError(null);
    setAuthStatus("checking");
    try {
      if (customAuthType === "openid") {
        await appAuth.login();
      } else {
        await authMethod.login();
        await checkAuth();
      }
    } catch (error) {
      setAuthError(error?.message ?? "Sign-in failed.");
      setAuthStatus("anonymous");
    }
  };

  const signOut = async () => {
    if (!hasAuthMethod) {
      return;
    }

    setAuthError(null);
    setAuthStatus("checking");
    try {
      if (customAuthType === "openid") {
        await appAuth.logout();
      } else {
        await authMethod.logout();
        await checkAuth();
      }
    } catch (error) {
      setAuthError(error?.message ?? "Sign-out failed.");
      setAuthStatus("anonymous");
    }
  };

  const isAuthenticated =
    customAuthType === "openid" ? appAuth.isAuth : authStatus === "authenticated";
  const isCheckingAuth =
    customAuthType === "openid" ? appAuth.isLoading : authStatus === "checking";
  const unavailableMessage = getUnavailableMessage();
  const showAuthBar = authUiMode !== "hidden";
  const showUnavailableMessage =
    showAuthBar && !hasAuthMethod && !isCheckingAuth;

  return (
    <>
      {showAuthBar && (
        <div className="my-3 flex flex-col gap-4 rounded-md border border-sky-200 bg-sky-50 p-4 sm:flex-row sm:items-center sm:justify-between">
          <div className="min-w-0">
            <strong className="block text-base text-slate-800">{authLabel}</strong>
            <span className="block text-sm text-slate-600">
              {authUiMode === "cwms-login"
                ? isAuthenticated
                  ? "Signed in with the shared CWMS session. Swagger requests will include it automatically."
                  : "Sign in with the district CWMS AAA login before using secured endpoints."
                : authUiMode === "local-keycloak"
                  ? isAuthenticated
                    ? "Signed in with the local Keycloak dev account."
                    : "Local Keycloak dev login uses m5hectest / m5hectest."
                  : "Sign in through the OpenID authorization flow for this CDA deployment."}
            </span>
            {showUnavailableMessage && (
              <span className="mt-2 block text-sm text-amber-800">
                {unavailableMessage}
              </span>
            )}
            {authError && (
              <span className="mt-2 block text-sm text-red-700">{authError}</span>
            )}
          </div>
          <div className="flex flex-wrap items-center gap-2 sm:justify-end">
            {hasAuthMethod && (
              <button
                className="h-9 rounded border border-blue-700 bg-blue-600 px-4 text-sm font-semibold text-white hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-55"
                disabled={isCheckingAuth}
                onClick={startLogin}
                type="button"
              >
                {isAuthenticated ? "Reauthenticate" : "Sign in"}
              </button>
            )}
            {isAuthenticated && (
              <button
                className="h-9 rounded border border-red-800 bg-red-700 px-4 text-sm font-semibold text-white hover:bg-red-800 disabled:cursor-not-allowed disabled:opacity-55"
                disabled={isCheckingAuth}
                onClick={signOut}
                type="button"
              >
                Sign out
              </button>
            )}
            {authUiMode === "cwms-login" && (
              <button
                className="h-9 rounded border border-sky-200 bg-white px-4 text-sm font-semibold text-slate-800 hover:bg-sky-100 disabled:cursor-not-allowed disabled:opacity-55"
                disabled={isCheckingAuth}
                onClick={checkAuth}
                type="button"
              >
                Refresh status
              </button>
            )}
          </div>
        </div>
      )}
      {swaggerError && (
        <div className="my-3 rounded-md border border-red-200 bg-red-50 p-4 text-sm text-red-800">
          {swaggerError}
        </div>
      )}
      <div id="swagger-ui"></div>
    </>
  );
}
