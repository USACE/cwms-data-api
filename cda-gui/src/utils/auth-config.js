export function isLoopbackHost(hostname) {
  return ["localhost", "127.0.0.1", "::1"].includes(hostname);
}

export function getOpenIdConnectScheme(spec) {
  const schemes = spec.components?.securitySchemes ?? {};
  return Object.values(schemes).find((scheme) => scheme.type === "openIdConnect");
}

export function normalizeOpenIdConnectUrls(spec, currentOrigin) {
  const schemes = spec.components?.securitySchemes ?? {};
  for (const scheme of Object.values(schemes)) {
    if (scheme.type === "openIdConnect" && scheme.openIdConnectUrl) {
      const openIdConnectUrl = new URL(scheme.openIdConnectUrl, currentOrigin);
      if (openIdConnectUrl.hostname === "auth") {
        const currentUrl = new URL(currentOrigin);
        openIdConnectUrl.protocol = currentUrl.protocol;
        openIdConnectUrl.host = currentUrl.host;
        scheme.openIdConnectUrl = openIdConnectUrl.toString();
      }
    }
  }
}

export function getKeycloakConfig(spec, currentUrl) {
  const scheme = getOpenIdConnectScheme(spec);
  if (!scheme?.openIdConnectUrl || !scheme["x-oidc-client-id"]) {
    return null;
  }

  const pageUrl = new URL(currentUrl);
  const openIdConnectUrl = new URL(scheme.openIdConnectUrl, pageUrl.origin);
  const realmMatch = openIdConnectUrl.pathname.match(/^(.*)\/realms\/([^/]+)\//);
  if (!realmMatch) {
    return null;
  }

  const providerHint = scheme["x-kc_idp_hint"]?.values?.[0];
  const useLocalDevCredentials = isLoopbackHost(openIdConnectUrl.hostname);
  const redirectUri = `${pageUrl.origin}${pageUrl.pathname}`;
  return {
    host: `${openIdConnectUrl.origin}${realmMatch[1]}`,
    realm: realmMatch[2],
    client: scheme["x-oidc-client-id"],
    flow: useLocalDevCredentials ? "direct-grant" : "authorization-code-pkce",
    username: useLocalDevCredentials ? "m5hectest" : undefined,
    password: useLocalDevCredentials ? "m5hectest" : undefined,
    redirectUri,
    postLogoutRedirectUri: redirectUri,
    providerHint: useLocalDevCredentials ? undefined : providerHint,
  };
}
