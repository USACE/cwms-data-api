import assert from "node:assert/strict";
import test from "node:test";

import { getKeycloakConfig, normalizeOpenIdConnectUrls } from "./auth-config.js";

function specWithOpenId(openIdConnectUrl, client = "cwms") {
  return {
    components: {
      securitySchemes: {
        OpenID: {
          type: "openIdConnect",
          openIdConnectUrl,
          "x-oidc-client-id": client,
          "x-kc_idp_hint": { values: ["federation-eams"] },
        },
      },
    },
  };
}

test("returns null when the deployment does not advertise OpenID", () => {
  assert.equal(getKeycloakConfig({}, "https://water.dev.cwbi.us/cwms-data/"), null);
});

test("returns null when the OpenID client is missing", () => {
  const spec = specWithOpenId(
    "https://identity-test.cwbi.us/auth/realms/cwbi/.well-known/openid-configuration",
  );
  delete spec.components.securitySchemes.OpenID["x-oidc-client-id"];

  assert.equal(getKeycloakConfig(spec, "https://water.dev.cwbi.us/cwms-data/"), null);
});

test("derives deployed Keycloak configuration from the OpenAPI document", () => {
  const spec = specWithOpenId(
    "https://identity-test.cwbi.us/auth/realms/cwbi/.well-known/openid-configuration",
  );

  assert.deepEqual(
    getKeycloakConfig(
      spec,
      "https://water.dev.cwbi.us/cwms-data/user-lists?office=SWT",
    ),
    {
      host: "https://identity-test.cwbi.us/auth",
      realm: "cwbi",
      client: "cwms",
      flow: "authorization-code-pkce",
      username: undefined,
      password: undefined,
      redirectUri: "https://water.dev.cwbi.us/cwms-data/user-lists",
      postLogoutRedirectUri: "https://water.dev.cwbi.us/cwms-data/user-lists",
      providerHint: "federation-eams",
    },
  );
});

test("rewrites the compose-only auth hostname to the visible origin", () => {
  const spec = specWithOpenId(
    "http://auth:8080/auth/realms/cwms/.well-known/openid-configuration",
  );

  normalizeOpenIdConnectUrls(spec, "http://localhost:8081");

  assert.equal(
    spec.components.securitySchemes.OpenID.openIdConnectUrl,
    "http://localhost:8081/auth/realms/cwms/.well-known/openid-configuration",
  );
});
