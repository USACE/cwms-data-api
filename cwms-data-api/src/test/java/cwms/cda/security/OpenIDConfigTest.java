package cwms.cda.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.swagger.v3.oas.models.security.SecurityScheme;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OpenIDConfigTest {

    @Test
    void providerRemainsDisabledWhenWellKnownUrlIsMissing() {
        String previousWellKnown = System.getProperty(OpenIdConnectIdentitityProvider.WELL_KNOWN_PROPERTY);
        try {
            System.setProperty(OpenIdConnectIdentitityProvider.WELL_KNOWN_PROPERTY, "");

            OpenIdConnectIdentitityProvider provider = new OpenIdConnectIdentitityProvider();

            assertNull(provider.getScheme());
        } finally {
            if (previousWellKnown == null) {
                System.clearProperty(OpenIdConnectIdentitityProvider.WELL_KNOWN_PROPERTY);
            } else {
                System.setProperty(OpenIdConnectIdentitityProvider.WELL_KNOWN_PROPERTY, previousWellKnown);
            }
        }
    }

    @Test
    void buildSchemeUsesWellKnownDiscoveryUrlWithoutHttpAuthScheme() {
        SecurityScheme scheme = OpenIDConfig.buildScheme(
            "https://identityc.sec.usace.army.mil/auth/realms/cwbi/.well-known/openid-configuration",
            "cwms",
            "federation-eams, login.gov"
        );

        assertEquals(SecurityScheme.Type.OPENIDCONNECT, scheme.getType());
        assertEquals(
            "https://identityc.sec.usace.army.mil/auth/realms/cwbi/.well-known/openid-configuration",
            scheme.getOpenIdConnectUrl()
        );
        assertTrue(scheme.getScheme() == null || scheme.getScheme().isEmpty());
        assertNotNull(scheme.getExtensions());
        assertEquals("cwms", scheme.getExtensions().get("x-oidc-client-id"));

        @SuppressWarnings("unchecked")
        Map<String, Object> hint = (Map<String, Object>) scheme.getExtensions().get("x-kc_idp_hint");
        assertNotNull(hint);
        assertEquals("kc_idp_hint", hint.get("query-parameter"));

        @SuppressWarnings("unchecked")
        List<String> values = (List<String>) hint.get("values");
        assertEquals(List.of("federation-eams", "login.gov"), values);
        assertFalse(scheme.getExtensions().containsKey("flows"));
    }
}
