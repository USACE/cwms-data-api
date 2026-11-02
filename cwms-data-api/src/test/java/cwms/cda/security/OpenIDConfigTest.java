package cwms.cda.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import cwms.cda.security.OpenIdConfig.OpenIdWithExtension;
import io.javalin.openapi.OpenID;
import io.javalin.openapi.SecurityScheme;

class OpenIDConfigTest {

    @Test
    void providerRemainsDisabledWhenWellKnownUrlIsMissing() {
        String previousWellKnown = System.getProperty(OpenIdConnectIdentityProvider.WELL_KNOWN_PROPERTY);
        try {
            System.setProperty(OpenIdConnectIdentityProvider.WELL_KNOWN_PROPERTY, "");

            OpenIdConnectIdentityProvider provider = new OpenIdConnectIdentityProvider();

            assertNull(provider.getScheme());
        } finally {
            if (previousWellKnown == null) {
                System.clearProperty(OpenIdConnectIdentityProvider.WELL_KNOWN_PROPERTY);
            } else {
                System.setProperty(OpenIdConnectIdentityProvider.WELL_KNOWN_PROPERTY, previousWellKnown);
            }
        }
    }

    @Test
    void buildSchemeUsesWellKnownDiscoveryUrlWithoutHttpAuthScheme() {
        SecurityScheme scheme = OpenIdConfig.buildScheme(
            "https://identityc.sec.usace.army.mil/auth/realms/cwbi/.well-known/openid-configuration",
            "cwms",
            "federation-eams, login.gov"
        );
        var oidcScheme = assertInstanceOf(OpenIdWithExtension.class, scheme);
        assertEquals(new OpenID("don't need url").getType(), scheme.getType());
        assertEquals(
            "https://identityc.sec.usace.army.mil/auth/realms/cwbi/.well-known/openid-configuration",
            oidcScheme.openIdConnectUrl()
        );
        
        assertNotNull(oidcScheme.xKcIdpHint());
        assertEquals("cwms", oidcScheme.xOidcClientId());

        Map<String, Object> hint = (Map<String, Object>) oidcScheme.xKcIdpHint();
        assertNotNull(hint);
        assertEquals("kc_idp_hint", hint.get("query-parameter"));

        @SuppressWarnings("unchecked")
        List<String> values = (List<String>) hint.get("values");
        assertEquals(List.of("federation-eams", "login.gov"), values);
    }
}
