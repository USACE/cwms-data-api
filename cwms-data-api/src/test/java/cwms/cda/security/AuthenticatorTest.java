package cwms.cda.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.Principal;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import cwms.cda.spi.IdentityProvider;
import io.javalin.http.Context;
import io.swagger.v3.oas.models.security.SecurityScheme;

class AuthenticatorTest {

    @Test
    void providerBecomesActiveAfterSchemeIsAvailable() {
        AtomicReference<SecurityScheme> scheme = new AtomicReference<>();
        IdentityProvider provider = new IdentityProvider() {
            @Override
            public String getName() {
                return "DelayedProvider";
            }

            @Override
            public boolean canAuth(Context ctx) {
                return false;
            }

            @Override
            public Principal authenticate(Context ctx) {
                return null;
            }

            @Override
            public SecurityScheme getScheme() {
                return scheme.get();
            }
        };
        Authenticator authenticator = new Authenticator(List.of(provider).iterator(), List.of());

        assertTrue(authenticator.getActiveProviders().isEmpty());

        scheme.set(new SecurityScheme());

        assertEquals(List.of(provider), authenticator.getActiveProviders());
    }
}
