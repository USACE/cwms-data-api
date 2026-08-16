package cwms.cda.security;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.flogger.FluentLogger;

import cwms.cda.spi.CdaIdentityProviders;
import cwms.cda.spi.IdentityProvider;
import io.javalin.http.Context;
import io.javalin.http.Handler;

public final class Authenticator implements Handler {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    private final ArrayList<IdentityProvider> providers = new ArrayList<>();

    public Authenticator() {
        var surpressed = System.getenv("cwms.dataapi.access.providers.surpress");
        final var supressedList = surpressed == null ? List.of() : List.of(surpressed.split(","));

        CdaIdentityProviders.providers().forEachRemaining(provider -> {
            if (!supressedList.contains(provider.getName()) && provider.getScheme() != null) {
                providers.add(provider);
            } else {
                logger.atSevere()
                      .log("Unable to add Identity Provider %s. See earlier logs for specific error message.",
                           provider.getName());
            }
        });
    }

    @Override
    public void handle(Context ctx) throws Exception {
        for (IdentityProvider provider: providers) {
            if (provider.canAuth(ctx)) {
                Principal p = provider.authenticate(ctx);
                ctx.sessionAttribute(IdentityProvider.PRINCIPAL_KEY, p);
                return;
            }
        }
    }

    public List<IdentityProvider> getActiveProviders() {
        return Collections.unmodifiableList(providers);
    }
}
