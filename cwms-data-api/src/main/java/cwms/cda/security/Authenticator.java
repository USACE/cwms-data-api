package cwms.cda.security;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
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
        final List<String> supressedList = surpressed == null
                ? Collections.emptyList() : List.of(surpressed.split(","));

        loadProviders(CdaIdentityProviders.providers(), supressedList);
    }

    Authenticator(Iterator<IdentityProvider> availableProviders, List<String> suppressedProviders) {
        loadProviders(availableProviders, suppressedProviders);
    }

    private void loadProviders(Iterator<IdentityProvider> availableProviders, List<String> suppressedProviders) {
        availableProviders.forEachRemaining(provider -> {
            if (!suppressedProviders.contains(provider.getName())) {
                providers.add(provider);
            } else {
                logger.atInfo().log("Suppressing configured Identity Provider %s.", provider.getName());
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
        ArrayList<IdentityProvider> activeProviders = new ArrayList<>();
        for (IdentityProvider provider: providers) {
            if (provider.getScheme() != null) {
                activeProviders.add(provider);
            }
        }
        return Collections.unmodifiableList(activeProviders);
    }
}
