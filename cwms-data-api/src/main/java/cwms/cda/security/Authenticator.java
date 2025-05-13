package cwms.cda.security;

import java.security.Principal;
import java.util.ArrayList;

import cwms.cda.spi.CdaIdentityProviders;
import cwms.cda.spi.IdentityProvider;
import io.javalin.http.Context;
import io.javalin.http.Handler;

public final class Authenticator implements Handler {
    private final ArrayList<IdentityProvider> providers = new ArrayList<>();

    public Authenticator() {
        CdaIdentityProviders.providers().forEachRemaining(providers::add);
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
 
}
