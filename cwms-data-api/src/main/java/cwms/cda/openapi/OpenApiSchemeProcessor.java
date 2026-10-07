package cwms.cda.openapi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import cwms.cda.security.Authenticator;
import io.javalin.http.Context;
import io.javalin.openapi.schema.OpenApiSchemaBuilder;
import io.swagger.v3.oas.models.security.SecurityRequirement;

// TODO: it works different now.
public class OpenApiSchemeProcessor {

    private final Authenticator authenticator;
    private final ArrayList<SecurityRequirement> secReqs = new ArrayList<>();

    public OpenApiSchemeProcessor(Authenticator authenticator)
    {
        this.authenticator = authenticator;
    }

    
    public OpenApiSchemaBuilder apply(Context ctx, OpenApiSchemaBuilder api) {
        synchronized (secReqs) {
            secReqs.clear();
            authenticator.getActiveProviders().forEach(identityProvider -> {
                api.withSecurityScheme(identityProvider.getName(), identityProvider.getScheme());
                SecurityRequirement req = new SecurityRequirement();
                if (!identityProvider.getName().equalsIgnoreCase("guestauth")
                        && !identityProvider.getName().equalsIgnoreCase("noauth")) {
                    req.addList(identityProvider.getName());
                    secReqs.add(req);
                    api.withGlobalSecurity(identityProvider.getName());
                }
            });
            api.withGlobalSecurity("");
        }
        return api;
    }


    public List<SecurityRequirement> getSecurityRequirements()
    {
        return Collections.unmodifiableList(secReqs);
    }
}
